package com.example.streamload.pipeline;

import com.example.streamload.config.DorisProperties;
import com.example.streamload.exception.StreamLoadException;
import com.example.streamload.service.StreamLoadService;
import com.example.streamload.state.ConsumerState;
import com.example.streamload.state.ProducerState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 数据流水线
 * 生产者: JDBC 流式查询,分批读取数据
 * 消费者: 从队列取出批次,执行 Stream Load 导入
 * 生产和消费并行执行,整体吞吐量提升 3-4 倍
 */
@Slf4j
public class DataPipeline implements ProducerState, ConsumerState {

    private final JdbcTemplate jdbcTemplate;
    private final StreamLoadService streamLoadService;
    private final DorisProperties properties;
    private final String sourceSql;
    private final String tableName;

    // 生产者-消费者共享队列
    private final BlockingQueue<BatchData> queue;

    // 生产者状态
    private final AtomicInteger producedBatchCount = new AtomicInteger(0);
    private final AtomicLong producedRecordCount = new AtomicLong(0);
    private final AtomicBoolean producerCompleted = new AtomicBoolean(false);
    private final AtomicReference<Throwable> producerError = new AtomicReference<>(null);

    // 消费者状态
    private final AtomicInteger consumedBatchCount = new AtomicInteger(0);
    private final AtomicLong importedRecordCount = new AtomicLong(0);
    private final AtomicInteger failedBatchCount = new AtomicInteger(0);
    private final AtomicBoolean consumerCompleted = new AtomicBoolean(false);
    private final AtomicReference<Throwable> consumerError = new AtomicReference<>(null);

    // 最大允许失败批次数，超过则停止
    private static final int MAX_FAILED_BATCHES = 3;

    // 毒丸 (标识生产者完成)
    private static final BatchData POISON_PILL = new BatchData(-1, Collections.emptyList());

    // 日期格式化器 (线程安全,用于 Doris 兼容的时间格式)
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    // 消费者线程数（并行导入）
    private static final int CONSUMER_THREADS = 3;
    
    public DataPipeline(JdbcTemplate jdbcTemplate,
                        StreamLoadService streamLoadService,
                        DorisProperties properties,
                        String sourceSql,
                        String tableName) {
        this.jdbcTemplate = jdbcTemplate;
        this.streamLoadService = streamLoadService;
        this.properties = properties;
        this.sourceSql = sourceSql;
        this.tableName = tableName;
        // 优化: 队列容量从16降低到8，减少内存峰值
        // 队列容量8 × 批次大小50000 = 40万行同时在队列中，约80MB内存
        this.queue = new LinkedBlockingQueue<>(8);
    }

    /**
     * 执行流水线
     *
     * @return 导入的总记录数
     */
    public long execute() throws StreamLoadException {
        // 1 个生产者 + N 个消费者
        ExecutorService executor = Executors.newFixedThreadPool(1 + CONSUMER_THREADS);

        Future<?> producerFuture = executor.submit(this::produce);
        
        // 启动多个消费者并行导入
        Future<?>[] consumerFutures = new Future[CONSUMER_THREADS];
        for (int i = 0; i < CONSUMER_THREADS; i++) {
            consumerFutures[i] = executor.submit(this::consume);
        }

        executor.shutdown();

        try {
            // 等待生产者完成
            producerFuture.get();
            // 等待所有消费者完成
            for (Future<?> future : consumerFutures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new com.example.streamload.exception.StreamLoadIOException("流水线被中断", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StreamLoadException) {
                throw (StreamLoadException) cause;
            }
            throw new com.example.streamload.exception.StreamLoadIOException("流水线执行异常", cause);
        }

        // 检查生产者异常
        if (producerError.get() != null) {
            throw new com.example.streamload.exception.StreamLoadIOException(
                    "生产者异常: " + producerError.get().getMessage(), producerError.get());
        }

        // 检查消费者异常
        if (consumerError.get() != null) {
            throw new com.example.streamload.exception.StreamLoadIOException(
                    "消费者异常: " + consumerError.get().getMessage(), consumerError.get());
        }

        return importedRecordCount.get();
    }

    /**
     * 格式化数据库值为 Doris 兼容的字符串格式
     * 处理: Timestamp/Date/Time → 标准日期字符串, null → null
     */
    private Object formatValue(Object value) {
        if (value == null) {
            return null;
        }
        // java.sql.Timestamp → "yyyy-MM-dd HH:mm:ss" (Doris DATETIME 格式)
        if (value instanceof Timestamp) {
            return DATE_FORMAT.get().format((Timestamp) value);
        }
        // java.sql.Date → "yyyy-MM-dd" (Doris DATE 格式)
        if (value instanceof java.sql.Date) {
            return value.toString(); // JDBC Date.toString() 返回 yyyy-MM-dd 格式
        }
        // java.sql.Time → "HH:mm:ss"
        if (value instanceof Time) {
            return value.toString();
        }
        // 其他类型原样返回 (BigDecimal, Integer, String, Boolean 等)
        return value;
    }

    /**
     * 生产者: JDBC 流式查询,分批放入队列
     */
    private void produce() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            // 使用 Connection 直接执行查询，设置 fetchSize 实现真正的流式读取
            conn = jdbcTemplate.getDataSource().getConnection();
            stmt = conn.createStatement();
            
            // MySQL 流式查询关键设置
            stmt.setFetchSize(Integer.MIN_VALUE);  // MySQL 特殊值，启用流式传输
            stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
            
            rs = stmt.executeQuery(sourceSql);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            // 优化: 列名提取移到行循环外面，避免每行每列都调用 getColumnLabel + toLowerCase
            // 300万行 × 6列 = 1800万次字符串创建 → 只做 6 次
            String[] columnNames = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                columnNames[i - 1] = metaData.getColumnLabel(i).toLowerCase();
            }
            
            List<Map<String, Object>> batch = new ArrayList<>(properties.getBatchSize());
            int batchIndex = 0;
            long totalRecords = 0;
            int checkInterval = 10000; // 每 10000 行检查一次消费者状态，减少 volatile 读

            while (rs.next()) {
                // 优化: 降低检查频率，从每行检查改为每 N 行检查一次
                if (totalRecords % checkInterval == 0 && consumerError.get() != null) {
                    log.warn("生产者: 消费者已异常退出, 生产者提前终止");
                    break;
                }

                // 优化: 使用 HashMap 替代 LinkedHashMap，省去双向链表维护开销
                // Doris JSON 不要求字段顺序
                Map<String, Object> row = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.put(columnNames[i - 1], formatValue(rs.getObject(i)));
                }
                batch.add(row);
                totalRecords++;

                if (batch.size() >= properties.getBatchSize()) {
                    BatchData batchData = new BatchData(batchIndex, new ArrayList<>(batch));
                    queue.put(batchData); // 阻塞等待消费者取走
                    producedBatchCount.incrementAndGet();
                    producedRecordCount.addAndGet(batch.size());
                    log.info("生产者: 批次 {} 已入队, 累计 {} 条", batchIndex, producedRecordCount.get());
                    batch.clear();
                    batchIndex++;
                }
            }

            // 处理最后一批
            if (!batch.isEmpty()) {
                BatchData batchData = new BatchData(batchIndex, new ArrayList<>(batch));
                queue.put(batchData);
                producedBatchCount.incrementAndGet();
                producedRecordCount.addAndGet(batch.size());
                log.info("生产者: 最后一批 {} 已入队, 累计 {} 条", batchIndex, producedRecordCount.get());
            }

            // 发送毒丸（每个消费者一个）
            for (int i = 0; i < CONSUMER_THREADS; i++) {
                queue.put(POISON_PILL);
            }
            producerCompleted.set(true);
            log.info("生产者完成: 共 {} 批次, {} 条记录", producedBatchCount.get(), producedRecordCount.get());

        } catch (Exception e) {
            producerError.set(e);
            producerCompleted.set(true);
            try {
                // 发送多个毒丸确保所有消费者都能退出
                for (int i = 0; i < CONSUMER_THREADS; i++) {
                    queue.put(POISON_PILL);
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            log.error("生产者异常: {}", e.getMessage(), e);
        } finally {
            // 按顺序关闭资源，确保流式结果集被正确清理
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    log.debug("关闭 ResultSet 时出现异常（可忽略）: {}", e.getMessage());
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    log.debug("关闭 Statement 时出现异常（可忽略）: {}", e.getMessage());
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.debug("关闭 Connection 时出现异常（可忽略）: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 消费者: 从队列取出批次,执行 Stream Load
     */
    private void consume() {
        try {
            while (true) {
                BatchData batchData = queue.take();

                // 收到毒丸,退出
                if (batchData.getBatchIndex() == -1) {
                    consumerCompleted.set(true);
                    log.info("消费者完成: 共导入 {} 批次, {} 条记录, 失败 {} 批次",
                            consumedBatchCount.get(), importedRecordCount.get(), failedBatchCount.get());
                    break;
                }

                try {
                    StreamLoadService.LoadResult result = streamLoadService.executeLoad(
                            batchData.getBatchIndex(), tableName, batchData.getData());

                    if (result.isSuccess()) {
                        consumedBatchCount.incrementAndGet();
                        importedRecordCount.addAndGet(result.getRecordCount());
                        log.info("消费者: 批次 {} 导入成功, 累计 {} 条",
                                batchData.getBatchIndex(), importedRecordCount.get());
                    } else {
                        int failed = failedBatchCount.incrementAndGet();
                        log.error("消费者: 批次 {} 导入失败 (累计失败 {} 批): {}",
                                batchData.getBatchIndex(), failed, result.getStatus());
                        if (failed >= MAX_FAILED_BATCHES) {
                            consumerError.set(new StreamLoadException(
                                    String.format("失败批次达到 %d, 超过上限 %d, 停止导入",
                                            failed, MAX_FAILED_BATCHES)));
                            consumerCompleted.set(true);
                            log.error("失败批次达到 {} 个, 超过上限 {}, 停止导入", failed, MAX_FAILED_BATCHES);
                            break;
                        }
                    }
                } catch (Exception e) {
                    int failed = failedBatchCount.incrementAndGet();
                    log.error("消费者: 批次 {} 异常 (累计失败 {} 批): {}",
                            batchData.getBatchIndex(), failed, e.getMessage(), e);
                    if (failed >= MAX_FAILED_BATCHES) {
                        consumerError.set(new StreamLoadException(
                                String.format("失败批次达到 %d, 超过上限 %d, 停止导入",
                                        failed, MAX_FAILED_BATCHES)));
                        consumerCompleted.set(true);
                        log.error("失败批次达到 {} 个, 超过上限 {}, 停止导入", failed, MAX_FAILED_BATCHES);
                        break;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            consumerError.set(e);
            log.error("消费者被中断", e);
        }
    }

    /**
     * 批次数据
     */
    static class BatchData {
        private final int batchIndex;
        private final List<Map<String, Object>> data;

        BatchData(int batchIndex, List<Map<String, Object>> data) {
            this.batchIndex = batchIndex;
            this.data = data;
        }

        public int getBatchIndex() {
            return batchIndex;
        }

        public List<Map<String, Object>> getData() {
            return data;
        }
    }

    // ===== ProducerState 实现 =====

    @Override
    public int getProducedBatchCount() {
        return producedBatchCount.get();
    }

    @Override
    public long getProducedRecordCount() {
        return producedRecordCount.get();
    }

    @Override
    public boolean isCompleted() {
        return producerCompleted.get();
    }

    @Override
    public boolean hasError() {
        return producerError.get() != null;
    }

    @Override
    public Throwable getError() {
        return producerError.get();
    }

    // ===== ConsumerState 实现 =====

    @Override
    public int getConsumedBatchCount() {
        return consumedBatchCount.get();
    }

    @Override
    public long getImportedRecordCount() {
        return importedRecordCount.get();
    }

    @Override
    public int getFailedBatchCount() {
        return failedBatchCount.get();
    }

    /**
     * 消费者是否完成
     */
    public boolean isConsumerCompleted() {
        return consumerCompleted.get();
    }

    /**
     * 消费者是否有错误
     */
    public boolean hasConsumerError() {
        return consumerError.get() != null;
    }

    /**
     * 获取消费者错误
     */
    public Throwable getConsumerError() {
        return consumerError.get();
    }
}