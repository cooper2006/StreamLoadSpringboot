package com.example.streamload.runner;

import com.example.streamload.checkpoint.CheckpointManager;
import com.example.streamload.config.DorisProperties;
import com.example.streamload.config.VerifyProperties;
import com.example.streamload.exception.StreamLoadException;
import com.example.streamload.pipeline.DataPipeline;
import com.example.streamload.service.StreamLoadService;
import com.example.streamload.verify.VerifyParam;
import com.example.streamload.verify.VerifyResult;
import com.example.streamload.verify.VerifyService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * Stream Load 命令行运行器
 * 程序入口: 执行数据导入流水线,显示实时进度,完成后验证
 */
@Slf4j
@Component
public class StreamLoadRunner implements CommandLineRunner {

    private final JdbcTemplate jdbcTemplate;
    private final StreamLoadService streamLoadService;
    private final DorisProperties dorisProperties;
    private final VerifyProperties verifyProperties;
    private final VerifyService verifyService;
    private final CheckpointManager checkpointManager;
    private final DataSource dataSource;

    public StreamLoadRunner(JdbcTemplate jdbcTemplate,
                            StreamLoadService streamLoadService,
                            DorisProperties dorisProperties,
                            VerifyProperties verifyProperties,
                            VerifyService verifyService,
                            CheckpointManager checkpointManager,
                            DataSource dataSource) {
        this.jdbcTemplate = jdbcTemplate;
        this.streamLoadService = streamLoadService;
        this.dorisProperties = dorisProperties;
        this.verifyProperties = verifyProperties;
        this.verifyService = verifyService;
        this.checkpointManager = checkpointManager;
        this.dataSource = dataSource;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("========================================");
        log.info("Stream Load Doris 数据导入系统启动");
        log.info("========================================");
        log.info("源数据库: {}", dorisProperties.getLoadUrl());
        log.info("目标数据库: {}", dorisProperties.getDatabase());
        log.info("批次大小: {}", dorisProperties.getBatchSize());
        log.info("启用压缩: {}", dorisProperties.isEnableCompression());
        log.info("最大重试: {}", dorisProperties.getMaxRetry());
        log.info("断点文件: {}", dorisProperties.getCheckpointFile());

        long startTime = System.currentTimeMillis();

        try {
            // 构建验证参数
            VerifyParam verifyParam = verifyService.buildVerifyParam();
            String sourceSql = verifyParam.getSourceSql();
            String tableName = verifyParam.getTargetTable();

            log.info("源 SQL: {}", sourceSql);
            log.info("目标表: {}", tableName);

            // 创建数据流水线
            DataPipeline pipeline = new DataPipeline(
                    jdbcTemplate,
                    streamLoadService,
                    dorisProperties,
                    sourceSql,
                    tableName
            );

            // 启动进度监控线程 (进度报告间隔从配置读取，默认5秒)
            int progressReportInterval = dorisProperties.getProgressReportInterval();
            Thread progressThread = new Thread(() -> {
                try {
                    while (!pipeline.isCompleted() || !pipeline.isConsumerCompleted()) {
                        log.info("[进度] 生产: {} 批次/{} 条, 消费: {} 批次/{} 条, 失败: {} 批次",
                                pipeline.getProducedBatchCount(),
                                pipeline.getProducedRecordCount(),
                                pipeline.getConsumedBatchCount(),
                                pipeline.getImportedRecordCount(),
                                pipeline.getFailedBatchCount());
                        Thread.sleep(progressReportInterval * 1000L);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            progressThread.setDaemon(true);
            progressThread.start();

            // 执行流水线
            long totalRecords = pipeline.execute();

            long endTime = System.currentTimeMillis();
            long duration = (endTime - startTime) / 1000;

            log.info("========================================");
            log.info("导入完成!");
            log.info("总记录数: {}", totalRecords);
            log.info("总耗时: {} 秒", duration);
            log.info("吞吐量: {} 条/秒", duration > 0 ? totalRecords / duration : totalRecords);
            log.info("========================================");

            // 执行验证
            if (verifyParam.getMode() != VerifyParam.VerifyMode.NONE) {
                log.info("开始执行导入验证...");
                VerifyResult verifyResult = verifyService.verify(verifyParam);
                log.info("验证结果: {}", verifyResult.isPassed() ? "通过" : "失败");
                log.info("验证详情: {}", verifyResult.getMessage());

                if (!verifyResult.isPassed()) {
                    log.error("导入验证失败,请检查数据!");
                    System.exit(1);
                }
            }

            // 清除断点 (导入成功完成后)
            checkpointManager.clearCheckpoint();

            log.info("任务完成,退出码: 0");
            System.exit(0);

        } catch (StreamLoadException e) {
            log.error("导入失败: {}", e.getMessage(), e);
            log.info("任务失败,退出码: 1");
            System.exit(1);
        } catch (Exception e) {
            log.error("未知错误: {}", e.getMessage(), e);
            log.info("任务失败,退出码: 2");
            System.exit(2);
        }
    }
}
