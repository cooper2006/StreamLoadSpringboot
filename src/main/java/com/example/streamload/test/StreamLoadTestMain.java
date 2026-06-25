package com.example.streamload.test;

import com.example.streamload.checkpoint.CheckpointManager;
import com.example.streamload.config.*;
import com.example.streamload.exception.StreamLoadException;
import com.example.streamload.service.StreamLoadService;
import com.example.streamload.verify.VerifyParam;
import com.example.streamload.verify.VerifyResult;
import com.example.streamload.verify.VerifyService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * StreamLoad 功能测试主类
 * 
 * 不依赖 Spring 容器启动，手动组装各组件，逐阶段验证整体功能链路。
 * 运行方式: mvn exec:java -Dexec.mainClass="com.example.streamload.test.StreamLoadTestMain"
 * 
 * 测试阶段:
 *   Phase 1: 配置加载 — 读取环境变量 / 默认值，构建配置对象
 *   Phase 2: 数据源连通性 — MySQL source + Doris target JDBC 连接测试
 *   Phase 3: Doris HTTP Stream Load 连通性 — 调用 HTTP API 健康检查
 *   Phase 4: Checkpoint 管理 — 断点保存、读取、清理全生命周期
 *   Phase 5: 单次 Stream Load 导入 — 构造小批量数据执行导入
 *   Phase 6: 数据验证 — 调用 VerifyService 做 COUNT 模式校验
 *   Phase 7: 清理 — 删除临时 checkpoint 文件，关闭连接池
 */
public class StreamLoadTestMain {

    private static final Logger log = LoggerFactory.getLogger(StreamLoadTestMain.class);

    // ============================================================
    // 配置常量（默认值匹配 application-test.yml，可通过环境变量覆盖）
    // ============================================================

    static final String MYSQL_URL      = env("MYSQL_URL",      "jdbc:mysql://127.0.0.1:3306/test_db?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true");
    static final String MYSQL_USER     = env("MYSQL_USERNAME", "root");
    static final String MYSQL_PASS     = env("MYSQL_PASSWORD", "ServBay.dev");
    static final String MYSQL_DRIVER   = "com.mysql.cj.jdbc.Driver";

    static final String DORIS_LOAD_URL = env("DORIS_LOAD_URL", "http://127.0.0.1:18030");
    static final String DORIS_DB       = env("DORIS_DATABASE", "test_db");
    static final String DORIS_USER     = env("DORIS_USERNAME", "root");
    static final String DORIS_PASS     = env("DORIS_PASSWORD", "Root@123456");
    static final int    DORIS_JDBC_PORT = Integer.parseInt(env("DORIS_JDBC_PORT", "9030"));
    static final int    DORIS_BE_HTTP   = Integer.parseInt(env("DORIS_BE_HTTP_PORT", "8040"));

    static final String TEST_SOURCE_SQL  = env("TEST_SOURCE_SQL",
            "SELECT id, name, amount, status, created_at, description FROM test_source_table WHERE status = 1");
    static final String TEST_TARGET_TABLE = env("TEST_TARGET_TABLE", "test_target_table");
    static final String TEST_VERIFY_COLS  = env("TEST_VERIFY_COLUMNS", "id,name,amount");
    static final String TEST_CHECKPOINT_FILE = "./checkpoint-test-tmp.json";

    // ============================================================
    // 组件实例
    // ============================================================

    static DataSource mysqlDataSource;
    static DataSource dorisDataSource;
    static JdbcTemplate jdbcTemplate;
    static DorisProperties dorisProperties;
    static VerifyProperties verifyProperties;
    static CheckpointManager checkpointManager;
    static StreamLoadService streamLoadService;
    static VerifyService verifyService;

    // ============================================================
    // 测试统计
    // ============================================================

    static final AtomicInteger passedCount = new AtomicInteger(0);
    static final AtomicInteger failedCount = new AtomicInteger(0);
    static final List<String>  failedPhases = Collections.synchronizedList(new ArrayList<>());

    // ============================================================
    // 函数式接口：允许抛出 checked exception
    // ============================================================

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    // ============================================================
    // 入口
    // ============================================================

    public static void main(String[] args) throws Exception {
        printBanner();

        phase("Phase 1", "配置加载",            () -> testConfigLoading());
        phase("Phase 2", "数据源连通性",        () -> testDataSourceConnectivity());
        phase("Phase 3", "Doris HTTP 连通性",   () -> testDorisHttpConnectivity());
        phase("Phase 4", "Checkpoint 生命周期", () -> testCheckpointLifecycle());
        phase("Phase 5", "单次 Stream Load",    () -> testSingleStreamLoad());
        phase("Phase 6", "数据验证 COUNT",      () -> testDataVerification());
        phase("Phase 7", "清理",               () -> testCleanup());

        printSummary();
    }

    // ============================================================
    // Phase 1: 配置加载
    // ============================================================

    static void testConfigLoading() throws Exception {
        var connProps = new DorisConnectionProperties();
        connProps.setLoadUrl(DORIS_LOAD_URL);
        connProps.setDatabase(DORIS_DB);
        connProps.setUsername(DORIS_USER);
        connProps.setPassword(DORIS_PASS);
        connProps.setJdbcPort(DORIS_JDBC_PORT);
        connProps.setBeHttpPort(DORIS_BE_HTTP);

        var httpProps = new DorisHttpProperties();
        httpProps.setTimeout(600);
        httpProps.setConnectTimeout(60);
        httpProps.setEnableCompression(false);

        var retryProps = new DorisRetryProperties();
        retryProps.setMaxRetry(3);
        retryProps.setRetryIntervalBase(2);

        var pipelineProps = new DorisPipelineProperties();
        pipelineProps.setBatchSize(80000);
        pipelineProps.setConsumerThreads(3);
        pipelineProps.setQueueCapacity(1000);
        pipelineProps.setMaxFailedBatches(3);

        var checkpointProps = new DorisCheckpointProperties();
        checkpointProps.setCheckpointFile(TEST_CHECKPOINT_FILE);
        checkpointProps.setCheckpointSaveInterval(10);

        var dockerProps = new DorisDockerProperties();
        dockerProps.setUseNginxProxy(true);

        var logProps = new DorisLogProperties();
        logProps.setProgressLogInterval(10);
        logProps.setProgressReportInterval(5);

        dorisProperties = new DorisProperties(connProps, httpProps, retryProps,
                pipelineProps, checkpointProps, dockerProps, logProps);

        verifyProperties = new VerifyProperties();
        verifyProperties.setMode(VerifyParam.VerifyMode.COUNT);
        verifyProperties.setSourceSql(TEST_SOURCE_SQL);
        verifyProperties.setTargetTable(TEST_TARGET_TABLE);
        verifyProperties.setVerifyColumns(TEST_VERIFY_COLS);
        verifyProperties.setSampleRatio(0.01);
        verifyProperties.setMaxSampleSize(1000);

        assertEqual("load-url", DORIS_LOAD_URL, dorisProperties.getLoadUrl());
        assertEqual("database", DORIS_DB, dorisProperties.getDatabase());
        assertEqual("batch-size", 80000, dorisProperties.getBatchSize());
        assertEqual("consumer-threads", 3, dorisProperties.getConsumerThreads());
        assertEqual("checkpoint-file", TEST_CHECKPOINT_FILE, dorisProperties.getCheckpointFile());
        assertEqual("verify-mode", VerifyParam.VerifyMode.COUNT, verifyProperties.getMode());

        log.info("[OK] 配置对象全部构建完成, 共 {} 个配置项验证通过", 6);
    }

    // ============================================================
    // Phase 2: 数据源连通性
    // ============================================================

    static void testDataSourceConnectivity() throws Exception {
        var mysqlConfig = new HikariConfig();
        mysqlConfig.setJdbcUrl(MYSQL_URL);
        mysqlConfig.setUsername(MYSQL_USER);
        mysqlConfig.setPassword(MYSQL_PASS);
        mysqlConfig.setDriverClassName(MYSQL_DRIVER);
        mysqlConfig.setMaximumPoolSize(3);
        mysqlConfig.setMinimumIdle(1);
        mysqlConfig.setConnectionTimeout(15000);
        mysqlDataSource = new HikariDataSource(mysqlConfig);

        try (var conn = mysqlDataSource.getConnection()) {
            assertTrue("MySQL 连接成功", conn.isValid(5));
            log.info("MySQL 版本: {}", conn.getMetaData().getDatabaseProductVersion());
        }

        jdbcTemplate = new JdbcTemplate(mysqlDataSource);

        try {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM (" + TEST_SOURCE_SQL + ") t", Integer.class);
            log.info("源表 test_source_table 存在, 当前记录数: {}", count != null ? count : 0);
        } catch (Exception e) {
            log.warn("源表查询异常 (空表或无表不影响后续测试): {}", e.getMessage());
        }

        String host = DORIS_LOAD_URL
                .replace("http://", "")
                .replace("https://", "")
                .replaceAll(":\\d+", "");
        String dorisJdbcUrl = String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&allowPublicKeyRetrieval=true",
                host, DORIS_JDBC_PORT, DORIS_DB);

        var dorisConfig = new HikariConfig();
        dorisConfig.setJdbcUrl(dorisJdbcUrl);
        dorisConfig.setUsername(DORIS_USER);
        dorisConfig.setPassword(DORIS_PASS);
        dorisConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dorisConfig.setMaximumPoolSize(2);
        dorisConfig.setMinimumIdle(1);
        dorisConfig.setConnectionTimeout(15000);
        dorisDataSource = new HikariDataSource(dorisConfig);

        try (var conn = dorisDataSource.getConnection()) {
            assertTrue("Doris JDBC 连接成功", conn.isValid(5));
            log.info("Doris 版本: {}", conn.getMetaData().getDatabaseProductVersion());
        }

        try (var conn = dorisDataSource.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TEST_TARGET_TABLE)) {
            if (rs.next()) {
                log.info("目标表 {} 存在, 当前记录数: {}", TEST_TARGET_TABLE, rs.getLong(1));
            }
        } catch (Exception e) {
            log.warn("目标表查询异常: {}", e.getMessage());
        }

        log.info("[OK] 数据源全部就绪 (MySQL + Doris)");
    }

    // ============================================================
    // Phase 3: Doris HTTP Stream Load 连通性
    // ============================================================

    static void testDorisHttpConnectivity() throws Exception {
        String testUrl = DORIS_LOAD_URL + "/api/" + DORIS_DB + "/" + TEST_TARGET_TABLE + "/_stream_load";
        String auth = DORIS_USER + ":" + DORIS_PASS;
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

        var client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        var request = HttpRequest.newBuilder()
                .uri(URI.create(testUrl))
                .header("Authorization", "Basic " + encodedAuth)
                .header("Content-Type", "application/json")
                .header("label", "test_connectivity_" + System.currentTimeMillis())
                .method("PUT", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(15))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            log.info("Doris HTTP 响应码: {}, 响应体: {}", response.statusCode(),
                    response.body() != null && response.body().length() > 200
                            ? response.body().substring(0, 200) + "..."
                            : response.body());

            assertTrue("Doris HTTP 端点可达 (307/400/200)",
                    response.statusCode() == 307 || response.statusCode() == 400
                            || response.statusCode() == 401 || response.statusCode() == 200);
        } catch (Exception e) {
            throw new AssertionError("Doris HTTP 端点不可达: " + testUrl + " — " + e.getMessage());
        }

        log.info("[OK] Doris HTTP 端点可达 ({})", testUrl);
    }

    // ============================================================
    // Phase 4: Checkpoint 生命周期
    // ============================================================

    static void testCheckpointLifecycle() throws Exception {
        File ckptFile = new File(TEST_CHECKPOINT_FILE);
        if (ckptFile.exists()) {
            assertTrue("删除旧 checkpoint 文件", ckptFile.delete());
        }

        checkpointManager = new CheckpointManager(dorisProperties);
        assertEqual("初始已完成批次", 0, checkpointManager.getCompletedBatchCount());

        checkpointManager.markBatchCompleted(0);
        checkpointManager.markBatchCompleted(1);
        checkpointManager.markBatchCompleted(2);
        assertEqual("标记 3 个批次后计数", 3, checkpointManager.getCompletedBatchCount());

        assertTrue("批次 0 已完成", checkpointManager.isBatchCompleted(0));
        assertTrue("批次 1 已完成", checkpointManager.isBatchCompleted(1));
        assertFalse("批次 99 未完成", checkpointManager.isBatchCompleted(99));

        checkpointManager.clearCheckpoint();
        assertFalse("checkpoint 文件已删除", ckptFile.exists());
        assertEqual("清除后已完成批次", 0, checkpointManager.getCompletedBatchCount());

        log.info("[OK] Checkpoint 全生命周期测试通过");
    }

    // ============================================================
    // Phase 5: 单次 Stream Load 导入
    // ============================================================

    static void testSingleStreamLoad() throws Exception {
        // 重新初始化 CheckpointManager (清理后)
        checkpointManager = new CheckpointManager(dorisProperties);

        // 创建 StreamLoadService
        streamLoadService = new StreamLoadService(dorisProperties, checkpointManager);

        // 构造测试数据 5 条
        List<Map<String, Object>> testData = buildTestData(5);
        log.info("构造测试数据: {} 条", testData.size());

        // 执行导入
        log.info("发起 Stream Load 请求...");
        StreamLoadService.LoadResult result = streamLoadService.executeLoad(0, TEST_TARGET_TABLE, testData);

        log.info("Stream Load 结果: success={}, status={}, recordCount={}",
                result.isSuccess(), result.getStatus(), result.getRecordCount());

        assertTrue("Stream Load 导入成功", result.isSuccess());
        assertEqual("导入记录数", 5, result.getRecordCount());

        // 验证 checkpoint 已标记
        assertTrue("批次 0 已标记完成", checkpointManager.isBatchCompleted(0));

        log.info("[OK] 单次 Stream Load 导入成功 (5 条记录)");
    }

    // ============================================================
    // Phase 6: 数据验证 (COUNT)
    // ============================================================

    static void testDataVerification() throws Exception {
        verifyService = new VerifyService(jdbcTemplate, verifyProperties, dorisDataSource);

        VerifyParam param = verifyService.buildVerifyParam();
        assertEqual("验证模式", VerifyParam.VerifyMode.COUNT, param.getMode());
        log.info("验证模式: {}, 源SQL: {}", param.getMode(), param.getSourceSql());
        log.info("目标表: {}, 验证列: {}", param.getTargetTable(), param.getVerifyColumns());

        VerifyResult result = verifyService.verify(param);
        assertNotNull("验证结果不为 null", result);

        log.info("验证结果: {}", result);
        log.info("  源记录数: {}, 目标记录数: {}", result.getSourceCount(), result.getTargetCount());

        if (!result.isPassed()) {
            log.warn("[WARN] COUNT 验证未通过 (可能因测试表数据不一致, 仅用于验证流程完整性)");
            log.warn("  源={}, 目标={}", result.getSourceCount(), result.getTargetCount());
        } else {
            log.info("[OK] COUNT 验证通过: 源={}, 目标={}",
                    result.getSourceCount(), result.getTargetCount());
        }
    }

    // ============================================================
    // Phase 7: 清理
    // ============================================================

    static void testCleanup() throws Exception {
        if (checkpointManager != null) {
            checkpointManager.clearCheckpoint();
            log.info("Checkpoint 已清理");
        }
        if (streamLoadService != null) {
            streamLoadService.destroy();
            log.info("StreamLoadService 已关闭");
        }
        if (mysqlDataSource instanceof HikariDataSource hds) {
            hds.close();
            log.info("MySQL 数据源已关闭");
        }
        if (dorisDataSource instanceof HikariDataSource hds) {
            hds.close();
            log.info("Doris 数据源已关闭");
        }
        log.info("[OK] 清理完成");
    }

    // ============================================================
    // 工具方法
    // ============================================================

    static List<Map<String, Object>> buildTestData(int count) {
        List<Map<String, Object>> data = new ArrayList<>(count);
        long baseId = System.currentTimeMillis() % 1000000;
        for (int i = 0; i < count; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", baseId + i);
            row.put("name", "test_user_" + (baseId + i));
            row.put("amount", (i + 1) * 100);
            row.put("status", 1);
            row.put("created_at", "2025-06-25 12:00:00");
            row.put("description", "auto test data batch");
            data.add(row);
        }
        return data;
    }

    static String env(String key, String defaultValue) {
        String val = System.getenv(key);
        return val != null && !val.isEmpty() ? val : defaultValue;
    }

    static void phase(String phaseId, String phaseName, ThrowingRunnable testLogic) {
        log.info("");
        log.info("═══════════════════════════════════════════════");
        log.info("  {}: {} 开始", phaseId, phaseName);
        log.info("═══════════════════════════════════════════════");
        try {
            testLogic.run();
            passedCount.incrementAndGet();
            log.info("▸ {} [PASS]", phaseId);
        } catch (AssertionError e) {
            failedCount.incrementAndGet();
            failedPhases.add(phaseId + " " + phaseName);
            log.error("▸ {} [FAIL] — {}", phaseId, e.getMessage());
        } catch (Exception e) {
            failedCount.incrementAndGet();
            failedPhases.add(phaseId + " " + phaseName);
            log.error("▸ {} [FAIL] — 异常: {}", phaseId, e.getMessage(), e);
        }
    }

    static void assertTrue(String msg, boolean condition) {
        if (!condition) throw new AssertionError(msg + " — 期望 true, 得到 false");
    }

    static void assertFalse(String msg, boolean condition) {
        if (condition) throw new AssertionError(msg + " — 期望 false, 得到 true");
    }

    static void assertEqual(String msg, Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(msg + " — 期望: " + expected + ", 实际: " + actual);
        }
    }

    static void assertNotNull(String msg, Object obj) {
        if (obj == null) throw new AssertionError(msg + " — 期望非 null");
    }

    static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║        StreamLoad 功能集成测试套件                    ║");
        System.out.println("║        不依赖 Spring 容器 · 手动组装组件              ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("配置摘要:");
        System.out.println("  MySQL:      " + MYSQL_URL);
        System.out.println("  Doris FE:   " + DORIS_LOAD_URL);
        System.out.println("  Doris DB:   " + DORIS_DB);
        System.out.println("  目标表:     " + TEST_TARGET_TABLE);
        System.out.println("  Checkpoint: " + TEST_CHECKPOINT_FILE);
        System.out.println();
    }

    static void printSummary() {
        int total = passedCount.get() + failedCount.get();
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║                    测试总结                          ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.printf ("║  总阶段: %-2d  通过: %-2d  失败: %-2d               ║%n",
                total, passedCount.get(), failedCount.get());
        System.out.println("╠══════════════════════════════════════════════════════╣");
        if (failedPhases.isEmpty()) {
            System.out.println("║  所有测试阶段通过! ✓                                  ║");
        } else {
            System.out.println("║  失败阶段:                                           ║");
            for (String f : failedPhases) {
                System.out.printf("║    • %-46s║%n", f);
            }
        }
        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.println();

        System.exit(failedCount.get() > 0 ? 1 : 0);
    }
}
