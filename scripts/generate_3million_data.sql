-- 生成 300 万条测试数据（临时表方式，避免 MySQL 自引用锁定问题）
SET @batch = 10000;

-- 1. 先插入 10000 条种子数据
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT 
    CONCAT('User_', LPAD(seq, 6, '0')),
    ROUND(RAND() * 10000, 2),
    CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
    DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
    CONCAT('Description for user ', seq)
FROM (
    SELECT @rownum := @rownum + 1 AS seq
    FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
         (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
         (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3,
         (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t4,
         (SELECT @rownum := 0) r
    LIMIT 10000
) numbers;

SELECT COUNT(*) AS 'After seed: 10000' FROM test_source_table;

-- 2. 创建临时表用于批量生成
DROP TEMPORARY TABLE IF EXISTS tmp_batch;
CREATE TEMPORARY TABLE tmp_batch (seq INT) ENGINE=MEMORY;
INSERT INTO tmp_batch SELECT @rownum2 := @rownum2 + 1 
FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t4,
     (SELECT @rownum2 := 0) r
LIMIT 10000;

-- 3. 倍增: 10000 -> 20000
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT CONCAT('User_', LPAD(id + 10000, 6, '0')), ROUND(RAND() * 10000, 2),
       CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
       DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
       CONCAT('Description for user ', id + 10000)
FROM test_source_table LIMIT 10000;
SELECT COUNT(*) AS 'Step 2: 20000' FROM test_source_table;

-- 4. 倍增: 20000 -> 40000
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT CONCAT('User_', LPAD(id + 20000, 6, '0')), ROUND(RAND() * 10000, 2),
       CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
       DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
       CONCAT('Description for user ', id + 20000)
FROM test_source_table LIMIT 20000;
SELECT COUNT(*) AS 'Step 3: 40000' FROM test_source_table;

-- 5. 倍增: 40000 -> 80000
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT CONCAT('User_', LPAD(id + 40000, 6, '0')), ROUND(RAND() * 10000, 2),
       CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
       DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
       CONCAT('Description for user ', id + 40000)
FROM test_source_table LIMIT 40000;
SELECT COUNT(*) AS 'Step 4: 80000' FROM test_source_table;

-- 6. 倍增: 80000 -> 160000
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT CONCAT('User_', LPAD(id + 80000, 6, '0')), ROUND(RAND() * 10000, 2),
       CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
       DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
       CONCAT('Description for user ', id + 80000)
FROM test_source_table LIMIT 80000;
SELECT COUNT(*) AS 'Step 5: 160000' FROM test_source_table;

-- 7. 倍增: 160000 -> 320000
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT CONCAT('User_', LPAD(id + 160000, 6, '0')), ROUND(RAND() * 10000, 2),
       CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
       DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
       CONCAT('Description for user ', id + 160000)
FROM test_source_table LIMIT 160000;
SELECT COUNT(*) AS 'Step 6: 320000' FROM test_source_table;

-- 8. 倍增: 320000 -> 640000
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT CONCAT('User_', LPAD(id + 320000, 6, '0')), ROUND(RAND() * 10000, 2),
       CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
       DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
       CONCAT('Description for user ', id + 320000)
FROM test_source_table LIMIT 320000;
SELECT COUNT(*) AS 'Step 7: 640000' FROM test_source_table;

-- 9. 倍增: 640000 -> 1280000
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT CONCAT('User_', LPAD(id + 640000, 6, '0')), ROUND(RAND() * 10000, 2),
       CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
       DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
       CONCAT('Description for user ', id + 640000)
FROM test_source_table LIMIT 640000;
SELECT COUNT(*) AS 'Step 8: 1280000' FROM test_source_table;

-- 10. 倍增: 1280000 -> 2560000
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT CONCAT('User_', LPAD(id + 1280000, 6, '0')), ROUND(RAND() * 10000, 2),
       CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
       DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
       CONCAT('Description for user ', id + 1280000)
FROM test_source_table LIMIT 1280000;
SELECT COUNT(*) AS 'Step 9: 2560000' FROM test_source_table;

-- 11. 补充到 3000000 (还需 440000 条)
INSERT INTO test_source_table (name, amount, status, created_at, description)
SELECT CONCAT('User_', LPAD(id + 2560000, 6, '0')), ROUND(RAND() * 10000, 2),
       CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
       DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),
       CONCAT('Description for user ', id + 2560000)
FROM test_source_table LIMIT 440000;
SELECT COUNT(*) AS 'Step 10: 3000000' FROM test_source_table;

-- 最终验证
SELECT COUNT(*) AS total_records FROM test_source_table;
SELECT COUNT(*) AS active_records FROM test_source_table WHERE status = 1;

DROP TEMPORARY TABLE IF EXISTS tmp_batch;
