-- 测试数据生成脚本
-- 用于在源数据库 (MySQL) 中创建测试表和数据

-- 1. 创建测试表
CREATE TABLE IF NOT EXISTS test_source_table (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. 插入测试数据 (10000 条)
-- 使用递归方式生成测试数据
INSERT INTO test_source_table (name, amount, status, description)
SELECT 
    CONCAT('User_', LPAD(seq, 6, '0')),
    ROUND(RAND() * 10000, 2),
    CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END,
    CONCAT('Description for user ', seq)
FROM (
    SELECT @rownum := @rownum + 1 AS seq
    FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
         (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
         (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3,
         (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t4,
         (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t5,
         (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t6,
         (SELECT @rownum := 0) r
    LIMIT 10000
) numbers;

-- 3. 验证数据量
SELECT COUNT(*) AS total_records FROM test_source_table;
SELECT COUNT(*) AS active_records FROM test_source_table WHERE status = 1;

-- 4. 查看示例数据
SELECT * FROM test_source_table LIMIT 10;
