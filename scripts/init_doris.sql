-- 创建测试数据库
CREATE DATABASE IF NOT EXISTS test_db;

-- 使用数据库
USE test_db;

-- 创建目标表
CREATE TABLE IF NOT EXISTS test_target_table (
    id BIGINT,
    name VARCHAR(100),
    amount DECIMAL(10,2),
    status INT,
    created_at DATETIME,
    description VARCHAR(500)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES ('replication_num'='1');

-- 验证表创建成功
SHOW TABLES;
