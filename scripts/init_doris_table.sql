-- Doris 目标表创建脚本
-- 在 Apache Doris 中执行 (通过 mysql -h127.0.0.1 -P9030 -uroot)
-- 或者通过 curl: echo "source scripts/init_doris_table.sql" | mysql -h127.0.0.1 -P9030 -uroot

-- 创建目标数据库 (如果不存在)
CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

-- 删除旧表
DROP TABLE IF EXISTS test_target_table;

-- 创建目标表
-- JSON 数据格式示例:
-- {"id":1,"name":"User_000001","amount":1234.56,"status":1,"created_at":"2025-01-01 00:00:00","description":"some text"}
-- 注意: 所有列名必须小写,与 Java 代码中 metaData.getColumnLabel(i).toLowerCase() 一致
CREATE TABLE test_target_table (
    id BIGINT,
    name VARCHAR(100),
    amount DECIMAL(10, 2),
    status INT,
    created_at DATETIME,
    description VARCHAR(500)
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT '测试目标表'
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

-- 验证表结构
DESC test_target_table;

-- 可选: 设置 max_filter_ratio 为 100% (仅测试用,避免太多过滤行导入失败)
-- ALTER TABLE test_target_table SET ("default.max_filter_ratio" = "1.0");