# Stream Load Doris 测试指南

## 快速开始

### 1. 环境要求

- Java 17+
- Maven 3.6+
- MySQL 8.0+ (或其他关系型数据库)
- Apache Doris 2.1.7+

### 2. 准备测试数据

#### 方式一：使用测试脚本（推荐）

```bash
# 赋予执行权限
chmod +x scripts/run_test.sh

# 运行测试脚本
./scripts/run_test.sh
```

#### 方式二：手动准备

**MySQL 源数据：**

```bash
# 创建测试数据库
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS test_db"

# 导入测试数据
mysql -u root -p test_db < scripts/init_test_data.sql
```

**Doris 目标表：**

```bash
# 连接 Doris (默认端口 9030)
mysql -h localhost -P 9030 -u root

# 执行建表脚本
source scripts/init_doris_table.sql
```

### 3. 配置修改

编辑 `src/main/resources/application-test.yml`：

```yaml
spring:
  datasource:
    # MySQL 连接配置
    url: jdbc:mysql://127.0.0.1:3306/test_db?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true
    username: root
    password: your_mysql_password  # 修改为你的 MySQL 密码

doris:
  # Doris Stream Load HTTP 地址
  # 直连模式：http://127.0.0.1:8030
  # Nginx 代理模式：http://127.0.0.1:18030（自动检测代理模式）
  load-url: http://127.0.0.1:8030
  database: test_db
  username: root
  password: your_doris_password  # 修改为你的 Doris 密码
  
  # 批次大小（每批记录数）
  # 建议值：
  # - 小数据量（< 100 万）：50000
  # - 大数据量（> 100 万）：50000-100000
  batch-size: 50000
  
  # 是否启用 gzip 压缩
  # 注意：Doris 2.1.7 Docker 版不支持 gzip，需设置为 false
  enable-compression: false
  
  # 超时时间（秒）
  timeout: 600
  
  # 最大重试次数
  max-retry: 3
```

**Nginx 代理配置（可选）：**

如果使用 Nginx 代理 Doris，需要在 Nginx 配置中添加：

```nginx
location / {
    proxy_pass http://doris_fe;
    proxy_set_header Expect $http_expect;  # 转发 Expect 头给 Doris
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}
```

程序会自动检测代理模式（通过端口判断），正确处理 307 重定向。

### 4. 运行程序

```bash
# 使用 test profile 运行
mvn spring-boot:run -Dspring-boot.run.profiles=test

# 或者打包后运行
mvn clean package
java -jar target/stream-load-doris-1.0.0.jar --spring.profiles.active=test
```

### 5. 验证结果

**查看导入日志：**

```
13:15:20 [main] INFO  StreamLoadRunner - ========================================
13:15:20 [main] INFO  StreamLoadRunner - Stream Load Doris 数据导入系统启动
13:15:20 [main] INFO  StreamLoadRunner - ========================================
13:15:20 [main] INFO  StreamLoadRunner - 批次大小: 1000
13:15:20 [main] INFO  StreamLoadRunner - 启用压缩: true
...
13:15:25 [main] INFO  StreamLoadRunner - 导入完成!
13:15:25 [main] INFO  StreamLoadRunner - 总记录数: 9000
13:15:25 [main] INFO  StreamLoadRunner - 总耗时: 5 秒
13:15:25 [main] INFO  StreamLoadRunner - 吞吐量: 1800 条/秒
```

**验证 Doris 数据：**

```sql
-- 连接 Doris
mysql -h localhost -P 9030 -u root

-- 查询记录数
SELECT COUNT(*) FROM test_db.test_target_table;

-- 查看示例数据
SELECT * FROM test_db.test_target_table LIMIT 10;
```

## 测试场景

### 场景 1：正常导入

```bash
# 默认配置，导入所有 status=1 的数据
mvn spring-boot:run -Dspring-boot.run.profiles=test
```

### 场景 2：断点续传测试

```bash
# 1. 运行导入（中途 Ctrl+C 中断）
mvn spring-boot:run -Dspring-boot.run.profiles=test

# 2. 查看断点文件
cat checkpoint-test.json

# 3. 再次运行（会从断点继续）
mvn spring-boot:run -Dspring-boot.run.profiles=test
```

### 场景 3：验证模式测试

修改 `application-test.yml`：

```yaml
verify:
  mode: COUNT    # 仅验证记录数
  # 或
  mode: SAMPLE   # 抽样验证（对比部分记录）
```

### 场景 4：大数据量测试

修改配置：

```yaml
doris:
  batch-size: 100000  # 每批 10 万条

# 生成更多测试数据
mysql -u root -p test_db -e "
INSERT INTO test_source_table (name, amount, status, description)
SELECT CONCAT('User_', LPAD(seq, 8, '0')), ROUND(RAND() * 10000, 2), 1, CONCAT('Desc ', seq)
FROM (SELECT @rownum := @rownum + 1 AS seq FROM 
  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3,
  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t4,
  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t5,
  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t6,
  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t7,
  (SELECT @rownum := 0) r LIMIT 1000000) numbers;
"
```

## 常见问题

### Q1: 无法连接到 MySQL

**解决：**
- 检查 MySQL 服务是否运行：`systemctl status mysql`
- 验证用户名密码
- 确认数据库存在：`mysql -u root -p -e "SHOW DATABASES"`

### Q2: 无法连接到 Doris

**解决：**
- 检查 Doris FE 服务：`curl http://localhost:8030/api/health`
- 确认 HTTP 端口（默认 8030）
- 检查防火墙设置

### Q3: 导入失败 "Label Already Exists"

**原因：** 重试时 Label 冲突

**解决：** 
- 这是正常现象，程序会自动处理
- 如果持续出现，检查 Doris 的 Label 保留时间

### Q4: 内存溢出 (OOM)

**解决：**
- 减小批次大小：`doris.batch-size: 5000`
- 增加 JVM 内存：`export MAVEN_OPTS="-Xmx2g"`

### Q5: 如何查看实时进度

程序会自动每 5 秒输出进度：

```
[进度] 生产: 5 批次/5000 条, 消费: 4 批次/4000 条, 失败: 0 批次
```

## 性能调优

### 优化建议

1. **批次大小**
   - 小数据量（< 100 万）：`batch-size: 50000`
   - 大数据量（> 100 万）：`batch-size: 50000`（推荐，平衡内存和效率）
   - 超大批次（> 500 万）：可尝试 `batch-size: 100000`（需监控内存）

2. **压缩**
   - 网络带宽有限：启用压缩（如果 Doris 支持）
   - 本地网络或 Docker 环境：关闭压缩减少 CPU 开销
   - 注意：Doris 2.1.7 Docker 版不支持 gzip，需设置 `enable-compression: false`

3. **并发**
   - 当前版本支持 3 个消费者线程并行导入
   - 可通过多实例并行导入不同表
   - 生产者-消费者模式，队列容量 8，平衡内存和吞吐量

4. **JVM 参数**
   ```bash
   java -Xms1g -Xmx2g -jar target/stream-load-doris-1.0.0.jar
   ```

5. **失败批次控制**
   - 默认超过 3 批失败自动停止，避免无效重试
   - 如需调整，修改 `DataPipeline.java` 中的 `MAX_FAILED_BATCHES` 常量

### 性能数据参考

| 数据量 | 耗时 | 吞吐量 | 内存峰值 |
|--------|------|--------|----------|
| 315 万条 | 14 秒 | 225,145 条/秒 | ~250MB |
| 100 万条 | ~4.5 秒 | ~222,000 条/秒 | ~200MB |
| 10 万条 | ~0.5 秒 | ~200,000 条/秒 | ~150MB |

## 清理测试数据

```bash
# 清理 MySQL
mysql -u root -p -e "DROP DATABASE IF EXISTS test_db"

# 清理 Doris
mysql -h localhost -P 9030 -u root -e "DROP DATABASE IF EXISTS test_db"

# 清理断点文件
rm -f checkpoint-test.json
```
