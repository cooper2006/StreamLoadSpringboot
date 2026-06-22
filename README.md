# Stream Load Doris 数据导入系统

基于 Spring Boot 的高性能数据导入工具，实现从 MySQL/PostgreSQL 向 Apache Doris 的批量数据迁移。

## 主要特性

- **高性能导入**：315万条数据仅需14秒，吞吐量达 225,145 条/秒
- **多消费者并行**：3个消费者线程并行导入，充分利用网络带宽
- **JDBC 流式查询**：避免内存溢出，支持大数据量导入
- **断点续传**：记录导入进度，支持失败后从断点继续
- **自动重试**：失败批次自动重试（最多3次），指数退避策略
- **实时进度**：每5秒输出导入进度和统计信息
- **数据验证**：导入完成后自动验证源表和目标表数据一致性
- **Nginx 代理支持**：自动检测代理模式，正确处理 307 重定向
- **失败批次控制**：超过 3 批失败自动停止，避免无效重试

## 技术栈

- **框架**：Spring Boot 3.2.0
- **Java**：JDK 17+
- **数据库**：MySQL 8.0+ / PostgreSQL
- **目标系统**：Apache Doris 2.1.7+
- **构建工具**：Maven 3.6+

## 快速开始

### 1. 环境要求

```bash
# 检查 Java 版本
java -version  # 需要 17+

# 检查 Maven 版本
mvn -version   # 需要 3.6+
```

### 2. 克隆项目

```bash
git clone https://github.com/cooper2006/StreamLoadSpringboot.git
cd StreamLoadSpringboot
```

### 3. 准备源数据库

```bash
# 创建测试数据库
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS test_db"

# 导入测试数据（可选，使用提供的脚本）
mysql -u root -p test_db < scripts/init_test_data.sql

# 生成300万测试数据（可选）
mysql -u root -p test_db < scripts/generate_3million_data.sql
```

### 4. 创建 Doris 目标表

```bash
# 连接 Doris
mysql -h 127.0.0.1 -P 9030 -u root

# 执行建表脚本
source scripts/init_doris_table.sql
```

### 5. 配置数据库连接

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
}
```

程序会自动检测代理模式（通过端口判断），正确处理 307 重定向。

### 6. 运行导入

```bash
# 使用 test profile 运行
mvn spring-boot:run -Dspring-boot.run.profiles=test

# 或打包后运行
mvn clean package
java -jar target/stream-load-doris-1.0.0.jar --spring.profiles.active=test
```

## 配置说明

### 核心配置项

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `doris.batch-size` | 每批导入条数 | 50000 |
| `doris.timeout` | 超时时间（秒） | 600 |
| `doris.max-retry` | 最大重试次数 | 3 |
| `doris.enable-compression` | 启用 gzip 压缩 | false |
| `verify.mode` | 验证模式（NONE/COUNT/SAMPLE） | COUNT |

### 验证模式

- **NONE**：不验证
- **COUNT**：仅验证记录数
- **SAMPLE**：抽样验证（按比例对比数据）

## 性能数据

### 测试结果（315万条数据）

| 指标 | 数值 |
|------|------|
| 总耗时 | 14 秒 |
| 吞吐量 | 225,145 条/秒 |
| 导入批次 | 64 批 |
| 失败批次 | 0 |
| 数据验证 | 通过 |

### 性能优化

1. **多消费者并行**：1个消费者 → 3个消费者并行
2. **队列容量优化**：4 → 8，平衡内存和吞吐量
3. **列名缓存**：避免每行重复计算列名
4. **HashMap 替代 LinkedHashMap**：减少内存开销
5. **Checkpoint 无锁化**：使用 ConcurrentHashMap 替代 synchronized
6. **Base64 认证缓存**：避免每次请求重新计算
7. **307 重定向缓存**：缓存 BE 地址，减少 HTTP 连接

详细优化说明请参考 [CHANGELOG.md](CHANGELOG.md)

## 项目结构

```
StreamLoadSpringboot/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/example/streamload/
│       │       ├── checkpoint/      # 断点续传管理
│       │       ├── config/          # 配置类
│       │       ├── exception/       # 自定义异常
│       │       ├── pipeline/        # 数据流水线
│       │       ├── runner/          # 运行器
│       │       ├── service/         # 核心服务
│       │       ├── state/           # 状态接口
│       │       └── verify/          # 数据验证
│       └── resources/
│           ├── application.yml
│           └── application-test.yml
├── scripts/                         # SQL 脚本
│   ├── init_test_data.sql
│   ├── generate_3million_data.sql
│   └── init_doris_table.sql
├── docs/                            # 文档
├── CHANGELOG.md                     # 修改记录
├── TEST_GUIDE.md                    # 测试指南
└── pom.xml
```

## 断点续传

系统会自动记录导入进度到 `checkpoint-test.json` 文件。

### 中断后继续

```bash
# 导入过程中按 Ctrl+C 中断
# 再次运行相同命令，会从断点继续
mvn spring-boot:run -Dspring-boot.run.profiles=test
```

### 清除断点

```bash
rm checkpoint-test.json
```

## 常见问题

### 1. 无法连接到 Doris

**问题**：HTTP 307 重定向到内网 IP

**解决**：系统已自动处理，会将内网 IP 重写为 `127.0.0.1:8040`

### 2. 数据被过滤

**问题**：`too many filtered rows`

**解决**：检查数据类型是否匹配，或调整 `max_filter_ratio` 参数

### 3. 内存溢出

**问题**：OOM

**解决**：
- 减小 `batch-size`（如 10000）
- 增加 JVM 内存：`export MAVEN_OPTS="-Xmx2g"`

### 4. gzip 压缩失败

**问题**：Doris Docker 版本不支持 gzip

**解决**：设置 `enable-compression: false`

## 测试指南

详细测试说明请参考 [TEST_GUIDE.md](TEST_GUIDE.md)

### 快速测试

```bash
# 运行测试脚本
chmod +x scripts/run_test.sh
./scripts/run_test.sh
```

## 修改记录

详细的修改记录请参考 [CHANGELOG.md](CHANGELOG.md)

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

## 联系方式

如有问题，请通过 GitHub Issues 反馈。
