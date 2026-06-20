# Nginx 代理 Doris 配置说明

## 背景

在 Docker 环境中部署 Doris 时，通常需要通过 Nginx 反向代理来统一管理 Doris FE 和 BE 的访问。本项目的 `application-test.yml` 中配置了通过 Nginx 代理（端口 18030）访问 Doris。

## Nginx 配置示例

```nginx
# Doris FE 代理
upstream doris_fe {
    server 127.0.0.1:8030;  # Doris FE HTTP 端口
}

# Doris BE 代理
upstream doris_be {
    server 127.0.0.1:8040;  # Doris BE HTTP 端口
}

server {
    listen 18030;
    
    # FE 请求转发
    location / {
        proxy_pass http://doris_fe;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Stream Load 需要较长的超时时间
        proxy_read_timeout 600s;
        proxy_connect_timeout 60s;
        proxy_send_timeout 600s;
        
        # 禁用缓冲，Stream Load 需要实时传输
        proxy_buffering off;
        proxy_request_buffering off;
        
        # 允许 PUT 方法
        proxy_method PUT;
    }
}
```

## 应用配置修改

### 1. `application-test.yml` 配置

```yaml
doris:
  # Stream Load HTTP 地址 (Nginx 代理地址)
  load-url: http://127.0.0.1:18030
  # 目标数据库
  database: test_db
  # 认证信息
  username: root
  password: Root@123456
  # 批次大小 (300万数据建议 5万条/批)
  batch-size: 50000
  # 启用 gzip 压缩（Nginx 代理下建议关闭）
  enable-compression: false
  # 超时时间 (秒)
  timeout: 600
  # 最大重试次数
  max-retry: 3
  # 重试间隔基数 (秒)
  retry-interval-base: 2
  # 断点续传文件路径
  checkpoint-file: ./checkpoint-test.json
```

### 2. 关键配置说明

| 配置项 | 说明 |
|--------|------|
| `load-url` | 必须设置为 Nginx 代理地址，如 `http://127.0.0.1:18030` |
| `enable-compression` | Nginx 代理下建议关闭 gzip 压缩，避免双重压缩问题 |
| `timeout` | 大数据量导入时需适当增加超时时间 |

## 重定向处理机制

### 问题描述

Doris Stream Load 的工作流程如下：

1. 客户端向 FE 发送导入请求
2. FE 返回 **307 重定向**，指向 BE 的地址（如 `http://172.28.0.11:8040/...`）
3. 客户端需要跟随重定向，将数据直接发送到 BE

在 Nginx 代理模式下，如果直接将重定向 URL 重写回 Nginx 代理地址，会导致：

```
客户端 → Nginx(18030) → FE(8030) → 307重定向到BE
    ↓ (重写回Nginx)
客户端 → Nginx(18030) → FE(8030) → 307重定向到BE
    ↓ (重写回Nginx)
... 无限循环 ...
```

### 解决方案

代码中的 `rewriteRedirectUrl()` 方法检测到重定向目标为 Docker 内部 IP（`172.`、`10.`、`192.168.` 开头）时，直接重写为 `127.0.0.1:8040`（Docker 暴露的 BE HTTP 端口），绕过 Nginx/FE 直接访问 BE，避免重定向循环。

```java
// StreamLoadService.java - rewriteRedirectUrl 方法
private String rewriteRedirectUrl(String location) {
    URL redirectUrl = new URL(location);
    String host = redirectUrl.getHost();
    boolean isInternalIp = host.startsWith("172.") || host.startsWith("10.") || host.startsWith("192.168.");
    
    if (isInternalIp) {
        // 直接访问 BE 的 Docker 映射端口 8040
        return String.format("%s://127.0.0.1:8040%s?%s",
                redirectUrl.getProtocol(),
                redirectUrl.getPath(),
                redirectUrl.getQuery() != null ? redirectUrl.getQuery() : "");
    }
    return location;
}
```

### 前提条件

Docker 必须暴露 BE 的 HTTP 端口（8040），确保宿主机可以通过 `127.0.0.1:8040` 直接访问 BE。

```bash
# Docker 运行 BE 时需映射 8040 端口
docker run -p 8040:8040 ... apache/doris:be-2.1.7
```

## 测试结果（2026-06-20）

### 环境信息

| 项目 | 配置 |
|------|------|
| Doris 版本 | 2.1.7 |
| 部署方式 | Docker |
| Nginx 代理端口 | 18030 |
| BE 直连端口 | 8040 |
| 数据源 | MySQL (315万条) |

### 导入结果

| 项目 | 结果 |
|------|------|
| 数据量 | **3,152,034 条**（315万） |
| 总耗时 | **27 秒** |
| 吞吐量 | **116,742 条/秒** |
| 总批次 | 64 批（63批×50,000条 + 1批×2,034条） |
| 失败批次 | **0** |
| 数据过滤 | **0 条** |
| 验证结果 | **通过**（源表=目标表=3,152,034） |

### 日志示例

```
13:00:03 [main] INFO  StreamLoadRunner - ========================================
13:00:03 [main] INFO  StreamLoadRunner - Stream Load Doris 数据导入系统启动
13:00:03 [main] INFO  StreamLoadRunner - 源数据库: http://127.0.0.1:18030
13:00:03 [main] INFO  StreamLoadRunner - 目标数据库: test_db
13:00:03 [main] INFO  StreamLoadRunner - 批次大小: 50000
13:00:03 [main] INFO  StreamLoadRunner - 启用压缩: false
...
13:00:04 [pool-2-thread-3] DEBUG StreamLoadService - 收到 307 重定向到: http://root:Root%40123456@172.28.0.11:8040/api/test_db/test_target_table/_stream_load?
13:00:04 [pool-2-thread-3] DEBUG StreamLoadService - 内部 IP 重定向，重写为直接访问 BE: http://127.0.0.1:8040/api/test_db/test_target_table/_stream_load?
...
13:01:23 [main] INFO  StreamLoadRunner - 导入完成!
13:01:23 [main] INFO  StreamLoadRunner - 总记录数: 3152034
13:01:23 [main] INFO  StreamLoadRunner - 总耗时: 27 秒
13:01:23 [main] INFO  StreamLoadRunner - 吞吐量: 116742 条/秒
13:01:23 [main] INFO  StreamLoadRunner - 开始执行导入验证...
13:01:24 [main] INFO  VerifyService - 源表记录数: 3152034
13:01:24 [main] INFO  VerifyService - 目标表记录数: 3152034
13:01:24 [main] INFO  StreamLoadRunner - 验证结果: 通过
13:01:24 [main] INFO  StreamLoadRunner - 任务完成,退出码: 0
```

## 常见问题

### Q1: 导入时出现 502 Bad Gateway

**原因**：Nginx 代理超时或 BE 服务不可用。

**解决**：
- 检查 BE 服务状态：`curl http://127.0.0.1:8040/api/health`
- 增加 Nginx 超时配置：`proxy_read_timeout 600s;`
- 确认 Docker 已暴露 8040 端口

### Q2: 重定向次数超过限制

**原因**：重定向 URL 被错误地重写回 Nginx 代理地址，造成无限循环。

**解决**：
- 确保使用最新版本的 `StreamLoadService.java`（`rewriteRedirectUrl` 方法已修复）
- 确认 Docker 已暴露 BE 的 8040 端口
- 检查日志中是否有 "内部 IP 重定向，重写为直接访问 BE" 的日志

### Q3: 验证失败，目标表记录数多于源表

**原因**：之前测试的残留数据未清理。

**解决**：
```sql
-- 清空 Doris 目标表
TRUNCATE TABLE test_db.test_target_table;