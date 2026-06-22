# Nginx 代理 Doris 配置说明

## 背景

在 Docker 环境中部署 Doris 时，通常需要通过 Nginx 反向代理来统一管理 Doris FE 和 BE 的访问。本项目的 `application-test.yml` 中默认配置了通过 Nginx 代理（端口 18030）访问 Doris。

也支持不通过 Nginx 代理，直连 Doris FE（端口 8030）的方式。

## Nginx 配置示例

如果使用 Nginx 代理，配置示例如下：

```nginx
# Doris FE 代理
upstream doris_fe {
    server 127.0.0.1:8030;  # Doris FE HTTP 端口
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
    }
}
```

## 应用配置

### 1. Nginx 代理模式（默认）

```yaml
doris:
  load-url: http://127.0.0.1:18030
  use-nginx-proxy: true
  batch-size: 80000
```

### 2. 直连模式（不经过 Nginx）

```yaml
doris:
  load-url: http://127.0.0.1:8030
  use-nginx-proxy: false
  batch-size: 80000
```

### 3. 完整配置说明

| 配置项 | 说明 |
|--------|------|
| `load-url` | Nginx 代理模式下设为 `http://127.0.0.1:18030`；直连模式下设为 `http://127.0.0.1:8030` |
| `use-nginx-proxy` | `true` 为 Nginx 代理模式，`false` 为直连模式 |
| `enable-compression` | Nginx 代理下建议关闭 gzip 压缩，避免双重压缩问题 |
| `timeout` | 大数据量导入时需适当增加超时时间 |
| `batch-size` | 推荐 80000 条/批，兼顾吞吐和内存 |
| `be-host` | 重定向重写目标主机地址，默认 `127.0.0.1` |
| `be-http-port` | 重定向重写目标端口，默认 `8040` |
| `docker-internal-ip-prefixes` | Docker 内部 IP 前缀列表，默认 `"172.,10.,192.168."` |
| `docker-container-names` | Docker 容器名列表，默认 `"doris-be,doris-fe,doris"` |

## 重定向处理机制

### 问题描述

Doris Stream Load 的工作流程如下：

1. 客户端向 FE 发送导入请求（通过 Nginx 代理或直连）
2. FE 返回 **307 重定向**，指向 BE 的地址（如 `doris-be:8040` 或 `172.28.0.11:8040`）
3. 客户端需要跟随重定向，将数据直接发送到 BE

### 解决方案

代码中的 `rewriteRedirectUrl()` 方法会检测重定向目标是否为 Docker 内部 IP 或 Docker 容器名（均可通过配置自定义），并将它们重写为可访问的 BE 地址，避免重定向循环。

```java
/**
 * 重写重定向 URL，处理 Docker 内部 IP 和容器名
 * <p>
 * 可配置项（application.yml）：
 * - doris.be-host: 重写目标主机地址，默认 127.0.0.1
 * - doris.be-http-port: 重写目标端口，默认 8040
 * - doris.docker-internal-ip-prefixes: 内部 IP 前缀列表，默认 "172.,10.,192.168."
 * - doris.docker-container-names: 容器名列表，默认 "doris-be,doris-fe,doris"
 */
private String rewriteRedirectUrl(String location) {
    try {
        URL redirectUrl = new URL(location);
        String host = redirectUrl.getHost();
        
        // 从配置读取内部 IP 前缀列表
        String[] internalIpPrefixes = properties.getDockerInternalIpPrefixes().split(",");
        boolean isInternalIp = false;
        for (String prefix : internalIpPrefixes) {
            if (host.startsWith(prefix.trim())) {
                isInternalIp = true;
                break;
            }
        }
        
        // 从配置读取 Docker 容器名列表
        String[] containerNames = properties.getDockerContainerNames().split(",");
        boolean isDockerContainer = false;
        for (String name : containerNames) {
            if (host.equalsIgnoreCase(name.trim()) || host.contains(name.trim())) {
                isDockerContainer = true;
                break;
            }
        }
        
        if (isInternalIp || isDockerContainer) {
            // 重写为配置的 BE 地址:端口
            String rewritten = String.format("%s://%s:%d%s?%s",
                    redirectUrl.getProtocol(),
                    properties.getBeHost(),
                    properties.getBeHttpPort(),
                    redirectUrl.getPath(),
                    redirectUrl.getQuery() != null ? redirectUrl.getQuery() : "");
            log.debug("Docker 内部地址重定向，重写为直接访问 BE: {}", rewritten);
            return rewritten;
        }
        return location;
    } catch (Exception e) {
        log.warn("重写重定向 URL 失败，使用原始 URL: {}", location, e);
        return location;
    }
}
```

### 前提条件

Docker 必须暴露 BE 的 HTTP 端口（8040），确保宿主机可以通过 `127.0.0.1:8040` 直接访问 BE。

```bash
# Docker 运行 BE 时需映射 8040 端口
docker run -p 8040:8040 ... apache/doris:be-2.1.7
```

## 测试结果（2026-06-22）

### 环境信息

| 项目 | 配置 |
|------|------|
| Doris 版本 | 2.1.7 |
| 部署方式 | Docker |
| Nginx 代理端口 | 18030 |
| BE 直连端口 | 8040 |
| 数据源 | MySQL (3,161,055 条 active) |

### 导入结果（两种模式对比）

| 项目 | Nginx 代理模式 | 直连模式 |
|------|:---:|:---:|
| **数据量** | **3,161,055 条** | **3,161,055 条** |
| **总耗时** | **29 秒** | **29 秒** |
| **吞吐量** | **109,001 条/秒** | **109,001 条/秒** |
| **批次大小** | 80,000 条/批 | 80,000 条/批 |
| **总批次** | 40 批 | 40 批 |
| **失败批次** | **0** | **0** |
| **数据过滤** | **0 条** | **0 条** |
| **验证结果** | **通过** | **通过** |

### 日志示例

```
14:53:31 [main] INFO  StreamLoadRunner - ========================================
14:53:31 [main] INFO  StreamLoadRunner - Stream Load Doris 数据导入系统启动
14:53:31 [main] INFO  StreamLoadRunner - 源数据库: http://127.0.0.1:18030
14:53:31 [main] INFO  StreamLoadRunner - 目标数据库: test_db
14:53:31 [main] INFO  StreamLoadRunner - 批次大小: 80000
14:53:31 [main] INFO  StreamLoadRunner - 启用压缩: false
...
14:53:32 [pool-2-thread-2] DEBUG StreamLoadService - 收到 307 重定向到: http://root:Root%40123456@doris-be:8040/api/test_db/test_target_table/_stream_load?
14:53:32 [pool-2-thread-2] DEBUG StreamLoadService - Docker 内部地址重定向，重写为直接访问 BE: http://127.0.0.1:8040/api/test_db/test_target_table/_stream_load?
...
14:54:15 [main] INFO  StreamLoadRunner - 导入完成!
14:54:15 [main] INFO  StreamLoadRunner - 总记录数: 3161055
14:54:15 [main] INFO  StreamLoadRunner - 总耗时: 29 秒
14:54:15 [main] INFO  StreamLoadRunner - 吞吐量: 109001 条/秒
14:54:15 [main] INFO  StreamLoadRunner - 开始执行导入验证...
14:54:17 [main] INFO  VerifyService - 源表记录数: 3161055
14:54:17 [main] INFO  VerifyService - 目标表记录数: 3161055
14:54:17 [main] INFO  StreamLoadRunner - 验证结果: 通过
14:54:17 [main] INFO  StreamLoadRunner - 任务完成,退出码: 0
```

## 常见问题

### Q1: 导入时出现 502 Bad Gateway

**原因**：Nginx 代理超时或 BE 服务不可用。

**解决**：
- 检查 BE 服务状态：`curl http://127.0.0.1:8040/api/health`
- 增加 Nginx 超时配置：`proxy_read_timeout 600s;`
- 确认 Docker 已暴露 8040 端口

### Q2: 重定向次数超过限制

**原因**：重定向 URL 未被正确重写，如遇到无法识别的 Docker 容器名。

**解决**：
- 确保使用最新版本的 `StreamLoadService.java`（`rewriteRedirectUrl` 方法支持 `doris-be` 容器名）
- 确认 Docker 已暴露 BE 的 8040 端口
- 检查日志中是否有 "Docker 内部地址重定向，重写为直接访问 BE" 的日志
- 可通过 `doris.docker-container-names` 和 `doris.docker-internal-ip-prefixes` 自定义识别规则

### Q3: 验证失败，目标表记录数多于源表

**原因**：之前测试的残留数据未清理。

**解决**：
```sql
-- 清空 Doris 目标表
TRUNCATE TABLE test_db.test_target_table;
```

### Q4: 连接被拒绝（Connection refused）

**原因**：
- Nginx 代理未运行（18030 端口不可用）
- Doris BE 未暴露 8040 端口
- 端口配置错误

**解决**：
- 检查 Nginx/Doris 服务状态
- 确保使用正确的端口配置
- 直连模式下使用 `load-url: http://127.0.0.1:8030`，`use-nginx-proxy: false`