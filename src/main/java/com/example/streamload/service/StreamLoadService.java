package com.example.streamload.service;

import com.example.streamload.checkpoint.CheckpointManager;
import com.example.streamload.config.DorisProperties;
import com.example.streamload.exception.StreamLoadCompressionException;
import com.example.streamload.exception.StreamLoadHttpException;
import com.example.streamload.exception.StreamLoadIOException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Stream Load 核心服务
 * 负责将数据批量导入到 Doris，支持 gzip 压缩、自动重试、幂等提交
 * 使用 Java 原生 HttpURLConnection，避免第三方 HTTP 客户端的代理问题
 */
@Slf4j
@Service
public class StreamLoadService {

    private final DorisProperties properties;
    private final CheckpointManager checkpointManager;
    private final ObjectMapper objectMapper;
    
    // 优化: 缓存 Base64 编码的认证信息，避免每次请求都重新计算
    private final String encodedAuth;

    public StreamLoadService(DorisProperties properties, CheckpointManager checkpointManager) {
        this.properties = properties;
        this.checkpointManager = checkpointManager;
        this.objectMapper = new ObjectMapper();
        
        // 启用受限 HTTP头的支持，允许手动设置 Expect: 100-continue
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
        
        // 预计算 Base64 认证信息
        String auth = properties.getUsername() + ":" + properties.getPassword();
        this.encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 执行 Stream Load
     *
     * @param batchIndex 批次索引
     * @param tableName  目标表名
     * @param data       数据列表 (每行是一个 Map)
     * @return 导入结果
     */
    public LoadResult executeLoad(int batchIndex, String tableName, List<Map<String, Object>> data)
            throws StreamLoadHttpException, StreamLoadIOException, StreamLoadCompressionException {
        // 检查是否已完成 (断点续传)
        if (checkpointManager.isBatchCompleted(batchIndex)) {
            log.info("批次 {} 已完成,跳过", batchIndex);
            return new LoadResult(true, "SKIPPED", data.size());
        }

        // 生成唯一 label (幂等性)
        String label = generateLabel(batchIndex);

        // 转换为 JSON
        String jsonData = convertToJson(data);

        // 压缩数据 (如果启用)
        byte[] payload;
        boolean compressed;
        if (properties.isEnableCompression()) {
            payload = compress(jsonData);
            compressed = true;
            log.debug("批次 {}: 原始大小 {} bytes, 压缩后 {} bytes, 压缩率 {}%",
                    batchIndex, jsonData.length(), payload.length,
                    String.format("%.2f", (1.0 - (double) payload.length / jsonData.length()) * 100));
        } else {
            payload = jsonData.getBytes(StandardCharsets.UTF_8);
            compressed = false;
        }

        // 构建 HTTP 请求 URL
        String url = buildLoadUrl(tableName);

        // 重试逻辑
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount <= properties.getMaxRetry()) {
            try {
                LoadResult result = doLoad(url, label, payload, compressed, data.size());
                if (result.isSuccess()) {
                    checkpointManager.markBatchCompleted(batchIndex);
                }
                return result;
            } catch (StreamLoadHttpException | StreamLoadIOException e) {
                lastException = e;
                retryCount++;

                if (retryCount <= properties.getMaxRetry()) {
                    long waitTime = (long) Math.pow(properties.getRetryIntervalBase(), retryCount);
                    log.warn("批次 {} 失败,第 {} 次重试,等待 {} 秒: {}",
                            batchIndex, retryCount, waitTime, e.getMessage());

                    try {
                        Thread.sleep(waitTime * 1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new StreamLoadIOException("重试被中断", ie);
                    }
                }
            }
        }

        throw new StreamLoadHttpException(0, "",
                String.format("批次 %d 失败,已重试 %d 次", batchIndex, properties.getMaxRetry()),
                lastException);
    }

    /**
     * 执行实际的 HTTP 请求 (使用 Java 原生 HttpURLConnection)
     */
    private LoadResult doLoad(String urlStr, String label, byte[] payload, boolean compressed, int recordCount)
            throws StreamLoadHttpException, StreamLoadIOException {

        String currentUrl = urlStr;
        int maxRedirects = properties.getMaxRedirects();
        int redirectCount = 0;
        
        while (redirectCount < maxRedirects) {
            HttpURLConnection conn = null;
            try {
                URL url = new URL(currentUrl);
                // 使用默认代理配置，允许通过 Nginx 代理访问
                conn = (HttpURLConnection) url.openConnection();

                // 设置请求方法
                conn.setRequestMethod("PUT");
                conn.setDoOutput(true);
                conn.setDoInput(true);
                conn.setInstanceFollowRedirects(false);  // 禁用自动重定向，手动处理

                // 设置超时
                conn.setConnectTimeout(properties.getConnectTimeout() * 1000);
                conn.setReadTimeout(properties.getTimeout() * 1000);

                // 设置请求头
                conn.setRequestProperty("label", label);
                conn.setRequestProperty("format", "json");
                conn.setRequestProperty("strip_outer_array", "true");
                conn.setRequestProperty("timeout", String.valueOf(properties.getTimeout()));
                conn.setRequestProperty("max_filter_ratio", String.valueOf(properties.getMaxFilterRatio()));
                conn.setRequestProperty("Expect", "100-continue");  // Doris 要求必须有此头
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setRequestProperty("Content-Length", String.valueOf(payload.length));

                if (compressed) {
                    conn.setRequestProperty("Content-Encoding", "gzip");
                }

                // 优化: 使用预计算的 Base64 认证信息
                conn.setRequestProperty("Authorization", "Basic " + encodedAuth);

                // 发送请求数据
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(payload);
                    os.flush();
                }

                // 读取响应
                int statusCode = conn.getResponseCode();
                
                // 处理 307 重定向
                if (statusCode == 307) {
                    String location = conn.getHeaderField("Location");
                    if (location != null && !location.isEmpty()) {
                        log.debug("收到 307 重定向到: {}", location);
                        // 处理 Docker 内部 IP，替换为 Nginx 代理地址
                        currentUrl = rewriteRedirectUrl(location);
                        log.debug("重写后的重定向 URL: {}", currentUrl);
                        redirectCount++;
                        conn.disconnect();
                        continue;  // 继续循环，发送到新的 URL
                    } else {
                        throw new StreamLoadHttpException(statusCode, "", "307 重定向但没有 Location 头");
                    }
                }
                
                String responseBody;
                try (InputStream is = (statusCode >= 200 && statusCode < 300) ? conn.getInputStream() : conn.getErrorStream()) {
                    if (is != null) {
                        responseBody = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    } else {
                        responseBody = "";
                    }
                }

                // 解析响应
                log.debug("Doris 响应: statusCode={}, body={}", statusCode, responseBody);
                LoadResponse loadResponse = parseResponse(responseBody);

                // 处理响应结果
                return handleResponse(statusCode, responseBody, loadResponse, label, recordCount);

            } catch (IOException e) {
                throw new StreamLoadIOException("HTTP 请求异常: " + e.getMessage(), e);
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
        }
        
        throw new StreamLoadHttpException(0, "", "重定向次数超过限制: " + maxRedirects);
    }

    /**
     * 处理原始响应，转换为 LoadResult
     */
    private LoadResult handleResponse(int statusCode, String responseBody, LoadResponse loadResponse, String label, int recordCount)
            throws StreamLoadHttpException {

        if (statusCode != 200) {
            throw new StreamLoadHttpException(statusCode, responseBody,
                    String.format("HTTP 请求失败: %d", statusCode));
        }

        String status = loadResponse.getStatus();

        if ("Success".equalsIgnoreCase(status) || "Publish Timeout".equalsIgnoreCase(status)) {
            log.info("批次 {} 导入成功: Label={}, 记录数={}", label, loadResponse.getLabel(), recordCount);
            return new LoadResult(true, status, recordCount);
        } else if ("Label Already Exists".equalsIgnoreCase(status)) {
            if ("FINISHED".equalsIgnoreCase(loadResponse.getExistingJobStatus())) {
                log.info("批次 {} 已导入 (幂等): Label={}", label, loadResponse.getLabel());
                return new LoadResult(true, "FINISHED", recordCount);
            } else {
                throw new StreamLoadHttpException(statusCode, responseBody,
                        String.format("Label 已存在但状态异常: %s", loadResponse.getExistingJobStatus()));
            }
        } else {
            log.error("导入失败详情: Status={}, Message={}, FilteredRows={}, ErrorURL={}", 
                    status, loadResponse.getMessage(), loadResponse.getNumberFilteredRows(), loadResponse.getErrorURL());
            throw new StreamLoadHttpException(statusCode, responseBody,
                    String.format("导入失败: Status=%s, Message=%s", status, loadResponse.getMessage()));
        }
    }

    /**
     * 将数据转换为 JSON 格式
     */
    private String convertToJson(List<Map<String, Object>> data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (IOException e) {
            log.error("JSON 序列化失败", e);
            throw new RuntimeException("JSON 序列化失败", e);
        }
    }

    /**
     * gzip 压缩
     */
    private byte[] compress(String data) throws StreamLoadCompressionException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(data.getBytes(StandardCharsets.UTF_8));
            gzipOut.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new StreamLoadCompressionException("gzip 压缩失败: " + e.getMessage(), e);
        }
    }

    /**
     * 生成唯一 Label
     */
    private String generateLabel(int batchIndex) {
        return String.format("stream_load_%s_%d_%d",
                properties.getDatabase(), System.currentTimeMillis(), batchIndex);
    }

    /**
     * 重写重定向 URL，处理 Docker 内部 IP
     * 在 Nginx 代理场景下，将重定向 URL 重写回 Nginx 代理地址
     * 在直连场景下，将内部 IP 替换为 127.0.0.1，端口改为 BE HTTP 端口
     */
    private String rewriteRedirectUrl(String location) {
        try {
            URL redirectUrl = new URL(location);
            
            String host = redirectUrl.getHost();
            boolean isInternalIp = host.startsWith("172.") || host.startsWith("10.") || host.startsWith("192.168.");
            
            if (isInternalIp) {
                // 内部 IP 说明是 Docker 容器 IP，需要用宿主机映射端口直接访问 BE
                // 避免重新走 Nginx/FE 造成无限重定向循环
                String rewritten = String.format("%s://127.0.0.1:%d%s?%s",
                        redirectUrl.getProtocol(),
                        properties.getBeHttpPort(),
                        redirectUrl.getPath(),
                        redirectUrl.getQuery() != null ? redirectUrl.getQuery() : "");
                log.debug("内部 IP 重定向，重写为直接访问 BE: {}", rewritten);
                return rewritten;
            }
            return location;
        } catch (Exception e) {
            log.warn("重写重定向 URL 失败，使用原始 URL: {}", location, e);
            return location;
        }
    }

    /**
     * 构建 Load URL
     */
    private String buildLoadUrl(String tableName) {
        return String.format("%s/api/%s/%s/_stream_load",
                properties.getLoadUrl(), properties.getDatabase(), tableName);
    }

    /**
     * 解析响应
     */
    private LoadResponse parseResponse(String responseBody) {
        try {
            return objectMapper.readValue(responseBody, LoadResponse.class);
        } catch (Exception e) {
            log.error("解析响应失败: {}", responseBody, e);
            LoadResponse response = new LoadResponse();
            response.setStatus("Unknown");
            response.setMessage("解析响应失败: " + e.getMessage());
            return response;
        }
    }

    /**
     * 加载结果
     */
    @Data
    @AllArgsConstructor
    public static class LoadResult {
        private boolean success;
        private String status;
        private int recordCount;
    }

    /**
     * 加载响应
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LoadResponse {
        private int code;
        @JsonProperty("Status")
        private String status;
        @JsonProperty("Label")
        private String label;
        @JsonProperty("Message")
        private String message;
        @JsonProperty("msg")
        private String msg;  // Doris 错误响应使用 msg 字段
        @JsonProperty("ExistingJobStatus")
        private String existingJobStatus;
        @JsonProperty("TxnId")
        private long txnId;
        @JsonProperty("NumberTotalRows")
        private long numberTotalRows;
        @JsonProperty("NumberLoadedRows")
        private long numberLoadedRows;
        @JsonProperty("NumberFilteredRows")
        private long numberFilteredRows;
        @JsonProperty("ErrorURL")
        private String errorURL;
        
        /**
         * 获取消息（兼容 message 和 msg）
         */
        public String getMessage() {
            return message != null ? message : msg;
        }
    }
}