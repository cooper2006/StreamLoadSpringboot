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
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

/**
 * Stream Load 核心服务
 * 负责将数据批量导入到 Doris，支持 gzip 压缩、自动重试、幂等提交
 * 使用 Apache HttpClient 连接池，支持真正的 Keep-Alive 和连接复用
 */
@Slf4j
@Service
public class StreamLoadService {

    private final DorisProperties properties;
    private final CheckpointManager checkpointManager;
    private final ObjectMapper objectMapper;
    private final com.fasterxml.jackson.databind.ObjectWriter objectWriter;
    private final String encodedAuth;
    private final CloseableHttpClient httpClient;

    public StreamLoadService(DorisProperties properties, CheckpointManager checkpointManager) {
        this.properties = properties;
        this.checkpointManager = checkpointManager;
        this.objectMapper = new ObjectMapper();
        this.objectWriter = objectMapper.writer();

        String auth = properties.getUsername() + ":" + properties.getPassword();
        this.encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

        this.httpClient = createHttpClient();
        log.info("Apache HttpClient 连接池初始化完成");
    }

    private CloseableHttpClient createHttpClient() {
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(20);
        connManager.setDefaultMaxPerRoute(10);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.ofSeconds(properties.getConnectTimeout()))
                .setResponseTimeout(Timeout.ofSeconds(properties.getTimeout()))
                .setRedirectsEnabled(false)
                .build();

        return HttpClients.custom()
                .setConnectionManager(connManager)
                .setDefaultRequestConfig(requestConfig)
                .disableAutomaticRetries()
                .build();
    }

    @PreDestroy
    public void destroy() {
        if (httpClient != null) {
            try {
                httpClient.close();
                log.info("Apache HttpClient 连接池已关闭");
            } catch (IOException e) {
                log.warn("关闭 HttpClient 连接池时出现异常", e);
            }
        }
    }

    public LoadResult executeLoad(int batchIndex, String tableName, List<Map<String, Object>> data)
            throws StreamLoadHttpException, StreamLoadIOException, StreamLoadCompressionException {
        if (checkpointManager.isBatchCompleted(batchIndex)) {
            log.info("批次 {} 已完成,跳过", batchIndex);
            return new LoadResult(true, "SKIPPED", data.size());
        }

        String label = generateLabel(batchIndex);
        String jsonData = convertToJson(data);

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

        String url = buildLoadUrl(tableName);

        int retryCount = 0;
        Exception lastException = null;

        while (retryCount <= properties.getMaxRetry()) {
            try {
                LoadResult result = doLoad(url, label, payload, compressed, data.size());
                if (result.isSuccess()) {
                    checkpointManager.markBatchCompleted(batchIndex);
                }
                return result;
            } catch (StreamLoadHttpException e) {
                lastException = e;
                if (!isRetriable(e)) {
                    throw e;
                }
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
            } catch (StreamLoadIOException e) {
                lastException = e;
                retryCount++;

                if (retryCount <= properties.getMaxRetry()) {
                    long waitTime = (long) Math.pow(properties.getRetryIntervalBase(), retryCount);
                    log.warn("批次 {} IO异常,第 {} 次重试,等待 {} 秒: {}",
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

    private boolean isRetriable(StreamLoadHttpException e) {
        int statusCode = e.getStatusCode();
        if (statusCode >= 500 && statusCode < 600) {
            return true;
        }
        if (statusCode >= 400 && statusCode < 500) {
            log.warn("批次 {} 收到 4xx 错误 (HTTP {}), 不进行重试: {}",
                    e.getMessage(), statusCode, e.getMessage());
            return false;
        }
        String msg = e.getMessage();
        if (msg != null && msg.contains("Label Already Exists")) {
            return false;
        }
        return true;
    }

    private LoadResult doLoad(String urlStr, String label, byte[] payload, boolean compressed, int recordCount)
            throws StreamLoadHttpException, StreamLoadIOException {

        String currentUrl = urlStr;
        int maxRedirects = properties.getMaxRedirects();
        int redirectCount = 0;

        while (redirectCount < maxRedirects) {
            try {
                URL url = new URL(currentUrl);
                HttpHost target = new HttpHost(url.getProtocol(), url.getHost(), url.getPort());
                String path = url.getPath() + (url.getQuery() != null ? "?" + url.getQuery() : "");

                HttpPut httpPut = new HttpPut(path);

                httpPut.setHeader("label", label);
                httpPut.setHeader("format", "json");
                httpPut.setHeader("strip_outer_array", "true");
                httpPut.setHeader("timeout", String.valueOf(properties.getTimeout()));
                httpPut.setHeader("max_filter_ratio", String.valueOf(properties.getMaxFilterRatio()));
                httpPut.setHeader("Expect", "100-continue");
                httpPut.setHeader("Content-Type", "application/json");

                if (compressed) {
                    httpPut.setHeader("Content-Encoding", "gzip");
                }

                httpPut.setHeader("Authorization", "Basic " + encodedAuth);

                HttpEntity entity = new ByteArrayEntity(payload,
                        compressed ? ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8)
                                : ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8));
                httpPut.setEntity(entity);

                log.debug("发送请求到: {}://{}:{}{}", url.getProtocol(), url.getHost(), url.getPort(), path);

                try (CloseableHttpResponse response = httpClient.execute(target, httpPut)) {
                    int statusCode = response.getCode();
                    String responseBody;
                    try {
                        responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    } catch (org.apache.hc.core5.http.ParseException e) {
                        throw new StreamLoadIOException("解析响应体失败: " + e.getMessage(), e);
                    }

                    log.debug("Doris 响应: statusCode={}, body={}", statusCode, responseBody);

                    if (statusCode == 307) {
                        String location = response.getFirstHeader("Location") != null
                                ? response.getFirstHeader("Location").getValue() : null;
                        if (location != null && !location.isEmpty()) {
                            log.debug("收到 307 重定向到: {}", location);
                            currentUrl = rewriteRedirectUrl(location);
                            log.debug("重写后的重定向 URL: {}", currentUrl);
                            redirectCount++;
                            continue;
                        } else {
                            throw new StreamLoadHttpException(statusCode, responseBody, "307 重定向但没有 Location 头");
                        }
                    }

                    LoadResponse loadResponse = parseResponse(responseBody);
                    return handleResponse(statusCode, responseBody, loadResponse, label, recordCount);
                }

            } catch (IOException e) {
                throw new StreamLoadIOException("HTTP 请求异常: " + e.getMessage(), e);
            }
        }

        throw new StreamLoadHttpException(0, "", "重定向次数超过限制: " + maxRedirects);
    }

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

    private String convertToJson(List<Map<String, Object>> data) {
        try {
            return objectWriter.writeValueAsString(data);
        } catch (IOException e) {
            log.error("JSON 序列化失败", e);
            throw new RuntimeException("JSON 序列化失败", e);
        }
    }

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

    private String generateLabel(int batchIndex) {
        return String.format("stream_load_%s_%s_%d",
                properties.getDatabase(), UUID.randomUUID().toString().replace("-", "").substring(0, 16), batchIndex);
    }

    private String rewriteRedirectUrl(String location) {
        try {
            URL redirectUrl = new URL(location);

            String host = redirectUrl.getHost();

            String[] internalIpPrefixes = properties.getDockerInternalIpPrefixes().split(",");
            boolean isInternalIp = false;
            for (String prefix : internalIpPrefixes) {
                if (host.startsWith(prefix.trim())) {
                    isInternalIp = true;
                    break;
                }
            }

            String[] containerNames = properties.getDockerContainerNames().split(",");
            boolean isDockerContainer = false;
            for (String name : containerNames) {
                if (host.equalsIgnoreCase(name.trim()) || host.contains(name.trim())) {
                    isDockerContainer = true;
                    break;
                }
            }

            if (isInternalIp || isDockerContainer) {
                String rewritten = String.format("%s://%s:%d%s%s",
                        redirectUrl.getProtocol(),
                        properties.getBeHost(),
                        properties.getBeHttpPort(),
                        redirectUrl.getPath(),
                        redirectUrl.getQuery() != null ? "?" + redirectUrl.getQuery() : "");
                log.debug("Docker 内部地址重定向，重写为直接访问 BE: {}", rewritten);
                return rewritten;
            }
            return removeUserInfo(location);
        } catch (Exception e) {
            log.warn("重写重定向 URL 失败，使用原始 URL: {}", location, e);
            return location;
        }
    }

    private String buildLoadUrl(String tableName) {
        return String.format("%s/api/%s/%s/_stream_load",
                properties.getLoadUrl(), properties.getDatabase(), tableName);
    }

    private String removeUserInfo(String url) {
        try {
            URL u = new URL(url);
            if (u.getUserInfo() != null) {
                return new URL(u.getProtocol(), u.getHost(), u.getPort(), u.getFile()).toString();
            }
        } catch (Exception e) {
            // ignore
        }
        return url;
    }

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

    @Data
    @AllArgsConstructor
    public static class LoadResult {
        private boolean success;
        private String status;
        private int recordCount;
    }

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
        private String msg;
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

        public String getMessage() {
            return message != null ? message : msg;
        }
    }
}
