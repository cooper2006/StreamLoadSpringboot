package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Doris Stream Load 配置属性
 * 组合模式，引用多个子配置类，保持向后兼容性
 */
@Data
@Component
@ConfigurationProperties(prefix = "doris")
public class DorisProperties {

    private final DorisConnectionProperties connection;
    private final DorisHttpProperties http;
    private final DorisRetryProperties retry;
    private final DorisPipelineProperties pipeline;
    private final DorisCheckpointProperties checkpoint;
    private final DorisDockerProperties docker;
    private final DorisLogProperties log;

    public DorisProperties(DorisConnectionProperties connection,
                           DorisHttpProperties http,
                           DorisRetryProperties retry,
                           DorisPipelineProperties pipeline,
                           DorisCheckpointProperties checkpoint,
                           DorisDockerProperties docker,
                           DorisLogProperties log) {
        this.connection = connection;
        this.http = http;
        this.retry = retry;
        this.pipeline = pipeline;
        this.checkpoint = checkpoint;
        this.docker = docker;
        this.log = log;
    }

    // ==================== 向后兼容的 getter/setter 方法 ====================

    public String getLoadUrl() { return connection.getLoadUrl(); }
    public void setLoadUrl(String loadUrl) { connection.setLoadUrl(loadUrl); }

    public String getDatabase() { return connection.getDatabase(); }
    public void setDatabase(String database) { connection.setDatabase(database); }

    public String getUsername() { return connection.getUsername(); }
    public void setUsername(String username) { connection.setUsername(username); }

    public String getPassword() { return connection.getPassword(); }
    public void setPassword(String password) { connection.setPassword(password); }

    public int getJdbcPort() { return connection.getJdbcPort(); }
    public void setJdbcPort(int jdbcPort) { connection.setJdbcPort(jdbcPort); }

    public int getBeHttpPort() { return connection.getBeHttpPort(); }
    public void setBeHttpPort(int beHttpPort) { connection.setBeHttpPort(beHttpPort); }

    public String getBeHost() { return connection.getBeHost(); }
    public void setBeHost(String beHost) { connection.setBeHost(beHost); }

    public int getTimeout() { return http.getTimeout(); }
    public void setTimeout(int timeout) { http.setTimeout(timeout); }

    public int getConnectTimeout() { return http.getConnectTimeout(); }
    public void setConnectTimeout(int connectTimeout) { http.setConnectTimeout(connectTimeout); }

    public int getMaxRedirects() { return http.getMaxRedirects(); }
    public void setMaxRedirects(int maxRedirects) { http.setMaxRedirects(maxRedirects); }

    public double getMaxFilterRatio() { return http.getMaxFilterRatio(); }
    public void setMaxFilterRatio(double maxFilterRatio) { http.setMaxFilterRatio(maxFilterRatio); }

    public boolean isEnableCompression() { return http.isEnableCompression(); }
    public void setEnableCompression(boolean enableCompression) { http.setEnableCompression(enableCompression); }

    public int getMaxRetry() { return retry.getMaxRetry(); }
    public void setMaxRetry(int maxRetry) { retry.setMaxRetry(maxRetry); }

    public int getRetryIntervalBase() { return retry.getRetryIntervalBase(); }
    public void setRetryIntervalBase(int retryIntervalBase) { retry.setRetryIntervalBase(retryIntervalBase); }

    public int getBatchSize() { return pipeline.getBatchSize(); }
    public void setBatchSize(int batchSize) { pipeline.setBatchSize(batchSize); }

    public int getConsumerThreads() { return pipeline.getConsumerThreads(); }
    public void setConsumerThreads(int consumerThreads) { pipeline.setConsumerThreads(consumerThreads); }

    public int getQueueCapacity() { return pipeline.getQueueCapacity(); }
    public void setQueueCapacity(int queueCapacity) { pipeline.setQueueCapacity(queueCapacity); }

    public int getMaxFailedBatches() { return pipeline.getMaxFailedBatches(); }
    public void setMaxFailedBatches(int maxFailedBatches) { pipeline.setMaxFailedBatches(maxFailedBatches); }

    public int getConsumerCheckInterval() { return pipeline.getConsumerCheckInterval(); }
    public void setConsumerCheckInterval(int consumerCheckInterval) { pipeline.setConsumerCheckInterval(consumerCheckInterval); }

    public String getDateFormat() { return pipeline.getDateFormat(); }
    public void setDateFormat(String dateFormat) { pipeline.setDateFormat(dateFormat); }

    public String getCheckpointFile() { return checkpoint.getCheckpointFile(); }
    public void setCheckpointFile(String checkpointFile) { checkpoint.setCheckpointFile(checkpointFile); }

    public int getCheckpointSaveInterval() { return checkpoint.getCheckpointSaveInterval(); }
    public void setCheckpointSaveInterval(int checkpointSaveInterval) { checkpoint.setCheckpointSaveInterval(checkpointSaveInterval); }

    public String getDockerInternalIpPrefixes() { return docker.getDockerInternalIpPrefixes(); }
    public void setDockerInternalIpPrefixes(String dockerInternalIpPrefixes) { docker.setDockerInternalIpPrefixes(dockerInternalIpPrefixes); }

    public String getDockerContainerNames() { return docker.getDockerContainerNames(); }
    public void setDockerContainerNames(String dockerContainerNames) { docker.setDockerContainerNames(dockerContainerNames); }

    public boolean isUseNginxProxy() { return docker.isUseNginxProxy(); }
    public void setUseNginxProxy(boolean useNginxProxy) { docker.setUseNginxProxy(useNginxProxy); }

    public int getProgressLogInterval() { return log.getProgressLogInterval(); }
    public void setProgressLogInterval(int progressLogInterval) { log.setProgressLogInterval(progressLogInterval); }

    public int getProgressReportInterval() { return log.getProgressReportInterval(); }
    public void setProgressReportInterval(int progressReportInterval) { log.setProgressReportInterval(progressReportInterval); }
}
