package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Doris 流水线配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "doris")
public class DorisPipelineProperties {

    private int batchSize = 100000;
    private int consumerThreads = 3;
    private int queueCapacity = 8;
    private int maxFailedBatches = 3;
    private int consumerCheckInterval = 10000;
    private String dateFormat = "yyyy-MM-dd HH:mm:ss";
}
