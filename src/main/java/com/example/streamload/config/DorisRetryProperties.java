package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Doris 重试配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "doris")
public class DorisRetryProperties {

    private int maxRetry = 3;
    private int retryIntervalBase = 2;
}
