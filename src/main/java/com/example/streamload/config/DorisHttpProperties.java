package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Doris HTTP 配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "doris")
public class DorisHttpProperties {

    private int timeout = 300;
    private int connectTimeout = 60;
    private int maxRedirects = 5;
    private double maxFilterRatio = 0.1;
    private boolean enableCompression = true;
}
