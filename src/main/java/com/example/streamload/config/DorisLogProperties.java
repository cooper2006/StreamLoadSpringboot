package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Doris 日志配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "doris")
public class DorisLogProperties {

    private int progressLogInterval = 10;
    private int progressReportInterval = 5;
}
