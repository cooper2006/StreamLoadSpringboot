package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Doris 连接配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "doris")
public class DorisConnectionProperties {

    private String loadUrl;
    private String database;
    private String username;
    private String password;
    private int jdbcPort = 9030;
    private int beHttpPort = 8040;
    private String beHost = "127.0.0.1";
}
