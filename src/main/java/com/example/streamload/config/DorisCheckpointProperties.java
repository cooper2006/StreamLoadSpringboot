package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Doris 断点续传配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "doris")
public class DorisCheckpointProperties {

    private String checkpointFile = "./checkpoint.json";
    private int checkpointSaveInterval = 10;
}
