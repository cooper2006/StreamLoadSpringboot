package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Doris Docker 网络配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "doris")
public class DorisDockerProperties {

    private String dockerInternalIpPrefixes = "172.,10.,192.168.";
    private String dockerContainerNames = "doris-be,doris-fe,doris";
    private boolean useNginxProxy = true;
}
