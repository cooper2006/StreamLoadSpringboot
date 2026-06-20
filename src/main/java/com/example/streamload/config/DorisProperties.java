package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Doris Stream Load 配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "doris")
public class DorisProperties {
    
    /**
     * Stream Load HTTP 地址
     */
    private String loadUrl;
    
    /**
     * 目标数据库
     */
    private String database;
    
    /**
     * 认证用户名
     */
    private String username;
    
    /**
     * 认证密码
     */
    private String password;
    
    /**
     * 批次大小 (每批记录数)
     */
    private int batchSize = 100000;
    
    /**
     * 是否启用 gzip 压缩
     */
    private boolean enableCompression = true;
    
    /**
     * 超时时间 (秒)
     */
    private int timeout = 300;
    
    /**
     * 最大重试次数
     */
    private int maxRetry = 3;
    
    /**
     * 重试间隔基数 (秒,指数退避)
     */
    private int retryIntervalBase = 2;
    
    /**
     * 断点续传文件路径
     */
    private String checkpointFile = "./checkpoint.json";
}
