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
     * 连接超时时间 (秒)
     */
    private int connectTimeout = 60;
    
    /**
     * 最大重试次数
     */
    private int maxRetry = 3;
    
    /**
     * 重试间隔基数 (秒,指数退避)
     */
    private int retryIntervalBase = 2;
    
    /**
     * 最大重定向次数
     */
    private int maxRedirects = 5;
    
    /**
     * 最大过滤比例 (0-1)
     */
    private double maxFilterRatio = 0.1;
    
    /**
     * 断点续传文件路径
     */
    private String checkpointFile = "./checkpoint.json";
    
    /**
     * Doris MySQL 协议端口 (用于 JDBC 连接)
     */
    private int jdbcPort = 9030;
    
    /**
     * Doris BE HTTP 端口 (用于重定向后直接访问 BE)
     */
    private int beHttpPort = 8040;
    
    /**
     * Doris BE 主机地址 (重定向重写目标)
     */
    private String beHost = "127.0.0.1";
    
    /**
     * Docker 内部 IP 前缀列表（逗号分隔），用于识别需要重写的重定向地址
     */
    private String dockerInternalIpPrefixes = "172.,10.,192.168.";
    
    /**
     * Docker 容器名列表（逗号分隔），用于识别需要重写的重定向地址
     */
    private String dockerContainerNames = "doris-be,doris-fe,doris";
    
    /**
     * 日期格式化字符串 (用于 Doris 兼容的时间格式)
     */
    private String dateFormat = "yyyy-MM-dd HH:mm:ss";
    
    /**
     * 进度日志输出间隔 (批次)
     */
    private int progressLogInterval = 10;
    
    /**
     * 进度报告间隔 (秒)
     */
    private int progressReportInterval = 5;
    
    /**
     * 消费者线程数 (并行导入)
     */
    private int consumerThreads = 3;
    
    /**
     * 生产者-消费者队列容量
     */
    private int queueCapacity = 8;
    
    /**
     * 最大允许失败批次数
     */
    private int maxFailedBatches = 3;
    
    /**
     * 断点保存间隔 (批次)
     */
    private int checkpointSaveInterval = 10;
    
    /**
     * 消费者状态检查间隔 (行数)
     */
    private int consumerCheckInterval = 10000;
    
    /**
     * 是否使用 Nginx 代理
     * true: 开发环境，通过 Nginx 代理访问 Doris
     * false: 生产环境，直接连接 Doris
     */
    private boolean useNginxProxy = true;
}