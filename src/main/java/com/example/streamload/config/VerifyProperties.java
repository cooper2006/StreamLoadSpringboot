package com.example.streamload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 验证配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "verify")
public class VerifyProperties {
    
    /**
     * 验证模式: NONE, COUNT, SAMPLE
     */
    private VerifyMode mode = VerifyMode.COUNT;
    
    /**
     * 抽样验证比例 (0-1)
     */
    private double sampleRatio = 0.01;
    
    /**
     * 源表查询 SQL
     */
    private String sourceSql;
    
    /**
     * 目标表名称
     */
    private String targetTable;
    
    /**
     * 验证字段 (用于抽样对比)
     */
    private String verifyColumns;
    
    /**
     * 抽样验证最大样本数
     */
    private int maxSampleSize = 1000;
    
    /**
     * 验证模式枚举
     */
    public enum VerifyMode {
        /**
         * 不验证
         */
        NONE,
        /**
         * 仅验证记录数
         */
        COUNT,
        /**
         * 抽样验证 (对比部分记录)
         */
        SAMPLE
    }
}
