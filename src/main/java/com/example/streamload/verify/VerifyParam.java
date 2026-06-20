package com.example.streamload.verify;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 验证参数对象
 * 统一管理导入验证所需的参数
 */
@Data
@AllArgsConstructor
public class VerifyParam {

    /**
     * 验证模式
     */
    private VerifyMode mode;

    /**
     * 源表查询 SQL
     */
    private String sourceSql;

    /**
     * 目标表名称
     */
    private String targetTable;

    /**
     * 抽样验证比例 (0-1)
     */
    private double sampleRatio;

    /**
     * 验证字段 (逗号分隔)
     */
    private String verifyColumns;

    /**
     * 验证模式枚举
     */
    public enum VerifyMode {
        /** 不验证 */
        NONE,
        /** 仅验证记录数 */
        COUNT,
        /** 抽样验证 (对比部分记录) */
        SAMPLE
    }
}
