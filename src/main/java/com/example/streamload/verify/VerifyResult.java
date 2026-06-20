package com.example.streamload.verify;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 验证结果
 */
@Data
@AllArgsConstructor
public class VerifyResult {

    /**
     * 是否验证通过
     */
    private boolean passed;

    /**
     * 验证模式
     */
    private String mode;

    /**
     * 源记录数
     */
    private long sourceCount;

    /**
     * 目标记录数
     */
    private long targetCount;

    /**
     * 抽样匹配数
     */
    private int sampleMatched;

    /**
     * 抽样总数
     */
    private int sampleTotal;

    /**
     * 描述信息
     */
    private String message;

    public static VerifyResult skip() {
        return new VerifyResult(true, "NONE", 0, 0, 0, 0, "跳过验证");
    }

    public static VerifyResult countResult(long sourceCount, long targetCount) {
        boolean passed = sourceCount == targetCount;
        String msg = passed
                ? String.format("记录数一致: %d", sourceCount)
                : String.format("记录数不一致: 源=%d, 目标=%d", sourceCount, targetCount);
        return new VerifyResult(passed, "COUNT", sourceCount, targetCount, 0, 0, msg);
    }

    public static VerifyResult sampleResult(long sourceCount, long targetCount, int matched, int total) {
        boolean countPassed = sourceCount == targetCount;
        boolean samplePassed = matched == total;
        boolean passed = countPassed && samplePassed;
        String msg = String.format("记录数: 源=%d, 目标=%d (%s); 抽样: %d/%d (%s)",
                sourceCount, targetCount, countPassed ? "一致" : "不一致",
                matched, total, samplePassed ? "全部匹配" : "存在差异");
        return new VerifyResult(passed, "SAMPLE", sourceCount, targetCount, matched, total, msg);
    }
}
