package com.example.streamload.verify;

import com.example.streamload.config.VerifyProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 导入验证服务
 * 支持三种验证模式: NONE, COUNT, SAMPLE
 */
@Slf4j
@Service
public class VerifyService {

    private final JdbcTemplate sourceJdbcTemplate;
    private final DataSource dorisDataSource;
    private final VerifyProperties verifyProperties;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public VerifyService(JdbcTemplate sourceJdbcTemplate,
                         VerifyProperties verifyProperties,
                         @Qualifier("dorisDataSource") DataSource dorisDataSource) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.verifyProperties = verifyProperties;
        this.dorisDataSource = dorisDataSource;
    }

    /**
     * 构建验证参数
     */
    public VerifyParam buildVerifyParam() {
        return new VerifyParam(
                VerifyParam.VerifyMode.valueOf(verifyProperties.getMode().name()),
                verifyProperties.getSourceSql(),
                verifyProperties.getTargetTable(),
                verifyProperties.getSampleRatio(),
                verifyProperties.getVerifyColumns()
        );
    }

    /**
     * 执行验证
     */
    public VerifyResult verify(VerifyParam param) {
        if (param.getMode() == VerifyParam.VerifyMode.NONE) {
            log.info("验证模式: NONE, 跳过验证");
            return VerifyResult.skip();
        }

        // 获取源表记录数
        long sourceCount = countSource(param.getSourceSql());
        log.info("源表记录数: {}", sourceCount);

        if (param.getMode() == VerifyParam.VerifyMode.COUNT) {
            long targetCount = countTarget(param.getTargetTable());
            log.info("目标表记录数: {}", targetCount);
            return VerifyResult.countResult(sourceCount, targetCount);
        }

        if (param.getMode() == VerifyParam.VerifyMode.SAMPLE) {
            long targetCount = countTarget(param.getTargetTable());
            int sampleSize = Math.max(1, (int) (sourceCount * param.getSampleRatio()));
            sampleSize = Math.min(sampleSize, verifyProperties.getMaxSampleSize()); // 上限配置

            int matched = sampleVerify(param, sampleSize);
            return VerifyResult.sampleResult(sourceCount, targetCount, matched, sampleSize);
        }

        return VerifyResult.skip();
    }

    /**
     * 查询源表记录数
     */
    private long countSource(String sourceSql) {
        String countSql = "SELECT COUNT(*) FROM (" + sourceSql + ") t";
        Long count = sourceJdbcTemplate.queryForObject(countSql, Long.class);
        return count != null ? count : 0;
    }

    /**
     * 查询目标表记录数 (通过 Doris JDBC)
     */
    private long countTarget(String targetTable) {
        try (Connection conn = dorisDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + targetTable)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (Exception e) {
            log.error("查询目标表记录数失败: {}", e.getMessage());
        }
        return 0;
    }

    /**
     * 抽样验证
     */
    @SuppressWarnings("unchecked")
    private int sampleVerify(VerifyParam param, int sampleSize) {
        String[] columns = param.getVerifyColumns().split(",");
        String columnList = param.getVerifyColumns();

        // 从源表抽样
        String sourceSampleSql = String.format("SELECT %s FROM (%s) t ORDER BY RAND() LIMIT %d",
                columnList, param.getSourceSql(), sampleSize);
        List<Map<String, Object>> sourceRows = sourceJdbcTemplate.queryForList(sourceSampleSql);

        // 从目标表查询对应记录
        if (sourceRows.isEmpty()) {
            return 0;
        }

        // 获取主键列 (假设第一列是主键)
        String pkColumn = columns[0].trim();
        List<Object> pkValues = sourceRows.stream()
                .map(row -> row.get(pkColumn))
                .collect(Collectors.toList());

        String inClause = pkValues.stream()
                .map(v -> "'" + v + "'")
                .collect(Collectors.joining(","));

        int matched = 0;
        try (Connection conn = dorisDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     String.format("SELECT %s FROM %s WHERE %s IN (%s)",
                             columnList, param.getTargetTable(), pkColumn, inClause))) {

            Map<String, Map<String, Object>> targetMap = new HashMap<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (String col : columns) {
                    row.put(col.trim(), rs.getObject(col.trim()));
                }
                targetMap.put(String.valueOf(row.get(pkColumn)), row);
            }

            for (Map<String, Object> sourceRow : sourceRows) {
                String pk = String.valueOf(sourceRow.get(pkColumn));
                Map<String, Object> targetRow = targetMap.get(pk);
                if (targetRow != null && rowsMatch(sourceRow, targetRow, columns)) {
                    matched++;
                }
            }

        } catch (Exception e) {
            log.error("抽样验证查询失败: {}", e.getMessage());
        }

        log.info("抽样验证: {}/{} 匹配", matched, sampleSize);
        return matched;
    }

    /**
     * 比较两行数据是否匹配
     */
    private boolean rowsMatch(Map<String, Object> source, Map<String, Object> target, String[] columns) {
        for (String col : columns) {
            col = col.trim();
            Object sv = source.get(col);
            Object tv = target.get(col);
            if (!Objects.equals(String.valueOf(sv), String.valueOf(tv))) {
                return false;
            }
        }
        return true;
    }
}
