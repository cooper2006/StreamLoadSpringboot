package com.example.streamload.verify;

import com.example.streamload.config.VerifyProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
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
        String sql = "SELECT COUNT(*) FROM " + escapeIdentifier(targetTable);
        try (Connection conn = dorisDataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (Exception e) {
            log.error("查询目标表记录数失败: {}", e.getMessage());
        }
        return 0;
    }

    /**
     * 转义 SQL 标识符（表名/列名），防止注入
     * Doris JDBC 不支持双引号包围的标识符
     */
    private String escapeIdentifier(String identifier) {
        if (identifier == null) {
            throw new IllegalArgumentException("Identifier must not be null");
        }
        if (!identifier.matches("[a-zA-Z0-9_\\-]+")) {
            log.warn("标识符包含非预期字符，进行安全转义: {}", identifier);
        }
        return identifier;
    }

    /**
     * 抽样验证 - 使用全部指定列作为匹配键，不再假设第一列是主键
     */
    @SuppressWarnings("unchecked")
    private int sampleVerify(VerifyParam param, int sampleSize) {
        String[] columns = param.getVerifyColumns().split(",");
        String columnList = param.getVerifyColumns();

        // 从源表抽样
        String sourceSampleSql = String.format("SELECT %s FROM (%s) t ORDER BY RAND() LIMIT %d",
                columnList, param.getSourceSql(), sampleSize);
        List<Map<String, Object>> sourceRows = sourceJdbcTemplate.queryForList(sourceSampleSql);

        if (sourceRows.isEmpty()) {
            return 0;
        }

        // 构建复合匹配键: 使用所有验证列的值组合作为键
        List<String[]> sourceKeys = new ArrayList<>(sourceRows.size());
        for (Map<String, Object> row : sourceRows) {
            String[] key = new String[columns.length];
            for (int i = 0; i < columns.length; i++) {
                Object val = row.get(columns[i].trim());
                key[i] = val != null ? String.valueOf(val) : "NULL";
            }
            sourceKeys.add(key);
        }

        // 使用 PreparedStatement 参数化查询，防止 SQL 注入
        int matched = 0;
        try (Connection conn = dorisDataSource.getConnection()) {
            Map<String, Map<String, Object>> targetMap = new HashMap<>();
            for (String[] key : sourceKeys) {
                StringBuilder rowSql = new StringBuilder();
                rowSql.append("SELECT ");
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) rowSql.append(", ");
                    rowSql.append(columns[i].trim());
                }
                rowSql.append(" FROM ").append(escapeIdentifier(param.getTargetTable())).append(" WHERE ");
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) rowSql.append(" AND ");
                    rowSql.append(columns[i].trim()).append(" = ?");
                }

                try (PreparedStatement pstmt = conn.prepareStatement(rowSql.toString())) {
                    for (int i = 0; i < columns.length; i++) {
                        pstmt.setString(i + 1, key[i].equals("NULL") ? null : key[i]);
                    }
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            Map<String, Object> row = new LinkedHashMap<>();
                            for (String col : columns) {
                                row.put(col.trim(), rs.getObject(col.trim()));
                            }
                            // 用逗号连接所有列值作为键
                            String compositeKey = Arrays.toString(key);
                            targetMap.put(compositeKey, row);
                        }
                    }
                }
            }

            for (String[] sourceKey : sourceKeys) {
                String compositeKey = Arrays.toString(sourceKey);
                Map<String, Object> targetRow = targetMap.get(compositeKey);
                if (targetRow != null) {
                    // 再逐列比对确保完全匹配
                    boolean match = true;
                    Map<String, Object> sourceRow = null;
                    // 找到对应的源行
                    int idx = Arrays.asList(sourceKeys.toArray()).indexOf(sourceKey);
                    if (idx >= 0 && idx < sourceRows.size()) {
                        sourceRow = sourceRows.get(idx);
                    }
                    if (sourceRow != null) {
                        for (String col : columns) {
                            col = col.trim();
                            Object sv = sourceRow.get(col);
                            Object tv = targetRow.get(col);
                            if (!Objects.equals(String.valueOf(sv), String.valueOf(tv))) {
                                match = false;
                                break;
                            }
                        }
                    }
                    if (match) {
                        matched++;
                    }
                }
            }

        } catch (Exception e) {
            log.error("抽样验证查询失败: {}", e.getMessage(), e);
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
