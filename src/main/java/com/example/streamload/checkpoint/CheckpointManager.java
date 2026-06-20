package com.example.streamload.checkpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 断点续传管理器
 * 记录已完成的批次,支持失败后从断点继续
 */
@Slf4j
@Component
public class CheckpointManager {
    
    private final String checkpointFile;
    private final ObjectMapper objectMapper;
    private final Set<Integer> completedBatches;
    private final AtomicInteger lastSaveCount;
    private static final int SAVE_INTERVAL = 10; // 每 10 个批次保存一次
    
    public CheckpointManager(com.example.streamload.config.DorisProperties properties) {
        this.checkpointFile = properties.getCheckpointFile();
        this.objectMapper = new ObjectMapper();
        this.completedBatches = loadCheckpoint();
        this.lastSaveCount = new AtomicInteger(completedBatches.size());
    }
    
    /**
     * 加载断点信息
     */
    private Set<Integer> loadCheckpoint() {
        File file = new File(checkpointFile);
        if (!file.exists()) {
            log.info("断点文件不存在,从头开始导入");
            return Collections.newSetFromMap(new ConcurrentHashMap<>());
        }
        
        try {
            CheckpointData data = objectMapper.readValue(file, CheckpointData.class);
            log.info("加载断点信息: 已完成 {} 个批次", data.getCompletedBatches().size());
            return Collections.newSetFromMap(new ConcurrentHashMap<>(
                data.getCompletedBatches().stream()
                    .collect(java.util.stream.Collectors.toMap(k -> k, v -> Boolean.TRUE))
            ));
        } catch (IOException e) {
            log.warn("加载断点文件失败,从头开始: {}", e.getMessage());
            return Collections.newSetFromMap(new ConcurrentHashMap<>());
        }
    }
    
    /**
     * 记录批次完成
     * 优化: 使用 ConcurrentHashMap 替代 synchronized + HashSet，减少锁竞争
     */
    public void markBatchCompleted(int batchIndex) {
        completedBatches.add(batchIndex);
        
        // 优化: 使用 AtomicInteger 计数，每 SAVE_INTERVAL 个批次保存一次
        int currentCount = completedBatches.size();
        if (currentCount - lastSaveCount.get() >= SAVE_INTERVAL) {
            // 只有第一个到达阈值的线程执行保存
            int expected = currentCount - (currentCount % SAVE_INTERVAL);
            if (lastSaveCount.compareAndSet(expected, currentCount)) {
                saveCheckpoint();
            }
        }
        
        log.debug("批次 {} 已完成,累计完成 {} 个批次", batchIndex, completedBatches.size());
    }
    
    /**
     * 强制保存断点（用于程序退出前）
     */
    public void forceSave() {
        saveCheckpoint();
        lastSaveCount.set(completedBatches.size());
    }
    
    /**
     * 检查批次是否已完成
     */
    public boolean isBatchCompleted(int batchIndex) {
        return completedBatches.contains(batchIndex);
    }
    
    /**
     * 保存断点信息到文件
     */
    private void saveCheckpoint() {
        try {
            CheckpointData data = new CheckpointData();
            data.setCompletedBatches(new java.util.ArrayList<>(completedBatches));
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(checkpointFile), data);
        } catch (IOException e) {
            log.error("保存断点文件失败: {}", e.getMessage());
        }
    }
    
    /**
     * 清除断点 (导入完成后调用)
     */
    public void clearCheckpoint() {
        File file = new File(checkpointFile);
        if (file.exists() && file.delete()) {
            log.info("断点文件已清除");
        }
        completedBatches.clear();
    }
    
    /**
     * 获取已完成的批次数量
     */
    public int getCompletedBatchCount() {
        return completedBatches.size();
    }
    
    /**
     * 断点数据结构
     */
    @lombok.Data
    private static class CheckpointData {
        private java.util.List<Integer> completedBatches = new java.util.ArrayList<>();
    }
}
