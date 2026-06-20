package com.example.streamload.state;

/**
 * 消费者状态接口
 * 用于暴露消费者线程的执行状态和异常信息
 */
public interface ConsumerState {
    
    /**
     * 获取已消费的批次数量
     */
    int getConsumedBatchCount();
    
    /**
     * 获取已导入的记录总数
     */
    long getImportedRecordCount();
    
    /**
     * 获取失败的批次数量
     */
    int getFailedBatchCount();
    
    /**
     * 判断消费者是否完成
     */
    boolean isCompleted();
    
    /**
     * 判断消费者是否发生异常
     */
    boolean hasError();
    
    /**
     * 获取消费者异常 (如果有)
     */
    Throwable getError();
}
