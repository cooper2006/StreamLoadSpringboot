package com.example.streamload.state;

/**
 * 生产者状态接口
 * 用于暴露生产者线程的执行状态和异常信息
 */
public interface ProducerState {
    
    /**
     * 获取已生产的批次数量
     */
    int getProducedBatchCount();
    
    /**
     * 获取已生产的记录总数
     */
    long getProducedRecordCount();
    
    /**
     * 判断生产者是否完成
     */
    boolean isCompleted();
    
    /**
     * 判断生产者是否发生异常
     */
    boolean hasError();
    
    /**
     * 获取生产者异常 (如果有)
     */
    Throwable getError();
}
