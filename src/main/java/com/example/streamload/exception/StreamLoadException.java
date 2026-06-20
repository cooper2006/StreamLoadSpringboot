package com.example.streamload.exception;

/**
 * Stream Load 自定义异常基类
 * 用于区分 Stream Load 过程中的不同类型错误
 */
public class StreamLoadException extends Exception {
    
    public StreamLoadException(String message) {
        super(message);
    }
    
    public StreamLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
