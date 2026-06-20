package com.example.streamload.exception;

/**
 * IO 异常
 * 当数据读取、写入或压缩过程中发生 IO 错误时抛出
 */
public class StreamLoadIOException extends StreamLoadException {
    
    public StreamLoadIOException(String message) {
        super(message);
    }
    
    public StreamLoadIOException(String message, Throwable cause) {
        super(message, cause);
    }
}
