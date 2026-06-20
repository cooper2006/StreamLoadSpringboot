package com.example.streamload.exception;

/**
 * 压缩异常
 * 当 gzip 压缩过程中发生错误时抛出
 */
public class StreamLoadCompressionException extends StreamLoadException {
    
    public StreamLoadCompressionException(String message) {
        super(message);
    }
    
    public StreamLoadCompressionException(String message, Throwable cause) {
        super(message, cause);
    }
}
