package com.example.streamload.exception;

/**
 * HTTP 请求异常
 * 当 Stream Load HTTP 请求失败时抛出 (如网络错误、超时、Doris 返回错误状态)
 */
public class StreamLoadHttpException extends StreamLoadException {
    
    private final int statusCode;
    private final String responseBody;
    
    public StreamLoadHttpException(int statusCode, String responseBody, String message) {
        super(message);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }
    
    public StreamLoadHttpException(int statusCode, String responseBody, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }
    
    public int getStatusCode() {
        return statusCode;
    }
    
    public String getResponseBody() {
        return responseBody;
    }
}
