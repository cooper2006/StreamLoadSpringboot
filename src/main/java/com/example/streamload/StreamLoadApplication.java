package com.example.streamload;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Stream Load Doris 应用启动类
 * 高性能数据导入系统:从关系型数据库流式查询并批量导入到 Apache Doris
 */
@SpringBootApplication
public class StreamLoadApplication {
    
    static {
        // 禁用系统代理，确保直连 Doris
        System.setProperty("java.net.useSystemProxies", "false");
        System.clearProperty("socksProxyHost");
        System.clearProperty("socksProxyPort");
        System.clearProperty("http.proxyHost");
        System.clearProperty("http.proxyPort");
        System.clearProperty("https.proxyHost");
        System.clearProperty("https.proxyPort");
    }
    
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(StreamLoadApplication.class, args)));
    }
}
