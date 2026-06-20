package com.example.streamload.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

/**
 * 数据源配置类
 * 配置源数据库 (MySQL) 和目标数据库 (Doris)
 */
@Configuration
public class DataSourceConfig {

    @Value("${spring.datasource.url}")
    private String mysqlUrl;

    @Value("${spring.datasource.username}")
    private String mysqlUsername;

    @Value("${spring.datasource.password}")
    private String mysqlPassword;

    @Value("${spring.datasource.driver-class-name}")
    private String mysqlDriverClassName;

    @Autowired
    private DorisProperties dorisProperties;

    /**
     * 源数据库 (MySQL) - 用于读取数据
     */
    @Bean
    @Primary
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(mysqlUrl);
        config.setUsername(mysqlUsername);
        config.setPassword(mysqlPassword);
        config.setDriverClassName(mysqlDriverClassName);
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        return new HikariDataSource(config);
    }

    /**
     * 目标数据库 (Doris) - 用于验证数据
     * Doris 兼容 MySQL 协议，使用 MySQL JDBC 驱动连接
     */
    @Bean
    public DataSource dorisDataSource() {
        // 从 load-url 提取主机
        // load-url 格式: http://127.0.0.1:18030 (nginx 代理)
        String host = dorisProperties.getLoadUrl()
                .replace("http://", "")
                .replace("https://", "")
                .replaceAll(":\\d+", ""); // 移除端口部分
        
        String dorisJdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&allowPublicKeyRetrieval=true",
                host, dorisProperties.getJdbcPort(), dorisProperties.getDatabase());

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dorisJdbcUrl);
        config.setUsername(dorisProperties.getUsername());
        config.setPassword(dorisProperties.getPassword());
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(30000);
        return new HikariDataSource(config);
    }
}