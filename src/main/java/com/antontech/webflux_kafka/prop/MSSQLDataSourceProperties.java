package com.antontech.webflux_kafka.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * MS SQL Server source properties used by Flink Job 1 (MSSQL → Kafka).
 * Bound from {@code spring.datasource.*} in {@code application.yml}.
 */
@Component
@ConfigurationProperties(prefix = "spring.datasource")
public class MSSQLDataSourceProperties {
    private String url;
    private String username;
    private String password;
    private String sourceTableName;

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public String getSourceTableName() { return sourceTableName; }
    public void setSourceTableName(String sourceTableName) { this.sourceTableName = sourceTableName; }
}

