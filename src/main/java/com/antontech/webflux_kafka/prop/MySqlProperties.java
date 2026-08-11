package com.antontech.webflux_kafka.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * MySQL sink database properties used by Flink Job 2 (Kafka → MySQL).
 * Bound from {@code spring.mysql.*} in {@code application.yml}.
 */
@Component
@ConfigurationProperties(prefix = "spring.mysql")
public class MySqlProperties {
    private String jdbcUrl;
    private String driverClassName;
    private String username;
    private String password;
    private String itemTableName;

    public String getJdbcUrl() { return jdbcUrl; }
    public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }
    public String getDriverClassName() { return driverClassName; }
    public void setDriverClassName(String driverClassName) { this.driverClassName = driverClassName; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public String getItemTableName() { return itemTableName; }
    public void setItemTableName(String itemTableName) { this.itemTableName = itemTableName; }
}

