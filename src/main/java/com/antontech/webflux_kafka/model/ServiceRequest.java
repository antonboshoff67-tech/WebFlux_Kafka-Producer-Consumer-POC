package com.antontech.webflux_kafka.model;

/**
 * DTO carrying a message payload from the React UI to the gateway routing service.
 * Identical to the imperative POC's {@code ServiceRequest}.
 */
public class ServiceRequest {
    private String msg;

    public ServiceRequest() {}
    public ServiceRequest(String msg) { this.msg = msg; }

    public String getMsg() { return msg; }
    public void setMsg(String msg) { this.msg = msg; }
}

