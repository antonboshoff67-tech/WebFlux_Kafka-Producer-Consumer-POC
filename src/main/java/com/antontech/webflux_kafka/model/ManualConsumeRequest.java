package com.antontech.webflux_kafka.model;

/**
 * DTO for a manual-consume request – carries the consumer group id to use.
 */
public class ManualConsumeRequest {
    private String groupId;
    private String message;

    public ManualConsumeRequest() {}

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}

