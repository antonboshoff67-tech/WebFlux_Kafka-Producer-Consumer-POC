package com.antontech.webflux_kafka.exceptions;

/** Thrown when Kafka consumption or message routing fails. */
public class ConsumerException extends Exception {
    public ConsumerException(String message) { super(message); }
    public ConsumerException(String message, Throwable cause) { super(message, cause); }
}

