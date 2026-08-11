package com.antontech.webflux_kafka.service;

/** Enumeration of possible Flink job execution states. */
public enum JobStatus {
    /** Job has never been triggered. */
    PENDING,
    /** Job is currently executing. */
    RUNNING,
    /** Job finished successfully. */
    COMPLETED,
    /** Job threw an exception. */
    FAILED
}

