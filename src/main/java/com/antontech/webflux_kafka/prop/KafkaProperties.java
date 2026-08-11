package com.antontech.webflux_kafka.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Type-safe binding of the {@code spring.kafka.*} configuration tree.
 * Same semantics as the imperative POC – values come from {@code application.yml}
 * and can be overridden per environment via environment variables.
 */
@Component
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaProperties {

    private String bootstrapServers;
    private String itemTopicName;
    private ProducerProperties producer;
    private ConsumerProperties consumer;

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
    public String getItemTopicName() { return itemTopicName; }
    public void setItemTopicName(String itemTopicName) { this.itemTopicName = itemTopicName; }
    public ProducerProperties getProducer() { return producer; }
    public void setProducer(ProducerProperties producer) { this.producer = producer; }
    public ConsumerProperties getConsumer() { return consumer; }
    public void setConsumer(ConsumerProperties consumer) { this.consumer = consumer; }

    public static class ProducerProperties {
        private int retries;
        public int getRetries() { return retries; }
        public void setRetries(int retries) { this.retries = retries; }
    }

    public static class ConsumerProperties {
        private String groupId;
        private String autoOffsetReset;
        private int maxPollIntervalMs;
        private int sessionTimeoutMs;

        public String getGroupId() { return groupId; }
        public void setGroupId(String groupId) { this.groupId = groupId; }
        public String getAutoOffsetReset() { return autoOffsetReset; }
        public void setAutoOffsetReset(String autoOffsetReset) { this.autoOffsetReset = autoOffsetReset; }
        public int getMaxPollIntervalMs() { return maxPollIntervalMs; }
        public void setMaxPollIntervalMs(int maxPollIntervalMs) { this.maxPollIntervalMs = maxPollIntervalMs; }
        public int getSessionTimeoutMs() { return sessionTimeoutMs; }
        public void setSessionTimeoutMs(int sessionTimeoutMs) { this.sessionTimeoutMs = sessionTimeoutMs; }
    }
}

