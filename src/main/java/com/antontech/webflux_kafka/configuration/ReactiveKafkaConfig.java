package com.antontech.webflux_kafka.configuration;

import com.antontech.webflux_kafka.prop.KafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Reactive Kafka configuration.
 *
 * <h2>reactor-kafka vs spring-kafka</h2>
 * <table border="1">
 *   <tr><th>spring-kafka (imperative)</th><th>reactor-kafka (reactive)</th></tr>
 *   <tr><td>{@code KafkaTemplate.send()} – blocks on get()</td>
 *       <td>{@code ReactiveKafkaProducerTemplate.send()} – returns {@code Mono<SenderResult>}</td></tr>
 *   <tr><td>{@code @KafkaListener} – runs on a consumer thread pool</td>
 *       <td>{@code ReactiveKafkaConsumerTemplate.receive()} – returns {@code Flux<ReceiverRecord>}</td></tr>
 * </table>
 *
 * <h2>Beans provided</h2>
 * <ul>
 *   <li>{@link SenderOptions} – configuration for {@code ReactiveKafkaProducerTemplate}</li>
 *   <li>{@link ReceiverOptions} – configuration for {@code ReactiveKafkaConsumerTemplate}</li>
 *   <li>{@link KafkaTemplate} – kept for backwards compatibility with manual consumer + Flink configs</li>
 *   <li>{@link ConcurrentKafkaListenerContainerFactory} – used by the manual blocking consumer fallback</li>
 * </ul>
 */
@Slf4j
@Configuration
public class ReactiveKafkaConfig {

    private final KafkaProperties kafkaProperties;

    public ReactiveKafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    // ─── Producer ────────────────────────────────────────────────────────────

    /**
     * Builds the {@link SenderOptions} used by {@code ReactiveKafkaProducerTemplate}.
     * {@code SenderOptions} is immutable – create once, share everywhere.
     *
     * @return configured {@link SenderOptions} for String key/value messages.
     */
    @Bean
    public SenderOptions<String, String> reactiveSenderOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        return SenderOptions.<String, String>create(props)
                .maxInFlight(1024);
    }

    /**
     * Classic {@link KafkaTemplate} still needed by manual consumer poll (which is blocking
     * and offloaded to {@code Schedulers.boundedElastic()}) and by Flink job configs.
     *
     * @return a standard {@link KafkaTemplate}.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(pf);
    }

    // ─── Consumer ────────────────────────────────────────────────────────────

    /**
     * Builds {@link ReceiverOptions} for reactive consumption via
     * {@code ReactiveKafkaConsumerTemplate}.
     * Subscription to the Item topic is set here; the template subscribes lazily.
     *
     * @return configured {@link ReceiverOptions}.
     */
    @Bean
    public ReceiverOptions<String, String> reactiveReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reactive-item-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // manual ack via ReceiverRecord
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        return ReceiverOptions.<String, String>create(props)
                .commitInterval(Duration.ZERO)   // commit on demand
                .commitBatchSize(0);             // commit on demand
    }

    /**
     * {@link ConsumerFactory} and {@link ConcurrentKafkaListenerContainerFactory} for the
     * manual (blocking) consumer used by {@code ReactiveItemConsumerService#manualConsumeBlocking}.
     * This is wrapped in {@code Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())}
     * so the Netty event loop is never blocked.
     *
     * @return configured factory.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualKafkaListenerContainerFactory() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-item-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000);

        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        return factory;
    }
}

