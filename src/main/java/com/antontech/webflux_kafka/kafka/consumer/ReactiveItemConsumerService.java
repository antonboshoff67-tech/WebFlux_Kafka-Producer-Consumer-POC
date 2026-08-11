package com.antontech.webflux_kafka.kafka.consumer;

import com.antontech.webflux_kafka.model.Item;
import com.antontech.webflux_kafka.prop.KafkaProperties;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reactive Kafka consumer service demonstrating two consumption patterns:
 *
 * <h2>Pattern 1: Fully Reactive (reactor-kafka)</h2>
 * <p>
 * {@link #streamItems()} returns a cold {@code Flux<Item>} backed by
 * {@link KafkaReceiver}. When subscribed, it continuously polls Kafka and
 * emits deserialized {@link Item} objects. Manual acknowledgement is used
 * ({@code ReceiverRecord.receiverOffset().acknowledge()}) – no thread blocks.
 * </p>
 * <pre>
 * // Reactive pattern
 * kafkaReceiver.receive()           // Flux<ReceiverRecord<K,V>>
 *     .map(r -> deserialize(r))     // Flux<Item>
 *     .doOnNext(item -> ack(r))     // acknowledge after processing
 *     .subscribe();
 * </pre>
 *
 * <h2>Pattern 2: Blocking poll wrapped in boundedElastic()</h2>
 * <p>
 * {@link #manualConsume(String)} wraps the traditional blocking
 * {@link KafkaConsumer#poll(Duration)} call inside
 * {@code Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())}.
 * The blocking work runs on a dedicated thread pool, freeing the Netty
 * event-loop thread immediately. The caller sees a non-blocking {@code Mono<String>}.
 * </p>
 * <pre>
 * // Wrapping blocking code reactively
 * return Mono.fromCallable(() -> doBlockingKafkaPoll(groupId))
 *            .subscribeOn(Schedulers.boundedElastic());
 * //                      ^-- runs on separate thread pool, not event-loop
 * </pre>
 *
 * <h2>Which pattern to use?</h2>
 * <ul>
 *   <li>Use Pattern 1 (reactor-kafka) when you want continuous, back-pressured streaming from Kafka.</li>
 *   <li>Use Pattern 2 (bounded elastic) when you need legacy Kafka consumer behaviour (e.g. manual group id) without blocking Netty.</li>
 * </ul>
 */
@Slf4j
@Service
public class ReactiveItemConsumerService {

    private static final long POLL_TIMEOUT_MILLIS = 30_000;

    private final KafkaProperties kafkaProperties;
    private final ReceiverOptions<String, String> receiverOptions;
    private final Gson gson;
    private final AtomicBoolean reactiveStreamActive = new AtomicBoolean(false);

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> manualKafkaListenerContainerFactory;

    public ReactiveItemConsumerService(KafkaProperties kafkaProperties,
                                       ReceiverOptions<String, String> receiverOptions) {
        this.kafkaProperties = kafkaProperties;
        this.receiverOptions = receiverOptions;
        this.gson = new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
                .create();
    }

    /**
     * Returns a cold, non-blocking {@code Flux<Item>} that continuously receives
     * records from the configured Kafka topic using reactor-kafka's {@link KafkaReceiver}.
     *
     * <p>Each record is acknowledged after successful deserialization.
     * Back-pressure is automatically handled by the reactive pipeline.
     *
     * <p><strong>How to limit:</strong> the caller can apply {@code .take(n)} or
     * {@code .limitRequest(n)} to consume only N items then cancel.
     *
     * @return a {@link Flux} of {@link Item} objects from the Kafka topic.
     */
    public Flux<Item> streamItems() {
        ReceiverOptions<String, String> options = receiverOptions
                .subscription(Collections.singletonList(kafkaProperties.getItemTopicName()));

        reactiveStreamActive.set(true);
        return KafkaReceiver.create(options)
                .receive()
                .doOnNext(record -> log.debug("Received record key={} partition={} offset={}",
                        record.key(), record.receiverOffset().topicPartition(), record.receiverOffset().offset()))
                .flatMap(record -> {
                    try {
                        Item item = gson.fromJson(record.value(), Item.class);
                        if (item != null) {
                            record.receiverOffset().acknowledge(); // non-blocking ack
                            log.info("Reactively consumed item {}", item.getItemId());
                            return Mono.just(item);
                        }
                    } catch (Exception e) {
                        log.error("Error deserializing Kafka record: {}", e.getMessage());
                    }
                    return Mono.empty();
                })
                .doFinally(sig -> reactiveStreamActive.set(false));
    }

    /**
     * One-shot manual poll wrapped in a bounded-elastic scheduler so the Netty
     * event-loop thread is never blocked.
     *
     * <p>Internally uses a traditional blocking {@link KafkaConsumer}, but by wrapping it
     * in {@code Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())} the
     * blocking work is offloaded to a separate thread pool and the caller sees a
     * non-blocking {@link Mono}.
     *
     * @param groupId the consumer group id to use ({@code item_group} or {@code manual-item-group}).
     * @return a {@link Mono} emitting a human-readable summary of consumed records.
     */
    public Mono<String> manualConsume(String groupId) {
        return Mono.fromCallable(() -> doBlockingManualPoll(groupId))
                   .subscribeOn(Schedulers.boundedElastic());
                   // ^^^ Key: offload blocking work to dedicated thread pool.
                   //     The event-loop thread that invoked this method is freed
                   //     immediately and can serve other requests.
    }

    /**
     * The actual blocking Kafka poll – runs on {@code Schedulers.boundedElastic()}.
     *
     * @param groupId consumer group id.
     * @return summary string.
     */
    private String doBlockingManualPoll(String groupId) {
        List<Item> processedItems = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                manualKafkaListenerContainerFactory.getConsumerFactory().getConfigurationProperties())) {

            consumer.subscribe(Collections.singletonList(kafkaProperties.getItemTopicName()));
            long startTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - startTime < POLL_TIMEOUT_MILLIS) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Item item = gson.fromJson(record.value(), Item.class);
                        if (item != null) {
                            processedItems.add(item);
                            log.info("Manually consumed item {} from group {}", item.getItemId(), groupId);
                        }
                        consumer.commitSync();
                    } catch (Exception e) {
                        log.error("Error processing record key={}", record.key(), e);
                    }
                }
            }
            return processedItems.isEmpty() ? "No records found." :
                    "Manually consumed " + processedItems.size() + " items from group: " + groupId;
        } catch (Exception e) {
            log.error("Error in manual consume", e);
            return "Error occurred during manual consumption: " + e.getMessage();
        }
    }

    /**
     * @return {@code true} if a reactive stream is currently active (subscribed), {@code false} otherwise.
     */
    public boolean isRunning() {
        return reactiveStreamActive.get();
    }
}

