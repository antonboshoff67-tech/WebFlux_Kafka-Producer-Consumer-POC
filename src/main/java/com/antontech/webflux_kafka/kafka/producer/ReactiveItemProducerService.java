package com.antontech.webflux_kafka.kafka.producer;

import com.antontech.webflux_kafka.model.Item;
import com.antontech.webflux_kafka.prop.KafkaProperties;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Reactive Kafka producer service.
 *
 * <h2>reactor-kafka vs spring-kafka (blocking) pattern comparison</h2>
 * <pre>
 * // BLOCKING (imperative) - spring-kafka
 * kafkaTemplate.send(topic, key, value).get();  // blocks current thread
 *
 * // NON-BLOCKING (reactive) - reactor-kafka
 * kafkaSender.send(records)                     // returns Mono - no blocking
 *     .doOnNext(result -> log.info("sent"))
 *     .subscribe();
 * </pre>
 *
 * <h2>How KafkaSender works</h2>
 * <p>
 * {@link KafkaSender} wraps the standard Kafka producer in a reactive pipeline.
 * When you call {@link KafkaSender#send(org.reactivestreams.Publisher)}, it:
 * <ol>
 *   <li>Subscribes to the provided {@code Flux<SenderRecord>}</li>
 *   <li>Batches records and hands them to the internal Kafka producer</li>
 *   <li>Returns a {@code Flux<SenderResult>} that emits one result per sent record</li>
 *   <li>Never blocks the calling thread</li>
 * </ol>
 * </p>
 */
@Slf4j
@Service
public class ReactiveItemProducerService {

    private static final String ITEM_AUTO_GROUP = "item_group";
    private static final String ITEM_MANUAL_GROUP = "manual-item-group";

    private final KafkaSender<String, String> kafkaSender;
    private final KafkaProperties kafkaProperties;
    private final Gson gson;

    /**
     * @param senderOptions   reactor-kafka sender options (bootstrap servers, serializers etc.)
     * @param kafkaProperties shared Kafka topic / bootstrap config from {@code application.yml}.
     */
    public ReactiveItemProducerService(SenderOptions<String, String> senderOptions,
                                       KafkaProperties kafkaProperties) {
        this.kafkaSender = KafkaSender.create(senderOptions);
        this.kafkaProperties = kafkaProperties;
        this.gson = new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
                .create();
    }

    /**
     * Reactively publishes a list of items to Kafka.
     *
     * <p>The list is converted to a {@code Flux<SenderRecord>} and sent via
     * {@link KafkaSender#send}. The returned {@code Mono<Integer>} emits the total number
     * of successfully sent records when the send completes.
     *
     * <p><strong>Reactive flow:</strong>
     * <pre>
     * Flux.fromIterable(items)                  // wrap list in Flux
     *     .map(item -> toSenderRecord(item, i)) // convert each item to SenderRecord
     * kafkaSender.send(records)                 // send all – returns Flux<SenderResult>
     *     .doOnNext(log success/error)
     *     .count()                              // count successful sends
     *     .map(n -> "Sent " + n + " items")     // transform count to String result
     * </pre>
     *
     * @param items the items to publish; may be empty but must not be {@code null}.
     * @return a {@link Mono} emitting a human-readable summary of how many items were sent.
     */
    public Mono<String> sendItems(List<Item> items) {
        if (items == null || items.isEmpty()) {
            log.warn("sendItems called with empty list");
            return Mono.just("No items to send.");
        }

        int midpoint = items.size() / 2;

        // Convert each item to a SenderRecord (reactor-kafka's wrapper around ProducerRecord)
        Flux<SenderRecord<String, String, String>> senderRecords = Flux
                .range(0, items.size())
                .map(i -> {
                    Item item = items.get(i);
                    String groupId = i < midpoint ? ITEM_AUTO_GROUP : ITEM_MANUAL_GROUP;
                    String key = groupId + "_" + item.getItemId();
                    String value = gson.toJson(item);
                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>(kafkaProperties.getItemTopicName(), key, value);
                    // correlationMetadata (3rd arg) = key for logging
                    return SenderRecord.create(producerRecord, key);
                });

        return kafkaSender.send(senderRecords)
                .doOnNext((SenderResult<String> result) -> {
                    if (result.exception() != null) {
                        log.error("Failed to send item {}: {}", result.correlationMetadata(), result.exception().getMessage());
                    } else {
                        log.info("Sent item {} to partition {} offset {}",
                                result.correlationMetadata(),
                                result.recordMetadata().partition(),
                                result.recordMetadata().offset());
                    }
                })
                .filter(result -> result.exception() == null)
                .count()
                .map(successCount -> "Items sent to Kafka topic successfully! (sent=" + successCount + "/" + items.size() + ")")
                .onErrorResume(e -> {
                    log.error("Error in reactive Kafka send", e);
                    return Mono.just("Error sending items to Kafka: " + e.getMessage());
                });
    }
}

