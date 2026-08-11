package com.antontech.webflux_kafka.controller;

import com.antontech.webflux_kafka.kafka.consumer.ReactiveItemConsumerService;
import com.antontech.webflux_kafka.model.Item;
import com.antontech.webflux_kafka.model.ManualConsumeRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive Kafka consumer controller.
 *
 * <h2>New endpoint: reactive stream</h2>
 * <p>
 * In addition to the manual consume endpoint (same as the imperative POC, but returning
 * {@code Mono<String>} instead of {@code String}), this controller adds a new streaming
 * endpoint {@code GET /consume-stream/v1} that returns a {@code Flux<Item>} backed by
 * reactor-kafka's {@link reactor.kafka.receiver.KafkaReceiver}.
 * </p>
 *
 * <h2>Server-Sent Events (SSE)</h2>
 * <p>
 * The {@code /consume-stream/v1} endpoint uses {@code MediaType.TEXT_EVENT_STREAM_VALUE},
 * which causes WebFlux to keep the HTTP connection open and push each {@link Item} as a
 * Server-Sent Event as soon as it is emitted from Kafka. The browser/client receives
 * items in real time without polling.
 * </p>
 * <pre>
 * curl -N http://localhost:8083/item-kafka/consumer/consume-stream/v1
 * </pre>
 */
@Slf4j
@RestController
@RequestMapping(path = "item-kafka/consumer/")
@Tag(name = "Item Consumer Controller (Reactive)", description = "Reactive Kafka consumer – manual poll + reactive stream")
public class ItemConsumerController {

    private final ReactiveItemConsumerService reactiveItemConsumerService;
    private static final String ITEM_AUTO_GROUP = "item_group";
    private static final String ITEM_MANUAL_GROUP = "manual-item-group";

    public ItemConsumerController(ReactiveItemConsumerService reactiveItemConsumerService) {
        this.reactiveItemConsumerService = reactiveItemConsumerService;
    }

    /**
     * Returns the reactive stream status.
     *
     * @return {@link Mono} emitting a status string.
     */
    @Operation(summary = "Consumer status (reactive)")
    @GetMapping(path = "consume-status/v1", produces = "application/json")
    public Mono<String> checkConsumerStatus() {
        return Mono.just(reactiveItemConsumerService.isRunning()
                ? "Reactive consumer stream is active."
                : "No active reactive consumer stream.");
    }

    /**
     * One-shot manual consume – wraps blocking Kafka poll in {@code Schedulers.boundedElastic()}.
     *
     * <p>Returns {@code Mono<String>} so the HTTP response is non-blocking.
     * The actual poll runs on a separate thread pool (not the Netty event-loop).
     *
     * @param request the group id to use for this poll.
     * @return {@link Mono} emitting a summary of consumed records.
     */
    @Operation(summary = "Manual consume (reactive wrapped)",
               description = "One-shot blocking Kafka poll wrapped in Mono.fromCallable().subscribeOn(boundedElastic()). Non-blocking from HTTP perspective.")
    @PostMapping(path = "manual-consume/v1", produces = "text/plain")
    public Mono<String> manualConsumeItem(@RequestBody ManualConsumeRequest request) {
        if (ITEM_AUTO_GROUP.equalsIgnoreCase(request.getGroupId())
                || ITEM_MANUAL_GROUP.equalsIgnoreCase(request.getGroupId())) {
            return reactiveItemConsumerService.manualConsume(request.getGroupId());
        }
        return Mono.just("Incorrect consumer group. Use item_group or manual-item-group.");
    }

    /**
     * Reactive streaming consume via reactor-kafka {@link reactor.kafka.receiver.KafkaReceiver}.
     *
     * <p>Returns a {@code Flux<Item>} streamed as Server-Sent Events (SSE). Each item
     * emitted by Kafka is forwarded to the HTTP client as it arrives, demonstrating
     * true reactive streaming end-to-end.
     *
     * <p>Use {@code ?limit=N} to cap the number of items streamed before the Flux completes.
     * Without a limit, the stream stays open until the client disconnects or Kafka has
     * no more messages.
     *
     * @param limit optional: max number of items to stream (0 = unlimited).
     * @return a {@link Flux} of {@link Item} objects.
     */
    @Operation(summary = "Stream items from Kafka (SSE / Flux)",
               description = "Reactive Kafka consumer using reactor-kafka. Items streamed as Server-Sent Events. ?limit=N to cap.")
    @GetMapping(path = "consume-stream/v1", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Item> streamItemsFromKafka(@RequestParam(defaultValue = "0") long limit) {
        Flux<Item> stream = reactiveItemConsumerService.streamItems();
        if (limit > 0) {
            stream = stream.take(limit);
        }
        return stream
                .doOnSubscribe(s -> log.info("Reactive Kafka stream subscribed (limit={})", limit))
                .doOnComplete(() -> log.info("Reactive Kafka stream completed"))
                .doOnError(e -> log.error("Reactive Kafka stream error", e));
    }
}

