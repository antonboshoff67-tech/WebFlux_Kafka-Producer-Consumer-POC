package com.antontech.webflux_kafka.controller;

import com.antontech.webflux_kafka.kafka.producer.ReactiveItemProducerService;
import com.antontech.webflux_kafka.repos.ItemRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Reactive Kafka producer controller.
 *
 * <h2>Reactive pipeline from HTTP request to Kafka send</h2>
 * <pre>
 * HTTP POST → WebFlux handler → itemRepository.findFirst100() [Flux, no blocking]
 *           → .collectList()  [collects to Mono&lt;List&lt;Item&gt;&gt;]
 *           → .flatMap(items → producerService.sendItems(items))  [reactor-kafka, no blocking]
 *           → Mono&lt;String&gt; result emitted back as HTTP response body
 * </pre>
 *
 * <p>Compare with the imperative version where:
 * <ul>
 *   <li>{@code itemRepository.findFirst100()} blocked until the DB responded.</li>
 *   <li>{@code kafkaTemplate.send().get()} blocked until Kafka acknowledged each message.</li>
 *   <li>The HTTP thread was held for the entire duration.</li>
 * </ul>
 * In this reactive version the HTTP thread is freed after assembling the pipeline.
 * Netty re-uses it for other requests while waiting.
 */
@Slf4j
@RestController
@RequestMapping(path = "item-kafka/app/")
@Tag(name = "Item Producer Controller (Reactive)", description = "Reactive Kafka Item producer – reactor-kafka")
public class ItemProducerController {

    private final ItemRepository itemRepository;
    private final ReactiveItemProducerService reactiveItemProducerService;

    public ItemProducerController(ItemRepository itemRepository,
                                  ReactiveItemProducerService reactiveItemProducerService) {
        this.itemRepository = itemRepository;
        this.reactiveItemProducerService = reactiveItemProducerService;
    }

    /**
     * Reads up to 100 Item rows from the source table reactively, then publishes them
     * to the Kafka topic using {@link ReactiveItemProducerService}.
     *
     * <p><strong>Reactive operator chain:</strong>
     * <ol>
     *   <li>{@code findFirst100ByItemIdIsNotNull()} → cold {@code Flux<Item>} (no DB access yet)</li>
     *   <li>{@code .collectList()} → {@code Mono<List<Item>>} (DB query runs on subscription)</li>
     *   <li>{@code .flatMap(items -> producerService.sendItems(items))} → delegates to reactor-kafka,
     *       returns {@code Mono<String>} of send result</li>
     *   <li>{@code .onErrorResume(...)} → error handling without exceptions propagating to Netty</li>
     * </ol>
     *
     * @return {@link Mono} emitting a plain-text confirmation or error message.
     */
    @Operation(summary = "Read items and publish to Kafka (reactive)",
               description = "Reads items from R2DBC and publishes them to Kafka via reactor-kafka. Fully non-blocking.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Items published"),
            @ApiResponse(responseCode = "500", description = "Service error")})
    @PostMapping(path = "publish-items/v1", produces = "text/plain")
    public Mono<String> createItemKafkaTopic() {
        return itemRepository.findFirst100ByItemIdIsNotNull()
                .collectList()
                .flatMap(items -> {
                    if (items.isEmpty()) {
                        log.warn("No items found to publish");
                        return Mono.just("No items found to publish.");
                    }
                    log.debug("Publishing {} items to Kafka", items.size());
                    return reactiveItemProducerService.sendItems(items);
                })
                .onErrorResume(e -> {
                    log.error("Error in createItemKafkaTopic", e);
                    return Mono.just("Error occurred publishing items to Kafka: " + e.getMessage());
                });
    }
}

