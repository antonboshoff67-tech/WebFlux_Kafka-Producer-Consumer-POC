package com.antontech.webflux_kafka.controller;

import com.antontech.webflux_kafka.model.Item;
import com.antontech.webflux_kafka.model.PageResult;
import com.antontech.webflux_kafka.repos.ItemRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive Item REST controller.
 *
 * <h2>WebFlux Controller Pattern</h2>
 * <p>
 * The class looks almost identical to a Spring MVC controller – same {@code @RestController},
 * {@code @GetMapping} annotations – but <strong>every method returns a reactive type</strong>
 * ({@link Mono} or {@link Flux}) instead of a plain Java object.
 * </p>
 * <p>
 * Spring WebFlux subscribes to the returned publisher on your behalf and streams the
 * result back to the HTTP client. The Netty event-loop thread is never blocked waiting
 * for the database.
 * </p>
 *
 * <h2>Comparison: MVC vs WebFlux</h2>
 * <pre>
 * // Spring MVC (blocking – thread waits for DB)
 * {@code @GetMapping} Page&lt;Item&gt; listItems() {
 *     return itemRepository.findAll(pageable);  // thread blocked here
 * }
 *
 * // Spring WebFlux (non-blocking – thread freed immediately)
 * {@code @GetMapping} Mono&lt;PageResult&lt;Item&gt;&gt; listItems() {
 *     return itemRepository.findAllOrderByItemIdAsc()  // returns Flux immediately
 *         .skip(page * size)
 *         .take(size)
 *         .collectList()                               // collect to Mono<List>
 *         .zipWith(itemRepository.count())             // combine with count
 *         .map((tuple) -> new PageResult&lt;&gt;(...));     // build response
 * }
 * </pre>
 *
 * <h2>Pagination without Page&lt;T&gt;</h2>
 * <p>
 * R2DBC's {@link org.springframework.data.repository.reactive.ReactiveCrudRepository}
 * does not expose {@code Page<T>} because that would require blocking on a count query
 * and a data query simultaneously. Instead we use {@code Mono.zip()} to run both
 * queries in parallel and combine the results into a {@link PageResult}.
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("item-kafka/app/")
@Tag(name = "Item Controller (Reactive)", description = "Paginated item grid – R2DBC + WebFlux")
public class ItemController {

    private final ItemRepository itemRepository;

    public ItemController(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
    }

    /**
     * Returns a single page of {@link Item} rows reactively.
     *
     * <p><strong>Reactive operator breakdown:</strong>
     * <ul>
     *   <li>{@code findAllOrderByItemIdAsc()} – cold {@code Flux<Item>} (query not executed yet)</li>
     *   <li>{@code .skip(page * size)} – skip rows for previous pages (processed in-memory after DB streams them)</li>
     *   <li>{@code .take(size)} – take only the requested page size</li>
     *   <li>{@code .collectList()} – collect stream into {@code Mono<List<Item>>}</li>
     *   <li>{@code .zipWith(count())} – execute count query in parallel, combine results</li>
     *   <li>{@code .map()} – build {@link PageResult} response object</li>
     * </ul>
     *
     * @param page zero-based page index (default {@code 0}).
     * @param size number of records per page (default {@code 15}).
     * @return {@link Mono} emitting a {@link PageResult} of items.
     */
    @Operation(summary = "List items (paginated, reactive)",
               description = "Returns a reactive paginated result of Item rows. Uses R2DBC + Mono.zip() for non-blocking count+data.")
    @GetMapping(path = "items/v1", produces = "application/json")
    public Mono<PageResult<Item>> listItems(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "15") int size) {

        Mono<Long> countMono = itemRepository.countByItemIdIsNotNull();

        Mono<java.util.List<Item>> itemsMono = itemRepository
                .findAllOrderByItemIdAsc()
                .skip((long) page * size)
                .take(size)
                .collectList();

        // Mono.zip() subscribes to BOTH monos concurrently and emits when both complete.
        // This is a key reactive pattern for parallel queries.
        return Mono.zip(itemsMono, countMono)
                .map(tuple -> {
                    java.util.List<Item> items = tuple.getT1();
                    long total = tuple.getT2();
                    log.debug("Returning items page={} size={} totalElements={}", page, size, total);
                    return new PageResult<>(items, total, page, size);
                });
    }

    /**
     * Returns the total count of Item rows as a reactive {@link Mono}.
     *
     * @return {@link Mono} emitting the total number of items.
     */
    @Operation(summary = "Count items (reactive)", description = "Returns total Item row count via R2DBC non-blocking query.")
    @GetMapping(path = "items/count/v1", produces = "application/json")
    public Mono<Long> countItems() {
        return itemRepository.count();
    }

    /**
     * Streams ALL items as a {@link Flux} – Server-Sent Events (SSE) compatible.
     *
     * <p>This endpoint demonstrates reactive streaming: items are emitted one-by-one
     * as the DB cursor moves forward, without buffering the entire result set in memory.
     * Useful for large datasets.
     *
     * @return a {@link Flux} of all {@link Item} objects.
     */
    @Operation(summary = "Stream all items (Flux / SSE)",
               description = "Streams all items from R2DBC as a reactive Flux. Back-pressure is applied automatically.")
    @GetMapping(path = "items/stream/v1", produces = "application/json")
    public Flux<Item> streamAllItems() {
        log.debug("Streaming all items reactively");
        return itemRepository.findAllOrderByItemIdAsc();
    }
}

