package com.antontech.webflux_kafka.repos;

import com.antontech.webflux_kafka.model.Item;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive R2DBC repository for {@link Item}.
 *
 * <h2>ReactiveCrudRepository vs JpaRepository</h2>
 * <table border="1">
 *   <tr><th>JPA (blocking)</th><th>R2DBC (reactive)</th></tr>
 *   <tr><td>{@code List<Item> findAll()}</td><td>{@code Flux<Item> findAll()}</td></tr>
 *   <tr><td>{@code Optional<Item> findById(id)}</td><td>{@code Mono<Item> findById(id)}</td></tr>
 *   <tr><td>{@code long count()}</td><td>{@code Mono<Long> count()}</td></tr>
 *   <tr><td>{@code Page<Item> findAll(Pageable)}</td><td>No built-in; use skip/limit + count (see {@code ItemController})</td></tr>
 * </table>
 *
 * <h2>How reactive queries work</h2>
 * <p>
 * When you call {@code repository.findAll()}, R2DBC does NOT block the calling thread.
 * Instead it registers an asynchronous callback with the MySQL R2DBC driver and returns
 * a cold {@code Flux<Item>} immediately. The query only starts executing when a subscriber
 * subscribes to that Flux (typically the WebFlux HTTP pipeline, which subscribes on behalf
 * of the HTTP response).
 * </p>
 */
@Repository
public interface ItemRepository extends ReactiveCrudRepository<Item, String> {

    /**
     * Reactive equivalent of {@code findFirst100ByItemIdIsNotNull()}.
     * Returns a cold {@code Flux} that, when subscribed, streams up to 100 {@link Item} rows.
     *
     * @return a {@link Flux} emitting at most 100 items.
     */
    @Query("SELECT * FROM ITEM WHERE item_id IS NOT NULL LIMIT 100")
    Flux<Item> findFirst100ByItemIdIsNotNull();

    /**
     * Returns all items sorted by itemId ascending.
     * Used internally for pagination (skip/limit applied after).
     *
     * @return a {@link Flux} of all items ordered by {@code item_id}.
     */
    @Query("SELECT * FROM ITEM WHERE item_id IS NOT NULL ORDER BY item_id ASC")
    Flux<Item> findAllOrderByItemIdAsc();

    /**
     * Reactive count of rows with a non-null item_id (effectively the total row count).
     *
     * @return a {@link Mono} emitting the total row count.
     */
    @Query("SELECT COUNT(*) FROM ITEM WHERE item_id IS NOT NULL")
    Mono<Long> countByItemIdIsNotNull();
}

