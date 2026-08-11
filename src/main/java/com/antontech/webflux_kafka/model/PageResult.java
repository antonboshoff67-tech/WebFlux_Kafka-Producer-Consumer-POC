package com.antontech.webflux_kafka.model;

import java.util.List;

/**
 * Reactive paginated result – replaces Spring Data JPA's {@code Page<T>}.
 *
 * <p>R2DBC's {@code ReactiveCrudRepository} does not expose a {@code Page<T>}
 * return type because there is no blocking {@code count()} + {@code findAll(Pageable)}
 * pair available in a single reactive call. Instead, this wrapper is assembled
 * from two separate reactive queries:
 * <ol>
 *   <li>{@code repository.count()} → {@code Mono<Long>} for totalElements</li>
 *   <li>{@code repository.findAll()} with skip/limit → {@code Flux<T>} collected to list</li>
 * </ol>
 * Both are combined via {@code Mono.zip()}, which subscribes to both publishers
 * in parallel and emits a result when both complete – demonstrating reactive
 * composition patterns.
 *
 * @param <T> the element type (e.g. {@link Item}).
 */
public class PageResult<T> {

    private final List<T> content;
    private final long totalElements;
    private final int totalPages;
    private final int page;
    private final int size;

    /**
     * @param content       the items on this page.
     * @param totalElements total rows across all pages.
     * @param page          zero-based page index.
     * @param size          requested page size.
     */
    public PageResult(List<T> content, long totalElements, int page, int size) {
        this.content = content;
        this.totalElements = totalElements;
        this.page = page;
        this.size = size;
        this.totalPages = size == 0 ? 1 : (int) Math.ceil((double) totalElements / size);
    }

    public List<T> getContent() { return content; }
    public long getTotalElements() { return totalElements; }
    public int getTotalPages() { return totalPages; }
    public int getPage() { return page; }
    public int getSize() { return size; }

    public boolean isFirst() { return page == 0; }
    public boolean isLast() { return page >= totalPages - 1; }
    public boolean hasNext() { return page < totalPages - 1; }
    public boolean hasPrevious() { return page > 0; }
}

