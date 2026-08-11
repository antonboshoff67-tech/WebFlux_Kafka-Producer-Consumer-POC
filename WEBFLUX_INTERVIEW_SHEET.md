# WebFlux Interview Sheet

This document is the **interview-focused study sheet** for the `WebFlux_Kafka-Producer-Consumer-POC` project.

Use it when you need to answer:

- What is WebFlux?
- Why is this project reactive?
- How do `Mono` and `Flux` work here?
- How does this compare with a Spring Boot + Hibernate + REST design?
- How do Kafka, R2DBC, and Flink fit together?

---

## 1) The one-sentence interview answer

> This project demonstrates how to build a Spring Boot backend that keeps the web layer reactive with WebFlux, uses R2DBC for non-blocking database access, uses reactor-kafka for reactive Kafka send/receive, and bridges blocking workloads like manual Kafka polling and Flink jobs safely so the Netty event-loop is never blocked.

If the interviewer asks for the short version, say that.

---

## 2) What the project proves in practical terms

This repository is **not** a toy WebFlux demo. It shows how to combine:

- **Reactive HTTP** with Spring WebFlux
- **Reactive DB access** with R2DBC
- **Reactive Kafka** with `reactor-kafka`
- **Blocking legacy integration** wrapped safely using `boundedElastic`
- **Long-running batch/stream jobs** bridged into reactive endpoints with `Mono.fromFuture()`

That means you can say:

- The HTTP layer is reactive.
- The database layer is reactive.
- Kafka producer/consumer paths are reactive.
- Flink is not reactive itself, but it is integrated in a reactive-friendly way.

---

## 3) Core interview questions and model answers

### 3.1 What is WebFlux?

**Answer:**

Spring WebFlux is Spring’s reactive web framework. Instead of binding one blocking thread to each request, it uses a small number of Netty event-loop threads and asynchronous publishers (`Mono` and `Flux`) to represent results.

**Why that matters here:**

In this repo, controllers return `Mono<T>` or `Flux<T>` so requests can be served without blocking the request thread while the DB or Kafka is doing I/O.

---

### 3.2 Why use WebFlux instead of Spring MVC?

**Answer:**

Use WebFlux when the application is I/O-heavy and you want better scalability under concurrent waiting: DB calls, Kafka calls, HTTP calls, streaming results, and long-running workflows.

**In this project:**

- item list queries are reactive,
- Kafka publishing is reactive,
- Kafka streaming is reactive,
- legacy blocking calls are isolated and wrapped,
- Flink jobs are submitted asynchronously.

---

### 3.3 What is the difference between `Mono` and `Flux`?

**Answer:**

- `Mono<T>` = zero or one result
- `Flux<T>` = zero to many results

**In this repo:**

- `Mono<Long>` is used for counts
- `Mono<PageResult<Item>>` is used for paginated results
- `Flux<Item>` is used for item streaming
- `Flux<Item>` is used for Kafka streaming

---

### 3.4 Why is this project not purely reactive everywhere?

**Answer:**

Because some technologies here are inherently blocking or use their own runtime model.

Examples:

- Kafka manual polling uses `KafkaConsumer.poll(...)`
- Flink runs its own jobs and is not based on Reactor

The project shows the correct way to integrate those systems without blocking WebFlux threads.

---

## 4) How to explain the code class by class

---

## 4.1 `WebFluxKafkaApplication.java`

**Purpose:** application entry point.

Key idea from the code:

```java
@SpringBootApplication
@EnableConfigurationProperties
public class WebFluxKafkaApplication {
    public static void main(String[] args) {
        SpringApplication.run(WebFluxKafkaApplication.class, args);
    }
}
```

### How to explain it in an interview

- `@SpringBootApplication` starts the Spring Boot app.
- `@EnableConfigurationProperties` allows strongly typed config classes such as `KafkaProperties`, `MySqlProperties`, and `SSLProperties`.
- `main()` starts the reactive Netty server.

### Why this is different from a Hibernate REST app

A traditional Spring MVC app typically starts a servlet container like Tomcat and handles requests in a blocking request thread. WebFlux starts a reactive stack and works with publishers instead of blocking calls.

---

## 4.2 `configuration/CorsConfig.java`

**Purpose:** let the browser-based React UI call the backend.

This is the important pattern:

```java
@Bean
public CorsWebFilter corsWebFilter() {
    CorsConfiguration config = new CorsConfiguration();
    config.setAllowedOrigins(origins);
    config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));
    config.setAllowedHeaders(List.of("*"));
    config.setAllowCredentials(true);

    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", config);
    return new CorsWebFilter(source);
}
```

### How to explain it

- Browsers enforce Same-Origin Policy.
- The React frontend usually runs on a different origin from the backend.
- `CorsWebFilter` tells the browser which origins and methods are allowed.

### WebFlux vs Spring MVC

- MVC often uses `WebMvcConfigurer`.
- WebFlux uses `CorsWebFilter`.

---

## 4.3 `configuration/ReactiveKafkaConfig.java`

**Purpose:** configure reactive Kafka sender and receiver support.

The important idea is that this repo uses **reactor-kafka** for reactive messaging.

### Key interview point

If asked, say:

> The producer and consumer use Reactor Kafka so that Kafka send and receive operations can participate in the reactive pipeline instead of blocking the caller.

### Why it matters

This is the bridge that makes the Kafka parts feel “WebFlux-native.”

---

## 4.4 `repos/ItemRepository.java`

**Purpose:** reactive repository for the `Item` entity.

Key code pattern:

```java
public interface ItemRepository extends ReactiveCrudRepository<Item, String> {

    @Query("SELECT * FROM ITEM WHERE item_id IS NOT NULL LIMIT 100")
    Flux<Item> findFirst100ByItemIdIsNotNull();

    @Query("SELECT * FROM ITEM WHERE item_id IS NOT NULL ORDER BY item_id ASC")
    Flux<Item> findAllOrderByItemIdAsc();

    @Query("SELECT COUNT(*) FROM ITEM WHERE item_id IS NOT NULL")
    Mono<Long> countByItemIdIsNotNull();
}
```

### How to explain it

- `ReactiveCrudRepository` is the R2DBC equivalent of `JpaRepository`.
- It returns `Flux` and `Mono`, not `List` and `Page`.
- The query is executed reactively when subscribed.

### Difference from Spring Data JPA

In JPA you would usually return blocking results like:

- `List<Item>`
- `Optional<Item>`
- `Page<Item>`

In WebFlux/R2DBC, you return:

- `Flux<Item>`
- `Mono<Item>`
- `Mono<Long>`

---

## 4.5 `controller/ItemController.java`

**Purpose:** reactive item grid and count endpoints.

This is one of the best classes to explain in an interview.

### Key code snippet

```java
Mono<Long> countMono = itemRepository.countByItemIdIsNotNull();

Mono<List<Item>> itemsMono = itemRepository
        .findAllOrderByItemIdAsc()
        .skip((long) page * size)
        .take(size)
        .collectList();

return Mono.zip(itemsMono, countMono)
        .map(tuple -> {
            List<Item> items = tuple.getT1();
            long total = tuple.getT2();
            return new PageResult<>(items, total, page, size);
        });
```

### How to explain it line by line

- `countMono` triggers a count query reactively.
- `itemsMono` streams all ordered rows, skips earlier pages, takes the requested page size, and collects the result into a list.
- `Mono.zip(itemsMono, countMono)` runs the two reactive queries together and waits for both.
- The `map(...)` builds a custom page response object.

### Why we used `PageResult` instead of `Page<Item>`

Reactive repositories do not naturally return Spring Data JPA `Page<T>` without introducing blocking behavior. `PageResult` is a lightweight custom wrapper that gives the UI everything it needs:

- content
- total count
- total pages
- current page
- page size

### How this differs from a Hibernate REST endpoint

A Hibernate version would probably look like:

```java
@GetMapping("/items")
public Page<Item> listItems(Pageable pageable) {
    return itemRepository.findAll(pageable);
}
```

That is simpler, but it is blocking and tied to JPA/Hibernate.

---

## 4.6 `controller/ItemProducerController.java`

**Purpose:** read items reactively and publish them to Kafka.

### Key code snippet

```java
return itemRepository.findFirst100ByItemIdIsNotNull()
        .collectList()
        .flatMap(items -> {
            if (items.isEmpty()) {
                return Mono.just("No items found to publish.");
            }
            return reactiveItemProducerService.sendItems(items);
        })
        .onErrorResume(e -> Mono.just("Error occurred publishing items to Kafka: " + e.getMessage()));
```

### How to explain it

- The item query returns a `Flux<Item>`.
- `collectList()` gathers the flux into one list.
- `flatMap(...)` passes the list to the reactive Kafka producer service.
- `onErrorResume(...)` gives a reactive fallback if something fails.

### WebFlux lesson here

Instead of doing work immediately and blocking, you compose publishers step by step.

---

## 4.7 `kafka/producer/ReactiveItemProducerService.java`

**Purpose:** push `Item` records to Kafka reactively.

### Key code snippet

```java
Flux<SenderRecord<String, String, String>> senderRecords = Flux
        .range(0, items.size())
        .map(i -> {
            Item item = items.get(i);
            String groupId = i < midpoint ? ITEM_AUTO_GROUP : ITEM_MANUAL_GROUP;
            String key = groupId + "_" + item.getItemId();
            String value = gson.toJson(item);
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(kafkaProperties.getItemTopicName(), key, value);
            return SenderRecord.create(producerRecord, key);
        });

return kafkaSender.send(senderRecords)
        .doOnNext(result -> { ... })
        .filter(result -> result.exception() == null)
        .count()
        .map(successCount -> "Items sent to Kafka topic successfully!");
```

### How to explain it

- `Flux.range(...)` creates a reactive sequence over list indexes.
- Each item becomes a Kafka `ProducerRecord`.
- `SenderRecord.create(...)` adds correlation metadata.
- `kafkaSender.send(...)` emits results reactively.
- `count()` gives the number of successful sends.

### Why this is important

This is the reactive replacement for:

```java
kafkaTemplate.send(...).get();
```

The old style blocks. The new style does not.

---

## 4.8 `controller/ItemConsumerController.java`

**Purpose:** show both manual consume and live Kafka stream consumption.

### Key code snippet: manual consume

```java
@PostMapping(path = "manual-consume/v1", produces = "text/plain")
public Mono<String> manualConsumeItem(@RequestBody ManualConsumeRequest request) {
    if (ITEM_AUTO_GROUP.equalsIgnoreCase(request.getGroupId())
            || ITEM_MANUAL_GROUP.equalsIgnoreCase(request.getGroupId())) {
        return reactiveItemConsumerService.manualConsume(request.getGroupId());
    }
    return Mono.just("Incorrect consumer group. Use item_group or manual-item-group.");
}
```

### How to explain it

- The controller validates the consumer group.
- It delegates to the service.
- It returns a `Mono<String>` so the endpoint is reactive from the HTTP perspective.

### Key code snippet: live stream

```java
@GetMapping(path = "consume-stream/v1", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<Item> streamItemsFromKafka(@RequestParam(defaultValue = "0") long limit) {
    Flux<Item> stream = reactiveItemConsumerService.streamItems();
    if (limit > 0) {
        stream = stream.take(limit);
    }
    return stream;
}
```

### How to explain it

- `Flux<Item>` means many items over time.
- `TEXT_EVENT_STREAM_VALUE` makes it Server-Sent Events (SSE).
- The browser can receive events as Kafka messages arrive.

### Why this is a good WebFlux example

This is the cleanest “reactive streaming” feature in the project.

---

## 4.9 `kafka/consumer/ReactiveItemConsumerService.java`

**Purpose:** demonstrate two consumption styles.

### Pattern 1: reactive Kafka stream

```java
return KafkaReceiver.create(options)
        .receive()
        .flatMap(record -> {
            Item item = gson.fromJson(record.value(), Item.class);
            record.receiverOffset().acknowledge();
            return Mono.just(item);
        });
```

### How to explain it

- `KafkaReceiver` gives you a reactive stream of Kafka records.
- Each record is deserialized into `Item`.
- `acknowledge()` marks the offset as processed.
- The method returns `Flux<Item>`.

### Pattern 2: wrapping blocking Kafka poll safely

```java
return Mono.fromCallable(() -> doBlockingManualPoll(groupId))
        .subscribeOn(Schedulers.boundedElastic());
```

### How to explain it

- `KafkaConsumer.poll()` is blocking.
- `Mono.fromCallable(...)` delays execution until subscription.
- `subscribeOn(Schedulers.boundedElastic())` moves the blocking work away from the Netty event-loop.

### Interview takeaway

Use this class to show that you understand how to integrate blocking legacy code into a reactive application safely.

---

## 4.10 `service/impl/ReactiveMsgRoutingServiceImpl.java`

**Purpose:** prepare message forwarding and JWT generation reactively.

### Key code snippet

```java
return Mono.fromCallable(this::createJWT)
        .subscribeOn(Schedulers.boundedElastic())
        .doOnNext(jwt -> {
            String requestId = UUID.randomUUID().toString();
            log.info("Prepared request for gateway endpoint {} (requestId={})", baseUrl, requestId);
        })
        .then();
```

### How to explain it

- JWT creation is CPU work, so we wrap it in `Mono.fromCallable(...)`.
- `boundedElastic()` prevents CPU/blocking work from tying up the event-loop.
- The pipeline ends with `.then()` because the endpoint only needs completion, not a returned value.

### Why this is better than a blocking MVC service

In a blocking service you might generate the token and call `RestTemplate` directly. In WebFlux you compose the work reactively and keep the request thread free.

### Important note

This repository prepares the outbound gateway request. It shows how auth and forwarding would fit into a reactive pipeline.

---

## 4.11 `service/FlinkJobService.java`

**Purpose:** trigger Flink jobs and keep their status.

### Key code snippet

```java
CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
    try {
        MssqlItemToKafkaJob job = new MssqlItemToKafkaJob(...);
        updateJobStatus("Flink Job 1", JobStatus.RUNNING);
        job.run();
        updateJobStatus("Flink Job 1", JobStatus.COMPLETED);
    } catch (Exception e) {
        updateJobStatus("Flink Job 1", JobStatus.FAILED);
    }
});

return Mono.fromFuture(future)
        .thenReturn("Flink Job 1 (MSSQL → Kafka) started successfully.");
```

### How to explain it

- `CompletableFuture.runAsync(...)` runs the Flink job in a background thread.
- `updateJobStatus(...)` records lifecycle state.
- `Mono.fromFuture(...)` bridges the async job into WebFlux.
- The HTTP response returns right away.

### Why this is important

Flink itself is not a reactive library. The project shows the proper integration pattern: keep Flink as Flink, but expose it through a reactive HTTP API.

---

## 4.12 `controller/FlinkJobController.java`

**Purpose:** expose Flink job triggers and status through WebFlux endpoints.

### Key code snippet

```java
@PostMapping("/start-job1")
public Mono<ResponseEntity<String>> triggerFlinkJob1() {
    return flinkJobService.runJob1()
            .map(ResponseEntity::ok)
            .onErrorResume(e -> Mono.just(ResponseEntity.internalServerError().body("Error: " + e.getMessage())));
}
```

### How to explain it

- The controller returns `Mono<ResponseEntity<String>>`.
- It delegates to the service.
- It maps the service result into an HTTP response.
- It uses `onErrorResume(...)` to convert reactive errors into a fallback response.

### Why the job status endpoint matters

```java
@GetMapping("/job-status")
public Mono<ResponseEntity<JobStatus>> getJobStatus(@RequestParam String jobName) { ... }
```

The frontend can poll this endpoint to learn whether the job is `PENDING`, `RUNNING`, `COMPLETED`, or `FAILED`.

---

## 4.13 `model/PageResult.java`

**Purpose:** a simple custom page wrapper for reactive pagination.

This class is useful because reactive repositories do not naturally hand you a blocking `Page<T>` like JPA does.

**How to explain it:**

- It contains the list of items.
- It contains paging metadata.
- It is built after combining count and list publishers.

---

## 5) WebFlux vs Spring Boot Hibernate REST: interview comparison table

| Topic | Spring MVC + Hibernate/JPA | WebFlux project here |
|---|---|---|
| Request handling | One servlet thread handles the request | Netty event-loop handles the request |
| DB access | JDBC blocks the thread | R2DBC returns `Mono`/`Flux` |
| Pagination | `Page<Item>` from JPA | `Mono.zip(count, list)` + `PageResult<Item>` |
| Kafka send | `KafkaTemplate.send().get()` blocks | `KafkaSender.send(...)` is reactive |
| Kafka consume | `@KafkaListener` or blocking `poll()` | `KafkaReceiver.receive()` or wrapped blocking poll |
| Long-running jobs | Often a fire-and-forget controller or background thread | `CompletableFuture` bridged with `Mono.fromFuture()` |
| Streaming to UI | Polling or websockets added later | Natural `Flux<Item>` / SSE support |
| Thread usage | More threads under load | Fewer threads, less blocking |

---

## 6) If the interviewer asks “What part of this repo is most WebFlux-like?”

Say this:

> The most WebFlux-like part is `ItemController.listItems()` because it composes two reactive database publishers using `Mono.zip()` and returns a `Mono<PageResult<Item>>` instead of blocking for a JPA `Page<Item>`.

Then show the code:

```java
Mono<Long> countMono = itemRepository.countByItemIdIsNotNull();
Mono<List<Item>> itemsMono = itemRepository.findAllOrderByItemIdAsc()
        .skip((long) page * size)
        .take(size)
        .collectList();

return Mono.zip(itemsMono, countMono)
        .map(tuple -> new PageResult<>(tuple.getT1(), tuple.getT2(), page, size));
```

That answer demonstrates that you understand reactive composition, not just the theory.

---

## 7) If the interviewer asks “How do you handle blocking code in a WebFlux app?”

Say this:

> I keep blocking work off the Netty event-loop by wrapping it in `Mono.fromCallable(...)` and running it on `Schedulers.boundedElastic()`. I do that for manual Kafka polling and for the service code that launches Flink jobs.

Then point to:

- `ReactiveItemConsumerService.manualConsume(...)`
- `ReactiveMsgRoutingServiceImpl.processSentMsgRequest(...)`
- `FlinkJobService.runJob1()` / `runJob2()`

---

## 8) If the interviewer asks “How would this code look in a Spring MVC + Hibernate app?”

A simple answer:

- `ItemRepository` would be `JpaRepository<Item, String>`
- `ItemController` would return `Page<Item>` or `List<Item>`
- Kafka send logic would likely call `KafkaTemplate.send(...).get()`
- Blocking database access would happen on the request thread
- Flink jobs would still need background execution, but the controller would likely be more imperative

A useful comparison snippet is:

**Hibernate/JPA style**
```java
@GetMapping("/items")
public Page<Item> listItems(Pageable pageable) {
    return itemRepository.findAll(pageable);
}
```

**WebFlux style in this repo**
```java
@GetMapping("/items/v1")
public Mono<PageResult<Item>> listItems(...) {
    return Mono.zip(itemsMono, countMono)
            .map(...);
}
```

---

## 9) How to describe the end-to-end flow

### Producer flow

1. Frontend calls `POST /item-kafka/app/publish-items/v1`
2. `ItemController` reads items reactively
3. `ReactiveItemProducerService` sends items to Kafka
4. Kafka topic receives the JSON messages

### Consumer flow

1. Frontend calls `GET /item-kafka/consumer/consume-stream/v1`
2. `ReactiveItemConsumerService` subscribes to Kafka
3. Items stream back to the UI as SSE

### Flink flow

1. Frontend calls `POST /flink/start-job1`
2. `FlinkJobController` triggers `FlinkJobService`
3. Flink runs in the background
4. Frontend polls `GET /flink/job-status`

---

## 10) Common interview sound bites

Use these short phrases in answers:

- “Mono is for one async result, Flux is for many.”
- “WebFlux frees the event-loop thread while I/O is in flight.”
- “R2DBC replaces blocking JDBC with a reactive database driver.”
- “reactor-kafka lets Kafka send and receive participate in the reactive pipeline.”
- “boundedElastic is my escape hatch for blocking integrations.”
- “Flink is not reactive itself, so I bridge it with CompletableFuture and Mono.fromFuture().”
- “Mono.zip lets me combine parallel reactive queries for pagination.”

---

## 11) What to study first in this repo

If you have limited time, study these files first:

1. `WEBFLUX_INTERVIEW_SHEET.md`  ← this file
2. `README.md`
3. `DEVELOPER_GUIDE.md`
4. `WEBFLUX_TUTORIAL_GUIDE.md`
5. `ARCHITECTURE.md`
6. `API_DOCUMENTATION.md`

Then open these code files in this order:

1. `src/main/java/com/antontech/webflux_kafka/controller/ItemController.java`
2. `src/main/java/com/antontech/webflux_kafka/repos/ItemRepository.java`
3. `src/main/java/com/antontech/webflux_kafka/controller/ItemProducerController.java`
4. `src/main/java/com/antontech/webflux_kafka/kafka/producer/ReactiveItemProducerService.java`
5. `src/main/java/com/antontech/webflux_kafka/controller/ItemConsumerController.java`
6. `src/main/java/com/antontech/webflux_kafka/kafka/consumer/ReactiveItemConsumerService.java`
7. `src/main/java/com/antontech/webflux_kafka/service/FlinkJobService.java`
8. `src/main/java/com/antontech/webflux_kafka/controller/FlinkJobController.java`

---

## 12) Final interview summary

The cleanest summary of this project is:

> This backend is a real-world example of a reactive Spring Boot application that uses WebFlux for HTTP, R2DBC for non-blocking data access, reactor-kafka for reactive messaging, and safe async bridges for blocking integrations such as manual Kafka polling and Flink jobs.

If you can explain the `ItemController`, `ReactiveItemProducerService`, `ReactiveItemConsumerService`, and `FlinkJobService` classes clearly, you understand the heart of this project.

