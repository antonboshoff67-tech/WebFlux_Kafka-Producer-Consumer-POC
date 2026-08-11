# WebFlux Tutorial Guide

This guide is the **study companion** for the `WebFlux_Kafka-Producer-Consumer-POC` project.

Use it when you want to learn:

- how Spring WebFlux works,
- how `Mono` and `Flux` are used in practice,
- how this project differs from a traditional Spring Boot + Hibernate + REST application,
- how to run the project manually, with Docker, and on AWS EKS,
- and how each class in this codebase fits together.

---

## 1) Quick start: how to run the project from scratch

### 1.1 Manual setup

#### Prerequisites

- Java 17+
- Maven 3.9+
- MySQL 8+
- Kafka broker (local or Docker)
- Optional: SQL Server for the MSSQL -> Kafka Flink job

#### Steps

```powershell
# 1) Clone the repo
cd C:\Workspaces
git clone https://github.com/antonboshoff67-tech/WebFlux_Kafka-Producer-Consumer-POC.git
cd WebFlux_Kafka-Producer-Consumer-POC

# 2) Install / verify tools
java -version
mvn -version

# 3) Start MySQL and Kafka using Docker (easy path)
docker compose -f docker-compose.full.yml up -d

# 4) Seed the database
mysql -u root -p < sql-scripts\02_mysql_item_source_seed_200.sql
mysql -u root -p < sql-scripts\03_mysql_item_sink_and_consumed_tables.sql

# 5) Build and run the backend
.\mvnw.cmd -q -DskipTests compile
.\mvnw.cmd spring-boot:run
```

Backend default URL:

- `http://localhost:8082`

Swagger UI:

- `http://localhost:8082/agent/swagger-ui.html`

#### Manual test commands

```powershell
curl -X GET "http://localhost:8082/item-kafka/app/items/v1?page=0&size=15"
curl -X GET "http://localhost:8082/item-kafka/app/items/count/v1"
curl -X POST "http://localhost:8082/item-kafka/app/publish-items/v1"
curl -X GET "http://localhost:8082/item-kafka/consumer/consume-status/v1"
```

Reactive streaming demos:

```powershell
curl -N "http://localhost:8082/item-kafka/app/items/stream/v1"
curl -N "http://localhost:8082/item-kafka/consumer/consume-stream/v1?limit=20"
```

---

### 1.2 Docker / Docker Compose setup

```powershell
# Start full stack: MySQL + Kafka + backend
docker compose -f docker-compose.full.yml up -d --build

# View logs
docker compose -f docker-compose.full.yml logs -f

# Stop everything
docker compose -f docker-compose.full.yml down
```

What runs in Docker mode:

- MySQL source database
- Kafka broker in KRaft mode
- Reactive Spring Boot backend
- Optional manual topic creation / validation commands in `KAFKA_SETUP.md`

---

### 1.3 AWS EKS setup

Use these docs:

- `EKS_README.md`
- `AWS_README_START_HERE.md`
- `AWS_QUICKSTART_CHEATSHEET.md`

Basic flow:

1. Create EKS cluster
2. Create ECR repositories
3. Build and push backend image
4. Apply Kubernetes manifests from `k8s/`
5. Point DNS to ALB ingress
6. Validate backend and frontend endpoints

---

## 2) WebFlux fundamentals used in this project

### 2.1 `Mono`

`Mono<T>` means:

- async result,
- zero or one item,
- completes once.

Examples in this project:

- `Mono<String>` for simple status responses
- `Mono<Long>` for item counts
- `Mono<PageResult<Item>>` for reactive pagination
- `Mono<ResponseEntity<String>>` for controller responses

### 2.2 `Flux`

`Flux<T>` means:

- async stream,
- zero to many items,
- can keep emitting over time.

Examples:

- `Flux<Item>` from `GET /item-kafka/app/items/stream/v1`
- `Flux<Item>` from `GET /item-kafka/consumer/consume-stream/v1`
- `Flux<SenderRecord<...>>` inside the Kafka producer pipeline

### 2.3 Why WebFlux is different from Spring MVC

In a normal Spring MVC + Hibernate app:

- a request comes in,
- a thread is assigned,
- the thread blocks while JDBC waits for the DB,
- the response is returned later.

In WebFlux:

- Netty event-loop threads are small and efficient,
- the handler returns a reactive publisher immediately,
- the thread is freed while I/O happens,
- the response is written later when the publisher emits.

This is why WebFlux is a good fit for I/O-heavy systems.

### 2.4 Backpressure

Backpressure means the consumer can slow the producer down.

That matters in this project because:

- Kafka streams can be continuous,
- DB writes may be slower than reads,
- the UI may want to consume in chunks.

### 2.5 `boundedElastic`

Some work here is still blocking:

- legacy Kafka `poll()`
- Flink job execution

We wrap those in `Schedulers.boundedElastic()` so they do not block Netty event-loop threads.

### 2.6 `Mono.fromFuture()`

Flink jobs are bridged into WebFlux via `Mono.fromFuture(...)`.

That means:

- the job is launched on a background thread,
- the controller returns immediately,
- the UI can poll `job-status`.

---

## 3) Class-by-class walkthrough

The notes below are written in a **line-by-line style** grouped by line ranges so you can study the code file by file.

### 3.1 `WebFluxKafkaApplication.java`

**Purpose:** application entry point.

- Lines 1-3: package/imports.
- Lines 5-16: class-level Javadoc explains why this project uses WebFlux.
- Lines 18-19: `@SpringBootApplication` makes this the Spring Boot main app.
- Lines 20-21: `@EnableConfigurationProperties` lets `@ConfigurationProperties` beans work.
- Lines 23-26: `main()` starts the application.

**Why this differs from a Hibernate REST app:**

- MVC apps usually start a servlet container (Tomcat) and use blocking request threads.
- This app starts a reactive Netty server.

---

### 3.2 `configuration/CorsConfig.java`

**Purpose:** allow the React frontend to call the backend in the browser.

- Package/imports: WebFlux CORS filter classes.
- Javadoc: explains browser same-origin policy.
- `@Configuration`: this class defines beans.
- `@Value("${cors.allowed-origins}")`: reads allowed frontend URLs.
- `corsWebFilter()`: creates a `CorsWebFilter` bean.
- `allowedMethods`, `allowedHeaders`, `allowCredentials`: tell the browser what is allowed.

**WebFlux difference:**

- In Spring MVC you would often use `WebMvcConfigurer`.
- In WebFlux you use `CorsWebFilter`.

---

### 3.3 `configuration/OpenApiConfig.java`

**Purpose:** Swagger/OpenAPI metadata.

- `customOpenAPI()`: sets title, version, description, contact and license.
- SpringDoc scans WebFlux controllers and builds Swagger UI automatically.

**Why it matters:**

- A new developer can inspect endpoints without reading every controller first.

---

### 3.4 `configuration/ReactiveKafkaConfig.java`

**Purpose:** central Kafka configuration for reactive producer/consumer support.

- `reactiveSenderOptions()`: configures producer bootstrap server and serializers.
- `kafkaTemplate()`: retained for compatibility where imperative Kafka support is still needed.
- `reactiveReceiverOptions()`: configures reactive Kafka consumer options.
- `manualKafkaListenerContainerFactory()`: supports legacy blocking manual poll code.

**WebFlux vs older Kafka style:**

- Older style: `KafkaTemplate.send(...).get()` blocks.
- Reactive style: `KafkaSender` / `KafkaReceiver` emit `Mono` / `Flux`.

---

### 3.5 `model/Item.java`

**Purpose:** the main domain object mapped to the `ITEM` table.

This file is large because the business domain has many columns. Important ideas:

- Top lines define the table mapping.
- `@Id` marks the primary key.
- `@Column` maps Java fields to DB columns.
- `@SerializedName` ensures JSON names stay stable.
- Getters/setters are explicit for clarity and predictable serialization.

**Important reactive note:**

- In a JPA app, this class would use `jakarta.persistence.*` and Hibernate.
- In this project, R2DBC uses Spring Data relational mapping instead.

**Why not Lombok `@Data` here?**

- Explicit methods make it easier for beginners to follow and debug.
- It also avoids hidden behavior in a large tutorial project.

---

### 3.6 `model/PageResult.java`

**Purpose:** reactive replacement for Spring Data JPA `Page<T>`.

- Stores page content, total count, page number and size.
- Calculates total pages and first/last flags.
- Built from `Mono.zip(count, content)` in the controller.

**Why this exists:**

- JPA has a built-in `Page<T>` abstraction.
- Reactive repositories do not usually return `Page<T>` directly without blocking.

---

### 3.7 DTOs: `ServiceRequest.java`, `ManualConsumeRequest.java`, `JwtResponse.java`

#### `ServiceRequest.java`

- Holds the simple message body used by the gateway test flow.

#### `ManualConsumeRequest.java`

- Holds the consumer group id used for manual poll requests.

#### `JwtResponse.java`

- Wraps the JWT token string.

**Why DTOs matter:**

- They keep request and response payloads separate from the database entity.
- That is a clean Web/API design pattern.

---

### 3.8 Property classes in `prop/`

#### `KafkaProperties.java`

- Holds Kafka bootstrap servers and topic name.
- Also holds producer/consumer sub-properties.

#### `MySqlProperties.java`

- Holds MySQL sink JDBC URL, username, password, and table name.

#### `MSSQLDataSourceProperties.java`

- Holds SQL Server source JDBC settings for Flink Job 1.

#### `SSLProperties.java`

- Holds keystore/truststore settings for optional mTLS.

**Why these classes exist:**

- They avoid hardcoded secrets in code.
- They map YAML to strongly-typed Java fields.
- They make deployment via environment variables much easier.

---

### 3.9 `repos/ItemRepository.java`

**Purpose:** reactive repository for `Item`.

- Extends `ReactiveCrudRepository<Item, String>`.
- `findFirst100ByItemIdIsNotNull()` returns a `Flux<Item>`.
- `findAllOrderByItemIdAsc()` returns a stream of all items.
- `countByItemIdIsNotNull()` returns a `Mono<Long>`.

**WebFlux difference:**

- JPA repository methods usually return `List`, `Optional`, `Page`.
- Reactive repository methods return `Flux` and `Mono`.

---

### 3.10 `kafka/producer/ReactiveItemProducerService.java`

**Purpose:** publish `Item` records to Kafka reactively.

Key ideas:

- Builds JSON using Gson.
- Converts each `Item` into a `SenderRecord`.
- Uses `KafkaSender.send(...)`.
- Counts successful sends via a reactive pipeline.

**Why this is different from `KafkaTemplate`:**

- `KafkaTemplate.send(...).get()` blocks until the send completes.
- `KafkaSender.send(...)` returns a reactive stream and does not block.

---

### 3.11 `kafka/consumer/ReactiveItemConsumerService.java`

**Purpose:** demonstrate both reactive Kafka streaming and safe wrapping of blocking poll logic.

It has two important modes:

1. **Reactive stream mode**
   - Uses `KafkaReceiver.create(...)`
   - Returns `Flux<Item>`
   - Suitable for SSE/streaming UI updates

2. **Manual poll mode**
   - Uses blocking `KafkaConsumer.poll(...)`
   - Wrapped with `Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())`

**Why this matters:**

- You can use the reactive model where it fits.
- You can still integrate older blocking code safely.

---

### 3.12 `service/ReactiveMsgRoutingService.java` and `service/impl/ReactiveMsgRoutingServiceImpl.java`

**Purpose:** prepare outbound gateway requests and process received messages.

- `processReceivedMsgRequest(...)` logs the received payload.
- `processSentMsgRequest(...)` builds a JWT and prepares the forwarding request.

**WebFlux style:**

- The service methods return `Mono<Void>`.
- Controllers chain these methods into HTTP responses.

**Difference from imperative Spring MVC:**

- MVC would often do the work directly and return `ResponseEntity`.
- WebFlux composes async steps first, then emits the response when ready.

---

### 3.13 `service/FlinkJobService.java`

**Purpose:** launch and track Flink jobs.

- Uses `CompletableFuture.runAsync(...)` to run jobs in the background.
- Stores job status in a map.
- Returns `Mono<String>` or `Mono<JobStatus>` to the controller.

**Important note:**

- Flink itself is not converted into Reactor.
- The project bridges Flink into WebFlux safely.

---

### 3.14 `service/JobStatus.java`

**Purpose:** enum for `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`.

This is what the UI can poll to show job progress.

---

### 3.15 Controllers

#### `ItemController.java`

- `listItems()`: returns a reactive page result.
- `countItems()`: returns a reactive count.
- `streamAllItems()`: streams all items via `Flux<Item>`.

**What to learn here:**

- `Mono.zip(...)` combines DB count + data query.
- `skip(...)` and `take(...)` support pagination.

#### `ItemProducerController.java`

- Reads items reactively.
- Sends them to Kafka.
- Returns a `Mono<String>` confirmation.

#### `ItemConsumerController.java`

- Shows status.
- Wraps manual consume in reactive style.
- Streams Kafka messages as SSE.

#### `MsgConsumerController.java`

- Demonstrates JWT/gateway flow.
- Returns `Mono<ResponseEntity<String>>`.

#### `FlinkJobController.java`

- Starts the Flink jobs.
- Exposes job status.
- Returns `Mono<ResponseEntity<...>>`.

---

### 3.16 `util/JwtTokenUtil.java`

**Purpose:** build signed JWT tokens.

Study this to understand:

- private key loading,
- issuer/expiry settings,
- how the gateway flow gets authenticated.

---

### 3.17 `exceptions/ConsumerException.java`

**Purpose:** domain-specific exception for message routing/consumer errors.

This keeps errors readable and domain-focused.

---

## 4) WebFlux vs Spring Boot Hibernate REST: what is different?

| Topic | Hibernate / JPA REST | WebFlux project here |
|---|---|---|
| HTTP model | `ResponseEntity<T>` | `Mono<ResponseEntity<T>>` / `Flux<T>` |
| DB access | JDBC, blocking | R2DBC, non-blocking |
| Kafka send | `KafkaTemplate.send().get()` | `KafkaSender.send(...)` |
| Kafka consume | `@KafkaListener` or blocking poll | `KafkaReceiver` stream or wrapped blocking poll |
| Threading | request thread waits | event-loop thread is freed quickly |
| Long jobs | controller often blocks or delegates badly | async bridge with `Mono.fromFuture(...)` |
| Streaming UI | often polling | natural `Flux` / SSE support |

---

## 5) What each endpoint means for the UI

- **Immediate response endpoints**: can be awaited directly with loading spinners.
- **Async Flink endpoints**: start the job, then poll `job-status`.
- **Streaming endpoints**: the UI can use SSE or long-lived fetch subscriptions.

---

## 6) Study path recommendation

If you are new to WebFlux, read in this order:

1. `README.md`
2. `DEVELOPER_GUIDE.md`
3. `WEBFLUX_TUTORIAL_GUIDE.md`
4. `API_DOCUMENTATION.md`
5. `ARCHITECTURE.md`

Then open these files in order:

1. `WebFluxKafkaApplication.java`
2. `ItemRepository.java`
3. `ItemController.java`
4. `ReactiveItemProducerService.java`
5. `ReactiveItemConsumerService.java`
6. `FlinkJobService.java`
7. `FlinkJobController.java`

---

## 7) Summary

This project is a practical example of a migration from a classic blocking Spring Boot design to a reactive design.

The key lesson is:

- use `Mono` for one async result,
- use `Flux` for many async results,
- keep blocking work off the Netty event-loop,
- and bridge legacy systems safely when you cannot convert them fully.

