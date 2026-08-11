# WebFlux Developer Guide

This guide explains how to run and understand the **WebFlux** version of the backend.

## 1) What this project demonstrates

- Reactive HTTP APIs with **Spring WebFlux**
- Reactive DB access with **R2DBC**
- Reactive Kafka producer/consumer with **reactor-kafka**
- Flink job orchestration from reactive controllers

## 2) Core WebFlux concepts used here

### 2.1 Mono and Flux

- `Mono<T>`: async publisher of **0 or 1** value
- `Flux<T>`: async publisher of **0..N** values

In this project:

- `Mono<String>` is used for status/result endpoints
- `Flux<Item>` is used for streaming endpoints (`/items/stream/v1`, `/consume-stream/v1`)

### 2.2 Non-blocking I/O

With WebFlux + Netty, request threads are not blocked while waiting on I/O.

- DB calls return publishers from R2DBC
- Kafka sends/receives return publishers from reactor-kafka
- Responses are emitted when data arrives

### 2.3 When blocking code still exists

Some components are inherently blocking:

- Manual Kafka `poll(...)` API
- Flink runtime job execution

How this project handles it:

- Manual consume is wrapped with `Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())`
- Flink jobs are launched via `CompletableFuture.runAsync(...)` and bridged with `Mono.fromFuture(...)`

This keeps the Netty event-loop non-blocking even when integrating blocking libraries.

## 3) Key code locations

- App entry: `src/main/java/com/antontech/webflux_kafka/WebFluxKafkaApplication.java`
- Reactive item APIs: `src/main/java/com/antontech/webflux_kafka/controller/ItemController.java`
- Reactive Kafka producer API: `src/main/java/com/antontech/webflux_kafka/controller/ItemProducerController.java`
- Reactive consumer APIs: `src/main/java/com/antontech/webflux_kafka/controller/ItemConsumerController.java`
- Reactive message routing APIs: `src/main/java/com/antontech/webflux_kafka/controller/MsgConsumerController.java`
- Flink APIs: `src/main/java/com/antontech/webflux_kafka/controller/FlinkJobController.java`
- R2DBC repository: `src/main/java/com/antontech/webflux_kafka/repos/ItemRepository.java`
- Kafka producer service: `src/main/java/com/antontech/webflux_kafka/kafka/producer/ReactiveItemProducerService.java`
- Kafka consumer service: `src/main/java/com/antontech/webflux_kafka/kafka/consumer/ReactiveItemConsumerService.java`

## 4) Run locally (developer machine)

### 4.1 Prerequisites

- Java 17+
- Maven 3.9+ (or use `mvnw.cmd`)
- MySQL running
- Kafka running

### 4.2 Start infrastructure using Docker (recommended)

```powershell
docker compose -f docker-compose.full.yml up -d
```

### 4.3 Build and run backend

```powershell
.\mvnw.cmd -q -DskipTests compile
.\mvnw.cmd spring-boot:run
```

Backend default URL:

- `http://localhost:8082`

Swagger UI:

- `http://localhost:8082/agent/swagger-ui.html`

## 5) Test core endpoints

```powershell
curl -X GET "http://localhost:8082/item-kafka/app/items/v1?page=0&size=15"
curl -X GET "http://localhost:8082/item-kafka/app/items/count/v1"
curl -X POST "http://localhost:8082/item-kafka/app/publish-items/v1"
curl -X GET "http://localhost:8082/item-kafka/consumer/consume-status/v1"
```

Reactive stream examples:

```powershell
curl -N "http://localhost:8082/item-kafka/app/items/stream/v1"
curl -N "http://localhost:8082/item-kafka/consumer/consume-stream/v1?limit=20"
```

## 6) Flink with WebFlux

### Can Flink itself be converted to WebFlux?

Not directly.

- Flink is its own streaming runtime and execution model
- Reactor (`Mono`/`Flux`) is a different programming model

What we do in this project:

- Keep Flink jobs as Flink jobs
- Expose reactive HTTP triggers around them
- Return immediately while jobs run asynchronously

## 7) Docker mode

```powershell
docker compose -f docker-compose.full.yml up -d --build
```

## 8) AWS EKS mode

Use these docs:

- `EKS_README.md`
- `AWS_README_START_HERE.md`
- `AWS_QUICKSTART_CHEATSHEET.md`
- `WEBFLUX_TUTORIAL_GUIDE.md`
- `k8s/` manifests

## 9) Useful config variables

- `ITEM_KAFKA_BOOTSTRAP_SERVERS`
- `ITEM_KAFKA_TOPIC`
- `ITEM_R2DBC_URL`
- `ITEM_MYSQL_URL`
- `ITEM_MYSQL_USERNAME`
- `ITEM_MYSQL_PASSWORD`
- `ITEM_MSSQL_URL`
- `ITEM_CORS_ALLOWED_ORIGINS`

## 10) Validation commands

```powershell
.\mvnw.cmd -q -DskipTests compile
.\mvnw.cmd -q test
```

