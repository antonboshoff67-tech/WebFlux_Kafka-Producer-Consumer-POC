# API Documentation - WebFlux Kafka Producer-Consumer POC

Base URL (local): `http://localhost:8082`

Swagger UI: `http://localhost:8082/agent/swagger-ui.html`

## 1) Item APIs (`/item-kafka/app`)

### 1.1 List paginated items

`GET /item-kafka/app/items/v1?page=0&size=15`

```powershell
curl -X GET "http://localhost:8082/item-kafka/app/items/v1?page=0&size=15"
```

### 1.2 Count items

`GET /item-kafka/app/items/count/v1`

```powershell
curl -X GET "http://localhost:8082/item-kafka/app/items/count/v1"
```

### 1.3 Stream all items (reactive Flux)

`GET /item-kafka/app/items/stream/v1`

```powershell
curl -N "http://localhost:8082/item-kafka/app/items/stream/v1"
```

### 1.4 Publish first 100 items to Kafka

`POST /item-kafka/app/publish-items/v1`

```powershell
curl -X POST "http://localhost:8082/item-kafka/app/publish-items/v1"
```

### 1.5 Prepare outbound gateway request (JWT flow)

`POST /item-kafka/app/send-items/v1`

```powershell
curl -X POST "http://localhost:8082/item-kafka/app/send-items/v1" ^
  -H "Content-Type: application/json" ^
  -d "{\"msg\":\"hello from webflux\"}"
```

### 1.6 Simulate consume-side request

`GET /item-kafka/app/consume-items/v1`

```powershell
curl -X GET "http://localhost:8082/item-kafka/app/consume-items/v1" ^
  -H "Content-Type: application/json" ^
  -d "{\"msg\":\"consume test\"}"
```

## 2) Consumer APIs (`/item-kafka/consumer`)

### 2.1 Consumer stream status

`GET /item-kafka/consumer/consume-status/v1`

```powershell
curl -X GET "http://localhost:8082/item-kafka/consumer/consume-status/v1"
```

### 2.2 Manual consume

`POST /item-kafka/consumer/manual-consume/v1`

```powershell
curl -X POST "http://localhost:8082/item-kafka/consumer/manual-consume/v1" ^
  -H "Content-Type: application/json" ^
  -d "{\"groupId\":\"manual-item-group\",\"message\":\"manual trigger\"}"
```

### 2.3 Reactive Kafka stream (SSE)

`GET /item-kafka/consumer/consume-stream/v1?limit=20`

```powershell
curl -N "http://localhost:8082/item-kafka/consumer/consume-stream/v1?limit=20"
```

## 3) Flink APIs (`/flink`)

### 3.1 Start Job 1 (MSSQL -> Kafka)

`POST /flink/start-job1`

```powershell
curl -X POST "http://localhost:8082/flink/start-job1"
```

### 3.2 Start Job 2 (Kafka -> MySQL)

`POST /flink/start-job2`

```powershell
curl -X POST "http://localhost:8082/flink/start-job2"
```

### 3.3 Start simple Flink demo job

`POST /flink/start-simple-job`

```powershell
curl -X POST "http://localhost:8082/flink/start-simple-job"
```

### 3.4 Check Flink job status

`GET /flink/job-status?jobName=Flink%20Job%201`

```powershell
curl -X GET "http://localhost:8082/flink/job-status?jobName=Flink%20Job%201"
```

## 4) Reactive behavior notes

- Controllers return `Mono<T>` or `Flux<T>`.
- R2DBC calls are non-blocking.
- Reactor Kafka send/receive are non-blocking.
- Manual consume and Flink execution are blocking under the hood, but wrapped in reactive bridges (`boundedElastic` / `Mono.fromFuture`) so HTTP threads are not blocked.
