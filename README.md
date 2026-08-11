# WebFlux Kafka Producer / Consumer / Flink POC

This repository is the **reactive Spring Boot WebFlux** version of the Item Kafka POC.

It keeps the same business flow as the original backend, but moves the HTTP and database layers to a reactive stack:

1. **Read items reactively** from MySQL via **R2DBC**.
2. **Publish items reactively** to Kafka via **reactor-kafka**.
3. **Consume items** either as a reactive stream (`Flux<Item>`) or via manual poll.
4. **Trigger Flink jobs** for MSSQL -> Kafka and Kafka -> MySQL pipelines.

## What changed in this WebFlux version

- Spring Boot upgraded to **3.x**
- Java upgraded to **17**
- `spring-boot-starter-web` -> `spring-boot-starter-webflux`
- JPA repository -> `ReactiveCrudRepository` (R2DBC)
- Blocking Kafka producer calls -> reactive sender (`KafkaSender` / reactor-kafka)
- Controllers return `Mono<T>` / `Flux<T>`

## Run modes

You can run this backend in the same 3 modes as the original project:

1. **Developer machine mode** (local Java + local Kafka/MySQL)
2. **Docker mode** (Docker Compose)
3. **AWS EKS mode** (Kubernetes manifests + ECR/EKS docs)

## Start here

- `DEVELOPER_GUIDE.md` - full WebFlux-focused setup + Mono/Flux explanation + non-blocking I/O guide
- `API_DOCUMENTATION.md` - endpoint reference and curl examples
- `ARCHITECTURE.md` - end-to-end architecture and flow

## Key endpoints (same base flow, reactive implementations)

Base path: `item-kafka/app/`

- `POST /item-kafka/app/publish-items/v1`
- `POST /item-kafka/app/send-items/v1`
- `GET /item-kafka/app/consume-items/v1`
- `GET /item-kafka/app/items/v1?page=0&size=15`
- `GET /item-kafka/app/items/count/v1`
- `GET /item-kafka/app/items/stream/v1`

Consumer base path: `item-kafka/consumer/`

- `GET /item-kafka/consumer/consume-status/v1`
- `POST /item-kafka/consumer/manual-consume/v1`
- `GET /item-kafka/consumer/consume-stream/v1?limit=20`

Flink base path: `/flink`

- `POST /flink/start-job1`
- `POST /flink/start-job2`
- `POST /flink/start-simple-job`
- `GET /flink/job-status?jobName=Flink%20Job%201`

Swagger UI:

- `/agent/swagger-ui.html`

## Build and test

```powershell
.\mvnw.cmd -q -DskipTests compile
.\mvnw.cmd -q test
```

## Docker quick run

```powershell
docker compose -f docker-compose.full.yml up -d --build
```

## Repository docs

- `SETUP_GUIDE.md`
- `KAFKA_SETUP.md`
- `DATABASE_SETUP.md`
- `EKS_README.md`
- `AWS_QUICKSTART_CHEATSHEET.md`
- `AWS_README_START_HERE.md`
- `AWS_DEPLOYMENT_SUMMARY.md`
- `Dockerfile`
- `docker-compose.full.yml`
- `docker-compose.kafka.yml`
- `k8s/`
- `sql-scripts/`
