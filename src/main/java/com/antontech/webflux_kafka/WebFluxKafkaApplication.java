package com.antontech.webflux_kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Spring Boot WebFlux entry point for the Item Kafka Producer-Consumer POC.
 *
 * <h2>Why WebFlux?</h2>
 * <p>
 * This application uses <strong>Spring WebFlux</strong> (backed by Netty) instead of the
 * traditional Spring MVC (backed by Tomcat). The key difference:
 * <ul>
 *   <li><strong>Spring MVC (blocking)</strong>: each HTTP request consumes a thread from a thread-pool
 *       for its entire duration. The thread sits idle while waiting for DB or Kafka I/O.</li>
 *   <li><strong>Spring WebFlux (non-blocking)</strong>: Netty uses a small fixed number of event-loop
 *       threads. When I/O is needed (DB, Kafka, HTTP calls), the operation is registered as a callback
 *       and the thread is immediately freed to handle another request. Results are delivered via
 *       {@link reactor.core.publisher.Mono} (0-1 item) or {@link reactor.core.publisher.Flux} (0-N items).</li>
 * </ul>
 *
 * <h2>Reactive Stack</h2>
 * <ul>
 *   <li>HTTP layer: <strong>Spring WebFlux + Netty</strong></li>
 *   <li>DB layer: <strong>R2DBC</strong> (Reactive Relational DB Connectivity) via {@code io.asyncer:r2dbc-mysql}</li>
 *   <li>Kafka producer: <strong>reactor-kafka</strong> {@code ReactiveKafkaProducerTemplate}</li>
 *   <li>Kafka consumer: reactive {@code ReactiveKafkaConsumerTemplate} + blocking manual poll
 *       offloaded to {@code Schedulers.boundedElastic()}</li>
 *   <li>Flink jobs: Flink itself is NOT reactive (it runs in its own execution environment).
 *       HTTP endpoints trigger Flink using {@code Mono.fromFuture()} so the Netty event loop
 *       is never blocked.</li>
 * </ul>
 *
 * <h2>Package structure</h2>
 * <ul>
 *   <li>{@code configuration} – CORS, OpenAPI, R2DBC, ReactiveKafka beans</li>
 *   <li>{@code controller}    – WebFlux {@code @RestController}s returning {@code Mono}/{@code Flux}</li>
 *   <li>{@code model}         – R2DBC entity ({@code Item}) and DTOs</li>
 *   <li>{@code repos}         – {@code ReactiveCrudRepository} extending R2DBC</li>
 *   <li>{@code kafka}         – reactor-kafka producer and consumer services</li>
 *   <li>{@code flink}         – Flink jobs (same as imperative POC)</li>
 *   <li>{@code service}       – Reactive service layer (Mono/Flux return types)</li>
 *   <li>{@code prop}          – Type-safe {@code @ConfigurationProperties} beans</li>
 *   <li>{@code util}          – JWT token builder</li>
 * </ul>
 */
@SpringBootApplication
@EnableConfigurationProperties
public class WebFluxKafkaApplication {

    /**
     * Application entry point. Starts an embedded Netty server (default port 8083).
     *
     * @param args optional command-line arguments (passed through to Spring).
     */
    public static void main(String[] args) {
        SpringApplication.run(WebFluxKafkaApplication.class, args);
    }
}

