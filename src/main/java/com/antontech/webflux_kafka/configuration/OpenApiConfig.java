package com.antontech.webflux_kafka.configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI / Swagger UI configuration for the WebFlux application.
 *
 * <h2>WebFlux Swagger note</h2>
 * <p>
 * The {@code springdoc-openapi-starter-webflux-ui} library (v2.x) automatically
 * scans {@code @RestController} classes and generates an OpenAPI spec. The Swagger
 * UI is served at the path configured in {@code application.yml}:
 * {@code springdoc.swagger-ui.path = /agent/swagger-ui.html}.
 * </p>
 * <p>
 * There is NO additional configuration needed for WebFlux vs MVC; SpringDoc
 * detects which stack is in use and adjusts accordingly.
 * </p>
 */
@Configuration
public class OpenApiConfig {

    /**
     * Customises the API metadata shown in Swagger UI.
     *
     * @return a configured {@link OpenAPI} bean.
     */
    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("WebFlux Kafka Producer-Consumer POC")
                        .version("1.0.0")
                        .description("""
                                Reactive Spring Boot WebFlux backend for the Item Kafka POC.
                                Uses Netty (non-blocking) + R2DBC (reactive DB) + reactor-kafka (reactive Kafka).
                                All endpoints return Mono<T> or Flux<T>.
                                Flink jobs are triggered asynchronously via Mono.fromFuture().
                                See DEVELOPER_GUIDE.md for full setup instructions.
                                """)
                        .contact(new Contact()
                                .name("Anton Boshoff")
                                .url("https://github.com/antonboshoff67-tech"))
                        .license(new License().name("MIT")));
    }
}

