package com.antontech.webflux_kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.r2dbc.url=r2dbc:h2:mem:///testdb;DB_CLOSE_DELAY=-1",
        "spring.r2dbc.username=sa",
        "spring.r2dbc.password=",
        "spring.datasource.url=jdbc:h2:mem:testdb",
        "spring.kafka.bootstrap-servers=localhost:9092",
        "jwt.private-key="
    })
@ActiveProfiles("test")
class WebFluxKafkaApplicationTests {

    @Test
    void contextLoads() {
        // Verifies the Spring application context starts successfully
    }
}

