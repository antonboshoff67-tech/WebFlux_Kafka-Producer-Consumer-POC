# ---------------------------------------------------------------------------
# Multi-stage build for item-kafka-producer-poc.
# Stage 1 compiles the Spring Boot application with Maven.
# Stage 2 runs the resulting jar on a minimal JRE image.
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /build
# Cache dependencies separately from source changes for faster rebuilds.
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B clean package -DskipTests

FROM eclipse-temurin:11-jre-jammy
WORKDIR /app
COPY --from=build /build/target/item-kafka-producer-poc-*.jar app.jar
COPY src/main/resources/application.yml /app/application.yml

EXPOSE 8082

# All secrets/connection details are supplied via environment variables at
# `docker run` / docker-compose time - see DEVELOPER_GUIDE.md "Running in
# Docker" section for the full list (ITEM_MYSQL_URL, ITEM_KAFKA_BOOTSTRAP_SERVERS, etc).
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

