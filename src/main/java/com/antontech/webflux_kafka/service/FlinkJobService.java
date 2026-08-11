package com.antontech.webflux_kafka.service;

import com.antontech.webflux_kafka.flink.jobs.FlinkWordStreamDemoJob;
import com.antontech.webflux_kafka.flink.jobs.KafkaItemToMysqlJob;
import com.antontech.webflux_kafka.flink.jobs.MssqlItemToKafkaJob;
import com.antontech.webflux_kafka.prop.KafkaProperties;
import com.antontech.webflux_kafka.prop.MSSQLDataSourceProperties;
import com.antontech.webflux_kafka.prop.MySqlProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Reactive wrapper around the three Flink jobs.
 *
 * <h2>Why Flink is NOT reactive (and that's OK)</h2>
 * <p>
 * Apache Flink has its own execution model – it does NOT use Project Reactor or
 * Spring WebFlux. Flink jobs block the calling thread while they execute.
 * To integrate Flink with WebFlux without blocking the Netty event-loop thread,
 * we submit each job via {@link CompletableFuture#runAsync(Runnable)}, which runs
 * the job on the common ForkJoinPool. We then convert the {@code CompletableFuture}
 * to a {@link Mono} using {@code Mono.fromFuture()}.
 * </p>
 *
 * <h2>Pattern: Mono.fromFuture()</h2>
 * <pre>
 * // Step 1: Launch Flink job asynchronously (non-blocking from event-loop perspective)
 * CompletableFuture&lt;Void&gt; future = CompletableFuture.runAsync(() -> flinkJob.run());
 *
 * // Step 2: Wrap in Mono so it integrates with the WebFlux controller
 * return Mono.fromFuture(future)
 *            .thenReturn("Job started successfully.");
 * //   The HTTP response is sent immediately – the job runs in the background.
 * </pre>
 *
 * <h2>vs CompletableFuture alone (imperative POC)</h2>
 * <p>
 * The imperative version used {@code CompletableFuture.runAsync()} and then returned a
 * plain {@code ResponseEntity<String>} synchronously. The WebFlux version wraps the
 * same pattern in {@code Mono.fromFuture()} and returns {@code Mono<ResponseEntity<String>>},
 * which lets WebFlux handle the response emission non-blocking.
 * </p>
 */
@Slf4j
@Service
public class FlinkJobService {

    private final KafkaProperties kafkaProperties;
    private final MySqlProperties mySqlProperties;
    private final MSSQLDataSourceProperties mssqlDataSourceProperties;

    /** In-memory status map – same as imperative POC. */
    private final Map<String, JobStatus> jobStatuses = new HashMap<>();

    @Autowired
    public FlinkJobService(KafkaProperties kafkaProperties,
                           MySqlProperties mySqlProperties,
                           MSSQLDataSourceProperties mssqlDataSourceProperties) {
        this.kafkaProperties = kafkaProperties;
        this.mySqlProperties = mySqlProperties;
        this.mssqlDataSourceProperties = mssqlDataSourceProperties;
        StreamExecutionEnvironment.getExecutionEnvironment(); // warm up
    }

    /**
     * Triggers Flink Job 1 (MS SQL Server → Kafka) reactively.
     *
     * <p>Returns {@code Mono<String>} immediately (before the job finishes).
     * The job runs in background via {@link CompletableFuture#runAsync(Runnable)}.
     *
     * @return a {@link Mono} emitting a confirmation string once the job is <em>submitted</em>.
     */
    public Mono<String> runJob1() {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                MssqlItemToKafkaJob job = new MssqlItemToKafkaJob(
                        mssqlDataSourceProperties.getUrl(),
                        kafkaProperties.getBootstrapServers(),
                        kafkaProperties.getItemTopicName(),
                        mssqlDataSourceProperties.getSourceTableName());
                updateJobStatus("Flink Job 1", JobStatus.RUNNING);
                job.run();
                updateJobStatus("Flink Job 1", JobStatus.COMPLETED);
            } catch (Exception e) {
                updateJobStatus("Flink Job 1", JobStatus.FAILED);
                log.error("Error running MssqlItemToKafkaJob: {}", e.getMessage(), e);
            }
        });
        return Mono.fromFuture(future)
                .thenReturn("Flink Job 1 (MSSQL → Kafka) started successfully.")
                .onErrorReturn("Flink Job 1 encountered an error – check logs.");
    }

    /**
     * Triggers Flink Job 2 (Kafka → MySQL) reactively.
     *
     * @return a {@link Mono} emitting a confirmation string once the job is submitted.
     */
    public Mono<String> runJob2() {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                KafkaItemToMysqlJob job = new KafkaItemToMysqlJob(
                        kafkaProperties.getBootstrapServers(),
                        kafkaProperties.getItemTopicName(),
                        kafkaProperties.getConsumer().getGroupId(),
                        mySqlProperties.getJdbcUrl(),
                        mySqlProperties.getUsername(),
                        mySqlProperties.getPassword(),
                        mySqlProperties.getItemTableName());
                updateJobStatus("Flink Job 2", JobStatus.RUNNING);
                job.run();
                updateJobStatus("Flink Job 2", JobStatus.COMPLETED);
            } catch (Exception e) {
                updateJobStatus("Flink Job 2", JobStatus.FAILED);
                log.error("Error running KafkaItemToMysqlJob: {}", e.getMessage(), e);
            }
        });
        return Mono.fromFuture(future)
                .thenReturn("Flink Job 2 (Kafka → MySQL) started successfully.")
                .onErrorReturn("Flink Job 2 encountered an error – check logs.");
    }

    /**
     * Triggers the simple Flink demo job synchronously, wrapped in a {@link Mono}.
     * This job is lightweight and finishes quickly.
     *
     * @return a {@link Mono} emitting a result string.
     */
    public Mono<String> runSimpleJob() {
        return Mono.fromCallable(() -> {
                    try {
                        updateJobStatus("Flink Simple Job", JobStatus.RUNNING);
                        FlinkWordStreamDemoJob.main(new String[]{});
                        updateJobStatus("Flink Simple Job", JobStatus.COMPLETED);
                        return "Flink Simple Job executed successfully.";
                    } catch (Exception e) {
                        updateJobStatus("Flink Simple Job", JobStatus.FAILED);
                        log.error("Error running FlinkWordStreamDemoJob: {}", e.getMessage(), e);
                        return "Flink Simple Job failed: " + e.getMessage();
                    }
                });
        // Note: no subscribeOn() here – this demo job is synchronous and very fast.
        // For a long-running job use: .subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * Returns the last known {@link JobStatus} for a job.
     *
     * @param jobName e.g. {@code "Flink Job 1"}, {@code "Flink Job 2"} or {@code "Flink Simple Job"}.
     * @return a {@link Mono} emitting the status (non-blocking map lookup).
     */
    public Mono<JobStatus> getJobStatus(String jobName) {
        return Mono.just(jobStatuses.getOrDefault(jobName, JobStatus.PENDING));
    }

    private void updateJobStatus(String jobName, JobStatus status) {
        jobStatuses.put(jobName, status);
        log.info("Job {} status → {}", jobName, status);
    }
}

