package com.antontech.webflux_kafka.controller;

import com.antontech.webflux_kafka.service.FlinkJobService;
import com.antontech.webflux_kafka.service.JobStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Reactive Flink job controller.
 *
 * <h2>Flink + WebFlux integration pattern</h2>
 * <p>
 * Flink runs in its own execution environment and is inherently <strong>blocking</strong>.
 * However, from the HTTP layer's perspective, these endpoints are <strong>non-blocking</strong>
 * because we use {@code Mono.fromFuture(CompletableFuture.runAsync(...))} to offload the
 * Flink execution to the ForkJoinPool and immediately return the response.
 * </p>
 *
 * <h2>Imperative vs Reactive comparison</h2>
 * <pre>
 * // IMPERATIVE (spring-mvc)
 * ResponseEntity&lt;String&gt; triggerFlinkJob1() {
 *     CompletableFuture.runAsync(() -> flinkJobService.runJob1()); // fire-and-forget
 *     return ResponseEntity.ok("Job 1 started");
 * }
 *
 * // REACTIVE (webflux)
 * Mono&lt;ResponseEntity&lt;String&gt;&gt; triggerFlinkJob1() {
 *     return flinkJobService.runJob1()           // Mono&lt;String&gt; backed by CompletableFuture
 *         .map(msg -> ResponseEntity.ok(msg))    // wrap in ResponseEntity
 *         .onErrorResume(e -> Mono.just(         // reactive error handling
 *             ResponseEntity.internalServerError().body(e.getMessage())));
 * }
 * </pre>
 *
 * <h2>Can Flink be made fully reactive?</h2>
 * <p>
 * Short answer: <strong>No, not in the traditional sense.</strong>
 * Flink uses its own streaming model (DataStream API) which is separate from Project Reactor.
 * The best practice is exactly what we do here: run Flink asynchronously and bridge the result
 * back into the reactive pipeline via {@code Mono.fromFuture()}.
 * In Confluent Cloud / production, Flink jobs run as standalone cluster jobs, not as embedded
 * Spring beans – making the bridging concern irrelevant.
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/flink")
@Tag(name = "Flink Job Controller (Reactive)", description = "Flink jobs triggered via Mono.fromFuture() – non-blocking HTTP layer")
public class FlinkJobController {

    private final FlinkJobService flinkJobService;

    public FlinkJobController(FlinkJobService flinkJobService) {
        this.flinkJobService = flinkJobService;
    }

    /**
     * Triggers Flink Job 1 (MS SQL Server → Kafka) via a {@code CompletableFuture} wrapped in
     * a {@link Mono}. Returns immediately – the job runs in the background.
     *
     * @return {@link Mono} emitting {@link ResponseEntity} confirmation.
     */
    @Operation(summary = "Start Flink Job 1 – MSSQL → Kafka (reactive)",
               description = "Job executes in background via CompletableFuture.runAsync(). HTTP response returns immediately via Mono.fromFuture().")
    @PostMapping("/start-job1")
    public Mono<ResponseEntity<String>> triggerFlinkJob1() {
        return flinkJobService.runJob1()
                .map(ResponseEntity::ok)
                .onErrorResume(e -> {
                    log.error("Error triggering Flink Job 1", e);
                    return Mono.just(ResponseEntity.internalServerError().body("Error: " + e.getMessage()));
                });
    }

    /**
     * Triggers Flink Job 2 (Kafka → MySQL) – long-running, streams continuously from Kafka.
     *
     * @return {@link Mono} emitting {@link ResponseEntity} confirmation.
     */
    @Operation(summary = "Start Flink Job 2 – Kafka → MySQL (reactive)",
               description = "Continuous stream from Kafka to MySQL. Runs in background ForkJoinPool thread.")
    @PostMapping("/start-job2")
    public Mono<ResponseEntity<String>> triggerFlinkJob2() {
        return flinkJobService.runJob2()
                .map(ResponseEntity::ok)
                .onErrorResume(e -> {
                    log.error("Error triggering Flink Job 2", e);
                    return Mono.just(ResponseEntity.internalServerError().body("Error: " + e.getMessage()));
                });
    }

    /**
     * Triggers the lightweight Flink smoke-test/demo job.
     *
     * @return {@link Mono} emitting {@link ResponseEntity} confirmation.
     */
    @Operation(summary = "Start Flink Simple Demo Job (reactive)")
    @PostMapping("/start-simple-job")
    public Mono<ResponseEntity<String>> triggerFlinkSimpleJob() {
        return flinkJobService.runSimpleJob()
                .map(ResponseEntity::ok)
                .onErrorResume(e -> {
                    log.error("Error triggering Flink Simple Job", e);
                    return Mono.just(ResponseEntity.badRequest().body("Error: " + e.getMessage()));
                });
    }

    /**
     * Returns the last known status for a given job name.
     *
     * @param jobName e.g. {@code "Flink Job 1"}, {@code "Flink Job 2"} or {@code "Flink Simple Job"}.
     * @return {@link Mono} emitting the {@link JobStatus}.
     */
    @Operation(summary = "Get Flink job status (reactive)")
    @GetMapping("/job-status")
    public Mono<ResponseEntity<JobStatus>> getJobStatus(@RequestParam String jobName) {
        return flinkJobService.getJobStatus(jobName)
                .map(status -> {
                    log.debug("Job {} status requested: {}", jobName, status);
                    return ResponseEntity.ok(status);
                });
    }
}

