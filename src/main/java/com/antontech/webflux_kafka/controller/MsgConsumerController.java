package com.antontech.webflux_kafka.controller;

import com.antontech.webflux_kafka.model.ServiceRequest;
import com.antontech.webflux_kafka.service.ReactiveMsgRoutingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Reactive message routing (send/receive) controller.
 *
 * <h2>ResponseEntity in WebFlux</h2>
 * <p>
 * In Spring MVC you return {@code ResponseEntity<Object>} directly.
 * In WebFlux you return {@code Mono<ResponseEntity<Object>>}. Spring WebFlux
 * subscribes to the Mono and sends the HTTP response when it emits.
 * </p>
 *
 * <h2>Error handling in reactive controllers</h2>
 * <p>
 * Instead of try/catch blocks (which work but are imperative), WebFlux encourages:
 * <pre>
 * return service.doWork()
 *     .map(result -> ResponseEntity.ok(result))
 *     .onErrorResume(e -> Mono.just(
 *         ResponseEntity.badRequest().body("Error: " + e.getMessage())
 *     ));
 * </pre>
 * {@code onErrorResume()} is the reactive equivalent of a catch block – it intercepts
 * an error signal in the reactive pipeline and provides a fallback {@code Mono}.
 * </p>
 */
@Slf4j
@RestController
@RequestMapping(path = "item-kafka/app/")
@Tag(name = "Msg Routing Controller (Reactive)", description = "JWT-signed gateway message routing test flow")
public class MsgConsumerController {

    private final ReactiveMsgRoutingService reactiveMsgRoutingService;

    public MsgConsumerController(ReactiveMsgRoutingService reactiveMsgRoutingService) {
        this.reactiveMsgRoutingService = reactiveMsgRoutingService;
    }

    /**
     * Builds a signed JWT and prepares a forwarding request to the configured gateway
     * endpoint for the supplied message payload.
     *
     * <p><strong>Reactive flow:</strong>
     * <pre>
     * processSentMsgRequest()   // Mono&lt;Void&gt;
     *     .thenReturn(ok)       // on success: Mono&lt;ResponseEntity&gt;
     *     .onErrorResume(e -> bad_request)  // on error: fallback Mono
     * </pre>
     *
     * @param serviceRequest the message payload.
     * @return {@link Mono} emitting HTTP 200 on success, HTTP 400 on failure.
     */
    @Operation(summary = "Send items to Kafka gateway (reactive)",
               description = "Builds JWT, prepares forwarding request. Returns Mono<ResponseEntity>.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Request prepared"),
            @ApiResponse(responseCode = "400", description = "Error preparing request")})
    @PostMapping(path = "send-items/v1", produces = "text/plain")
    public Mono<ResponseEntity<String>> sendItemsToKafka(@RequestBody ServiceRequest serviceRequest) {
        return reactiveMsgRoutingService.processSentMsgRequest(serviceRequest)
                .thenReturn(ResponseEntity.ok("The items were prepared for publishing to the Kafka topic."))
                .onErrorResume(e -> {
                    log.error("Failed to prepare items for Kafka", e);
                    return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                            .body("There was a problem: " + e.getMessage()));
                });
    }

    /**
     * Simulates receiving a message from the consumer side of the test flow.
     *
     * @param authToken      optional Bearer token (logged if present).
     * @param serviceRequest the message payload received.
     * @return {@link Mono} emitting HTTP 200 on success, HTTP 400 on failure.
     */
    @Operation(summary = "Consume items from Kafka (reactive)",
               description = "Simulates message receipt. Returns Mono<ResponseEntity>.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Message processed"),
            @ApiResponse(responseCode = "400", description = "Processing error")})
    @GetMapping(path = "consume-items/v1", produces = "text/plain")
    public Mono<ResponseEntity<String>> consumeItemsFromKafka(
            @RequestHeader(name = "Authorization", required = false) String authToken,
            @RequestBody ServiceRequest serviceRequest) {

        if (authToken != null && !authToken.isBlank()) {
            log.debug("Authorization header received for consume request");
        }
        return reactiveMsgRoutingService.processReceivedMsgRequest(serviceRequest)
                .thenReturn(ResponseEntity.ok("Message items were processed successfully from the Kafka consumer."))
                .onErrorResume(e -> {
                    log.error("Failed to process consumed items", e);
                    return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                            .body("There was a problem: " + e.getMessage()));
                });
    }
}

