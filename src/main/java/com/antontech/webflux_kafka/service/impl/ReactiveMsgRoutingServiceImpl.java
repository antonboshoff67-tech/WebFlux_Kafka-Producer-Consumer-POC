package com.antontech.webflux_kafka.service.impl;

import com.antontech.webflux_kafka.model.JwtResponse;
import com.antontech.webflux_kafka.model.ServiceRequest;
import com.antontech.webflux_kafka.service.ReactiveMsgRoutingService;
import com.antontech.webflux_kafka.util.JwtTokenUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.UUID;

/**
 * Reactive implementation of {@link ReactiveMsgRoutingService}.
 *
 * <h2>Reactive vs imperative pattern</h2>
 * <pre>
 * // IMPERATIVE (blocking) – old style
 * void processSentMsgRequest(ServiceRequest req) {
 *     JwtResponse jwt = createJWT();   // blocks
 *     HttpEntity entity = createReq(); // runs
 *     restTemplate.post(entity);       // blocks waiting for HTTP response
 * }
 *
 * // REACTIVE – WebFlux style
 * Mono&lt;Void&gt; processSentMsgRequest(ServiceRequest req) {
 *     return Mono.fromCallable(() -> createJWT())           // lazy, non-blocking
 *                .subscribeOn(Schedulers.boundedElastic())  // JWT is CPU work, not I/O
 *                .flatMap(jwt -> Mono.fromCallable(() -> prepareRequest(jwt, req)))
 *                .doOnNext(entity -> log.info("prepared"))
 *                .then(); // discard result, complete as Mono&lt;Void&gt;
 * }
 * </pre>
 *
 * <p>In this POC the actual HTTP forward is omitted (same as the imperative version –
 * it logs the prepared request). For a real implementation you would use
 * {@code WebClient} (the WebFlux replacement for {@code RestTemplate}).
 */
@Slf4j
@Service
public class ReactiveMsgRoutingServiceImpl implements ReactiveMsgRoutingService {

    @Value("${gateway.endpoint.url}")
    private String baseUrl;

    private final JwtTokenUtil jwtTokenUtil;

    public ReactiveMsgRoutingServiceImpl(JwtTokenUtil jwtTokenUtil) {
        this.jwtTokenUtil = jwtTokenUtil;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Logs the received message on a bounded-elastic thread so heavy logging
     * does not tie up the event-loop thread.
     */
    @Override
    public Mono<Void> processReceivedMsgRequest(ServiceRequest serviceRequest) {
        return Mono.fromRunnable(() ->
                log.info("Received message request: {}", serviceRequest.getMsg()))
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    /**
     * {@inheritDoc}
     *
     * <p>JWT creation is CPU-bound work; wrapping it in {@code Mono.fromCallable(...).subscribeOn(boundedElastic())}
     * ensures it does not block the Netty I/O thread.
     */
    @Override
    public Mono<Void> processSentMsgRequest(ServiceRequest serviceRequest) {
        return Mono.fromCallable(this::createJWT)
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(jwt -> {
                    String requestId = UUID.randomUUID().toString();
                    log.info("Prepared request for gateway endpoint {} (requestId={})", baseUrl, requestId);
                    if (jwt.getToken() != null && !jwt.getToken().isBlank()) {
                        log.debug("JWT token present, length={}", jwt.getToken().length());
                    }
                })
                .then();
    }

    /**
     * @return a signed JWT, or an empty-token {@link JwtResponse} if no private key is configured.
     */
    private JwtResponse createJWT() {
        try {
            return new JwtResponse(jwtTokenUtil.buildGatewayToken());
        } catch (Exception e) {
            log.warn("JWT was not created (no private key configured): {}", e.getMessage());
            return new JwtResponse("");
        }
    }
}

