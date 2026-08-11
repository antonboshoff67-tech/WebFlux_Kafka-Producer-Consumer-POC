package com.antontech.webflux_kafka.service;

import com.antontech.webflux_kafka.model.ServiceRequest;
import reactor.core.publisher.Mono;

/**
 * Reactive variant of the message routing service.
 * All methods return {@link Mono} so they integrate naturally with WebFlux controllers.
 */
public interface ReactiveMsgRoutingService {

    /**
     * Processes an incoming (received) message reactively.
     *
     * @param serviceRequest the message received from the consumer side.
     * @return a {@link Mono<Void>} that completes when logging is done.
     */
    Mono<Void> processReceivedMsgRequest(ServiceRequest serviceRequest);

    /**
     * Builds a JWT, prepares and logs a forwarding request to the gateway reactively.
     *
     * @param serviceRequest the message to forward.
     * @return a {@link Mono<Void>} that completes when the request is prepared.
     */
    Mono<Void> processSentMsgRequest(ServiceRequest serviceRequest);
}

