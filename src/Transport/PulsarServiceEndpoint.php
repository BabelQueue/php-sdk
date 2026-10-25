<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * An optional capability of a {@see PulsarWebSocketConsumerClient} adapter: it tells the {@see PulsarConsumer} where
 * the Pulsar WebSocket service listens. With it the consumer can build its own retry producer
 * ({@see PulsarWebSocketProducer}) on the same host, so `new PulsarConsumer($client)` keeps a retry that is bounded
 * on an `Exclusive` / `Failover` subscription without a second, hand-wired producer client (ADR-0036 decision 1).
 *
 * The consumer endpoint the adapter already uses is
 * `ws://<host>/ws/v2/consumer/persistent/<tenant>/<namespace>/<topic>/<subscription>`; the producer endpoint is the
 * same service, `ws://<host>/ws/v2/producer/persistent/<tenant>/<namespace>/<topic>`, and its tenant, namespace and
 * topic come from the consumer's real (full) topic, never from a guess.
 */
interface PulsarServiceEndpoint
{
    /**
     * The base URL of the Pulsar WebSocket service: `ws://host:8080`, `wss://host:8443`, `http://host:8080` or
     * `https://host:8443` (`http`/`https` mean `ws`/`wss`), with an optional path prefix when a proxy mounts the
     * service below one. An empty string means the adapter does not know it.
     */
    public function serviceUrl(): string;

    /** A bearer token for the producer connection (the `Authorization: Bearer` header), or null for none. */
    public function authToken(): ?string;
}
