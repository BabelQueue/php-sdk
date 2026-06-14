<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * The one-method seam {@see PulsarTransport} produces through — publish one message to a Pulsar
 * topic over the **WebSocket producer API**. Implement it with a one-line adapter around a
 * pure-PHP WebSocket client (e.g. `textalk/websocket`); the transport stays dependency-free and
 * unit-tests against a fake.
 *
 * The adapter owns the Pulsar WebSocket wire details: it derives the producer endpoint
 * (`ws://<host>/ws/v2/producer/persistent/<tenant>/<namespace>/<topic>`) from `$topic`,
 * **base64-encodes** `$payload` into the frame's `payload` field, attaches `$properties` as the
 * message `properties` map, sends the JSON frame, and reads the producer ack — throwing on a
 * non-`ok` result so a failed publish is never silently lost (GR-1). It MUST NOT set the frame's
 * `eventTime`: Pulsar's `publishTimestamp` is broker-set and informational, and the body's
 * `meta.created_at` is authoritative (§5, GR-5).
 */
interface PulsarWebSocketClient
{
    /**
     * @param  string  $topic  the full Pulsar topic, e.g. `persistent://public/default/orders`
     * @param  string  $payload  the canonical envelope JSON (the adapter base64-encodes it)
     * @param  array<string, string>  $properties  the §5 `bq-` native property projection
     */
    public function publish(string $topic, string $payload, array $properties): void;
}
