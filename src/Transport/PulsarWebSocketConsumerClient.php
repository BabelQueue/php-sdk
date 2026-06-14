<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * The one-method-per-operation seam {@see PulsarConsumer} consumes through — receive / ack /
 * negative-ack on a Pulsar subscription over the **WebSocket consumer API**. Implement it with a
 * pure-PHP WebSocket client (e.g. `textalk/websocket`); the consumer stays dependency-free and
 * unit-tests against a fake.
 *
 * The adapter owns the Pulsar WebSocket wire details: it derives the consumer endpoint
 * (`ws://<host>/ws/v2/consumer/persistent/<tenant>/<namespace>/<topic>/<subscription>`),
 * **base64-decodes** the frame's `payload` back to the canonical envelope JSON, and sends the ack
 * frames (`{"messageId": …}` to acknowledge, `{"messageId": …, "type": "negativeAcknowledge"}`
 * to redeliver). `redeliveryCount` is read from the frame so the consumer can reconcile §5
 * attempts (`max(body.attempts, redeliveryCount)`).
 */
interface PulsarWebSocketConsumerClient
{
    /**
     * Receive the next message from the subscription, or null if none arrived within the client's
     * read window (so the caller can poll without blocking forever).
     *
     * @return array{messageId: string, payload: string, properties: array<string, string>, redeliveryCount: int}|null
     */
    public function receive(): ?array;

    /** Acknowledge a successfully-handled message (removes it from the subscription). */
    public function acknowledge(string $messageId): void;

    /** Negatively-acknowledge a failed message so Pulsar redelivers it (at-least-once). */
    public function negativeAcknowledge(string $messageId): void;
}
