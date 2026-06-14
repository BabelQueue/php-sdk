<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use Throwable;

/**
 * A framework-less **Apache Pulsar** consumer — PHP's path to the §5 binding's consume side over
 * Pulsar's native **WebSocket consumer API**, the counterpart to {@see PulsarTransport} (produce).
 *
 * **GR-7 stays intact** (like the producer, ADR-0020): the WebSocket client is pure-userland — no C
 * extension — so the consumer is opt-in behind the one-method {@see PulsarWebSocketConsumerClient}
 * seam, dependency-free and unit-testable with a fake.
 *
 * It exposes the §5.5 consume primitives — {@see self::receive()} (decode + reconcile attempts),
 * {@see self::acknowledge()} (§5 "delete") and {@see self::release()} (`negativeAcknowledge` →
 * redelivery, at-least-once) — plus a {@see self::consume()} convenience loop. Per §5.5 the
 * **`attempts` counter is reconciled** onto the envelope as `max(body.attempts, redeliveryCount)`
 * (Pulsar's redelivery count is 0-based, so no `-1`), exposed via {@see PulsarMessage::attempts()}
 * so a handler can apply its own retry / dead-letter policy on a poison message.
 *
 * URN dispatch, `on_unknown_urn` and DLQ routing can be handled by the framework-less
 * {@see \BabelQueue\Consume\Dispatcher} runtime (or a framework adapter): it is callable, so
 * `consume($dispatcher, $shouldStop)` routes on `bq-job` via the body's `job` URN
 * ({@see EnvelopeCodec::urn()}) and dead-letters poison messages — the consumer stays minimal.
 */
final class PulsarConsumer
{
    public function __construct(
        private readonly PulsarWebSocketConsumerClient $client,
    ) {
    }

    /**
     * Receive and decode the next message, with §5 `attempts` reconciled onto the envelope, or null
     * if none arrived within the client's read window.
     */
    public function receive(): ?PulsarMessage
    {
        $raw = $this->client->receive();

        if ($raw === null) {
            return null;
        }

        $envelope = EnvelopeCodec::decode($raw['payload']);

        // §5.5: attempts = max(body.attempts, redeliveryCount). Pulsar's redelivery count is 0-based
        // (0 on first delivery), so it maps directly with no -1, and a runtime-incremented body
        // count is never lowered.
        $body = $envelope['attempts'] ?? 0;
        $envelope['attempts'] = max(is_int($body) ? $body : 0, $raw['redeliveryCount']);

        return new PulsarMessage($envelope, $raw['messageId']);
    }

    /**
     * Acknowledge a successfully-handled message (§5 "delete").
     */
    public function acknowledge(PulsarMessage $message): void
    {
        $this->client->acknowledge($message->messageId());
    }

    /**
     * Release a message back for redelivery (§5 "release" — `negativeAcknowledge`, at-least-once).
     */
    public function release(PulsarMessage $message): void
    {
        $this->client->negativeAcknowledge($message->messageId());
    }

    /**
     * Run the consume loop: receive each message and pass it to $handler; acknowledge on success,
     * release (redeliver) if the handler throws. Idle (null) receives are skipped. The loop runs
     * until $shouldStop() returns true (omit it to run forever, the standard worker model).
     *
     * @param  callable(PulsarMessage): void  $handler
     * @param  (callable(): bool)|null  $shouldStop
     */
    public function consume(callable $handler, ?callable $shouldStop = null): void
    {
        while ($shouldStop === null || ! $shouldStop()) {
            $message = $this->receive();

            if ($message === null) {
                continue;
            }

            try {
                $handler($message);
                $this->acknowledge($message);
            } catch (Throwable) {
                // At-least-once: redeliver on failure. A handler owns poison handling via
                // $message->attempts() (e.g. ack-and-drop or republish to <queue>.dlq past a cap).
                $this->release($message);
            }
        }
    }
}
