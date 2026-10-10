<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use Closure;
use Throwable;

/**
 * A framework-less **Apache Pulsar** consumer — PHP's path to the §5 binding's consume side over
 * Pulsar's native **WebSocket consumer API**, the counterpart to {@see PulsarTransport} (produce).
 *
 * **GR-7 stays intact** (like the producer, ADR-0020): the WebSocket client is pure-userland — no C
 * extension — so the consumer is opt-in behind the one-method {@see PulsarWebSocketConsumerClient}
 * seam, dependency-free and unit-testable with a fake.
 *
 * It exposes the §5.5 consume primitives — {@see self::receive()} (decode + route + reconcile
 * attempts), {@see self::acknowledge()} (§5 "delete") and {@see self::release()}
 * (`negativeAcknowledge` → redelivery, at-least-once) — plus a {@see self::consume()} convenience
 * loop.
 *
 * Per §5.2/§5.7 **routing follows the `bq-job` message property** — {@see PulsarMessage::getUrn()}
 * returns the trimmed, non-blank property and falls back to the body's URN
 * ({@see EnvelopeCodec::urn()}) when it is absent or blank. The body is always decoded and
 * {@see PulsarMessage::envelope()} is always the body's envelope. A body without a `job` is never
 * routed, even when `bq-job` is set (it yields an empty URN, which the dispatcher treats as an
 * unknown URN).
 *
 * Per §5.2/§5.7 the **`attempts` counter** is the authoritative `bq-attempts` property (valid only
 * when it is plain decimal digits, `^[0-9]+$`, no greater than 2147483647), falling back to the
 * body's `attempts` when it is absent or invalid; the result is then floored with
 * `max(.., redeliveryCount)` (Pulsar's redelivery count is 0-based, so no `-1`) and exposed via
 * {@see PulsarMessage::attempts()} so a handler can apply its own retry / dead-letter policy.
 *
 * When the `bq-job` property and the body URN differ, the optional `$onRoutingMismatch` hook is
 * called as `fn(string $propertyUrn, string $bodyUrn, array $envelope)`. It is a separate,
 * best-effort diagnostic: any exception it throws is swallowed and the message flow is unchanged.
 * It runs after the `attempts` reconcile, so it sees the envelope the handler gets. Unlike the
 * Java/.NET/Go/Python/Node consumers, this consumer has no `accepts()` gate (the Dispatcher
 * validates downstream), so the hook is not suppressed for a non-conformant envelope.
 *
 * URN dispatch, `on_unknown_urn` and DLQ routing can be handled by the framework-less
 * {@see \BabelQueue\Consume\Dispatcher} runtime (or a framework adapter): it is callable, so
 * `consume($dispatcher, $shouldStop)` dispatches on {@see PulsarMessage::getUrn()} and
 * dead-letters poison messages — the consumer stays minimal.
 */
final class PulsarConsumer
{
    /** Largest valid `bq-attempts` value (int32, shared across the SDKs for parity). */
    private const MAX_ATTEMPTS = 2147483647;

    private readonly ?Closure $onRoutingMismatch;

    /**
     * @param  (callable(string, string, array<string, mixed>): mixed)|null  $onRoutingMismatch
     *         optional diagnostic hook `fn($propertyUrn, $bodyUrn, $envelope)` called when the
     *         `bq-job` property and the body URN differ; default no-op, its errors are swallowed
     */
    public function __construct(
        private readonly PulsarWebSocketConsumerClient $client,
        ?callable $onRoutingMismatch = null,
    ) {
        $this->onRoutingMismatch = $onRoutingMismatch === null ? null : Closure::fromCallable($onRoutingMismatch);
    }

    /**
     * Receive and decode the next message, routed per `bq-job` with §5 `attempts` reconciled onto
     * the envelope, or null if none arrived within the client's read window.
     */
    public function receive(): ?PulsarMessage
    {
        $raw = $this->client->receive();

        if ($raw === null) {
            return null;
        }

        $envelope = EnvelopeCodec::decode($raw['payload']);
        $properties = $raw['properties'];

        // §5.2/§5.7: attempts = the authoritative bq-attempts property, else the body's value; then
        // floored with the 0-based redelivery count (no -1).
        $attempts = self::parseAttempts($properties['bq-attempts'] ?? null);
        if ($attempts === null) {
            $body = $envelope['attempts'] ?? 0;
            $attempts = is_int($body) ? $body : 0;
        }
        $envelope['attempts'] = max($attempts, $raw['redeliveryCount']);

        // §5.2/§5.7: route on the bq-job property, falling back to the body URN when it is absent or
        // blank. A body without a job is never routed (the property cannot stand in for it).
        $bodyUrn = EnvelopeCodec::urn($envelope);
        $rawJob = $properties['bq-job'] ?? null;
        $propertyUrn = is_string($rawJob) ? trim($rawJob) : '';
        $routedUrn = null;

        if ($bodyUrn !== '' && $propertyUrn !== '') {
            $routedUrn = $propertyUrn;

            if ($propertyUrn !== $bodyUrn) {
                $this->notifyRoutingMismatch($propertyUrn, $bodyUrn, $envelope);
            }
        }

        return new PulsarMessage($envelope, $raw['messageId'], $raw['payload'], $routedUrn);
    }

    /**
     * Parse `bq-attempts`: valid only as plain decimal digits (`^[0-9]+$` — no sign, whitespace or
     * empty string) within 0..2147483647; anything else is invalid (null → fall back to the body).
     */
    private static function parseAttempts(mixed $value): ?int
    {
        // The D modifier stops `$` from matching before a trailing newline.
        if (! is_string($value) || preg_match('/^[0-9]+$/D', $value) !== 1) {
            return null;
        }

        $digits = ltrim($value, '0');
        if ($digits === '') {
            return 0;
        }

        if (strlen($digits) > 10 || (int) $digits > self::MAX_ATTEMPTS) {
            return null;
        }

        return (int) $digits;
    }

    /**
     * @param  array<string, mixed>  $envelope
     */
    private function notifyRoutingMismatch(string $propertyUrn, string $bodyUrn, array $envelope): void
    {
        if ($this->onRoutingMismatch === null) {
            return;
        }

        try {
            ($this->onRoutingMismatch)($propertyUrn, $bodyUrn, $envelope);
        } catch (Throwable) {
            // Diagnostic only: a failing hook must never change the ack / redelivery flow.
        }
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
