<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\InboundMessage;

/**
 * A record received by {@see KafkaConsumer} — the framework-agnostic, read-only view of the decoded
 * envelope ({@see InboundMessage}) plus the §6-reconciled `attempts` counter. On Kafka the
 * **`bq-attempts` record header is authoritative** (Kafka has no native delivery count), falling
 * back to the body's `attempts` only when the header is absent — so a handler can implement its own
 * retry / dead-letter (retry-topic) policy on a poison record.
 */
final class KafkaMessage implements InboundMessage
{
    /**
     * @param  array<string, mixed>  $envelope  the decoded envelope, with `attempts` already reconciled
     */
    public function __construct(
        private readonly array $envelope,
    ) {
    }

    public function getUrn(): string
    {
        return EnvelopeCodec::urn($this->envelope);
    }

    public function getTraceId(): string
    {
        $traceId = $this->envelope['trace_id'] ?? '';

        return is_string($traceId) ? $traceId : '';
    }

    /**
     * @return array<string, mixed>
     */
    public function getData(): array
    {
        return is_array($this->envelope['data'] ?? null) ? $this->envelope['data'] : [];
    }

    /**
     * @return array<string, mixed>
     */
    public function getMeta(): array
    {
        return is_array($this->envelope['meta'] ?? null) ? $this->envelope['meta'] : [];
    }

    /**
     * The §6-reconciled attempt count (`bq-attempts` header when present, else the body's own).
     */
    public function attempts(): int
    {
        $attempts = $this->envelope['attempts'] ?? 0;

        return is_int($attempts) ? $attempts : 0;
    }

    /**
     * The full decoded envelope.
     *
     * @return array<string, mixed>
     */
    public function envelope(): array
    {
        return $this->envelope;
    }
}
