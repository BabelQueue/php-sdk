<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ConsumedMessage;

/**
 * A record received by {@see KafkaConsumer} — the framework-agnostic, read-only view of the decoded
 * envelope ({@see ConsumedMessage}) plus the §6-reconciled `attempts` counter. On Kafka the
 * **`bq-attempts` record header is authoritative** (Kafka has no native delivery count), falling
 * back to the body's `attempts` only when the header is absent — so a handler can implement its own
 * retry / dead-letter (retry-topic) policy on a poison record.
 *
 * It also carries the raw §6 `bq-` record headers, which the §6.4/§6.5 retry-topic machinery
 * ({@see KafkaRetryRouter}) reads to recover the **work topic** of a record already in the retry
 * chain (`bq-original-topic`) so retries route back to the right work topic across hops.
 */
final class KafkaMessage implements ConsumedMessage
{
    /**
     * @param  array<string, mixed>  $envelope  the decoded envelope, with `attempts` already reconciled
     * @param  array<string, string>  $headers  the raw §6 `bq-` record headers (UTF-8 strings)
     */
    public function __construct(
        private readonly array $envelope,
        private readonly array $headers = [],
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
     * The raw §6 `bq-` record headers (UTF-8 strings).
     *
     * @return array<string, string>
     */
    public function headers(): array
    {
        return $this->headers;
    }

    /**
     * A single raw §6 `bq-` record header, or null when absent — e.g. `bq-original-topic` on a
     * record already in the retry chain.
     */
    public function header(string $name): ?string
    {
        return $this->headers[$name] ?? null;
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
