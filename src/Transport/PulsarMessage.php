<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasRawBody;

/**
 * A message received by {@see PulsarConsumer} — the framework-agnostic, read-only view of the
 * decoded envelope ({@see ConsumedMessage}) plus the Pulsar message id needed to ack/redeliver it
 * and the §5-reconciled `attempts` counter (`max(body.attempts, redeliveryCount)`), so a handler
 * can implement its own retry/dead-letter policy on poison messages.
 */
final class PulsarMessage implements ConsumedMessage, HasRawBody
{
    /**
     * @param  array<string, mixed>  $envelope  the decoded envelope, with `attempts` already reconciled
     * @param  string|null  $rawBody  the body bytes as received (kept for poison dead-lettering)
     */
    public function __construct(
        private readonly array $envelope,
        private readonly string $messageId,
        private readonly ?string $rawBody = null,
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
     * The §5-reconciled attempt count for this delivery (`max(body.attempts, redeliveryCount)`).
     */
    public function attempts(): int
    {
        $attempts = $this->envelope['attempts'] ?? 0;

        return is_int($attempts) ? $attempts : 0;
    }

    /**
     * The native Pulsar message id (the ack handle).
     */
    public function messageId(): string
    {
        return $this->messageId;
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

    /**
     * The raw body as received, or null when it was not captured.
     */
    public function rawBody(): ?string
    {
        return $this->rawBody;
    }
}
