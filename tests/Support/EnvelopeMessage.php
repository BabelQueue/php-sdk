<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Support;

use BabelQueue\Contracts\ConsumedMessage;

/**
 * A minimal {@see ConsumedMessage} over a decoded envelope, for conformance runners that drive the
 * consume-side helpers (idempotency) without a transport.
 */
final class EnvelopeMessage implements ConsumedMessage
{
    /** @param array<string, mixed> $envelope */
    public function __construct(private readonly array $envelope)
    {
    }

    public function getUrn(): string
    {
        return is_string($this->envelope['job'] ?? null) ? $this->envelope['job'] : '';
    }

    public function getTraceId(): string
    {
        return is_string($this->envelope['trace_id'] ?? null) ? $this->envelope['trace_id'] : '';
    }

    /** @return array<string, mixed> */
    public function getData(): array
    {
        return is_array($this->envelope['data'] ?? null) ? $this->envelope['data'] : [];
    }

    /** @return array<string, mixed> */
    public function getMeta(): array
    {
        return is_array($this->envelope['meta'] ?? null) ? $this->envelope['meta'] : [];
    }

    public function attempts(): int
    {
        return is_int($this->envelope['attempts'] ?? null) ? $this->envelope['attempts'] : 0;
    }

    /** @return array<string, mixed> */
    public function envelope(): array
    {
        return $this->envelope;
    }
}
