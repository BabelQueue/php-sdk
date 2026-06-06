<?php

declare(strict_types=1);

namespace BabelQueue\Exceptions;

/**
 * A decoded envelope failed consumer-side validation. The {@see self::reason()}
 * is one of the `EnvelopeValidator::REASON_*` constants, so a consumer can branch
 * on it — e.g. quarantine an unsupported schema version, drop a malformed body —
 * and {@see self::envelope()} carries the offending payload for logging or
 * dead-lettering.
 */
final class InvalidEnvelopeException extends BabelQueueException
{
    /**
     * @param  array<string, mixed>  $envelope
     */
    private function __construct(
        private readonly string $reason,
        private readonly array $envelope,
    ) {
        parent::__construct("Envelope rejected: {$reason}.");
    }

    /**
     * @param  array<string, mixed>  $envelope
     */
    public static function because(string $reason, array $envelope): self
    {
        return new self($reason, $envelope);
    }

    /** The machine-readable rejection reason (an `EnvelopeValidator::REASON_*` value). */
    public function reason(): string
    {
        return $this->reason;
    }

    /**
     * The offending envelope, for logging or dead-lettering.
     *
     * @return array<string, mixed>
     */
    public function envelope(): array
    {
        return $this->envelope;
    }
}
