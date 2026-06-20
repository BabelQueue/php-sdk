<?php

declare(strict_types=1);

namespace BabelQueue\Outbox;

/**
 * Summary of one {@see OutboxRelay::flush()} pass: how many pending rows were published
 * and how many failed (and were left pending for a later retry).
 */
final class OutboxRelayResult
{
    public function __construct(
        public readonly int $published,
        public readonly int $failed,
    ) {
    }

    /** Total rows the relay attempted in this pass. */
    public function attempted(): int
    {
        return $this->published + $this->failed;
    }
}
