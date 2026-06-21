<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

/**
 * What happened to one message during a {@see Redrive::run()} call.
 */
final class RedriveItem
{
    /**
     * @param  bool  $bypassed  True only when the `bq-replay-bypass` header was actually stamped on
     *                          the redriven message (options requested it AND the {@see RedriveIO}
     *                          is a {@see HeaderRedriveIO}); false otherwise, including when bypass
     *                          was requested but the transport could not carry it (ADR-0027).
     */
    public function __construct(
        public readonly string $messageId,
        public readonly string $traceId,
        public readonly string $urn,
        public readonly string $reason,
        public readonly string $from,
        public readonly string $to,
        public readonly bool $redriven,
        public readonly bool $bypassed = false,
    ) {
    }
}
