<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

/**
 * What happened to one message during a {@see Redrive::run()} call.
 */
final class RedriveItem
{
    public function __construct(
        public readonly string $messageId,
        public readonly string $traceId,
        public readonly string $urn,
        public readonly string $reason,
        public readonly string $from,
        public readonly string $to,
        public readonly bool $redriven,
    ) {
    }
}
