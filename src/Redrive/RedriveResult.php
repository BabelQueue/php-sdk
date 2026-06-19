<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

/**
 * Summary of a {@see Redrive::run()} call.
 */
final class RedriveResult
{
    /**
     * @param  list<RedriveItem>  $items
     */
    public function __construct(
        public readonly int $redriven,
        public readonly int $skipped,
        public readonly array $items,
    ) {
    }
}
