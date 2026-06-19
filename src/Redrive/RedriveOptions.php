<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

use Closure;

/**
 * Options for a {@see Redrive::run()} call.
 */
final class RedriveOptions
{
    /** @var (Closure(array<string, mixed>): bool)|null */
    public readonly ?Closure $select;

    /**
     * @param  string|null  $toQueue  Override the target: when null, each message goes back to
     *                                its own dead_letter.original_queue; set a sandbox queue to
     *                                replay safely.
     * @param  int  $max  Cap how many messages are pulled from the DLQ (0 = all available).
     * @param  bool  $dryRun  Inspect and report the plan, restoring every message unchanged.
     * @param  (callable(array<string, mixed>): bool)|null  $select  Pick which messages to
     *                                redrive (e.g. by reason or URN); unselected are restored.
     */
    public function __construct(
        public readonly ?string $toQueue = null,
        public readonly int $max = 0,
        public readonly bool $dryRun = false,
        ?callable $select = null,
    ) {
        $this->select = $select === null ? null : Closure::fromCallable($select);
    }
}
