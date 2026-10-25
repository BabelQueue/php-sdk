<?php

declare(strict_types=1);

namespace BabelQueue\Consume;

use BabelQueue\Exceptions\BabelQueueException;
use Throwable;

/**
 * Writing the dead-letter record failed (the transport threw). It is its own type so a consumer can tell it from
 * a handler failure: the contract (broker-bindings.md section 5.5) leaves the original unacknowledged and
 * negatively acknowledges it so the write is tried again, rather than re-publishing a retry copy.
 * `getPrevious()` is the transport's exception.
 */
class DeadLetterWriteFailed extends BabelQueueException
{
    public static function forQueue(string $queue, Throwable $previous): self
    {
        return new self(sprintf('Writing the dead-letter record to [%s] failed: %s', $queue, $previous->getMessage()), 0, $previous);
    }
}
