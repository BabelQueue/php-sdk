<?php

declare(strict_types=1);

namespace BabelQueue\Routing;

/**
 * The canonical names for what a consumer does with a message whose URN has no
 * mapped handler. Every SDK MUST offer the same four, with the same semantics
 * (see ../../.ssot/contracts/error-handling.md).
 */
final class UnknownUrnStrategy
{
    /** Throw — the worker retries, then dead-letters via the framework. */
    public const FAIL = 'fail';

    /** Acknowledge and silently drop the message. */
    public const DELETE = 'delete';

    /** Put it back on the queue for later (optionally after a delay). */
    public const RELEASE = 'release';

    /** Quarantine it on the dead-letter queue, then ack. */
    public const DEAD_LETTER = 'dead_letter';

    /** @return list<string> */
    public static function all(): array
    {
        return [self::FAIL, self::DELETE, self::RELEASE, self::DEAD_LETTER];
    }
}
