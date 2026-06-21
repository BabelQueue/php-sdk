<?php

declare(strict_types=1);

namespace BabelQueue\Idempotency;

use RuntimeException;

/**
 * Thrown by {@see ClaimingDispatch::wrap()} when a concurrent worker holds an unexpired in-flight
 * claim on this `meta.id`: the delivery is **parked**, not failed. Under the consume contract a
 * thrown handler is *not* acked, so the broker redelivers later — by which time the claim's owner
 * has committed and the redelivery will skip. This reuses the existing redeliver-on-throw path
 * instead of inventing a separate "defer" signal (ADR-0022).
 *
 * It is a distinct type so a consumer's error handler can tell a benign park (expected under
 * concurrency; do not alert, do not count toward DLQ retries) from a genuine handler failure.
 */
final class ClaimParkedException extends RuntimeException
{
    public function __construct(public readonly string $messageId)
    {
        parent::__construct(
            "Idempotency claim for message id '{$messageId}' is held by another worker; parking for redelivery."
        );
    }
}
