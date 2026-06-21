<?php

declare(strict_types=1);

namespace BabelQueue\Idempotency;

use BabelQueue\Contracts\ConsumedMessage;
use Throwable;

/**
 * The claim-based sibling of {@see Idempotent::wrap()}: wraps a consume handler so that, under a
 * shared persistent {@see ClaimingStore}, exactly one of N concurrent deliveries of the same
 * `meta.id` runs the handler — closing the in-flight window the post-success {@see Idempotent}
 * cannot (ADR-0022).
 *
 * It composes with the same ack-on-return / redeliver-on-throw consume contract:
 *
 *     $dispatch->on('urn:babel:orders:created', ClaimingDispatch::wrap($store, $handler));
 *
 * Per delivery, by `meta.id`:
 *   - **already committed** ({@see ClaimingStore::seen()}) → return (skip); the loop acks it.
 *   - **claim won** → run the handler; on success {@see ClaimingStore::remember()} (commit), so a
 *     later redelivery sees it committed and skips. On a throw, {@see ClaimingStore::release()} the
 *     claim (so a redelivery re-runs promptly) and **re-throw** so retry/DLQ still apply.
 *   - **claim lost** (a peer holds an unexpired in-flight claim) → **throw** a {@see ClaimParkedException}
 *     so the delivery is *not* acked: the broker redelivers it later, by when the winner has
 *     committed and this delivery will skip. Parking-via-throw reuses the existing redeliver path
 *     rather than inventing a new "defer" signal.
 *   - **no usable id** → run the handler unchanged (fail-open), exactly like {@see Idempotent}.
 */
final class ClaimingDispatch
{
    /** Default in-flight claim TTL (seconds): the crash backstop, not a handler timeout. */
    public const DEFAULT_TTL = 3600;

    /**
     * @param  callable(ConsumedMessage): void  $handler
     * @param  int  $ttlSeconds  How long a won-but-uncommitted claim is held before a crashed owner's
     *                          id may be re-claimed.
     * @return callable(ConsumedMessage): void
     */
    public static function wrap(ClaimingStore $store, callable $handler, int $ttlSeconds = self::DEFAULT_TTL): callable
    {
        return static function (ConsumedMessage $message) use ($store, $handler, $ttlSeconds): void {
            $meta = $message->getMeta();
            $id = isset($meta['id']) && is_string($meta['id']) ? $meta['id'] : '';

            // No usable id → cannot dedupe; run the handler unchanged (fail-open).
            if ($id === '') {
                $handler($message);

                return;
            }

            // Already committed on an earlier delivery: skip + return so the loop acks it.
            if ($store->seen($id)) {
                return;
            }

            // Atomic claim: exactly one concurrent worker wins.
            if (! $store->claim($id, $ttlSeconds)) {
                // Lost the race. Either a peer is mid-flight, or it just committed — re-check seen()
                // to ack a now-committed id instead of needlessly redelivering it.
                if ($store->seen($id)) {
                    return;
                }

                throw new ClaimParkedException($id);
            }

            // We own the claim. Run the handler; commit on success, release on failure.
            try {
                $handler($message);
            } catch (Throwable $e) {
                $store->release($id); // let a redelivery re-run promptly; TTL is the backstop
                throw $e;
            }
            $store->remember($id);
        };
    }
}
