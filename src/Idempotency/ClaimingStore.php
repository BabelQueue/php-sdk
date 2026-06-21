<?php

declare(strict_types=1);

namespace BabelQueue\Idempotency;

/**
 * An {@see IdempotencyStore} backed by a **shared, persistent** store (a database table, Redis)
 * that can also resolve the concurrency the base "seen-set" cannot: two workers handed the *same*
 * `meta.id` at the same time (at-least-once + a fan-out broker) must not both run the handler.
 *
 * The base {@see IdempotencyStore} is, by its own contract, post-success dedupe only — it answers
 * *"was this id processed?"* and explicitly does **not** lock an in-flight delivery. A
 * single-process {@see InMemoryStore} has no concurrent peers, so that is enough for it. A
 * persistent store shared by a fleet does have concurrent peers, so it additionally offers an
 * **atomic claim**: exactly one caller wins the right to run a given id, the rest are told it is
 * already claimed (parked → the broker redelivers later, by when the winner has committed).
 *
 * This is an **opt-in extension**, not a change to the frozen base interface (GR-1 in spirit):
 * `Idempotent::wrap()` still drives any {@see IdempotencyStore}; a caller that wants the stronger
 * claim/commit contract type-hints {@see ClaimingStore} and drives it with {@see ClaimingDispatch}.
 *
 * Lifecycle of one id under the claim contract:
 *
 *   claim(id, ttl) === true   → I won; run the handler, then remember(id) to commit (the claim is
 *                               upgraded to a permanent "seen" record).
 *   claim(id, ttl) === false  → either another worker holds an unexpired in-flight claim (park —
 *                               do not run), or the id is already committed (seen() === true, skip).
 *                               The caller distinguishes the two with seen().
 *   release(id)               → drop an *uncommitted* claim so a failed handler is retried promptly
 *                               instead of waiting out the TTL (best-effort; the TTL is the backstop).
 *
 * TTL bounds a crash between claim and commit: a worker that dies mid-handler leaves a claim that
 * expires after `ttlSeconds`, after which a redelivery can re-claim and re-run (still at-least-once,
 * never exactly-once — ADR-0022).
 */
interface ClaimingStore extends IdempotencyStore
{
    /**
     * Has this message id already been **committed** (a permanent record, not just an in-flight
     * claim)? Redeclared from {@see IdempotencyStore::seen()} to mark it impure: a persistent store
     * reads shared external state (a DB row, a Redis key) that a concurrent worker may commit
     * between two calls, so repeated calls can legitimately return different results — which is
     * exactly why {@see ClaimingDispatch} re-checks it after losing a claim race.
     *
     * @phpstan-impure
     */
    public function seen(string $messageId): bool;

    /**
     * Atomically attempt to claim `$messageId` for processing.
     *
     * Returns true to **exactly one** concurrent caller (it now owns the right to run the handler
     * and must {@see remember()} on success or {@see release()} on failure); false to every other
     * caller — whether because the id is already committed ({@see seen()} is true) or because a
     * peer holds an unexpired in-flight claim. The claim self-expires after `$ttlSeconds` so a
     * crashed owner cannot wedge the id forever.
     *
     * @param  int  $ttlSeconds  How long an uncommitted claim stays held before it may be re-claimed
     *                           (the crash backstop). Must be > 0; a non-positive value is treated
     *                           as a sensible default by the implementation.
     */
    public function claim(string $messageId, int $ttlSeconds): bool;

    /**
     * Release an **uncommitted** claim so a redelivery can re-claim it immediately, rather than
     * waiting out the TTL — call this when the handler threw. A no-op if the id was already
     * committed via {@see remember()} or was never claimed. Best-effort: the TTL is the backstop.
     */
    public function release(string $messageId): void;
}
