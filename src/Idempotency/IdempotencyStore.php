<?php

declare(strict_types=1);

namespace BabelQueue\Idempotency;

/**
 * A pluggable record of message ids that have already been processed, keyed on the
 * envelope's `meta.id` (the canonical per-message identity — see
 * {@see \BabelQueue\Codec\EnvelopeCodec} and message-envelope.md).
 *
 * The reference {@see InMemoryStore} is for tests / single-process consumers; production
 * backends (Redis, a database table, a PSR-16 cache) implement the same three methods.
 *
 * The contract is **"seen-set" dedupe**: it answers *"was this id processed?"*, not
 * *"what did it return"* — queue handlers have no response to replay (unlike an HTTP
 * idempotency key). It provides **post-success** dedupe under at-least-once + idempotent
 * handlers (error-handling.md §1), **not** exactly-once and **not** in-flight concurrency
 * locking. A transactional/outbox mode is a documented future direction (ADR-0022).
 */
interface IdempotencyStore
{
    /**
     * Has this message id already been processed (remembered)?
     */
    public function seen(string $messageId): bool;

    /**
     * Record this message id as processed. Called only after the handler returns
     * normally, so a thrown handler is retried rather than silently swallowed.
     */
    public function remember(string $messageId): void;

    /**
     * Drop a message id from the store (manual eviction; a backend may also expire ids
     * on its own TTL).
     */
    public function forget(string $messageId): void;
}
