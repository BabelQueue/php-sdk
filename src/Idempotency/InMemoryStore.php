<?php

declare(strict_types=1);

namespace BabelQueue\Idempotency;

/**
 * Process-local {@see IdempotencyStore} backed by an array. Suitable for tests and
 * single-process consumers; it is **not** shared across workers and **not** persistent —
 * use a Redis- or database-backed store for production fleets.
 */
final class InMemoryStore implements IdempotencyStore
{
    /** @var array<string, true> */
    private array $seen = [];

    public function seen(string $messageId): bool
    {
        return isset($this->seen[$messageId]);
    }

    public function remember(string $messageId): void
    {
        $this->seen[$messageId] = true;
    }

    public function forget(string $messageId): void
    {
        unset($this->seen[$messageId]);
    }
}
