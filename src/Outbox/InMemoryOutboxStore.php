<?php

declare(strict_types=1);

namespace BabelQueue\Outbox;

/**
 * Process-local reference {@see OutboxStore} backed by arrays — for tests and single-process
 * demos. It has **no real transaction**: {@see save()} just appends, so it cannot deliver the
 * atomic-with-the-business-write guarantee a production store gives. Use a database-backed
 * adapter (e.g. the InitORM one in `babelqueue-examples/`) in production.
 *
 * It still faithfully models the relay contract: rows are pending until {@see markPublished()},
 * {@see fetchUnpublished()} returns them oldest-first, and {@see markFailed()} bumps the attempt
 * count and stores the last error while leaving the row pending for retry.
 */
final class InMemoryOutboxStore implements OutboxStore
{
    /** @var array<string, array{body: string, queue: string, attempts: int, published: bool, error: string}> */
    private array $rows = [];

    private int $sequence = 0;

    public function save(string $encodedEnvelope, string $queue): string
    {
        // A non-numeric id keeps the array key a genuine string (PHP coerces numeric-string
        // keys to int), so the contract's string-keyed store stays honest.
        $id = 'ob-' . ++$this->sequence;
        $this->rows[$id] = [
            'body' => $encodedEnvelope,
            'queue' => $queue,
            'attempts' => 0,
            'published' => false,
            'error' => '',
        ];

        return $id;
    }

    public function fetchUnpublished(int $limit): array
    {
        $records = [];
        foreach ($this->rows as $id => $row) {
            if ($row['published']) {
                continue;
            }
            $records[] = new OutboxRecord($id, $row['body'], $row['queue'], $row['attempts']);
            if (count($records) >= $limit) {
                break;
            }
        }

        return $records;
    }

    public function markPublished(array $ids): void
    {
        foreach ($ids as $id) {
            if (isset($this->rows[$id])) {
                $this->rows[$id]['published'] = true;
            }
        }
    }

    public function markFailed(string $id, string $error): void
    {
        if (isset($this->rows[$id])) {
            $this->rows[$id]['attempts']++;
            $this->rows[$id]['error'] = $error;
        }
    }

    /**
     * Test/inspection helper: the number of rows still pending publish.
     */
    public function pendingCount(): int
    {
        $pending = 0;
        foreach ($this->rows as $row) {
            if (! $row['published']) {
                $pending++;
            }
        }

        return $pending;
    }

    /**
     * Test/inspection helper: the recorded attempt count for one row (0 if unknown).
     */
    public function attemptsOf(string $id): int
    {
        return $this->rows[$id]['attempts'] ?? 0;
    }

    /**
     * Test/inspection helper: the last recorded error for one row ('' if none).
     */
    public function lastErrorOf(string $id): string
    {
        return $this->rows[$id]['error'] ?? '';
    }
}
