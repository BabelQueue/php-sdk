<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

/**
 * The seam {@see KafkaRedriveIO} reads and positions a Kafka consumer through — poll a topic, commit
 * a partition offset and seek a partition back. Implement it with a thin adapter around
 * `RdKafka\KafkaConsumer`; the IO stays dependency-free and unit-tests against a fake (the same
 * `ext-rdkafka` opt-in as {@see \BabelQueue\Transport\KafkaConsumerClient}, ADR-0019).
 *
 * The consumer must have auto-commit disabled and an explicit `auto.offset.reset`; the IO decides
 * when an offset is committed (only after the redrive write succeeded, process-then-commit).
 */
interface KafkaRedriveClient
{
    /**
     * Poll the next record of $topic (subscribing / assigning on first use), or null if none arrived
     * within the poll timeout. `headers` are the record headers as received (UTF-8 strings), `key` the
     * key bytes as received (null = a null key; an empty string is a real, empty key).
     *
     * @return array{topic: string, partition: int, offset: int, payload: string, headers: array<string, string>, key?: string|null}|null
     */
    public function poll(string $topic): ?array;

    /** Commit that the record at $offset of $topic/$partition is done: the committed position becomes $offset + 1. */
    public function commitOffset(string $topic, int $partition, int $offset): void;

    /** Move the fetch position of $topic/$partition to $offset, so the record there is delivered again. */
    public function seek(string $topic, int $partition, int $offset): void;
}
