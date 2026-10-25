<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * The seam {@see KafkaConsumer} consumes through — poll the next record and commit offsets on an
 * `ext-rdkafka` consumer. Implement it with a one-line adapter around `RdKafka\KafkaConsumer`; the
 * consumer stays dependency-free and unit-tests against a fake.
 *
 * **GR-7 note:** like the producer, the only viable PHP Kafka client is the **C extension**
 * `ext-rdkafka` (ADR-0019), so this is the opt-in, GR-7-relaxed path (the seam keeps the core and
 * its test suite extension-free). Consume is **process-then-commit** (at-least-once): the adapter
 * polls with a timeout (returning null when idle) and {@see self::commit()} commits the offset of
 * the **last received** record, which {@see KafkaConsumer} calls only after a handler succeeds.
 */
interface KafkaConsumerClient
{
    /**
     * Poll the next record, or null if none arrived within the poll timeout. The `headers` carry
     * the §6 `bq-` record headers (UTF-8 strings) — `bq-attempts` is the authoritative attempt
     * counter (Kafka has no native delivery count).
     *
     * The optional `key` is the record key bytes as received (omit it, or null, for a null key; an
     * empty string is a real, empty key) — carried through the retry / dead-letter writes
     * ({@see KeyedKafkaProducer}).
     *
     * The `topic` is the topic the record was read from. Report it: it is the consumer's queue, and it alone
     * names `<topic>.dlq` for both the {@see \BabelQueue\Consume\Dispatcher} and a poison record
     * (ADR-0036 decision 3). Without it the consumer falls back to its `$topic` argument, then to a single-topic
     * {@see KafkaSubscriptionDescriptor}. If none names it, the body's `meta.queue` is still never used and
     * `default.dlq` is never written: a poison record is committed and dropped with a log line (section 6.5), and the
     * Dispatcher rethrows for a message that needs the DLQ.
     *
     * @return array{payload: string, headers: array<string, string>, key?: string|null, topic?: string|null}|null
     */
    public function receive(): ?array;

    /** Commit the offset of the last received record (process-then-commit, at-least-once). */
    public function commit(): void;
}
