<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * The seam {@see KafkaRetryConsumer} consumes the §6.4 retry topics through — poll a retry record,
 * keep group membership alive during the cooperative delay wait, and commit offsets, on an
 * `ext-rdkafka` consumer. A separate seam from {@see KafkaConsumerClient} (its example adapter has
 * no keep-alive primitive, so extending it would break it) — implement this with a one-line adapter
 * around `RdKafka\KafkaConsumer`; the re-injection consumer stays dependency-free and unit-tests
 * against a fake.
 *
 * **GR-7 note:** like the producer/consumer, the only viable PHP Kafka client is the **C extension**
 * `ext-rdkafka` (ADR-0019), so this is the opt-in, GR-7-relaxed path (the seam keeps the core and
 * its test suite extension-free).
 *
 * **Why the keep-alive matters (§6.4):** Kafka has no broker delay timer, so the re-injection
 * consumer must wait the tier delay itself before re-injecting. A single blocking sleep longer than
 * `max.poll.interval.ms` would make the broker think the consumer died and **evict it from its
 * group** (triggering a rebalance and re-delivering the record). So the wait is **cooperative**:
 * {@see KafkaRetryConsumer} calls {@see self::poll()} (a `poll(0)`-style heartbeat — or a
 * partition-pause + heartbeat — that does NOT advance the offset) at intervals across the delay, so
 * the consumer stays alive in its group while it waits.
 */
interface KafkaRetryConsumerClient
{
    /**
     * Poll the next retry record, or null if none arrived within the poll timeout. The `headers`
     * carry the §6 `bq-` record headers (UTF-8 strings) — `bq-delay` is the tier delay in ms to wait
     * and `bq-original-topic` is the work topic to re-inject into.
     *
     * @return array{payload: string, headers: array<string, string>}|null
     */
    public function receive(): ?array;

    /**
     * Keep the consumer alive in its group during the cooperative delay wait **without advancing the
     * offset** — a `poll(0)`-style heartbeat (or a partition-pause + heartbeat). Called repeatedly by
     * {@see KafkaRetryConsumer} across the wait so a delay longer than `max.poll.interval.ms` does
     * not evict the consumer from its group.
     *
     * @param  int  $timeoutMs  the poll timeout in ms (0 for a non-blocking heartbeat)
     */
    public function poll(int $timeoutMs): void;

    /** Commit the offset of the last received record (after it has been re-injected into the work topic). */
    public function commit(): void;
}
