<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;

/**
 * The §6.4 **re-injection** consumer — the second half of PHP's Kafka retry-topic machinery, the
 * counterpart to {@see KafkaRetryRouter} (which routes a failure onto a tiered delay topic). It
 * reads `<workTopic>.retry.<n>` records (`n` = 1-based tier index, e.g. `orders.retry.1`), **waits
 * the record's delay**, then **re-injects** each record into its original work topic
 * (`bq-original-topic`) and commits, so the work-topic worker re-processes it after the backoff.
 *
 * **Wait target (§6.4, ADR-0033 A6):** the deadline is `record.timestamp + bq-delay`, clamped into
 * `[now, now + bq-delay]` — a record that already sat on the retry topic for longer than its delay
 * is moved at once, a record with a timestamp in the future (clock skew) never waits longer than the
 * delay itself, and a record without a timestamp (`timestamp_ms` absent) waits `bq-delay` from now.
 * Retry topics SHOULD be created with `message.timestamp.type=LogAppendTime` so the timestamp is the
 * broker's append time rather than a producer-supplied CreateTime.
 *
 * **Cooperative wait (§6.4):** a single blocking sleep longer than `max.poll.interval.ms` would
 * evict the consumer from its group (a rebalance, re-delivering the record). So the wait loops a
 * keep-alive {@see KafkaRetryConsumerClient::poll()} heartbeat plus a short `$sleep` until the delay
 * elapses; when the client is a {@see PausableKafkaRetryConsumerClient} the assignment is
 * **paused** for the wait and **resumed** after, so the heartbeat polls cannot fetch further records.
 * The wait is driven by injected `$now` (current ms) and `$sleep` (sleep ms) seams, so it is **fully
 * unit-testable with no real time**.
 *
 * **Re-injection is byte-for-byte (§6.4, ADR-0033 A7):** the record value and key are written to
 * `bq-original-topic` exactly as received — no decode, no re-encode, `attempts` untouched
 * ({@see KafkaRetryRouter} already bumped it when it routed the failure). Only the `bq-delay` and
 * `bq-original-topic` headers are dropped; every other header (`bq-*`, `traceparent`, application
 * headers) is carried over. A record that cannot be decoded is carried the same way, so the work
 * topic can still dead-letter an inspectable copy. The envelope is decoded only to find the target
 * topic (`meta.queue`) when `bq-original-topic` is absent.
 *
 * **Ack before commit:** the offset is committed only after the producer has written the record to
 * the work topic and the broker acknowledged it, so a crash in between re-delivers (at-least-once)
 * instead of losing the record. The {@see KafkaProducer} given here MUST therefore be synchronous —
 * `produce()` returns only once the write is acknowledged (e.g. `RdKafka\Producer::flush()`), and
 * throws on failure.
 *
 * Run it in its own process, separate from the work-topic worker:
 *
 *     (new KafkaRetryConsumer($retryClient, $producer))->consume($shouldStop);
 *
 * It is decoupled from `ext-rdkafka` behind the {@see KafkaRetryConsumerClient} and {@see
 * KafkaProducer} seams, so it is dependency-free and unit-tests against fakes.
 */
final class KafkaRetryConsumer
{
    /** The heartbeat cadence (ms) of the cooperative wait — short enough to beat `max.poll.interval.ms`. */
    private const HEARTBEAT_INTERVAL_MS = 1000;

    /** @var callable(): int */
    private $now;

    /** @var callable(int): void */
    private $sleep;

    /**
     * @param  KafkaRetryConsumerClient  $client  the retry-topic consumer seam (receive / poll / commit)
     * @param  KafkaProducer  $producer  the §6 producer seam used to re-inject into the work topic
     * @param  (callable(): int)|null  $now  returns the current time in ms (default real `microtime`)
     * @param  (callable(int): void)|null  $sleep  sleeps for the given ms (default real `usleep`)
     */
    public function __construct(
        private readonly KafkaRetryConsumerClient $client,
        private readonly KafkaProducer $producer,
        ?callable $now = null,
        ?callable $sleep = null,
    ) {
        $this->now = $now ?? static fn (): int => (int) round(microtime(true) * 1000);
        $this->sleep = $sleep ?? static function (int $ms): void {
            if ($ms > 0) {
                usleep($ms * 1000);
            }
        };
    }

    /**
     * Run the re-injection loop: receive a retry record, wait until its deadline (cooperatively,
     * keeping group membership alive), re-inject it byte-for-byte into `bq-original-topic`, then
     * commit. Idle (null) receives are skipped. The loop runs until $shouldStop() returns true (omit
     * it to run forever, the standard worker model).
     *
     * @param  (callable(): bool)|null  $shouldStop
     */
    public function consume(?callable $shouldStop = null): void
    {
        while ($shouldStop === null || ! $shouldStop()) {
            $raw = $this->client->receive();

            if ($raw === null) {
                continue;
            }

            $headers = $raw['headers'];

            $this->waitUntil($this->delayMs($headers), $raw['timestamp_ms'] ?? null);
            $this->reinject($raw['payload'], $headers, $raw['key'] ?? null);
            $this->client->commit();
        }
    }

    /**
     * Wait until `timestamp + delay` clamped into `[now, now + delay]` (A6), cooperatively: heartbeat
     * the consumer (keep-alive poll, no offset advance) then sleep a short interval, so a long wait
     * never exceeds `max.poll.interval.ms`. A non-positive delay, or a deadline already passed,
     * re-injects immediately.
     */
    private function waitUntil(int $delayMs, ?int $timestampMs): void
    {
        if ($delayMs <= 0) {
            return;
        }

        $now = ($this->now)();
        $deadline = $timestampMs === null
            ? $now + $delayMs
            : max($now, min($timestampMs + $delayMs, $now + $delayMs));

        if ($deadline <= $now) {
            return;
        }

        $pausable = $this->client instanceof PausableKafkaRetryConsumerClient ? $this->client : null;
        $pausable?->pause();

        try {
            while (($remaining = $deadline - ($this->now)()) > 0) {
                $this->client->poll(0); // keep-alive heartbeat; does NOT advance the offset
                ($this->sleep)(min($remaining, self::HEARTBEAT_INTERVAL_MS));
            }
        } finally {
            $pausable?->resume();
        }
    }

    /**
     * Re-inject the retry record into its original work topic **byte-for-byte**: the received value
     * and key are produced unchanged to `bq-original-topic`, with every header carried over except
     * `bq-delay` and `bq-original-topic` (the record is back on the work topic now). Nothing is
     * decoded or re-encoded; the envelope is parsed only to find the target topic when
     * `bq-original-topic` is absent. The key is carried when the producer is a {@see KeyedKafkaProducer}.
     *
     * @param  array<string, string>  $headers
     */
    private function reinject(string $payload, array $headers, ?string $key): void
    {
        $workTopic = $this->originalTopic($headers, $payload);

        unset($headers['bq-delay'], $headers['bq-original-topic']);

        KafkaProduce::send($this->producer, $workTopic, $payload, $headers, ($this->now)(), $key);
    }

    /**
     * The tier delay (ms) to wait, from the `bq-delay` header (0 when absent → re-inject immediately).
     *
     * @param  array<string, string>  $headers
     */
    private function delayMs(array $headers): int
    {
        $delay = $headers['bq-delay'] ?? null;

        return $delay !== null && $delay !== '' ? (int) $delay : 0;
    }

    /**
     * The work topic to re-inject into, from the `bq-original-topic` header (the §6.1 logical-queue →
     * topic mapping the router stamped). Only when the header is absent (a hand-produced retry
     * record) is the body decoded, for its `meta.queue`; `default` is the last resort.
     *
     * @param  array<string, string>  $headers
     */
    private function originalTopic(array $headers, string $payload): string
    {
        $topic = $headers['bq-original-topic'] ?? null;
        if ($topic !== null && $topic !== '') {
            return $topic;
        }

        $envelope = EnvelopeCodec::decode($payload);
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];
        $queue = $meta['queue'] ?? 'default';

        return is_string($queue) && $queue !== '' ? $queue : 'default';
    }
}
