<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;

/**
 * The §6.4 **re-injection** consumer — the second half of PHP's Kafka retry-topic machinery, the
 * counterpart to {@see KafkaRetryRouter} (which routes a failure onto a tiered delay topic). It
 * reads `<workTopic>.retry.<delayMs>` records, **waits the tier delay** (cooperatively — there is no
 * broker timer), then **re-injects** each record into its original work topic (`bq-original-topic`)
 * and commits, so the work-topic worker re-processes it after the backoff.
 *
 * **Cooperative wait (§6.4):** a single blocking sleep longer than `max.poll.interval.ms` would
 * evict the consumer from its group (a rebalance, re-delivering the record). So the wait loops a
 * keep-alive {@see KafkaRetryConsumerClient::poll()} heartbeat plus a short `$sleep` until the delay
 * elapses — the consumer stays in its group while it waits. The wait is driven by injected `$now`
 * (current ms) and `$sleep` (sleep ms) seams, so it is **fully unit-testable with no real time**.
 *
 * **Attempts are NOT re-incremented here** — {@see KafkaRetryRouter} already bumped `bq-attempts`
 * when it routed the failure; the work-topic handler counts the attempt on re-processing. The
 * re-injected record is the decoded envelope re-encoded with its §6 `bq-` headers rebuilt (keeping
 * the current `bq-attempts`); `bq-trace-id` is preserved byte-for-byte (GR-4). The envelope is
 * unchanged (`schema_version` stays 1).
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
     * Run the re-injection loop: receive a retry record, wait its `bq-delay` (cooperatively, keeping
     * group membership alive), re-inject it into `bq-original-topic`, then commit. Idle (null)
     * receives are skipped. The loop runs until $shouldStop() returns true (omit it to run forever,
     * the standard worker model).
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

            $this->waitDelay($this->delayMs($headers));
            $this->reinject($raw['payload'], $headers);
            $this->client->commit();
        }
    }

    /**
     * Wait $delayMs cooperatively: until the deadline, heartbeat the consumer (keep-alive poll, no
     * offset advance) then sleep a short interval — so a long wait never exceeds `max.poll.interval.ms`
     * and evicts the consumer from its group. A non-positive delay re-injects immediately.
     */
    private function waitDelay(int $delayMs): void
    {
        if ($delayMs <= 0) {
            return;
        }

        $deadline = ($this->now)() + $delayMs;

        while (($remaining = $deadline - ($this->now)()) > 0) {
            $this->client->poll(0); // keep-alive heartbeat; does NOT advance the offset
            ($this->sleep)(min($remaining, self::HEARTBEAT_INTERVAL_MS));
        }
    }

    /**
     * Re-inject the retry record into its original work topic: decode the envelope, rebuild the §6
     * `bq-` headers (keeping the current `bq-attempts` — not re-incremented), and produce to
     * `bq-original-topic`. The `bq-delay` / `bq-original-topic` re-injection headers are dropped (the
     * record is back on the work topic now).
     *
     * @param  array<string, string>  $headers
     */
    private function reinject(string $payload, array $headers): void
    {
        $envelope = EnvelopeCodec::decode($payload);
        $workTopic = $this->originalTopic($headers, $envelope);

        $this->producer->produce(
            $workTopic,
            EnvelopeCodec::encode($envelope),
            $this->headers($envelope),
            ($this->now)(),
        );
    }

    /**
     * The §6 `bq-` header projection (UTF-8 strings; integers stringified), keeping the current
     * `bq-attempts` (the retry hop already bumped it). Mirrors {@see KafkaTransport} exactly.
     *
     * @param  array<string, mixed>  $envelope
     * @return array<string, string>
     */
    private function headers(array $envelope): array
    {
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $headers = [];

        $urn = EnvelopeCodec::urn($envelope);
        if ($urn !== '') {
            $headers['bq-job'] = $urn;
        }

        $traceId = $envelope['trace_id'] ?? null;
        if (is_string($traceId) && $traceId !== '') {
            $headers['bq-trace-id'] = $traceId;
        }

        if (isset($meta['id']) && is_scalar($meta['id'])) {
            $headers['bq-message-id'] = (string) $meta['id'];
        }

        if (isset($meta['schema_version']) && is_scalar($meta['schema_version'])) {
            $headers['bq-schema-version'] = (string) $meta['schema_version'];
        }

        if (isset($meta['lang']) && is_string($meta['lang']) && $meta['lang'] !== '') {
            $headers['bq-source-lang'] = $meta['lang'];
        }

        $attempts = $envelope['attempts'] ?? 0;
        $headers['bq-attempts'] = (string) (is_scalar($attempts) ? $attempts : 0);

        return $headers;
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
     * topic mapping the router stamped). Falls back to the body's `meta.queue` when the header is
     * absent (a hand-produced retry record), and to `default` as a last resort.
     *
     * @param  array<string, string>  $headers
     * @param  array<string, mixed>  $envelope
     */
    private function originalTopic(array $headers, array $envelope): string
    {
        $topic = $headers['bq-original-topic'] ?? null;
        if ($topic !== null && $topic !== '') {
            return $topic;
        }

        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];
        $queue = $meta['queue'] ?? 'default';

        return is_string($queue) && $queue !== '' ? $queue : 'default';
    }
}
