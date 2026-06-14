<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\DeadLetter;
use Throwable;

/**
 * The produce side of PHP's §6.4/§6.5 Kafka **retry-topic** machinery — the SDK-owned backoff +
 * dead-letter pattern Kafka can't give natively (no delayed delivery, no retry queue, no DLQ).
 *
 * A work-topic handler that **fails** hands the poison {@see KafkaMessage} to {@see self::route()};
 * the router either republishes it to the next **tiered delay topic** `<workTopic>.retry.<delayMs>`
 * (e.g. `orders.retry.5000`) with `bq-attempts` incremented, or — once attempts are exhausted —
 * produces it to `<workTopic>.dlq` with the additive `dead_letter` block ({@see DeadLetter::annotate},
 * ADR-0009). It then returns, and the work-topic consumer commits the original (so the work-topic
 * partition advances — process-then-commit, §6).
 *
 * A retry record carries two extra §6 headers so the {@see KafkaRetryConsumer} re-injection loop can
 * settle it: `bq-delay` (the tier delay in ms — there is no broker timer) and `bq-original-topic`
 * (the work topic to re-inject into). Every other `bq-` header (job/trace-id/message-id/schema-
 * version/source-lang) is rebuilt intact, and **`bq-trace-id` is preserved byte-for-byte across
 * every retry hop** (GR-4). The envelope itself is unchanged (`schema_version` stays 1) — only the
 * `bq-attempts` counter and the body's top-level `attempts` advance.
 *
 * It is decoupled from `ext-rdkafka` behind the existing {@see KafkaProducer} seam, so it is
 * dependency-free and unit-tests against a fake.
 *
 * **Wiring** (two cooperating processes): the work-topic worker routes a failure, then returns so
 * the original commits —
 *
 *     $router = new KafkaRetryRouter($producer);
 *     (new KafkaConsumer($workClient))->consume(function (KafkaMessage $m) use ($router): void {
 *         try {
 *             handle($m);
 *         } catch (\Throwable $e) {
 *             $router->route($m, $e); // → orders.retry.<tier> or orders.dlq
 *         }
 *     });
 *
 * — and a separate process runs {@see KafkaRetryConsumer::consume()} over the retry topics to wait
 * the tier delay and re-inject into the work topic. Because `bq-attempts` now grows across every
 * retry hop, the {@see \BabelQueue\Consume\Dispatcher} `maxAttempts` dead-letter cap becomes
 * effective on Kafka (it was effective only on Pulsar's native redelivery count before).
 */
final class KafkaRetryRouter
{
    /**
     * @param  KafkaProducer  $producer  the §6 producer seam used to republish retry / DLQ records
     * @param  list<int>  $delayTiersMs  ascending backoff tiers in ms (default 5s, 30s, 5m, 30m)
     * @param  int  $maxAttempts  attempts before a record is dead-lettered instead of retried
     * @param  string  $retryInfix  the retry-topic infix (`<workTopic>.retry.<delayMs>`)
     * @param  string  $dlqSuffix  the dead-letter-topic suffix (`<workTopic>.dlq`)
     * @param  string  $topicPrefix  the §6.1 logical-queue → topic prefix (matches the producer's)
     */
    public function __construct(
        private readonly KafkaProducer $producer,
        private readonly array $delayTiersMs = [5000, 30000, 300000, 1800000],
        private readonly int $maxAttempts = 5,
        private readonly string $retryInfix = '.retry.',
        private readonly string $dlqSuffix = '.dlq',
        private readonly string $topicPrefix = '',
    ) {
    }

    /**
     * Route a failed work-topic record. With `$next = attempts + 1`: when `$next` would reach
     * `maxAttempts` the record is **dead-lettered** to `<workTopic>.dlq` (returns false — exhausted);
     * otherwise it is **retried** to the tier delay topic for `$next` with `bq-attempts = $next`
     * (returns true — retried). The caller returns after this so the original record commits.
     *
     * @return bool  true when retried, false when dead-lettered (attempts exhausted)
     */
    public function route(KafkaMessage $message, ?Throwable $e = null): bool
    {
        $envelope = $message->envelope();
        $workTopic = $this->workTopic($message);
        $next = $message->attempts() + 1;
        $nowMs = $this->nowMs();

        if ($next >= $this->maxAttempts) {
            $this->deadLetter($envelope, $workTopic, $next, $e, $nowMs);

            return false;
        }

        $this->retry($envelope, $workTopic, $next, $nowMs);

        return true;
    }

    /**
     * Republish to the tier delay topic for `$next`, with `bq-attempts`/the body's `attempts` bumped
     * to `$next` and the `bq-delay` / `bq-original-topic` re-injection headers attached.
     *
     * @param  array<string, mixed>  $envelope
     */
    private function retry(array $envelope, string $workTopic, int $next, int $nowMs): void
    {
        $tierMs = $this->tierForAttempt($next);
        $envelope['attempts'] = $next;

        $headers = $this->headers($envelope, $next);
        $headers['bq-delay'] = (string) $tierMs;
        $headers['bq-original-topic'] = $workTopic;

        $this->producer->produce(
            $workTopic . $this->retryInfix . $tierMs,
            EnvelopeCodec::encode($envelope),
            $headers,
            $nowMs,
        );
    }

    /**
     * Annotate the envelope with the additive `dead_letter` block and produce it to `<workTopic>.dlq`.
     *
     * @param  array<string, mixed>  $envelope
     */
    private function deadLetter(array $envelope, string $workTopic, int $attempts, ?Throwable $e, int $nowMs): void
    {
        $queue = $this->queue($envelope);
        $annotated = DeadLetter::annotate($envelope, 'failed', $e, $queue, $attempts);

        $this->producer->produce(
            $workTopic . $this->dlqSuffix,
            EnvelopeCodec::encode($annotated),
            $this->headers($annotated, $attempts),
            $nowMs,
        );
    }

    /**
     * The §6.1 work topic: the `bq-original-topic` header when the record is already in the retry
     * chain (so retries route back to the right work topic across hops), else `<prefix><meta.queue>`.
     */
    private function workTopic(KafkaMessage $message): string
    {
        $original = $message->header('bq-original-topic');
        if ($original !== null && $original !== '') {
            return $original;
        }

        return $this->topicPrefix . $this->queue($message->envelope());
    }

    /**
     * The logical queue name from `meta.queue` (default `default`).
     *
     * @param  array<string, mixed>  $envelope
     */
    private function queue(array $envelope): string
    {
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];
        $queue = $meta['queue'] ?? 'default';

        return is_string($queue) && $queue !== '' ? $queue : 'default';
    }

    /** The tier delay (ms) for a retry at `$next` (tier index `min($next - 1, lastTier)`). */
    private function tierForAttempt(int $next): int
    {
        $index = min($next - 1, count($this->delayTiersMs) - 1);

        return $this->delayTiersMs[max($index, 0)];
    }

    /**
     * The §6 `bq-` header projection (UTF-8 strings; integers stringified) with `bq-attempts` set to
     * `$attempts` (the incremented value for a retry hop). Mirrors {@see KafkaTransport} exactly.
     *
     * @param  array<string, mixed>  $envelope
     * @return array<string, string>
     */
    private function headers(array $envelope, int $attempts): array
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

        $headers['bq-attempts'] = (string) $attempts;

        return $headers;
    }

    /** Current Unix time in milliseconds — the CreateTime stamped on the republished record. */
    private function nowMs(): int
    {
        return (int) round(microtime(true) * 1000);
    }
}
