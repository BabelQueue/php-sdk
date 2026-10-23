<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\Exceptions\BabelQueueException;
use Throwable;

/**
 * The produce side of PHP's §6.4/§6.5 Kafka **retry-topic** machinery — the SDK-owned backoff +
 * dead-letter pattern Kafka can't give natively (no delayed delivery, no retry queue, no DLQ).
 *
 * A work-topic handler that **fails** hands the poison {@see KafkaMessage} to {@see self::route()};
 * the router either republishes it to the next **tiered delay topic** `<workTopic>.retry.<n>`
 * (`n` = the 1-based tier index in ascending delay order, e.g. `orders.retry.1`) with `bq-attempts`
 * incremented, or — once attempts are exhausted — produces it to `<workTopic>.dlq` with the
 * additive `dead_letter` block ({@see DeadLetter::annotate}, ADR-0009). It then returns, and the
 * work-topic consumer commits the original (so the work-topic partition advances —
 * process-then-commit, §6).
 *
 * A retry record carries two extra §6 headers so the {@see KafkaRetryConsumer} re-injection loop can
 * settle it: `bq-delay` (the tier delay in ms — there is no broker timer) and `bq-original-topic`
 * (the work topic to re-inject into). Every other `bq-` header (job/trace-id/message-id/schema-
 * version/source-lang) is rebuilt intact, and **`bq-trace-id` is preserved byte-for-byte across
 * every retry hop** (GR-4). The envelope itself is unchanged (`schema_version` stays 1) — only the
 * `bq-attempts` counter and the body's top-level `attempts` advance.
 *
 * The original record's **key** is carried onto the retry and DLQ records unchanged (null stays null,
 * empty stays empty, never synthesised) when the producer is a {@see KeyedKafkaProducer}; otherwise
 * the key is dropped as before. Partition equality is not guaranteed.
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
     * The RECOMMENDED retry-tier delays in ms (5 s, 30 s, 5 min, 30 min; broker-bindings.md §6.4).
     * Opt-in: pass it (or your own list) as `$delayTiersMs`; a router built with an empty list has no
     * tiers, so any retry or requested delay raises {@see BabelQueueException}.
     */
    public const DEFAULT_RETRY_TIERS_MS = [5000, 30000, 300000, 1800000];

    /** @var list<int> */
    private readonly array $delayTiersMs;

    /**
     * @param  KafkaProducer  $producer  the §6 producer seam used to republish retry / DLQ records
     * @param  list<int>  $delayTiersMs  backoff tiers in ms (default {@see self::DEFAULT_RETRY_TIERS_MS}); sorted
     *                                   ascending on construction so `retry.<n>` always means the
     *                                   n-th smallest delay, as in the Java/.NET/Node SDKs
     * @param  int  $maxAttempts  attempts before a record is dead-lettered instead of retried
     * @param  string  $retryInfix  the retry-topic infix (`<workTopic>.retry.<n>`, `n` = 1-based
     *                              tier index)
     * @param  string  $dlqSuffix  the dead-letter-topic suffix (`<workTopic>.dlq`)
     * @param  string  $topicPrefix  the §6.1 logical-queue → topic prefix (matches the producer's)
     */
    public function __construct(
        private readonly KafkaProducer $producer,
        array $delayTiersMs = self::DEFAULT_RETRY_TIERS_MS,
        private readonly int $maxAttempts = 5,
        private readonly string $retryInfix = '.retry.',
        private readonly string $dlqSuffix = '.dlq',
        private readonly string $topicPrefix = '',
    ) {
        sort($delayTiersMs);
        $this->delayTiersMs = $delayTiersMs;
    }

    /**
     * Route a work-topic record. Two separate rules (broker-bindings.md §6.4 vs §6.5), never merged:
     *
     * **Retry (no `$delayMs`, §6.5)** — a failed record. With `$next = attempts + 1`: when `$next`
     * would reach `maxAttempts` the record is **dead-lettered** to `<workTopic>.dlq` (returns false);
     * otherwise it is **retried** to tier `n = min(attempts, N-1) + 1` — `<workTopic>.retry.<n>`,
     * staying on the last tier once the tiers run out — with `bq-attempts = $next`, the body's
     * `attempts` synced, `bq-delay` = that tier's own delay, `bq-original-topic` and the key kept
     * (returns true). With no tiers configured the retry raises.
     *
     * **Requested delay (`$delayMs`, §6.4)** — a deliberate deferral, not a failure: the record goes to
     * the smallest tier whose delay is >= `$delayMs` with `bq-delay` = the REQUESTED delay (the
     * re-injection consumer then waits that long, bounded by the tier) and `attempts` unchanged; a delay
     * above the largest tier, or any delay with no tiers configured, raises. It is never dead-lettered.
     *
     * The caller returns after this so the original record commits. The `retry_backoff` parameters are
     * not applied on Kafka (ADR-0034 §9).
     *
     * @return bool  true when retried/deferred, false when dead-lettered (attempts exhausted)
     *
     * @throws BabelQueueException  no tiers configured, or `$delayMs` above the largest tier
     */
    public function route(KafkaMessage $message, ?Throwable $e = null, ?int $delayMs = null): bool
    {
        $envelope = $message->envelope();
        $workTopic = $this->workTopic($message);
        $nowMs = $this->nowMs();

        if ($delayMs !== null) {
            $this->defer($envelope, $workTopic, $delayMs, $nowMs, $message->recordKey());

            return true;
        }

        $next = $message->attempts() + 1;

        if ($next >= $this->maxAttempts) {
            $this->deadLetter($envelope, $workTopic, $next, $e, $nowMs, $message->recordKey());

            return false;
        }

        $this->retry($envelope, $workTopic, $next, $nowMs, $message->recordKey());

        return true;
    }

    /**
     * The retry topic of tier `$n` (1-based): `<workTopic>.retry.<n>`.
     */
    public function retryTopic(string $workTopic, int $n): string
    {
        return $workTopic . $this->retryInfix . $n;
    }

    /**
     * Republish to the tier delay topic for `$next`, with `bq-attempts`/the body's `attempts` bumped
     * to `$next` and the `bq-delay` / `bq-original-topic` re-injection headers attached.
     *
     * @param  array<string, mixed>  $envelope
     */
    private function retry(array $envelope, string $workTopic, int $next, int $nowMs, ?string $key): void
    {
        $this->requireTiers();

        $tierIndex = $this->tierIndexForAttempt($next);
        $tierMs = $this->delayTiersMs[$tierIndex];
        $envelope['attempts'] = $next;

        $headers = $this->headers($envelope, $next);
        $headers['bq-delay'] = (string) $tierMs;
        $headers['bq-original-topic'] = $workTopic;

        KafkaProduce::send(
            $this->producer,
            $this->retryTopic($workTopic, $tierIndex + 1),
            EnvelopeCodec::encode($envelope),
            $headers,
            $nowMs,
            $key,
        );
    }

    /**
     * Republish to the smallest tier >= `$delayMs` with `bq-delay` = `$delayMs` (the requested delay),
     * leaving `attempts` untouched.
     *
     * @param  array<string, mixed>  $envelope
     */
    private function defer(array $envelope, string $workTopic, int $delayMs, int $nowMs, ?string $key): void
    {
        $this->requireTiers();

        $tierIndex = null;
        foreach ($this->delayTiersMs as $index => $tierMs) {
            if ($tierMs >= $delayMs) {
                $tierIndex = $index;
                break;
            }
        }

        if ($tierIndex === null) {
            throw new BabelQueueException(sprintf(
                'Requested Kafka delay %d ms is above the largest retry tier (%d ms); broker-bindings.md §6.4.',
                $delayMs,
                $this->delayTiersMs[count($this->delayTiersMs) - 1],
            ));
        }

        $attempts = $envelope['attempts'] ?? 0;
        $headers = $this->headers($envelope, is_int($attempts) ? $attempts : 0);
        $headers['bq-delay'] = (string) $delayMs;
        $headers['bq-original-topic'] = $workTopic;

        KafkaProduce::send(
            $this->producer,
            $this->retryTopic($workTopic, $tierIndex + 1),
            EnvelopeCodec::encode($envelope),
            $headers,
            $nowMs,
            $key,
        );
    }

    private function requireTiers(): void
    {
        if ($this->delayTiersMs === []) {
            throw new BabelQueueException('No Kafka retry tiers are configured; pass delay tiers (e.g. KafkaRetryRouter::DEFAULT_RETRY_TIERS_MS); broker-bindings.md §6.4.');
        }
    }

    /**
     * Annotate the envelope with the additive `dead_letter` block and produce it to `<workTopic>.dlq`.
     *
     * @param  array<string, mixed>  $envelope
     */
    private function deadLetter(array $envelope, string $workTopic, int $attempts, ?Throwable $e, int $nowMs, ?string $key): void
    {
        $queue = $this->queue($envelope);
        $annotated = DeadLetter::annotate($envelope, 'failed', $e, $queue, $attempts);

        KafkaProduce::send(
            $this->producer,
            $workTopic . $this->dlqSuffix,
            EnvelopeCodec::encode($annotated),
            $this->headers($annotated, $attempts),
            $nowMs,
            $key,
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

    /** The 0-based tier index for a retry at `$next`: `min($next - 1, lastTier)`, floored at 0. */
    private function tierIndexForAttempt(int $next): int
    {
        return max(min($next - 1, count($this->delayTiersMs) - 1), 0);
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
