<?php

declare(strict_types=1);

namespace BabelQueue\Outbox;

use BabelQueue\Contracts\Transport;
use Throwable;

/**
 * The **read/publish side** of the transactional outbox (ADR-0029): drain pending rows
 * the {@see Outbox} writer committed and forward each onto the broker through the frozen
 * {@see Transport} contract, marking every row published or failed.
 *
 * Run it on a short interval (a worker loop, a scheduled command) *after* the business
 * transaction commits. Because the message was committed atomically with the business
 * data, the relay is the only thing standing between "row exists" and "broker has it" —
 * and it only ever reads already-durable rows, so it never invents work.
 *
 * **Semantics — at-least-once handoff:**
 * - A row is marked **published only after** {@see Transport::publish()} returns; if the
 *   process dies between publish and {@see OutboxStore::markPublished()}, the row stays
 *   pending and is published **again** on the next pass. That is at-least-once: a
 *   downstream consumer must dedupe on the canonical `meta.id`
 *   ({@see \BabelQueue\Idempotency\Idempotent} is exactly that guard, the consumer-side
 *   mirror of this producer-side helper — ADR-0022).
 * - A publish that **throws** is caught, {@see OutboxStore::markFailed()} records the
 *   error and bumps the attempt count, and the row stays pending for a later retry. One
 *   poison row never blocks the rest of the batch.
 * - **`trace_id` is preserved end-to-end** (GR-4): the relay publishes the stored bytes
 *   *verbatim* — it never decodes, rebuilds or re-encodes the envelope — so the body that
 *   reaches the broker is byte-identical to what was stored (GR-1/GR-5).
 *
 * **Backoff:** between a failed publish and the next attempt within the same pass the
 * relay sleeps for a bounded, linearly-growing delay (capped), to avoid hammering a
 * broker that is briefly down. The sleeper is injectable so tests stay instant.
 */
final class OutboxRelay
{
    /** Hard safety ceiling on {@see drain()} passes when the caller passes 0. */
    private const DEFAULT_DRAIN_CEILING = 10000;

    /** @var callable(int): void */
    private $sleeper;

    /**
     * @param  Transport  $transport  Where published rows go (the same publish-only seam
     *                                every framework-less producer uses).
     * @param  OutboxStore  $store     The outbox to drain.
     * @param  int  $batchSize         How many rows to reserve and publish per {@see flush()}.
     * @param  int  $backoffStepMs     Base backoff added per prior attempt, in milliseconds.
     * @param  int  $backoffCapMs      Upper bound on a single backoff sleep, in milliseconds.
     * @param  (callable(int): void)|null  $sleeper  Sleep $ms milliseconds; defaults to
     *                                `usleep`. Inject a no-op in tests.
     */
    public function __construct(
        private readonly Transport $transport,
        private readonly OutboxStore $store,
        private readonly int $batchSize = 100,
        private readonly int $backoffStepMs = 50,
        private readonly int $backoffCapMs = 5000,
        ?callable $sleeper = null,
    ) {
        $this->sleeper = $sleeper ?? static function (int $ms): void {
            if ($ms > 0) {
                usleep($ms * 1000);
            }
        };
    }

    /**
     * Publish one batch of pending rows. Each row that the transport accepts is marked
     * published; each that throws is marked failed (with a backoff before continuing) and
     * left pending. Returns a per-pass tally. Call it repeatedly (a loop / cron) to drain
     * the outbox; {@see drain()} loops until it is empty.
     */
    public function flush(): OutboxRelayResult
    {
        $records = $this->store->fetchUnpublished($this->batchSize);

        $publishedIds = [];
        $failed = 0;

        foreach ($records as $record) {
            try {
                $this->transport->publish($record->body, $record->queue);
                $publishedIds[] = $record->id;
            } catch (Throwable $e) {
                $this->store->markFailed($record->id, $this->reason($e));
                $failed++;
                $this->sleep($this->backoffFor($record->attempts));
            }
        }

        if ($publishedIds !== []) {
            $this->store->markPublished($publishedIds);
        }

        return new OutboxRelayResult(count($publishedIds), $failed);
    }

    /**
     * Drain the outbox by repeatedly calling {@see flush()} while each pass keeps making
     * progress (publishes at least one row), then return the cumulative tally. The loop
     * stops as soon as a pass publishes nothing — the outbox is empty, or only currently
     * failing rows remain (those are left pending for a future {@see drain()} call once the
     * broker recovers). $maxPasses is a hard safety ceiling so a degenerate store can never
     * spin forever (0 = a generous internal default).
     */
    public function drain(int $maxPasses = 0): OutboxRelayResult
    {
        $ceiling = $maxPasses > 0 ? $maxPasses : self::DEFAULT_DRAIN_CEILING;
        $published = 0;
        $failed = 0;

        for ($pass = 0; $pass < $ceiling; $pass++) {
            $result = $this->flush();
            $published += $result->published;
            $failed += $result->failed;

            // No progress this pass → drained, or only failing rows remain. Stop.
            if ($result->published === 0) {
                break;
            }
        }

        return new OutboxRelayResult($published, $failed);
    }

    /**
     * The backoff (ms) for a row that has already failed $priorAttempts times: a linear
     * step per attempt, capped. Kept simple and deterministic so the budget is obvious.
     */
    private function backoffFor(int $priorAttempts): int
    {
        $delay = $this->backoffStepMs * max(1, $priorAttempts + 1);

        return min($delay, $this->backoffCapMs);
    }

    private function sleep(int $ms): void
    {
        ($this->sleeper)($ms);
    }

    /** A short, safe failure reason from a thrown error (class + message, no stack). */
    private function reason(Throwable $e): string
    {
        return $e::class . ': ' . $e->getMessage();
    }
}
