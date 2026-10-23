<?php

declare(strict_types=1);

namespace BabelQueue\Consume;

use InvalidArgumentException;

/**
 * The worker-side retry backoff of ADR-0034 §9 — how long a failed message should wait before the
 * next attempt on the transports whose worker path releases with a delay (SQS visibility timeout,
 * Redis delayed set, AMQP). Pure and broker-free:
 *
 *     d(n) = min(max, base * multiplier^(n-1))      n = attempts + 1   (1-based)
 *
 * `max = 0` means no cap; `jitter: full` replaces `d(n)` with `uniform(0, d(n))` (AWS "full jitter").
 * The defaults (`base 0`, `multiplier 1`, `max 0`, `jitter none`) yield 0 for every attempt — the
 * behaviour before the helper existed, so existing consumers are unchanged until they opt in.
 *
 * Not applied on Kafka (broker-bindings.md §6.5: retry topics carry their own tier delays) and it
 * does not touch Laravel's own job `backoff`. Transport limits (e.g. SQS's 12 h visibility ceiling)
 * still apply on top of the value returned here.
 */
final class RetryBackoff
{
    public const JITTER_NONE = 'none';

    public const JITTER_FULL = 'full';

    /** @var callable(): float */
    private $random;

    /**
     * @param  float|int  $retryBackoff  base delay in seconds (0 disables backoff)
     * @param  float|int  $multiplier  growth factor per attempt (1 = constant)
     * @param  float|int  $retryBackoffMax  cap in seconds (0 = no cap)
     * @param  string  $retryJitter  `none` (default) or `full`
     * @param  (callable(): float)|null  $random  uniform draw in [0, 1) for `full` jitter (default `mt_rand`)
     */
    public function __construct(
        private readonly float|int $retryBackoff = 0,
        private readonly float|int $multiplier = 1,
        private readonly float|int $retryBackoffMax = 0,
        private readonly string $retryJitter = self::JITTER_NONE,
        ?callable $random = null,
    ) {
        if ($retryBackoff < 0 || $multiplier < 0 || $retryBackoffMax < 0) {
            throw new InvalidArgumentException('retry_backoff, retry_backoff_multiplier and retry_backoff_max must not be negative.');
        }

        if ($retryJitter !== self::JITTER_NONE && $retryJitter !== self::JITTER_FULL) {
            throw new InvalidArgumentException(sprintf('retry_jitter must be "none" or "full", got "%s".', $retryJitter));
        }

        $this->random = $random ?? static fn (): float => mt_rand() / (mt_getrandmax() + 1);
    }

    /**
     * The deterministic `d(n)` (seconds) before jitter.
     *
     * @param  int  $n  1-based attempt number (`attempts + 1`); values below 1 count as 1
     */
    public function baseDelaySeconds(int $n): float
    {
        $delay = (float) $this->retryBackoff * ((float) $this->multiplier ** (max($n, 1) - 1));

        if (is_nan($delay)) {
            $delay = 0.0; // 0 * INF
        }

        if ($this->retryBackoffMax > 0) {
            $delay = min($delay, (float) $this->retryBackoffMax);
        }

        return $delay;
    }

    /**
     * The delay (seconds) before attempt `$n`: `d(n)`, or `uniform(0, d(n))` with full jitter.
     *
     * @param  int  $n  1-based attempt number (`attempts + 1`)
     */
    public function delaySeconds(int $n): float
    {
        $delay = $this->baseDelaySeconds($n);

        if ($this->retryJitter === self::JITTER_FULL && $delay > 0.0 && is_finite($delay)) {
            return $delay * ($this->random)();
        }

        return $delay;
    }
}
