<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Consume;

use BabelQueue\Consume\Dispatcher;
use BabelQueue\Consume\RetryBackoff;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

/**
 * The opt-in consume-side retry backoff (ADR-0034 §9): `d(n) = min(max, base * multiplier^(n-1))`,
 * optional full jitter, and the Dispatcher's whole-second delay for a transport release.
 */
final class RetryBackoffTest extends TestCase
{
    public function test_defaults_keep_todays_behaviour(): void
    {
        $backoff = new RetryBackoff();

        $this->assertSame(0.0, $backoff->delaySeconds(1));
        $this->assertSame(0.0, $backoff->delaySeconds(7));
    }

    public function test_constant_delay_with_multiplier_one(): void
    {
        $backoff = new RetryBackoff(retryBackoff: 5);

        $this->assertSame(5.0, $backoff->delaySeconds(1));
        $this->assertSame(5.0, $backoff->delaySeconds(4));
    }

    public function test_exponential_growth_is_capped_by_the_max(): void
    {
        $backoff = new RetryBackoff(retryBackoff: 2, multiplier: 2, retryBackoffMax: 20);

        $this->assertSame(2.0, $backoff->delaySeconds(1));
        $this->assertSame(4.0, $backoff->delaySeconds(2));
        $this->assertSame(16.0, $backoff->delaySeconds(4));
        $this->assertSame(20.0, $backoff->delaySeconds(5));
        $this->assertSame(20.0, $backoff->delaySeconds(50));
    }

    public function test_max_zero_means_no_cap(): void
    {
        $this->assertSame(1024.0, (new RetryBackoff(retryBackoff: 1, multiplier: 2))->delaySeconds(11));
    }

    public function test_full_jitter_draws_uniformly_below_the_deterministic_delay(): void
    {
        $backoff = new RetryBackoff(retryBackoff: 10, multiplier: 2, retryJitter: RetryBackoff::JITTER_FULL, random: static fn (): float => 0.5);

        $this->assertSame(5.0, $backoff->delaySeconds(1));
        $this->assertSame(10.0, $backoff->delaySeconds(2));
        $this->assertSame(10.0, $backoff->baseDelaySeconds(1));
    }

    public function test_invalid_parameters_are_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new RetryBackoff(retryBackoff: -1);
    }

    public function test_invalid_jitter_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new RetryBackoff(retryJitter: 'half');
    }

    public function test_dispatcher_rounds_the_delay_up_to_whole_seconds_for_attempts_plus_one(): void
    {
        $dispatcher = new Dispatcher(retryBackoff: new RetryBackoff(retryBackoff: 0.5, multiplier: 3));

        // attempts 0 -> n=1 -> 0.5 -> 1 ; attempts 2 -> n=3 -> 4.5 -> 5
        $this->assertSame(1, $dispatcher->retryDelaySeconds(new FakeConsumedMessage([], 0)));
        $this->assertSame(5, $dispatcher->retryDelaySeconds(new FakeConsumedMessage([], 2)));
    }

    public function test_dispatcher_without_a_backoff_releases_immediately(): void
    {
        $this->assertSame(0, (new Dispatcher())->retryDelaySeconds(new FakeConsumedMessage([], 3)));
    }
}
