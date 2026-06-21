<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Idempotency;

use BabelQueue\Idempotency\RedisStore;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use Predis\ClientInterface;
use Predis\Response\Status;

/**
 * The persistent Redis {@see \BabelQueue\Idempotency\ClaimingStore} (ADR-0022): its atomic claim is
 * `SET key value NX PX <ttl_ms>`. These tests pin the *exact* command the store issues against a
 * Mockery-mocked predis client — so the claim/commit/release/expiry contract is verified with **no
 * broker** (the `NX`/`PX` semantics are Redis's; here we prove the store invokes them correctly).
 */
final class RedisStoreTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_claim_issues_set_with_nx_and_px_ttl(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        // The atomic claim: NX (only-if-absent) + PX (ttl in ms). 60s → 60000ms.
        $client->shouldReceive('set')
            ->once()
            ->with('bq:idem:msg-1', '1', 'PX', 60000, 'NX')
            ->andReturn(new Status('OK'));

        $store = new RedisStore($client);

        self::assertTrue($store->claim('msg-1', 60));
    }

    public function test_claim_is_refused_when_nx_declines(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        // NX fails on an existing key → predis returns null → claim refused (parked).
        $client->shouldReceive('set')
            ->once()
            ->with('bq:idem:msg-1', '1', 'PX', 60000, 'NX')
            ->andReturnNull();

        $store = new RedisStore($client);

        self::assertFalse($store->claim('msg-1', 60));
    }

    public function test_a_non_positive_ttl_falls_back_to_the_default(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        // ttl<=0 → DEFAULT_TTL (3600s = 3_600_000ms), so a crashed owner still expires.
        $client->shouldReceive('set')
            ->once()
            ->with('bq:idem:msg-1', '1', 'PX', 3_600_000, 'NX')
            ->andReturn(new Status('OK'));

        $store = new RedisStore($client);

        self::assertTrue($store->claim('msg-1', 0));
    }

    public function test_remember_sets_a_permanent_seen_key_without_ttl(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        // Commit overwrites with the SEEN sentinel and NO expiry (a committed id is permanent).
        $client->shouldReceive('set')->once()->with('bq:idem:msg-1', '2');

        (new RedisStore($client))->remember('msg-1');
    }

    public function test_seen_is_true_only_for_the_committed_sentinel(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        // Redis GET returns the stored string verbatim (a plain string, not a Status reply).
        $client->shouldReceive('get')->once()->with('bq:idem:committed')->andReturn('2');
        $client->shouldReceive('get')->once()->with('bq:idem:inflight')->andReturn('1');
        $client->shouldReceive('get')->once()->with('bq:idem:absent')->andReturnNull();

        $store = new RedisStore($client);

        self::assertTrue($store->seen('committed'));   // SEEN sentinel
        self::assertFalse($store->seen('inflight'));   // a claim is not yet "seen"
        self::assertFalse($store->seen('absent'));     // no key at all
    }

    public function test_release_compare_and_deletes_only_an_inflight_claim(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        // A Lua compare-and-delete guards against clobbering a committed key on a release/commit race.
        $client->shouldReceive('eval')
            ->once()
            ->with(Mockery::type('string'), 1, 'bq:idem:msg-1', '1');

        (new RedisStore($client))->release('msg-1');
    }

    public function test_forget_deletes_the_key(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('del')->once()->with('bq:idem:msg-1');

        (new RedisStore($client))->forget('msg-1');
    }

    public function test_a_custom_prefix_namespaces_keys(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('set')
            ->once()
            ->with('orders:idem:msg-1', '1', 'PX', 60000, 'NX')
            ->andReturn(new Status('OK'));

        $store = new RedisStore($client, 'orders:idem:');

        self::assertTrue($store->claim('msg-1', 60));
    }
}
