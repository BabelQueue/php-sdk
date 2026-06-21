<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Idempotency;

use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Idempotency\ClaimingDispatch;
use BabelQueue\Idempotency\ClaimingStore;
use BabelQueue\Idempotency\ClaimParkedException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * A process-local {@see ClaimingStore} that honours the atomic claim contract (claim is exclusive
 * until released/committed/expired) — the in-memory analogue of {@see \BabelQueue\Idempotency\PdoStore}
 * / {@see \BabelQueue\Idempotency\RedisStore} for driving {@see ClaimingDispatch} without a backend.
 */
final class FakeClaimingStore implements ClaimingStore
{
    /** @var array<string, true> committed ids */
    public array $committed = [];

    /** @var array<string, int> id => expires_at (epoch seconds) for in-flight claims */
    public array $claims = [];

    public int $now = 1_000;

    /**
     * Ids that a "peer worker" commits at the instant our claim() loses — to simulate the race where
     * seen() is false on entry, claim() then loses, and a re-check finds the id already committed.
     *
     * @var array<string, true>
     */
    public array $commitOnLostClaim = [];

    public function seen(string $messageId): bool
    {
        return isset($this->committed[$messageId]);
    }

    public function claim(string $messageId, int $ttlSeconds): bool
    {
        if (isset($this->committed[$messageId])) {
            return false;
        }
        if (isset($this->claims[$messageId]) && $this->claims[$messageId] > $this->now) {
            // An unexpired claim is held by a peer. Simulate that peer committing right now, so the
            // dispatcher's re-check of seen() observes a now-committed id.
            if (isset($this->commitOnLostClaim[$messageId])) {
                $this->remember($messageId);
            }

            return false;
        }
        $this->claims[$messageId] = $this->now + ($ttlSeconds > 0 ? $ttlSeconds : 3600);

        return true;
    }

    public function remember(string $messageId): void
    {
        unset($this->claims[$messageId]);
        $this->committed[$messageId] = true;
    }

    public function forget(string $messageId): void
    {
        unset($this->claims[$messageId], $this->committed[$messageId]);
    }

    public function release(string $messageId): void
    {
        unset($this->claims[$messageId]);
    }
}

/**
 * The claim-based dispatch wrapper (ADR-0022): under a shared persistent store, exactly one of N
 * concurrent deliveries of the same id runs; a duplicate after commit skips; a concurrent in-flight
 * delivery parks (throws so it is redelivered, not acked); a thrown handler releases the claim.
 */
final class ClaimingDispatchTest extends TestCase
{
    public function test_runs_on_first_delivery_and_commits(): void
    {
        $store = new FakeClaimingStore();
        $calls = 0;
        $handler = ClaimingDispatch::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('msg-1'));

        self::assertSame(1, $calls);
        self::assertTrue($store->seen('msg-1'));
    }

    public function test_skips_a_duplicate_delivery_after_commit(): void
    {
        $store = new FakeClaimingStore();
        $calls = 0;
        $handler = ClaimingDispatch::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('msg-1'));
        $handler($this->message('msg-1')); // duplicate delivery → already committed → skipped

        self::assertSame(1, $calls);
    }

    public function test_a_concurrent_inflight_delivery_parks_for_redelivery(): void
    {
        $store = new FakeClaimingStore();
        // Simulate a peer worker holding an unexpired claim on the same id.
        $store->claim('msg-1', 60);

        $handler = ClaimingDispatch::wrap($store, function (ConsumedMessage $m): void {
            self::fail('the handler must not run while a peer holds the claim');
        });

        $this->expectException(ClaimParkedException::class);
        $handler($this->message('msg-1'));
    }

    public function test_a_lost_claim_that_was_meanwhile_committed_is_skipped_not_parked(): void
    {
        $store = new FakeClaimingStore();
        // The peer already committed between our seen()-check and our claim().
        $store->remember('msg-1');

        $calls = 0;
        $handler = ClaimingDispatch::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('msg-1')); // committed → returns (acked), does not park

        self::assertSame(0, $calls);
    }

    public function test_losing_a_claim_to_a_just_committed_peer_skips_instead_of_parking(): void
    {
        $store = new FakeClaimingStore();
        // A peer holds the claim AND commits at the moment our claim() loses (the tight race).
        $store->claim('msg-1', 60);
        $store->commitOnLostClaim['msg-1'] = true;

        $calls = 0;
        $handler = ClaimingDispatch::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        // No exception: the dispatcher re-checks seen(), finds it committed, and returns (acks).
        $handler($this->message('msg-1'));

        self::assertSame(0, $calls);
        self::assertTrue($store->seen('msg-1'));
    }

    public function test_a_thrown_handler_releases_the_claim_so_a_redelivery_reruns(): void
    {
        $store = new FakeClaimingStore();
        $calls = 0;
        $handler = ClaimingDispatch::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
            throw new RuntimeException('boom');
        });

        try {
            $handler($this->message('msg-1'));
            self::fail('the handler exception should propagate');
        } catch (RuntimeException) {
        }

        // Claim released, not committed → a redelivery re-runs (at-least-once preserved).
        self::assertFalse($store->seen('msg-1'));
        self::assertArrayNotHasKey('msg-1', $store->claims);

        try {
            $handler($this->message('msg-1'));
        } catch (RuntimeException) {
        }
        self::assertSame(2, $calls);
    }

    public function test_a_message_with_no_usable_id_runs_unchanged(): void
    {
        $store = new FakeClaimingStore();
        $calls = 0;
        $handler = ClaimingDispatch::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('')); // empty id → fail-open
        $handler($this->message('')); // still runs

        self::assertSame(2, $calls);
    }

    public function test_distinct_ids_run_independently(): void
    {
        $store = new FakeClaimingStore();
        $calls = 0;
        $handler = ClaimingDispatch::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('msg-1'));
        $handler($this->message('msg-2'));

        self::assertSame(2, $calls);
    }

    private function message(string $id): ConsumedMessage
    {
        return new FakeMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 7],
            'meta' => ['id' => $id, 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1],
            'attempts' => 0,
        ]);
    }
}
