<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Idempotency;

use BabelQueue\Idempotency\ClaimingStore;
use BabelQueue\Idempotency\PdoStore;
use InvalidArgumentException;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * The persistent PDO {@see ClaimingStore} (ADR-0022): claim/commit/already-seen/expiry against a
 * **real** database. It runs on an in-memory SQLite PDO when `pdo_sqlite` is available (every
 * supported PDO engine enforces the PRIMARY KEY that the atomic claim hinges on, so SQLite proves
 * the portable path); it skips cleanly when the driver is absent so the suite stays green anywhere.
 */
final class PdoStoreTest extends TestCase
{
    private PDO $pdo;

    protected function setUp(): void
    {
        if (! in_array('sqlite', PDO::getAvailableDrivers(), true)) {
            self::markTestSkipped('pdo_sqlite is not available; skipping the DB-gated PdoStore test.');
        }

        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->exec(PdoStore::ddl());
    }

    public function test_ddl_creates_a_usable_table(): void
    {
        // A second CREATE IF NOT EXISTS is a no-op (idempotent DDL), and the table is queryable.
        $this->pdo->exec(PdoStore::ddl());
        $store = new PdoStore($this->pdo);

        self::assertFalse($store->seen('never-touched'));
    }

    public function test_first_claim_wins_and_commit_marks_seen(): void
    {
        $store = new PdoStore($this->pdo);

        self::assertFalse($store->seen('msg-1'));
        self::assertTrue($store->claim('msg-1', 60), 'first claim should win');
        // Claimed but not yet committed: not "seen".
        self::assertFalse($store->seen('msg-1'));

        $store->remember('msg-1'); // commit
        self::assertTrue($store->seen('msg-1'));
    }

    public function test_a_second_concurrent_claim_of_an_inflight_id_is_refused(): void
    {
        $store = new PdoStore($this->pdo);

        self::assertTrue($store->claim('msg-1', 60));
        // A peer handed the same id while the first claim is still held + unexpired: parked.
        self::assertFalse($store->claim('msg-1', 60));
    }

    public function test_a_committed_id_can_never_be_reclaimed(): void
    {
        $store = new PdoStore($this->pdo);

        self::assertTrue($store->claim('msg-1', 60));
        $store->remember('msg-1');

        // A duplicate delivery after commit: claim is refused and seen() is true → the caller skips.
        self::assertFalse($store->claim('msg-1', 60));
        self::assertTrue($store->seen('msg-1'));
    }

    public function test_an_expired_claim_can_be_reclaimed(): void
    {
        $store = new PdoStore($this->pdo);

        // Claim with a TTL already in the past (ttl<=0 → default, so write the expired row directly).
        $this->pdo->exec(
            "INSERT INTO bq_idempotency (message_id, state, expires_at) VALUES ('msg-1', 'claimed', 1)"
        );

        // The owner "crashed"; the claim lapsed (expires_at=1 << now) → a redelivery re-claims it.
        self::assertTrue($store->claim('msg-1', 60), 'a lapsed claim is re-claimable');
        // And it is now held again (fresh expiry), so a concurrent peer is refused.
        self::assertFalse($store->claim('msg-1', 60));
    }

    public function test_release_drops_an_uncommitted_claim_for_prompt_retry(): void
    {
        $store = new PdoStore($this->pdo);

        self::assertTrue($store->claim('msg-1', 60));
        $store->release('msg-1'); // handler threw → release so a redelivery re-runs at once

        self::assertTrue($store->claim('msg-1', 60), 'a released id is immediately re-claimable');
    }

    public function test_release_does_not_remove_a_committed_id(): void
    {
        $store = new PdoStore($this->pdo);

        self::assertTrue($store->claim('msg-1', 60));
        $store->remember('msg-1'); // committed
        $store->release('msg-1');  // a stray release racing a commit must not resurrect the id

        self::assertTrue($store->seen('msg-1'), 'a committed id survives a release');
        self::assertFalse($store->claim('msg-1', 60));
    }

    public function test_remember_without_a_prior_claim_commits_directly(): void
    {
        // The base IdempotencyStore path (Idempotent::wrap) calls remember() with no claim.
        $store = new PdoStore($this->pdo);

        $store->remember('msg-1');
        self::assertTrue($store->seen('msg-1'));
    }

    public function test_forget_removes_a_committed_id(): void
    {
        $store = new PdoStore($this->pdo);
        $store->remember('msg-1');
        self::assertTrue($store->seen('msg-1'));

        $store->forget('msg-1');
        self::assertFalse($store->seen('msg-1'));
        self::assertTrue($store->claim('msg-1', 60), 'a forgotten id is claimable again');
    }

    public function test_distinct_ids_are_independent(): void
    {
        $store = new PdoStore($this->pdo);

        self::assertTrue($store->claim('msg-1', 60));
        self::assertTrue($store->claim('msg-2', 60), 'a different id claims independently');
    }

    public function test_a_custom_table_name_is_honoured(): void
    {
        $this->pdo->exec(PdoStore::ddl('bq_orders_idem'));
        $store = new PdoStore($this->pdo, 'bq_orders_idem');

        self::assertTrue($store->claim('msg-1', 60));
        $store->remember('msg-1');
        self::assertTrue($store->seen('msg-1'));
    }

    public function test_an_unsafe_table_name_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new PdoStore($this->pdo, 'bq_idem; DROP TABLE users');
    }

    public function test_ddl_rejects_an_unsafe_table_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        PdoStore::ddl('bad name');
    }
}
