<?php

declare(strict_types=1);

namespace BabelQueue\Idempotency;

use PDO;
use PDOException;

/**
 * A persistent {@see ClaimingStore} backed by a single SQL table over **PDO** — so a fleet of
 * workers shares one source of truth for "has this `meta.id` been processed?" and, additionally,
 * gets the atomic in-flight claim the in-memory store cannot (ADR-0022).
 *
 * **Zero heavy dependencies (GR-7).** PDO is a PHP *extension*, not a Composer package, so this
 * adds nothing to `require`. It speaks the portable SQL subset that **PostgreSQL, MySQL/MariaDB and
 * SQLite** all share, so one class covers every PDO driver without dialect branches.
 *
 * **The atomic-claim primitive is a unique-key INSERT, not a dialect-specific upsert.** Every one
 * of those engines enforces a PRIMARY KEY atomically: concurrent INSERTs of the same id, exactly
 * one succeeds and the rest raise a unique-violation. That is the cross-engine atom — no
 * `ON CONFLICT` (Postgres/SQLite only) or `INSERT IGNORE` (MySQL only) needed for correctness:
 *
 *   1. `INSERT (message_id, state='claimed', expires_at)` — wins iff no row exists for the id.
 *      A {@see PDOException} (integrity/unique violation) means a row already exists → step 2.
 *   2. The existing row is either a permanent commit ({@see seen()} → false, the caller skips) or
 *      an in-flight claim. If that claim has **expired** (its owner crashed), one atomic
 *      `UPDATE … WHERE expires_at < now AND state='claimed'` re-acquires it; the conditional WHERE
 *      makes the steal race-safe (only one updater matches a row).
 *
 * TTL is an `expires_at` epoch-seconds column; a crashed owner's claim is re-claimable after it
 * lapses. {@see remember()} upgrades a claim (or inserts) to `state='seen'` with no expiry — a
 * committed id is permanent. The DDL ships with {@see ddl()}; the table name is injectable.
 */
final class PdoStore implements ClaimingStore
{
    private const STATE_CLAIMED = 'claimed';
    private const STATE_SEEN = 'seen';
    private const DEFAULT_TTL = 3600;

    /** A SQLite/MySQL/Postgres integrity (unique/PK) violation surfaces as SQLSTATE class 23. */
    private const SQLSTATE_INTEGRITY_CLASS = '23';

    private readonly string $table;

    /**
     * @param  PDO  $pdo  An open PDO connection (any driver). The caller owns its lifecycle and
     *                    error mode; this store relies only on the default exception-or-bool from
     *                    {@see PDO::prepare()}/{@see \PDOStatement::execute()}, so it works whether
     *                    or not ERRMODE_EXCEPTION is set on the handle.
     * @param  string  $table  The dedupe table (validated to a safe identifier; default `bq_idempotency`).
     */
    public function __construct(private readonly PDO $pdo, string $table = 'bq_idempotency')
    {
        $this->table = self::safeIdentifier($table);
    }

    /**
     * The portable `CREATE TABLE IF NOT EXISTS` DDL for the dedupe table — one statement that
     * parses on PostgreSQL, MySQL/MariaDB and SQLite. `message_id` is the PRIMARY KEY (the atomic
     * claim hinges on it); `expires_at` is nullable epoch-seconds (NULL = a permanent committed id);
     * `state` distinguishes an in-flight `claimed` row from a committed `seen` row.
     *
     * Ship/run this once at deploy time (the store never issues DDL itself):
     *
     *     $pdo->exec(PdoStore::ddl());
     *
     * @param  string  $table  Defaults to the same `bq_idempotency` the constructor uses.
     */
    public static function ddl(string $table = 'bq_idempotency'): string
    {
        $t = self::safeIdentifier($table);

        return <<<SQL
            CREATE TABLE IF NOT EXISTS {$t} (
                message_id VARCHAR(255) NOT NULL,
                state      VARCHAR(16)  NOT NULL,
                expires_at BIGINT       NULL,
                PRIMARY KEY (message_id)
            )
            SQL;
    }

    public function seen(string $messageId): bool
    {
        $stmt = $this->pdo->prepare(
            "SELECT state FROM {$this->table} WHERE message_id = :id"
        );
        $stmt->execute(['id' => $messageId]);
        $state = $stmt->fetchColumn();

        return $state === self::STATE_SEEN;
    }

    public function claim(string $messageId, int $ttlSeconds): bool
    {
        $ttl = $ttlSeconds > 0 ? $ttlSeconds : self::DEFAULT_TTL;
        $now = $this->now();
        $expiresAt = $now + $ttl;

        // 1. Fast path: no row yet → an INSERT wins the claim atomically. A concurrent peer doing
        //    the same INSERT loses on the PRIMARY KEY and lands in catch → step 2.
        try {
            $insert = $this->pdo->prepare(
                "INSERT INTO {$this->table} (message_id, state, expires_at) VALUES (:id, :state, :exp)"
            );
            $insert->execute(['id' => $messageId, 'state' => self::STATE_CLAIMED, 'exp' => $expiresAt]);

            return true;
        } catch (PDOException $e) {
            if (! self::isUniqueViolation($e)) {
                throw $e;
            }
            // A row already exists: committed, in-flight, or a lapsed claim → step 2.
        }

        // 2. A row exists. Steal it iff it is an *expired* claim — a single conditional UPDATE so
        //    only one concurrent stealer can match. A committed ('seen') row never matches (its
        //    expires_at is NULL and its state is not 'claimed'), so a committed id is never re-run.
        $steal = $this->pdo->prepare(
            "UPDATE {$this->table}
                SET expires_at = :exp
              WHERE message_id = :id
                AND state = :claimed
                AND expires_at IS NOT NULL
                AND expires_at < :now"
        );
        $steal->execute([
            'exp' => $expiresAt,
            'id' => $messageId,
            'claimed' => self::STATE_CLAIMED,
            'now' => $now,
        ]);

        return $steal->rowCount() === 1;
    }

    public function remember(string $messageId): void
    {
        // Commit: upgrade an existing (claimed) row to a permanent 'seen' with no expiry. If the
        // caller committed without a prior claim (drove the base IdempotencyStore path), insert it.
        $update = $this->pdo->prepare(
            "UPDATE {$this->table} SET state = :seen, expires_at = NULL WHERE message_id = :id"
        );
        $update->execute(['seen' => self::STATE_SEEN, 'id' => $messageId]);
        if ($update->rowCount() === 1) {
            return;
        }

        try {
            $insert = $this->pdo->prepare(
                "INSERT INTO {$this->table} (message_id, state, expires_at) VALUES (:id, :seen, NULL)"
            );
            $insert->execute(['id' => $messageId, 'seen' => self::STATE_SEEN]);
        } catch (PDOException $e) {
            // A concurrent commit inserted the 'seen' row between our UPDATE and INSERT — the id is
            // committed either way, which is exactly what we wanted. Re-throw anything else.
            if (! self::isUniqueViolation($e)) {
                throw $e;
            }
        }
    }

    public function forget(string $messageId): void
    {
        $stmt = $this->pdo->prepare("DELETE FROM {$this->table} WHERE message_id = :id");
        $stmt->execute(['id' => $messageId]);
    }

    /**
     * Drop an **uncommitted** claim so a redelivery can re-run promptly (handler threw). Only a
     * 'claimed' row is removed; a committed 'seen' row is left intact, so a release after a
     * successful commit cannot resurrect the id.
     */
    public function release(string $messageId): void
    {
        $stmt = $this->pdo->prepare(
            "DELETE FROM {$this->table} WHERE message_id = :id AND state = :claimed"
        );
        $stmt->execute(['id' => $messageId, 'claimed' => self::STATE_CLAIMED]);
    }

    /**
     * Current wall-clock in epoch seconds — isolated so tests can reason about TTL without sleeping
     * is not needed (claims are compared, not slept on), and so a subclass could inject a clock.
     */
    private function now(): int
    {
        return time();
    }

    private static function isUniqueViolation(PDOException $e): bool
    {
        $sqlState = $e->getCode();

        return is_string($sqlState) && str_starts_with($sqlState, self::SQLSTATE_INTEGRITY_CLASS);
    }

    /**
     * Guard the (non-parameterisable) table identifier: only `[A-Za-z0-9_]`, so it can be
     * interpolated into the SQL safely. Anything else is a programmer error, not user input.
     */
    private static function safeIdentifier(string $table): string
    {
        if (preg_match('/^[A-Za-z0-9_]+$/', $table) !== 1) {
            throw new \InvalidArgumentException(
                "Invalid idempotency table name: {$table} (allowed: letters, digits, underscore)."
            );
        }

        return $table;
    }
}
