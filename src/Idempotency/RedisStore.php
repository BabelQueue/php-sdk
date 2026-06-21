<?php

declare(strict_types=1);

namespace BabelQueue\Idempotency;

use Predis\ClientInterface;

/**
 * A persistent {@see ClaimingStore} backed by Redis — the natural fit for a fleet of consumers that
 * already runs a Redis transport, and the store whose atomic claim is a **single round-trip**
 * (ADR-0022).
 *
 * **The atomic-claim primitive is `SET key value NX PX <ttl_ms>`.** Redis's `NX` flag makes the
 * SET succeed *only if the key does not exist*, atomically on the server — so exactly one of N
 * concurrent workers handed the same `meta.id` gets the OK and the rest get nil. That single
 * command both claims the id and arms its TTL (`PX` = expiry in milliseconds), so a worker that
 * crashes after claiming but before committing cannot wedge the id: the claim key self-expires and
 * a redelivery can re-claim it. This is the same `SET … NX PX` lock idiom Go's Redis idempotency
 * and Python's use — no Lua, no WATCH/MULTI.
 *
 * Two states share one key:
 *   - an **in-flight claim** holds the key with a TTL and the sentinel value {@see self::CLAIMED};
 *   - a **commit** ({@see remember()}) re-SETs the key to {@see self::SEEN} with **no** expiry, so a
 *     committed id is permanent (until an operator {@see forget()}s it).
 * {@see seen()} is therefore "the key exists with the SEEN value", and {@see claim()} is "I set the
 * key NX". A committed key (value SEEN, no NX) blocks a re-claim because NX fails on any existing
 * key — committed ids are never re-run.
 *
 * **Zero heavy dependencies (GR-7).** It rides the same optional `predis/predis` client the
 * reference {@see \BabelQueue\Transport\RedisTransport} uses; a phpredis (`ext-redis`) user can
 * implement {@see ClaimingStore} directly over the same two commands.
 */
final class RedisStore implements ClaimingStore
{
    /** Marks an in-flight claim (a TTL'd key); distinct from a committed SEEN key. */
    private const CLAIMED = '1';

    /** Marks a committed id (a no-expiry key). */
    private const SEEN = '2';

    private const DEFAULT_TTL = 3600;

    /**
     * @param  ClientInterface  $client  A predis client (the same one a RedisTransport can share).
     * @param  string  $prefix  Key namespace; the stored key is `<prefix><messageId>`.
     */
    public function __construct(
        private readonly ClientInterface $client,
        private readonly string $prefix = 'bq:idem:',
    ) {
    }

    public function seen(string $messageId): bool
    {
        /** @var mixed $value */
        $value = $this->client->get($this->key($messageId));

        return $value === self::SEEN;
    }

    public function claim(string $messageId, int $ttlSeconds): bool
    {
        $ttl = $ttlSeconds > 0 ? $ttlSeconds : self::DEFAULT_TTL;

        // SET key CLAIMED NX PX <ttl_ms> — atomic claim + TTL in one round-trip. predis returns a
        // truthy Status ('OK') on success and null when NX fails (key already exists).
        /** @var mixed $result */
        $result = $this->client->set(
            $this->key($messageId),
            self::CLAIMED,
            'PX',
            $ttl * 1000,
            'NX',
        );

        return self::isOk($result);
    }

    public function remember(string $messageId): void
    {
        // Commit: overwrite (claim or absent) with the permanent SEEN value and drop the TTL.
        $this->client->set($this->key($messageId), self::SEEN);
    }

    public function forget(string $messageId): void
    {
        $this->client->del($this->key($messageId));
    }

    /**
     * Release an uncommitted claim so a redelivery can re-claim immediately (handler threw). It only
     * deletes a key still holding the CLAIMED sentinel, so a release racing a successful commit
     * cannot delete the committed SEEN key — a Lua compare-and-delete keeps that atomic.
     */
    public function release(string $messageId): void
    {
        // EVAL: delete the key iff it still holds the CLAIMED value (don't clobber a commit).
        $this->client->eval(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            1,
            $this->key($messageId),
            self::CLAIMED,
        );
    }

    private function key(string $messageId): string
    {
        return $this->prefix . $messageId;
    }

    /**
     * predis returns a `Predis\Response\Status` (whose `__toString()` is `'OK'`) on a successful SET
     * and null when `NX` declined. Treat a value that stringifies to 'OK' as success; anything else
     * (null, false, or any other reply) as a declined claim.
     */
    private static function isOk(mixed $result): bool
    {
        if (is_string($result)) {
            return strtoupper($result) === 'OK';
        }

        if (is_object($result) && method_exists($result, '__toString')) {
            return strtoupper((string) $result) === 'OK';
        }

        return false;
    }
}
