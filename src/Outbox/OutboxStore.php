<?php

declare(strict_types=1);

namespace BabelQueue\Outbox;

/**
 * The persistence seam for the **transactional outbox** (ADR-0029) — the durable
 * "outbox" table that an {@see Outbox} writer fills and an {@see OutboxRelay} drains.
 *
 * The whole point of the pattern is to remove the *dual write* a plain producer makes:
 * "commit the business row" **and** "publish to the broker" are two systems that can
 * disagree on a crash. Instead the message is written **into the same database, inside
 * the same transaction** as the business data — so it commits or rolls back atomically
 * with it — and a separate relay publishes it afterwards. No distributed transaction,
 * exactly-once *handoff* into the broker (then at-least-once on the wire, as always).
 *
 * **The transaction boundary is the CALLER'S.** The php-sdk does not open, commit or
 * roll back anything: {@see save()} is invoked from *inside* a transaction the caller
 * already began (around its own `INSERT INTO orders …`), and the caller commits both
 * together. This keeps the core free of any DB driver (GR-7): the php-sdk defines this
 * contract; a concrete adapter (e.g. the InitORM one in `babelqueue-examples/`) binds it
 * to a real connection. The reference {@see InMemoryOutboxStore} is for tests.
 *
 * The stored value is the **frozen wire envelope, byte-for-byte unchanged** (GR-1): an
 * {@see \BabelQueue\Codec\EnvelopeCodec}-encoded JSON string. The outbox adds its own
 * bookkeeping columns (id, status, attempts, timestamps) *around* the envelope; it never
 * adds a field *to* it. What the relay publishes is the same bytes that were stored.
 */
interface OutboxStore
{
    /**
     * Persist one encoded envelope into the outbox, **within the transaction the caller
     * has already opened** around its business write. Returns the new row's outbox id
     * (the store's own primary key — NOT `meta.id`), which the caller may keep for
     * correlation. The body is stored verbatim; do not re-encode or mutate it.
     *
     * @param  string  $encodedEnvelope  The {@see EnvelopeCodec::encode()} output (UTF-8 JSON).
     * @param  string  $queue            The logical target queue, captured for the relay.
     * @return string  The outbox row id.
     */
    public function save(string $encodedEnvelope, string $queue): string;

    /**
     * Reserve up to $limit rows that are pending publish, oldest first, so a relay can
     * forward them. Implementations SHOULD lock/claim the rows they return (e.g.
     * `SELECT … FOR UPDATE SKIP LOCKED`, or a `picked_at` claim) so two concurrent relays
     * do not both publish the same row; at-least-once still tolerates a rare double send.
     *
     * @param  int  $limit  Maximum rows to return (a positive batch size).
     * @return list<OutboxRecord>  Pending rows, oldest first; empty when the outbox is drained.
     */
    public function fetchUnpublished(int $limit): array;

    /**
     * Mark the given outbox rows as successfully published (so they are never relayed
     * again). Called by the relay only **after** the transport accepted the message.
     *
     * @param  list<string>  $ids  Outbox row ids previously returned by {@see fetchUnpublished()}.
     */
    public function markPublished(array $ids): void;

    /**
     * Record a failed publish attempt for one row: increment its attempt counter and
     * store the last error, leaving it pending so a later relay pass retries it
     * (at-least-once). The store MAY move a row that exceeds a max-attempts threshold to
     * a terminal/parked state, but that policy is the adapter's, not the core's.
     *
     * @param  string  $id     The outbox row id.
     * @param  string  $error  A short, human-readable failure reason (never secrets).
     */
    public function markFailed(string $id, string $error): void;
}
