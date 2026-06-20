<?php

declare(strict_types=1);

namespace BabelQueue\Outbox;

use BabelQueue\Codec\EnvelopeCodec;

/**
 * The **write side** of the transactional outbox (ADR-0029): turn a BabelQueue envelope
 * into a stored outbox row, so the message is persisted *atomically with the business
 * data* and a separate {@see OutboxRelay} publishes it later.
 *
 * Usage — the caller owns the transaction boundary (this is the whole point):
 *
 *     $db->begin();
 *     try {
 *         $db->insertOrder($order);                       // the business write
 *         $envelope = EnvelopeCodec::fromJob($job, 'orders');
 *         $outbox->write($envelope);                       // same connection, same tx
 *         $db->commit();                                   // both, or neither
 *     } catch (\Throwable $e) {
 *         $db->rollBack();
 *         throw $e;
 *     }
 *
 * Because both writes share one transaction, a crash can never leave the business row
 * committed without its message (the classic dual-write bug) — they commit or roll back
 * together. The handoff to the broker becomes a *local* problem the relay solves.
 *
 * This helper is intentionally tiny and dependency-free: it only encodes via the frozen
 * {@see EnvelopeCodec} (GR-1 — the envelope bytes are stored unchanged; the outbox never
 * adds an envelope field) and delegates persistence to the injected {@see OutboxStore},
 * which the caller binds to their own DB (GR-7). It does **not** begin/commit anything.
 */
final class Outbox
{
    public function __construct(
        private readonly OutboxStore $store,
    ) {
    }

    /**
     * Encode the envelope (frozen codec, bytes unchanged) and persist it via the store,
     * inside the transaction the caller has already opened. Returns the new outbox row id.
     *
     * @param  array<string, mixed>  $envelope  A canonical envelope from
     *                                {@see EnvelopeCodec::make()} / {@see EnvelopeCodec::fromJob()}.
     * @return string  The outbox row id (for the caller's own correlation, if wanted).
     *
     * @throws \JsonException When the envelope is not cleanly encodable (same as the codec).
     */
    public function write(array $envelope): string
    {
        $queue = $this->queueOf($envelope);

        return $this->store->save(EnvelopeCodec::encode($envelope), $queue);
    }

    /**
     * The logical queue the message targets: its `meta.queue`, falling back to "default".
     * Captured at write time so the relay can publish to the right queue without decoding
     * the body.
     *
     * @param  array<string, mixed>  $envelope
     */
    private function queueOf(array $envelope): string
    {
        $meta = $envelope['meta'] ?? null;
        if (is_array($meta) && isset($meta['queue']) && is_string($meta['queue']) && $meta['queue'] !== '') {
            return $meta['queue'];
        }

        return 'default';
    }
}
