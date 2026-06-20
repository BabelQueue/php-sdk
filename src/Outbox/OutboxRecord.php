<?php

declare(strict_types=1);

namespace BabelQueue\Outbox;

/**
 * One pending row read back from an {@see OutboxStore} for the {@see OutboxRelay} to
 * publish. It pairs the store's own bookkeeping (id, attempts) with the verbatim,
 * frozen wire envelope ({@see body}) and the queue it should go to.
 *
 * {@see body} is the exact {@see \BabelQueue\Codec\EnvelopeCodec}-encoded JSON that was
 * handed to {@see OutboxStore::save()} — the relay publishes these bytes unchanged
 * (GR-1/GR-5), so `trace_id` is preserved end-to-end (GR-4) without the relay ever
 * decoding or rebuilding the envelope.
 */
final class OutboxRecord
{
    /**
     * @param  string  $id        The outbox row id (the store's primary key, not `meta.id`).
     * @param  string  $body      The frozen, encoded envelope JSON, byte-for-byte as stored.
     * @param  string  $queue     The logical queue the relay should publish to.
     * @param  int     $attempts  How many times the relay has already tried to publish this row.
     */
    public function __construct(
        public readonly string $id,
        public readonly string $body,
        public readonly string $queue,
        public readonly int $attempts = 0,
    ) {
    }
}
