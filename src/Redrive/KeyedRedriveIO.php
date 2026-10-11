<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

/**
 * An optional {@see RedriveIO} capability: re-publish a body onto a queue **with the broker record
 * key** the dead-lettered record carried (Kafka) — the redrive-side counterpart of
 * {@see \BabelQueue\Contracts\KeyedPublisher}, and the PHP mirror of Go's `KeyedRepublisher`.
 *
 * The key travels beside the wire envelope, never inside it (GR-1; `schema_version` stays 1). The
 * IO surfaces the DLQ record's key from {@see RedriveIO::pop()} through the optional `key` entry of
 * the reserved array (`null` / absent = a null key; an empty string is a real, empty key); {@see Redrive}
 * hands that key back to {@see publishKeyed()} on the redrive write and on every restore-to-DLQ write,
 * so the replayed record keeps the key (contract §6.5 "Record key on republish"). Partition equality
 * with the original record is not guaranteed.
 *
 * A {@see RedriveIO} that does **not** implement this republishes keyless, exactly as before.
 * Implementations MUST keep a `null` key byte-identical to {@see RedriveIO::publish()} (no key is
 * synthesised). The replay-bypass path ({@see HeaderRedriveIO::publishWithHeaders()}) carries no key.
 */
interface KeyedRedriveIO extends RedriveIO
{
    /**
     * Publish a raw, already-encoded envelope body onto `$queue` with the record key `$key`.
     *
     * @param  string|null  $key  The record key bytes, or null for a null key.
     */
    public function publishKeyed(string $queue, string $body, ?string $key): void;
}
