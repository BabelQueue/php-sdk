<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * An optional {@see Transport} capability: publish an already-encoded envelope **with a broker record
 * key** (Kafka). The key rides beside the wire envelope (GR-1) and is never inside it; `schema_version`
 * stays `1`. {@see \BabelQueue\Consume\DeadLetterPublisher} uses it to keep the original record's key
 * on the `<queue>.dlq` copy; a transport that does not implement it simply publishes keyless, as before.
 *
 * Implementations MUST keep a `null` key byte-identical to {@see Transport::publish()} (no key is
 * synthesised); an empty string is a real, empty key.
 */
interface KeyedPublisher extends Transport
{
    /**
     * @param  string  $payload  The already-encoded wire envelope (unchanged; GR-1).
     * @param  string|null  $queue  Logical queue name, or null for the default.
     * @param  string|null  $key  The record key bytes, or null for a null key.
     * @return string|null  The published message id, if the transport exposes one.
     */
    public function publishKeyed(string $payload, ?string $queue, ?string $key): ?string;
}
