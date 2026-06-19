<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

/**
 * The minimal seam {@see Redrive} drives a dead-letter queue through. The PHP core
 * {@see \BabelQueue\Contracts\Transport} is publish-only (PHP is produce-side; consume runs in
 * a framework worker), so redrive — which must also *read* the DLQ — takes this small reserve /
 * acknowledge / publish interface and the caller binds it to their broker.
 */
interface RedriveIO
{
    /**
     * Reserve the next message from $queue, or null when it is empty.
     *
     * @return array{body: string, handle: mixed}|null  The raw body plus a transport-internal
     *                                                   handle used to {@see ack()} it.
     */
    public function pop(string $queue): ?array;

    /**
     * Acknowledge (remove) a previously reserved message by its handle.
     */
    public function ack(mixed $handle): void;

    /**
     * Publish a raw, already-encoded envelope body onto $queue.
     */
    public function publish(string $queue, string $body): void;
}
