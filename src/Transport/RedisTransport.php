<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Contracts\Transport;
use Predis\ClientInterface;

/**
 * A framework-less Redis transport: publishes the canonical envelope with a plain
 * `RPUSH <queue> <payload>` — the list convention every BabelQueue SDK shares —
 * so a PHP producer interoperates with Go/Python/Node consumers on the identical
 * queue (they reserve with `BLMOVE <queue> <queue>:processing` and ack with
 * `LREM`). This is the seam used by a plain PHP / Slim / Mezzio app; the Laravel
 * and Symfony adapters publish through their own native queues instead.
 *
 * Optional dependency: `predis/predis` (a pure-PHP client; no extension needed).
 * phpredis (`ext-redis`) users can implement the one-method {@see Transport}
 * directly — it is just an `rpush`.
 */
final class RedisTransport implements Transport
{
    public function __construct(
        private readonly ClientInterface $client,
        private readonly string $defaultQueue = 'default',
    ) {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $this->client->rpush($queue ?? $this->defaultQueue, $payload);

        // Redis lists carry no broker-assigned id; the envelope's own meta.id is
        // the message identity. Returning null keeps the contract honest.
        return null;
    }
}
