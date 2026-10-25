<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * A consumed message that knows the BabelQueue queue (the broker topic) its consumer reads from, so
 * the dead-letter path can name `<queue>.dlq` for a message that carries no `meta.queue` of its own
 * (a poison body, a delivery rejected by the version gate). Implemented by
 * {@see \BabelQueue\Transport\PulsarMessage}.
 */
interface HasQueue
{
    /**
     * The consumer's queue (the topic's local name, e.g. `orders` for
     * `persistent://public/default/orders`), or null when the consumer was not told its topic.
     */
    public function queue(): ?string;
}
