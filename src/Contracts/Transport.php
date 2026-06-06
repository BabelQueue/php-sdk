<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * A minimal broker transport: publish an already-encoded envelope.
 *
 * This is the seam that lets the core be used **without** a framework (a plain
 * PHP / Slim / Mezzio app), and that adapters can implement over their own
 * machinery — e.g. the Laravel adapter wraps Laravel's queue, the Symfony
 * adapter its Messenger transport. The core's dead-letter helper and any
 * framework-less producer publish through this interface.
 */
interface Transport
{
    /**
     * Publish a raw, already-encoded (UTF-8 JSON) envelope onto a queue.
     *
     * @param  string|null  $queue  Logical queue name, or null for the default.
     * @return string|null  The published message id, if the transport exposes one.
     */
    public function publish(string $payload, ?string $queue = null): ?string;
}
