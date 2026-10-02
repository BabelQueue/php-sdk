<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * A consumed message that also carries the exact body bytes it was decoded from, so the dead-letter
 * path can keep an inspectable copy of a poison (undecodable) message instead of an empty envelope.
 * Implemented by the framework-less consumers' messages ({@see \BabelQueue\Transport\PulsarMessage},
 * {@see \BabelQueue\Transport\KafkaMessage}).
 */
interface HasRawBody
{
    /**
     * The raw message body as received from the broker, or null when it was not captured.
     */
    public function rawBody(): ?string;
}
