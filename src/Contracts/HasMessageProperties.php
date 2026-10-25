<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * A consumed message that keeps the native message properties it arrived with (Pulsar: the `bq-job`,
 * `bq-attempts`, ... map), so the retry and dead-letter writes can carry the incoming route onto the copy
 * (the Go runtime does the same). Unlike {@see HasHeaders} it makes no claim about out-of-band propagation
 * headers (`traceparent`, the replay-bypass marker): it is only the raw property map. Implemented by
 * {@see \BabelQueue\Transport\PulsarMessage}.
 */
interface HasMessageProperties
{
    /**
     * The message properties as received, string to string; an empty map when none.
     *
     * @return array<string, string>
     */
    public function properties(): array;
}
