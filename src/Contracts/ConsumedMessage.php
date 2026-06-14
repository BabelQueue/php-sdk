<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * An {@see InboundMessage} a consumer has fully decoded — the read-only envelope view plus the
 * reconciled attempt counter and the raw envelope, which the consume runtime ({@see
 * \BabelQueue\Consume\Dispatcher}, {@see \BabelQueue\Consume\DeadLetterPublisher}) needs to apply
 * retry / dead-letter policy. Implemented by the framework-less consumers' messages
 * ({@see \BabelQueue\Transport\PulsarMessage}, {@see \BabelQueue\Transport\KafkaMessage}).
 */
interface ConsumedMessage extends InboundMessage
{
    /**
     * The reconciled attempt count for this delivery (per the binding's §x.5 rule).
     */
    public function attempts(): int;

    /**
     * The full decoded envelope (so the runtime can re-publish it to a dead-letter queue with the
     * additive `dead_letter` block).
     *
     * @return array<string, mixed>
     */
    public function envelope(): array;
}
