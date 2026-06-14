<?php

declare(strict_types=1);

namespace BabelQueue\Consume;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\Transport;
use BabelQueue\DeadLetter\DeadLetter;
use Throwable;

/**
 * Routes a failed/undeliverable message to the cross-language dead-letter queue: it enriches the
 * envelope with the additive `dead_letter` block ({@see DeadLetter::annotate}, ADR-0009) and
 * publishes it, via any {@see Transport}, to `<original_queue>.dlq` (the default naming all SDKs
 * share). The body stays byte-identical; the block is additive, so `schema_version` remains 1.
 */
final class DeadLetterPublisher
{
    public function __construct(
        private readonly Transport $transport,
        private readonly string $suffix = '.dlq',
    ) {
    }

    /**
     * Publish $message to its `<queue>.dlq`, annotated with why it failed.
     *
     * @param  string  $reason  `failed` | `unknown_urn` | `poison`
     */
    public function publish(ConsumedMessage $message, string $reason, ?Throwable $e): void
    {
        $queue = $message->getMeta()['queue'] ?? 'default';
        $queue = is_string($queue) && $queue !== '' ? $queue : 'default';

        $annotated = DeadLetter::annotate($message->envelope(), $reason, $e, $queue, $message->attempts());

        $this->transport->publish(EnvelopeCodec::encode($annotated), $queue . $this->suffix);
    }
}
