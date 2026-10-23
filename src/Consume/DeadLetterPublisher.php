<?php

declare(strict_types=1);

namespace BabelQueue\Consume;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasRawBody;
use BabelQueue\Contracts\HasRecordKey;
use BabelQueue\Contracts\KeyedPublisher;
use BabelQueue\Contracts\Transport;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\DeadLetter\PoisonBody;
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

        $poisonRaw = $this->poisonRaw($message);

        // A poison copy is written byte-for-byte by PoisonBody (it cannot fail on the body: invalid UTF-8,
        // 1e400, ...); encode() would reshape a rejected list "data" into an object.
        $body = $poisonRaw !== null
            ? PoisonBody::annotate($poisonRaw, $reason, $e, $queue, $message->attempts())
            : EnvelopeCodec::encode(DeadLetter::annotate($message->envelope(), $reason, $e, $queue, $message->attempts()));

        // Carry the original broker record key (Kafka) onto the DLQ copy when there is one and the
        // transport can take it; a null key, or a transport without the capability, publishes as before.
        $key = $message instanceof HasRecordKey ? $message->recordKey() : null;
        if ($key !== null && $this->transport instanceof KeyedPublisher) {
            $this->transport->publishKeyed($body, $queue . $this->suffix, $key);

            return;
        }

        $this->transport->publish($body, $queue . $this->suffix);
    }

    /**
     * The raw body to dead-letter as a poison copy. A poison message (its body decodes to the empty
     * envelope, e.g. malformed JSON or a list `data` that decode rejects) keeps an inspectable copy when
     * the message carries its raw body ({@see HasRawBody}); see {@see PoisonBody}.
     *
     * Null when the message is not poison (or carries no raw body): its decoded envelope is used.
     */
    private function poisonRaw(ConsumedMessage $message): ?string
    {
        if (! $message instanceof HasRawBody) {
            return null;
        }

        $raw = $message->rawBody();
        if ($raw === null || EnvelopeCodec::decode($raw, static function (): void {}) !== []) {
            return null;
        }

        return $raw;
    }
}
