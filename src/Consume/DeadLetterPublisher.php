<?php

declare(strict_types=1);

namespace BabelQueue\Consume;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Codec\StrictBodyClassifier;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasMessageProperties;
use BabelQueue\Contracts\HasRawBody;
use BabelQueue\Contracts\HasRecordKey;
use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Contracts\KeyedPublisher;
use BabelQueue\Contracts\Transport;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\DeadLetter\PoisonBody;
use BabelQueue\Exceptions\BabelQueueException;
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
     * Publish $message to `<queue>.dlq`, annotated with why it failed.
     *
     * **The target is the caller's `$queue`** (the consumer's own queue / topic, ADR-0036 decision 3), not the
     * envelope's `meta.queue`: a producer may have written any `meta.queue` (`EnvelopeCodec::make()` defaults it to
     * `default`) onto a message that reached another topic. Without a `$queue` the message's own `meta.queue` is
     * used (callers that have no consumer queue, e.g. the Kafka consumers). `dead_letter.original_queue` is the
     * envelope's `meta.queue` when present, else the target. **Nothing is ever written to `default.dlq`:** when
     * neither names a queue, see {@see self::resolveQueue()}, the call throws.
     *
     * When the message carries its incoming native properties ({@see HasMessageProperties}, Pulsar) and the
     * transport can take them ({@see HeaderPublisher}), the routing properties (`bq-job`, ...) ride on the record
     * (all but `bq-attempts` and `bq-schema-version`, which the record's own body defines), so a redrive routes it
     * where the original was routed.
     *
     * @param  string  $reason  `failed` | `unknown_urn` | `poison`
     * @param  int|null  $attempts  the value for `dead_letter.attempts`; null = the message's delivered
     *                              {@see ConsumedMessage::attempts()}. A retry-exhaustion caller passes the
     *                              counted value (delivered + 1, the one compared with the cap; ADR-0036 decision 8)
     * @param  string|null  $queue  the queue the record goes to (the consumer's topic); null = the message's `meta.queue`
     *
     * @throws BabelQueueException When no queue can be named
     * @throws DeadLetterWriteFailed When the transport fails to write the record
     */
    public function publish(ConsumedMessage $message, string $reason, ?Throwable $e, ?int $attempts = null, ?string $queue = null): void
    {
        $attempts ??= $message->attempts();
        $target = $this->resolveQueue($message, $reason, $queue)
            ?? throw new BabelQueueException('Cannot name the dead-letter queue: the caller gave no queue and the message has no usable meta.queue (nothing is written to default.dlq).');

        $poisonRaw = $this->poisonRaw($message, $reason);

        if ($poisonRaw !== null) {
            // A poison copy is written byte-for-byte by PoisonBody (it cannot fail on the body: invalid UTF-8,
            // 1e400, ...); encode() would reshape a rejected list "data" into an object.
            $body = PoisonBody::annotate($poisonRaw, $reason, $e, $target, $attempts);
        } else {
            $metaQueue = $message->getMeta()['queue'] ?? null;
            $original = is_string($metaQueue) && $metaQueue !== '' ? $metaQueue : $target;
            $body = EnvelopeCodec::encode(DeadLetter::annotate($message->envelope(), $reason, $e, $original, $attempts));
        }

        try {
            $this->write($message, $body, $target . $this->suffix);
        } catch (Throwable $writeError) {
            throw DeadLetterWriteFailed::forQueue($target . $this->suffix, $writeError);
        }
    }

    private function write(ConsumedMessage $message, string $body, string $destination): void
    {
        // Carry the original broker record key (Kafka) onto the DLQ copy when there is one and the
        // transport can take it; a null key, or a transport without the capability, publishes as before.
        $key = $message instanceof HasRecordKey ? $message->recordKey() : null;
        if ($key !== null && $this->transport instanceof KeyedPublisher) {
            $this->transport->publishKeyed($body, $destination, $key);

            return;
        }

        if ($message instanceof HasMessageProperties && $this->transport instanceof HeaderPublisher) {
            $carried = $message->properties();
            unset($carried['bq-attempts'], $carried['bq-schema-version']);

            if ($carried !== []) {
                $this->transport->publishWithHeaders($body, $carried, $destination);

                return;
            }
        }

        $this->transport->publish($body, $destination);
    }

    /**
     * The queue `$message` is dead-lettered to, or null when it cannot be named: the caller's `$queue` (the
     * consumer's topic) when given; else, for a message that is not a poison copy (see {@see self::poisonRaw()},
     * which has no `meta.queue` of its own to trust), its own `meta.queue`. A null result is treated like "no
     * publisher": the {@see Dispatcher} drops (Pulsar) or rethrows (Kafka) instead of publishing.
     */
    public function resolveQueue(ConsumedMessage $message, string $reason, ?string $queue = null): ?string
    {
        if ($queue !== null && $queue !== '') {
            return $queue;
        }

        if ($this->poisonRaw($message, $reason) !== null) {
            return null;
        }

        $metaQueue = $message->getMeta()['queue'] ?? null;

        return is_string($metaQueue) && $metaQueue !== '' ? $metaQueue : null;
    }

    /**
     * The raw body to dead-letter as a poison copy. A poison message (reason `poison`, or a body that
     * decodes to the empty envelope, e.g. malformed JSON or a list `data` that decode rejects) keeps an
     * inspectable copy when the message carries its raw body ({@see HasRawBody}); see {@see PoisonBody}.
     * A `poison` copy also keeps the bytes of a body that decodes to a non-empty envelope but that
     * {@see StrictBodyClassifier} rejects (a wrong-typed member such as `data: [1, 2]`): its bytes must
     * be kept (ADR-0035).
     *
     * Null when the message is not poison (or carries no raw body): its decoded envelope is used.
     */
    private function poisonRaw(ConsumedMessage $message, string $reason): ?string
    {
        if (! $message instanceof HasRawBody) {
            return null;
        }

        $raw = $message->rawBody();
        if ($raw === null) {
            return null;
        }

        if (EnvelopeCodec::decode($raw, static function (): void {}) === []) {
            return $raw;
        }

        return $reason === 'poison' && StrictBodyClassifier::isPoison($raw) ? $raw : null;
    }
}
