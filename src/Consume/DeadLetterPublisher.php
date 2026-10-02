<?php

declare(strict_types=1);

namespace BabelQueue\Consume;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasRawBody;
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

        $poisonBody = $this->poisonBody($message);
        $annotated = DeadLetter::annotate($poisonBody ?? $message->envelope(), $reason, $e, $queue, $message->attempts());

        // A poison copy is written as-is: encode() would reshape a rejected list "data" into an object.
        $body = $poisonBody === null
            ? EnvelopeCodec::encode($annotated)
            : json_encode($annotated, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

        $this->transport->publish($body, $queue . $this->suffix);
    }

    /**
     * The envelope to dead-letter. A poison message (its body decodes to the empty envelope — e.g.
     * malformed JSON, or a list `data` that decode rejects) keeps an inspectable copy when the
     * message carries its raw body ({@see HasRawBody}): the original JSON object verbatim, else
     * `{"raw": "<body>"}` — the same shape the Laravel adapter's DLQ writes.
     *
     * Null when the message is not poison (or carries no raw body): its decoded envelope is used.
     *
     * @return array<string, mixed>|null
     */
    private function poisonBody(ConsumedMessage $message): ?array
    {
        if (! $message instanceof HasRawBody) {
            return null;
        }

        $raw = $message->rawBody();
        if ($raw === null || EnvelopeCodec::decode($raw, static function (): void {}) !== []) {
            return null;
        }

        $decoded = json_decode($raw, true);

        return is_array($decoded) ? $decoded : ['raw' => $raw];
    }
}
