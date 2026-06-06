<?php

declare(strict_types=1);

namespace BabelQueue\DeadLetter;

use Throwable;

/**
 * Builds the additive "dead_letter" block attached to an envelope when it is
 * routed to a dead-letter queue (ADR-0009). Pure: it takes the decoded envelope
 * array and returns an annotated copy — the original identity (trace_id,
 * meta.id, data) is preserved verbatim. Publishing is the adapter's job (via a
 * {@see \BabelQueue\Contracts\Transport} or the framework's queue).
 *
 * Because the field is additive and optional, the envelope stays at
 * schema_version 1; consumers of normal queues ignore it.
 */
final class DeadLetter
{
    /**
     * Return a copy of $envelope with a "dead_letter" block describing the failure.
     *
     * @param  array<string, mixed>  $envelope  The decoded original envelope.
     * @param  string  $reason  failed | unknown_urn | poison
     * @return array<string, mixed>
     */
    public static function annotate(
        array $envelope,
        string $reason,
        ?Throwable $e,
        string $originalQueue,
        int $attempts,
        string $lang = 'php',
    ): array {
        $envelope['dead_letter'] = [
            'reason' => $reason,
            'error' => $e?->getMessage(),
            'exception' => $e !== null ? $e::class : null,
            'failed_at' => (int) round(microtime(true) * 1000),
            'original_queue' => $originalQueue,
            'attempts' => $attempts,
            'lang' => $lang,
        ];

        return $envelope;
    }
}
