<?php

declare(strict_types=1);

namespace BabelQueue\DeadLetter;

use stdClass;
use Throwable;

/**
 * Builds the dead-letter copy of a POISON body (ADR-0034 section 10): a record whose value does not
 * decode to a usable envelope. The copy must always be producible, because a failure here is not
 * transient: the same bytes would fail again on every redelivery and wedge the partition/queue.
 *
 *  - A body that is a valid JSON object is written back BYTE-FOR-BYTE with the `dead_letter` block
 *    spliced in before the closing brace. It is never decoded and re-encoded, so `{}` stays `{}`
 *    (not `[]`), integers beyond int64 stay exact, and a literal such as `1e400` (decodes to INF,
 *    which `json_encode` refuses) is carried verbatim.
 *  - Anything else (and an object that already has a `dead_letter` key) is wrapped as `{"raw": "<body>", "dead_letter": {...}}`. Bytes that are not valid
 *    UTF-8 are carried as `"raw": "<base64>"` plus `"raw_encoding": "base64"`, so no byte is lost
 *    and nothing throws.
 *
 * The block is additive and optional, so the DLQ copy stays at schema_version 1 (ADR-0009).
 */
final class PoisonBody
{
    private const FLAGS = JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE;

    /**
     * The `meta.queue` of a body that is a JSON object, null otherwise. Used to pick the DLQ target.
     */
    public static function queueHint(string $raw): ?string
    {
        $decoded = json_decode($raw, false);
        if (! $decoded instanceof stdClass || ! ($decoded->meta ?? null) instanceof stdClass) {
            return null;
        }

        $queue = $decoded->meta->queue ?? null;

        return is_string($queue) && $queue !== '' ? $queue : null;
    }

    /**
     * The DLQ record value for $raw, annotated with a `dead_letter` block. Never throws on the body.
     */
    public static function annotate(
        string $raw,
        string $reason,
        ?Throwable $e,
        string $originalQueue,
        int $attempts,
        string $lang = 'php',
    ): string {
        $block = DeadLetter::annotate([], $reason, $e, $originalQueue, $attempts, $lang)['dead_letter'];
        $blockJson = json_encode($block, self::FLAGS);

        $decoded = json_decode($raw, false);
        if ($decoded instanceof stdClass && ! property_exists($decoded, 'dead_letter')) {
            $end = strlen(rtrim($raw, " \t\n\r"));
            $head = rtrim(substr($raw, 0, $end - 1), " \t\n\r");

            return $head . (ltrim($head, " \t\n\r") === '{' ? '' : ',') . '"dead_letter":' . $blockJson . '}';
        }

        // Not an object, or an object that already carries a `dead_letter` key (splicing would duplicate it):
        // keep the bytes exactly and wrap them.
        $wrapper = preg_match('//u', $raw) === 1
            ? ['raw' => $raw]
            : ['raw' => base64_encode($raw), 'raw_encoding' => 'base64'];
        $wrapper['dead_letter'] = $block;

        return json_encode($wrapper, self::FLAGS);
    }
}
