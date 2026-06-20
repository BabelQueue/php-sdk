<?php

declare(strict_types=1);

namespace BabelQueue\Support;

/**
 * Pure, dependency-free helpers for the out-of-band transport-header seam (ADR-0028) — the PHP
 * mirror of Go's `sanitizeHeaders` / `mergeHeaders` and Python's `merge_headers`. Used by the
 * reference transports ({@see \BabelQueue\Transport\AmqpTransport},
 * {@see \BabelQueue\Transport\SqsTransport}, {@see \BabelQueue\Transport\RedisTransport}) to fold an
 * injected `traceparent` onto their contract headers **without clobbering** them, and by
 * {@see \BabelQueue\Otel\Tracing} to clean an injected map.
 *
 * Headers ride beside the frozen wire envelope (GR-1), never inside it.
 */
final class Headers
{
    private function __construct()
    {
    }

    /**
     * Copy `$headers`, dropping blank keys and blank values, coercing both to `string`. Returns an
     * empty map when nothing survives, so callers can treat the result as "no headers". A
     * scalar/bool value is stringified; non-scalar values are skipped.
     *
     * @param  array<mixed, mixed>  $headers
     * @return array<string, string>
     */
    public static function sanitize(array $headers): array
    {
        $out = [];
        foreach ($headers as $key => $value) {
            // An array key is always int|string; an int key stringifies (e.g. a numeric header name).
            $name = (string) $key;
            if ($name === '') {
                continue;
            }
            if (! is_scalar($value)) {
                continue;
            }
            $string = is_bool($value) ? ($value ? '1' : '0') : (string) $value;
            if ($string === '') {
                continue;
            }
            $out[$name] = $string;
        }

        return $out;
    }

    /**
     * Merge header maps into one `array<string, string>`, dropping blank keys/values. **Later
     * sources win a key collision** — pass the contract (`bq-*` / `x-*`) headers *last* so they
     * always win over an out-of-band rider (the merge-not-clobber idiom shared by every SDK). The
     * result is a fresh array the caller may mutate freely.
     *
     * @param  array<mixed, mixed>  ...$sources
     * @return array<string, string>
     */
    public static function merge(array ...$sources): array
    {
        $out = [];
        foreach ($sources as $source) {
            foreach (self::sanitize($source) as $key => $value) {
                $out[$key] = $value;
            }
        }

        return $out;
    }
}
