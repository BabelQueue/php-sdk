<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

use BabelQueue\Codec\EnvelopeCodec;

/**
 * Reports the keywords of a schema that fall outside the validator's supported subset
 * (conformance `payload_schema_keywords`): one warning per occurrence, raised when a provider
 * first processes the schema (registration). The verdict never changes; this is only a notice.
 *
 * The sink has the shape of the codec's warning sink, `fn(string $message, string $pointer): void`.
 * Resolution order: the per-call `$onWarning`, then the process-wide handler installed with
 * {@see EnvelopeCodec::setWarningHandler()}, then `error_log()` with the codec's `[babelqueue]` prefix.
 */
final class KeywordWarnings
{
    /**
     * @param  array<string, mixed>  $schema
     * @param  (callable(string, string): void)|null  $onWarning  falls back to the codec's handler, then error_log()
     * @return list<array{pointer: string, keyword: string}>  what was reported
     */
    public static function report(string $urn, array $schema, ?callable $onWarning = null): array
    {
        $found = PayloadValidator::unsupportedKeywords($schema);
        $sink = $onWarning ?? EnvelopeCodec::warningHandler();

        foreach ($found as $warning) {
            $message = sprintf(
                'Schema for [%s] uses keyword "%s" at "%s", which the payload validator does not support; '
                . 'it is ignored (the verdict is unchanged).',
                $urn,
                $warning['keyword'],
                $warning['pointer'],
            );

            if ($sink !== null) {
                $sink($message, $warning['pointer']);

                continue;
            }

            error_log('[babelqueue] ' . $message);
        }

        return $found;
    }
}
