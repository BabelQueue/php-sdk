<?php

declare(strict_types=1);

namespace BabelQueue\Validation;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Exceptions\InvalidEnvelopeException;

/**
 * Consumer-side envelope validation *with a reason* — the lightweight gate a
 * framework-less consumer runs before dispatch so it can decide whether to
 * process, drop, or quarantine (dead-letter) a message.
 *
 * {@see EnvelopeCodec::accepts()} answers yes/no; this answers *why not*. The
 * "why" that matters most is an unsupported `meta.schema_version`: a newer
 * producer on the same queue is a forward-compatibility hazard, so it is
 * reported distinctly ({@see self::REASON_UNSUPPORTED_SCHEMA_VERSION},
 * {@see self::isUnsupportedSchemaVersion()}) to be quarantined rather than
 * silently dropped.
 */
final class EnvelopeValidator
{
    public const REASON_MISSING_URN = 'missing_urn';
    public const REASON_MISSING_META = 'missing_meta';
    public const REASON_UNSUPPORTED_SCHEMA_VERSION = 'unsupported_schema_version';
    public const REASON_INVALID_DATA = 'invalid_data';
    public const REASON_MISSING_TRACE_ID = 'missing_trace_id';
    public const REASON_INVALID_ATTEMPTS = 'invalid_attempts';

    /**
     * The first contract violation in `$envelope`, or null when it is acceptable.
     * Checks run most- to least-fundamental so the reason is the root cause.
     *
     * @param  array<string, mixed>  $envelope
     */
    public static function check(array $envelope): ?string
    {
        if (EnvelopeCodec::urn($envelope) === '') {
            return self::REASON_MISSING_URN;
        }

        $meta = $envelope['meta'] ?? null;
        if (! is_array($meta)) {
            return self::REASON_MISSING_META;
        }

        if (($meta['schema_version'] ?? null) !== EnvelopeCodec::SCHEMA_VERSION) {
            return self::REASON_UNSUPPORTED_SCHEMA_VERSION;
        }

        if (! is_array($envelope['data'] ?? null)) {
            return self::REASON_INVALID_DATA;
        }

        $traceId = $envelope['trace_id'] ?? null;
        if (! is_string($traceId) || $traceId === '') {
            return self::REASON_MISSING_TRACE_ID;
        }

        if (! is_int($envelope['attempts'] ?? null)) {
            return self::REASON_INVALID_ATTEMPTS;
        }

        return null;
    }

    /**
     * Whether the envelope satisfies the consumer contract. Mirrors
     * {@see EnvelopeCodec::accepts()}; kept here so callers using the validator
     * need not reach across to the codec.
     *
     * @param  array<string, mixed>  $envelope
     */
    public static function isValid(array $envelope): bool
    {
        return self::check($envelope) === null;
    }

    /**
     * Whether the envelope declares a `meta.schema_version` this SDK does not
     * understand (or carries no readable meta) — the canonical
     * "quarantine, don't drop" signal.
     *
     * @param  array<string, mixed>  $envelope
     */
    public static function isUnsupportedSchemaVersion(array $envelope): bool
    {
        $meta = $envelope['meta'] ?? null;

        return ! is_array($meta) || ($meta['schema_version'] ?? null) !== EnvelopeCodec::SCHEMA_VERSION;
    }

    /**
     * Assert the envelope is acceptable, throwing the reason otherwise.
     *
     * @param  array<string, mixed>  $envelope
     *
     * @throws InvalidEnvelopeException
     */
    public static function validate(array $envelope): void
    {
        $reason = self::check($envelope);
        if ($reason !== null) {
            throw InvalidEnvelopeException::because($reason, $envelope);
        }
    }
}
