<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use JsonException;
use stdClass;

/**
 * Classifies a raw Kafka record value the way the cross-SDK `kafka_poison` conformance section
 * requires (ADR-0034 §10, ADR-0032 §6; broker-bindings.md §6.5):
 *
 * - {@see self::OK} — the body parses into the envelope and carries a valid identity.
 * - {@see self::NO_IDENTITY} — the body parses (a top-level object or the literal `null`, every known
 *   member of the right type) but neither `job` nor the `urn` alias is a valid identity.
 * - {@see self::POISON} — the body cannot be decoded into the envelope: empty or invalid JSON,
 *   trailing data, a BOM, a non-finite number, a top-level value that is not an object, or a known
 *   member of the wrong type (including nested `meta.*` / `dead_letter.*`; integer members that are
 *   fractional, exponent-form or overflowing).
 *
 * On Kafka `no_identity` and `poison` share one fate (the record goes to `<topic>.dlq` with
 * `reason: poison`, the handler is not called). The identity rule itself is delegated to
 * {@see EnvelopeCodec::decode()}; this class only adds the strict typed-member pass, because the
 * codec is deliberately lenient about member types. Duplicate top-level keys resolve to the last
 * value (PHP `json_decode` semantics) and member names are case-sensitive.
 */
final class KafkaBodyClassifier
{
    public const OK = 'ok';
    public const NO_IDENTITY = 'no_identity';
    public const POISON = 'poison';

    /** Known envelope members and the kind each must have (`null` is always accepted). */
    private const TOP_LEVEL = [
        'trace_id' => 'string',
        'data' => 'object',
        'meta' => 'object',
        'attempts' => 'int',
        'dead_letter' => 'object',
    ];

    private const META = [
        'id' => 'string',
        'queue' => 'string',
        'lang' => 'string',
        'schema_version' => 'int',
        'created_at' => 'int',
    ];

    private const DEAD_LETTER = [
        'reason' => 'string',
        'error' => 'string',
        'exception' => 'string',
        'failed_at' => 'int',
        'original_queue' => 'string',
        'attempts' => 'int',
        'lang' => 'string',
    ];

    /**
     * Object decode for classification only. PHP refuses a property name that
     * starts with NUL (`JSON_ERROR_INVALID_PROPERTY_NAME`), although such a
     * key is valid JSON; the classifier only inspects known members, so the
     * offending escape is rewritten on a copy and the decode retried. The
     * forwarded body is never altered.
     *
     * @throws JsonException
     */
    private static function decode(string $body): mixed
    {
        try {
            return json_decode($body, false, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            if ($e->getCode() !== JSON_ERROR_INVALID_PROPERTY_NAME) {
                throw $e;
            }

            return json_decode(str_replace('"\\u0000', '"\\u0001', $body), false, 512, JSON_THROW_ON_ERROR);
        }
    }

    /**
     * @return self::OK|self::NO_IDENTITY|self::POISON
     */
    public static function classify(string $body): string
    {
        try {
            $decoded = self::decode($body);
        } catch (JsonException) {
            return self::POISON;
        }

        if ($decoded === null) {
            return self::NO_IDENTITY; // the literal `null`
        }

        if (! $decoded instanceof stdClass || self::hasNonFinite($decoded)) {
            return self::POISON;
        }

        if (! self::membersWellTyped($decoded, self::TOP_LEVEL)) {
            return self::POISON;
        }

        $meta = $decoded->meta ?? null;
        if ($meta instanceof stdClass && ! self::membersWellTyped($meta, self::META)) {
            return self::POISON;
        }

        $deadLetter = $decoded->dead_letter ?? null;
        if ($deadLetter instanceof stdClass && ! self::membersWellTyped($deadLetter, self::DEAD_LETTER)) {
            return self::POISON;
        }

        $silent = static function (): void {
        };

        return EnvelopeCodec::decode($body, $silent) === [] ? self::NO_IDENTITY : self::OK;
    }

    /**
     * @param  array<string, string>  $kinds
     */
    private static function membersWellTyped(stdClass $object, array $kinds): bool
    {
        foreach ($kinds as $member => $kind) {
            if (! property_exists($object, $member)) {
                continue;
            }

            $value = $object->{$member};
            if ($value === null) {
                continue;
            }

            $ok = match ($kind) {
                'string' => is_string($value),
                'int' => is_int($value), // 1.0, 1e3 and an overflowing integer all decode to float
                'object' => $value instanceof stdClass,
                default => false,
            };

            if (! $ok) {
                return false;
            }
        }

        return true;
    }

    /**
     * True when any number in the tree is not finite (`1e400` decodes to INF).
     */
    private static function hasNonFinite(mixed $value): bool
    {
        if (is_float($value)) {
            return ! is_finite($value);
        }

        if ($value instanceof stdClass || is_array($value)) {
            foreach ((array) $value as $item) {
                if (self::hasNonFinite($item)) {
                    return true;
                }
            }
        }

        return false;
    }
}
