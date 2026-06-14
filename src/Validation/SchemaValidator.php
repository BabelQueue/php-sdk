<?php

declare(strict_types=1);

namespace BabelQueue\Validation;

use BabelQueue\Exceptions\InvalidEnvelopeException;

/**
 * Optional, offline validation of a decoded envelope against the *bundled*
 * canonical JSON Schema — the strict, producer-side counterpart to the
 * consumer-focused {@see EnvelopeValidator}.
 *
 * Where {@see EnvelopeValidator} answers "may a consumer process this?" (and so
 * tolerates the `urn` inbound alias and ignores fields it does not need), this
 * answers "does this match the wire contract exactly as a producer must emit
 * it?" — required `job` (no `urn` alias), the full required `meta` block,
 * UUID-shaped ids, the `lang` enum, `schema_version` const, and the optional
 * `dead_letter` block when present. It is the in-process echo of the schema diff
 * CI runs against the conformance suite, usable by an app at runtime without any
 * network access or a JSON-Schema library.
 *
 * The bundled schema ({@see self::SCHEMA_PATH}) is a verbatim copy of the
 * canonical contract schema. Provenance / drift policy: see the class-level note
 * in {@see self::schema()} and the bundled file's sibling README in the package.
 *
 * Kept to a hand-rolled structural check (GR-7: zero heavy dependencies): it
 * enforces the subset of Draft-07 the envelope actually uses — `type`,
 * `required`, `enum`, `const`, `minLength`, `minimum`, `format: uuid` — not the
 * whole specification.
 */
final class SchemaValidator
{
    public const REASON_NOT_OBJECT = 'not_an_object';
    public const REASON_MISSING_REQUIRED = 'missing_required';
    public const REASON_WRONG_TYPE = 'wrong_type';
    public const REASON_EMPTY_STRING = 'empty_string';
    public const REASON_NOT_UUID = 'not_a_uuid';
    public const REASON_NOT_IN_ENUM = 'not_in_enum';
    public const REASON_WRONG_CONST = 'wrong_const';
    public const REASON_BELOW_MINIMUM = 'below_minimum';

    /** RFC 4122 textual UUID, as the schema's `format: uuid` requires. */
    private const UUID_PATTERN = '/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i';

    /** Path to the bundled, verbatim copy of the canonical contract schema. */
    private const SCHEMA_PATH = __DIR__ . '/message-envelope.schema.json';

    /** @var array<string, mixed>|null Lazily-parsed bundled schema. */
    private static ?array $schema = null;

    /**
     * The first schema violation in `$envelope` as a `"<json-pointer>: <reason>"`
     * string (e.g. `meta.schema_version: wrong_const`), or null when it conforms.
     * The `<reason>` is one of the `REASON_*` constants.
     *
     * @param  array<string, mixed>  $envelope
     */
    public static function check(array $envelope): ?string
    {
        return self::validateAgainst(self::schema(), $envelope, '');
    }

    /**
     * Whether the envelope conforms to the bundled schema.
     *
     * @param  array<string, mixed>  $envelope
     */
    public static function isValid(array $envelope): bool
    {
        return self::check($envelope) === null;
    }

    /**
     * Assert the envelope conforms, throwing the first violation otherwise.
     *
     * @param  array<string, mixed>  $envelope
     *
     * @throws InvalidEnvelopeException
     */
    public static function validate(array $envelope): void
    {
        $violation = self::check($envelope);
        if ($violation !== null) {
            throw InvalidEnvelopeException::because($violation, $envelope);
        }
    }

    /**
     * The bundled canonical JSON Schema, decoded once.
     *
     * Provenance: this file is a verbatim copy of the conformance suite's
     * `schema/message-envelope.schema.json`, itself a copy of the canonical
     * `.ssot/contracts/message-envelope.schema.json`. CI's `conformance` job
     * diffs the vendored `tests/conformance/schema/` copy against the canonical
     * suite; keep this `src/` copy byte-identical to that one so they cannot
     * drift. The contract is authoritative — if they disagree, fix this copy.
     *
     * @return array<string, mixed>
     */
    public static function schema(): array
    {
        if (self::$schema === null) {
            $raw = (string) file_get_contents(self::SCHEMA_PATH);
            /** @var array<string, mixed> $decoded */
            $decoded = (array) json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            self::$schema = $decoded;
        }

        return self::$schema;
    }

    /**
     * Validate `$value` against a single (sub-)schema node, returning the first
     * violation as `"<path>: <reason>"` or null. Recurses into object
     * `properties`. `$path` is the dotted JSON pointer to `$value`.
     *
     * @param  array<string, mixed>  $schema
     */
    private static function validateAgainst(array $schema, mixed $value, string $path): ?string
    {
        if (isset($schema['type']) && ($typeError = self::checkType($schema, $value, $path)) !== null) {
            return $typeError;
        }

        if (is_string($value) && ($stringError = self::checkString($schema, $value, $path)) !== null) {
            return $stringError;
        }

        if (is_int($value) && isset($schema['minimum']) && is_numeric($schema['minimum']) && $value < (int) $schema['minimum']) {
            return self::violation($path, self::REASON_BELOW_MINIMUM);
        }

        if (array_key_exists('const', $schema) && $value !== $schema['const']) {
            return self::violation($path, self::REASON_WRONG_CONST);
        }

        if (isset($schema['enum']) && is_array($schema['enum']) && ! in_array($value, $schema['enum'], true)) {
            return self::violation($path, self::REASON_NOT_IN_ENUM);
        }

        if (is_array($value) && self::isObjectSchema($schema)) {
            return self::checkObject($schema, $value, $path);
        }

        return null;
    }

    /**
     * Enforce the node's `type`, which may be a single string or a list of
     * accepted strings (e.g. `["string", "null"]`). Any match passes.
     *
     * @param  array<string, mixed>  $schema
     */
    private static function checkType(array $schema, mixed $value, string $path): ?string
    {
        $declared = $schema['type'];
        $types = is_array($declared) ? $declared : [$declared];

        foreach ($types as $type) {
            if (is_string($type) && self::matchesType($type, $value)) {
                return null;
            }
        }

        return self::violation($path, self::REASON_WRONG_TYPE);
    }

    /**
     * Whether `$value` satisfies a single JSON-Schema `type`. Only the types the
     * envelope schema actually declares are handled — `object`, `string`,
     * `integer`, `null` — keeping this a focused validator for *this* contract
     * rather than a general Draft-07 engine; any other `type` matches nothing.
     *
     * After `json_decode(..., true)` a JSON object becomes an associative PHP
     * array, so `object` rejects a non-empty list. An empty `[]` is ambiguous
     * (`{}` decodes identically) and is treated as a valid empty object — the
     * common wire case for `data`.
     */
    private static function matchesType(string $type, mixed $value): bool
    {
        return match ($type) {
            'object' => is_array($value) && ($value === [] || ! array_is_list($value)),
            'string' => is_string($value),
            // JSON has no int/float distinction; a whole-valued float still
            // satisfies "integer", matching JSON Schema's numeric semantics.
            'integer' => is_int($value) || (is_float($value) && floor($value) === $value),
            'null' => $value === null,
            default => false,
        };
    }

    /**
     * `minLength` and `format: uuid` string constraints.
     *
     * @param  array<string, mixed>  $schema
     */
    private static function checkString(array $schema, string $value, string $path): ?string
    {
        if (isset($schema['minLength']) && is_numeric($schema['minLength']) && mb_strlen($value) < (int) $schema['minLength']) {
            return self::violation($path, self::REASON_EMPTY_STRING);
        }

        if (($schema['format'] ?? null) === 'uuid' && preg_match(self::UUID_PATTERN, $value) !== 1) {
            return self::violation($path, self::REASON_NOT_UUID);
        }

        return null;
    }

    /**
     * `required` keys, then recurse into each declared `properties` entry that is
     * present. Unknown keys are allowed (`additionalProperties: true`).
     *
     * @param  array<string, mixed>  $schema
     * @param  array<array-key, mixed>  $value
     */
    private static function checkObject(array $schema, array $value, string $path): ?string
    {
        if (isset($schema['required']) && is_array($schema['required'])) {
            foreach (array_filter($schema['required'], 'is_string') as $key) {
                if (! array_key_exists($key, $value)) {
                    return self::violation(self::join($path, $key), self::REASON_MISSING_REQUIRED);
                }
            }
        }

        $properties = $schema['properties'] ?? null;
        if (! is_array($properties)) {
            return null;
        }

        foreach ($properties as $key => $propSchema) {
            $name = (string) $key;
            if (! is_array($propSchema) || ! array_key_exists($name, $value)) {
                continue;
            }

            /** @var array<string, mixed> $propSchema */
            $violation = self::validateAgainst($propSchema, $value[$name], self::join($path, $name));
            if ($violation !== null) {
                return $violation;
            }
        }

        return null;
    }

    /**
     * Whether a schema node describes an object — it declares `properties`,
     * `required`, or an explicit `object` type.
     *
     * @param  array<string, mixed>  $schema
     */
    private static function isObjectSchema(array $schema): bool
    {
        if (isset($schema['properties']) || isset($schema['required'])) {
            return true;
        }

        $type = $schema['type'] ?? null;

        return $type === 'object' || (is_array($type) && in_array('object', $type, true));
    }

    private static function violation(string $path, string $reason): string
    {
        return ($path === '' ? '<root>' : $path) . ': ' . $reason;
    }

    private static function join(string $path, string $key): string
    {
        return $path === '' ? $key : $path . '.' . $key;
    }
}
