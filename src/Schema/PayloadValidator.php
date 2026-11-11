<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

/**
 * Validates a message's `data` block against a per-URN JSON Schema (ADR-0024). Like the
 * envelope {@see \BabelQueue\Validation\SchemaValidator}, it is a hand-rolled subset of
 * Draft-07 (GR-7: zero heavy dependencies), but tuned for `data` rather than the envelope —
 * it adds `additionalProperties: false`, array `items`, and the `number`/`boolean`/`null`
 * types so its verdicts match the Go `schema` validator and babelqueue-registry's `compat`
 * linter. Supported keywords: `type` (a name or a union list such as `["string", "null"]`),
 * `required`, `properties`, `additionalProperties`, `items`, `enum`, `const`, `minLength`,
 * `maxLength`, `minimum`.
 *
 * `const` / `enum` compare numbers by mathematical value (`1` equals `1.0`; `true` is never `1`),
 * also inside arrays and objects. A keyword outside the subset never changes the verdict (it is
 * evaluated as absent); {@see unsupportedKeywords()} lists them, and the providers report one
 * warning per occurrence when a schema is registered (conformance `payload_schema_keywords`).
 */
final class PayloadValidator
{
    /** The keywords this validator enforces. */
    private const SUPPORTED_KEYWORDS = [
        'type', 'required', 'properties', 'additionalProperties', 'items', 'enum', 'const',
        'minLength', 'maxLength', 'minimum',
    ];

    /** Keywords that only annotate a schema (as does every keyword starting with `x-`); never a warning. */
    private const ANNOTATION_KEYWORDS = ['$schema', '$id', 'title', 'description', 'examples', 'default', '$comment'];

    private const KNOWN_TYPES = ['object', 'array', 'string', 'integer', 'number', 'boolean', 'null'];

    /**
     * Every keyword in $schema outside the supported subset, one entry per occurrence, as
     * `['pointer' => <RFC 6901 pointer of the schema object carrying it>, 'keyword' => <name>]`.
     * The root pointer is `''`, then `/properties/<name>` (`~` -> `~0`, `/` -> `~1`), `/items` and
     * `/additionalProperties`. The walk descends only into those three, and only when the value is a
     * JSON object, never into the value of an unsupported keyword (`allOf`, `definitions`, ...).
     *
     * @param  array<string, mixed>  $schema
     * @return list<array{pointer: string, keyword: string}>
     */
    public static function unsupportedKeywords(array $schema): array
    {
        $found = [];
        self::walkUnsupported($schema, '', $found);

        return $found;
    }

    /**
     * @param  array<mixed>  $node
     * @param  list<array{pointer: string, keyword: string}>  $found
     */
    private static function walkUnsupported(array $node, string $pointer, array &$found): void
    {
        // Only a JSON object is a schema node: a list (tuple-style `items`, a list-valued `properties`
        // or `additionalProperties`) is skipped, like a scalar. `[]` is an empty object and has no keys.
        if ($node !== [] && array_is_list($node)) {
            return;
        }

        foreach (array_keys($node) as $key) {
            $name = (string) $key;
            if (in_array($name, self::SUPPORTED_KEYWORDS, true)
                || in_array($name, self::ANNOTATION_KEYWORDS, true)
                || str_starts_with($name, 'x-')) {
                continue;
            }
            $found[] = ['pointer' => $pointer, 'keyword' => $name];
        }

        if (isset($node['properties']) && is_array($node['properties']) && ($node['properties'] === [] || ! array_is_list($node['properties']))) {
            foreach ($node['properties'] as $name => $sub) {
                if (is_array($sub)) {
                    self::walkUnsupported(
                        $sub,
                        $pointer . '/properties/' . str_replace(['~', '/'], ['~0', '~1'], (string) $name),
                        $found,
                    );
                }
            }
        }
        foreach (['items', 'additionalProperties'] as $keyword) {
            if (isset($node[$keyword]) && is_array($node[$keyword])) {
                self::walkUnsupported($node[$keyword], $pointer . '/' . $keyword, $found);
            }
        }
    }

    /**
     * The first violation in $value as `"<json-pointer>: <reason>"`, or null when it
     * conforms.
     *
     * @param  array<string, mixed>  $schema
     */
    public static function check(array $schema, mixed $value, string $path = ''): ?string
    {
        if (array_key_exists('const', $schema) && ! self::equal($value, $schema['const'])) {
            return self::violation($path, 'wrong_const');
        }
        if (isset($schema['enum']) && is_array($schema['enum'])) {
            $listed = false;
            foreach ($schema['enum'] as $candidate) {
                if (self::equal($value, $candidate)) {
                    $listed = true;
                    break;
                }
            }
            if (! $listed) {
                return self::violation($path, 'not_in_enum');
            }
        }

        $type = $schema['type'] ?? null;
        if (is_string($type)) {
            return self::checkType($schema, $type, $value, $path);
        }
        if (is_array($type)) {
            // Union (Draft-07): valid when the value matches ANY listed type, and then the keywords of
            // the matched type apply. A name outside the known types never matches; a list with no
            // known name constrains nothing, like an unknown single `type`.
            $names = array_values(array_filter(
                $type,
                static fn (mixed $t): bool => is_string($t) && in_array($t, self::KNOWN_TYPES, true),
            ));
            if ($names === []) {
                return null;
            }
            foreach ($names as $name) {
                if (self::matchesType($name, $value)) {
                    return self::checkType($schema, $name, $value, $path);
                }
            }

            return self::violation($path, 'wrong_type');
        }

        return null;
    }

    private static function matchesType(string $type, mixed $value): bool
    {
        return match ($type) {
            'object' => is_array($value) && ($value === [] || ! array_is_list($value)),
            'array' => is_array($value) && ($value === [] || array_is_list($value)),
            'string' => is_string($value),
            'integer' => self::isInteger($value),
            'number' => is_int($value) || is_float($value),
            'boolean' => is_bool($value),
            default => $value === null, // 'null'
        };
    }

    /**
     * @param  array<string, mixed>  $schema
     */
    private static function checkType(array $schema, string $type, mixed $value, string $path): ?string
    {
        return match ($type) {
            'object' => self::checkObject($schema, $value, $path),
            'array' => self::checkArray($schema, $value, $path),
            'string' => self::checkString($schema, $value, $path),
            'integer' => self::isInteger($value)
                ? self::checkMinimum($schema, $value, $path)
                : self::violation($path, 'not_an_integer'),
            'number' => (is_int($value) || is_float($value))
                ? self::checkMinimum($schema, $value, $path)
                : self::violation($path, 'not_a_number'),
            'boolean' => is_bool($value) ? null : self::violation($path, 'not_a_boolean'),
            'null' => $value === null ? null : self::violation($path, 'not_null'),
            default => null,
        };
    }

    /**
     * JSON equality for `const` / `enum`: numbers compare by mathematical value (`1` equals `1.0`),
     * recursively inside arrays and objects (object key order is irrelevant, array order is not);
     * `true` is never `1`, `"1"` is never `1` and `null` equals only `null`.
     */
    private static function equal(mixed $a, mixed $b): bool
    {
        if (is_bool($a) || is_bool($b)) {
            return is_bool($a) && is_bool($b) && $a === $b;
        }
        if ((is_int($a) || is_float($a)) && (is_int($b) || is_float($b))) {
            return self::numbersEqual($a, $b);
        }
        if ($a === null || $b === null) {
            return $a === null && $b === null;
        }
        if (is_string($a) && is_string($b)) {
            return $a === $b;
        }
        if (is_array($a) && is_array($b)) {
            if (count($a) !== count($b)) {
                return false;
            }
            if ($a === []) {
                return true;
            }
            if (array_is_list($a) !== array_is_list($b)) {
                return false;
            }
            if (array_is_list($a)) {
                foreach ($a as $i => $item) {
                    if (! self::equal($item, $b[$i])) {
                        return false;
                    }
                }

                return true;
            }
            foreach ($a as $key => $item) {
                if (! array_key_exists($key, $b) || ! self::equal($item, $b[$key])) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    private static function numbersEqual(int|float $a, int|float $b): bool
    {
        if (is_int($a) && is_int($b)) {
            return $a === $b;
        }
        if (is_float($a) && is_float($b)) {
            return $a === $b;
        }
        // int vs float: compare as integers when the float is integral and in int range (no precision
        // loss above 2^53), otherwise as floats.
        [$int, $float] = is_int($a) ? [$a, (float) $b] : [$b, (float) $a];
        if (is_finite($float) && floor($float) === $float && abs($float) < 9.2e18) {
            return (int) $float === $int;
        }

        return (float) $int === $float;
    }

    /**
     * @param  array<string, mixed>  $schema
     */
    private static function checkObject(array $schema, mixed $value, string $path): ?string
    {
        if (! is_array($value) || ($value !== [] && array_is_list($value))) {
            return self::violation($path, 'not_an_object');
        }

        if (isset($schema['required']) && is_array($schema['required'])) {
            foreach (array_filter($schema['required'], 'is_string') as $key) {
                if (! array_key_exists($key, $value)) {
                    return self::violation(self::join($path, $key), 'missing_required');
                }
            }
        }

        $properties = is_array($schema['properties'] ?? null) ? $schema['properties'] : [];
        $additionalAllowed = ! array_key_exists('additionalProperties', $schema)
            || $schema['additionalProperties'] !== false;

        foreach ($value as $key => $item) {
            $name = (string) $key;
            if (isset($properties[$name]) && is_array($properties[$name])) {
                /** @var array<string, mixed> $propSchema */
                $propSchema = $properties[$name];
                $violation = self::check($propSchema, $item, self::join($path, $name));
                if ($violation !== null) {
                    return $violation;
                }

                continue;
            }
            if (! $additionalAllowed) {
                return self::violation(self::join($path, $name), 'additional_not_allowed');
            }
        }

        return null;
    }

    /**
     * @param  array<string, mixed>  $schema
     */
    private static function checkArray(array $schema, mixed $value, string $path): ?string
    {
        if (! is_array($value) || ($value !== [] && ! array_is_list($value))) {
            return self::violation($path, 'not_an_array');
        }
        if (! isset($schema['items']) || ! is_array($schema['items'])) {
            return null;
        }

        /** @var array<string, mixed> $items */
        $items = $schema['items'];
        foreach ($value as $i => $item) {
            $violation = self::check($items, $item, $path . '[' . $i . ']');
            if ($violation !== null) {
                return $violation;
            }
        }

        return null;
    }

    /**
     * A `minLength` / `maxLength` bound as an int truncated toward zero, or null when the keyword is
     * absent, non-numeric or non-finite (NaN / +-INF) — such a bound is ignored, as in the other SDKs
     * (a raw `(int)` cast would turn NaN / INF into 0 and reject every non-empty string).
     */
    private static function lengthBound(mixed $bound): ?int
    {
        if (! is_numeric($bound)) {
            return null;
        }
        $number = $bound + 0;
        if (is_float($number) && ! is_finite($number)) {
            return null;
        }

        return (int) $bound;
    }

    /**
     * @param  array<string, mixed>  $schema
     */
    private static function checkString(array $schema, mixed $value, string $path): ?string
    {
        if (! is_string($value)) {
            return self::violation($path, 'not_a_string');
        }
        $min = self::lengthBound($schema['minLength'] ?? null);
        if ($min !== null && mb_strlen($value) < $min) {
            return self::violation($path, 'below_min_length');
        }
        $max = self::lengthBound($schema['maxLength'] ?? null);
        if ($max !== null && mb_strlen($value) > $max) {
            return self::violation($path, 'above_max_length');
        }

        return null;
    }

    /**
     * @param  array<string, mixed>  $schema
     */
    private static function checkMinimum(array $schema, mixed $value, string $path): ?string
    {
        if (! isset($schema['minimum']) || ! is_numeric($schema['minimum']) || ! is_numeric($value)) {
            return null;
        }

        return (float) $value < (float) $schema['minimum']
            ? self::violation($path, 'below_minimum')
            : null;
    }

    private static function isInteger(mixed $value): bool
    {
        return is_int($value) || (is_float($value) && is_finite($value) && floor($value) === $value);
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
