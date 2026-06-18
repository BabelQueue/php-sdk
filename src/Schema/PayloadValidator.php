<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

/**
 * Validates a message's `data` block against a per-URN JSON Schema (ADR-0024). Like the
 * envelope {@see \BabelQueue\Validation\SchemaValidator}, it is a hand-rolled subset of
 * Draft-07 (GR-7: zero heavy dependencies), but tuned for `data` rather than the envelope —
 * it adds `additionalProperties: false`, array `items`, and the `number`/`boolean`/`null`
 * types so its verdicts match the Go `schema` validator and babelqueue-registry's `compat`
 * linter. Supported keywords: `type`, `required`, `properties`, `additionalProperties`,
 * `items`, `enum`, `const`, `minLength`, `minimum`. Unknown keywords are ignored.
 */
final class PayloadValidator
{
    /**
     * The first violation in $value as `"<json-pointer>: <reason>"`, or null when it
     * conforms.
     *
     * @param  array<string, mixed>  $schema
     */
    public static function check(array $schema, mixed $value, string $path = ''): ?string
    {
        if (array_key_exists('const', $schema) && $value !== $schema['const']) {
            return self::violation($path, 'wrong_const');
        }
        if (isset($schema['enum']) && is_array($schema['enum']) && ! in_array($value, $schema['enum'], true)) {
            return self::violation($path, 'not_in_enum');
        }

        $type = isset($schema['type']) && is_string($schema['type']) ? $schema['type'] : '';

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
     * @param  array<string, mixed>  $schema
     */
    private static function checkString(array $schema, mixed $value, string $path): ?string
    {
        if (! is_string($value)) {
            return self::violation($path, 'not_a_string');
        }
        if (isset($schema['minLength']) && is_numeric($schema['minLength']) && mb_strlen($value) < (int) $schema['minLength']) {
            return self::violation($path, 'below_min_length');
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
        return is_int($value) || (is_float($value) && floor($value) === $value);
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
