<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

/**
 * Extracts the `x-gdpr-sensitive` marks (ADR-0030) from a decoded `data` JSON Schema — the same
 * raw `array<string, mixed>` shape {@see PayloadValidator} validates. It is the PHP mirror of the
 * Go `schema.Schema::SensitivePaths()` walk and babelqueue-registry's inventory.
 *
 * The `x-gdpr-sensitive` keyword is a recognised-but-**validation-neutral** JSON-Schema extension:
 * it never makes a value valid or invalid (so annotating a schema is never a breaking change,
 * GR-1). {@see \BabelQueue\Gdpr\Gdpr} consumes the paths this returns to encrypt each marked leaf
 * on produce and decrypt it on consume.
 *
 * The keyword is accepted in two forms, matching the Go/registry parser:
 *  - the boolean `true` — marked, with an empty category;
 *  - a non-empty string (e.g. `"email"`) — marked, with that documentation category.
 * Any other shape (`false`, `""`, a number, an object) leaves the property unmarked.
 */
final class SensitivePaths
{
    /**
     * Every property the schema marked `x-gdpr-sensitive`, in sorted path order. Descends nested
     * objects (dotted paths like `profile.full_name`), array `items` (`addresses[].line`), and
     * reports a mark on the root schema itself as the empty path `""`.
     *
     * @param  array<string, mixed>  $schema  a decoded `data` JSON Schema
     * @return list<SensitivePath>
     */
    public static function of(array $schema): array
    {
        $out = [];
        self::collect($schema, '', $out);
        usort($out, static fn (SensitivePath $a, SensitivePath $b): int => $a->path <=> $b->path);

        return $out;
    }

    /**
     * @param  array<string, mixed>  $schema
     * @param  list<SensitivePath>  $out
     */
    private static function collect(array $schema, string $path, array &$out): void
    {
        [$marked, $category] = self::mark($schema['x-gdpr-sensitive'] ?? null);
        if ($marked) {
            $out[] = new SensitivePath($path, $category);
        }

        $properties = $schema['properties'] ?? null;
        if (is_array($properties)) {
            foreach ($properties as $name => $sub) {
                if (is_array($sub)) {
                    /** @var array<string, mixed> $sub */
                    self::collect($sub, self::join($path, (string) $name), $out);
                }
            }
        }

        $items = $schema['items'] ?? null;
        if (is_array($items)) {
            /** @var array<string, mixed> $items */
            self::collect($items, $path . '[]', $out);
        }
    }

    /**
     * Resolve the `x-gdpr-sensitive` value into [marked, category], mirroring the Go/registry
     * parser: boolean `true` => marked with no category; a non-empty string => marked with that
     * category; anything else => unmarked.
     *
     * @return array{0: bool, 1: string}
     */
    private static function mark(mixed $value): array
    {
        if ($value === true) {
            return [true, ''];
        }
        if (is_string($value) && $value !== '') {
            return [true, $value];
        }

        return [false, ''];
    }

    private static function join(string $path, string $key): string
    {
        return $path === '' ? $key : $path . '.' . $key;
    }
}
