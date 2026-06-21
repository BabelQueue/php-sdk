<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Schema;

use BabelQueue\Schema\SensitivePath;
use BabelQueue\Schema\SensitivePaths;
use PHPUnit\Framework\TestCase;

/**
 * Extracting `x-gdpr-sensitive` paths (ADR-0030) from a decoded `data` JSON Schema. Mirrors the
 * Go `schema.Schema::SensitivePaths()` walk: nested objects, array items, the root mark, and the
 * boolean / category string forms — all validation-neutral.
 */
final class SensitivePathsTest extends TestCase
{
    public function test_marks_nested_object_array_and_root(): void
    {
        $schema = self::decode('{
            "type": "object",
            "properties": {
                "email": {"type": "string", "x-gdpr-sensitive": "email"},
                "order_id": {"type": "integer"},
                "profile": {
                    "type": "object",
                    "properties": {
                        "full_name": {"type": "string", "x-gdpr-sensitive": true},
                        "nickname": {"type": "string"}
                    }
                },
                "addresses": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "line": {"type": "string", "x-gdpr-sensitive": true},
                            "country": {"type": "string"}
                        }
                    }
                }
            }
        }');

        $paths = self::paths(SensitivePaths::of($schema));

        self::assertSame(['addresses[].line', 'email', 'profile.full_name'], $paths);
    }

    public function test_string_form_carries_category_boolean_form_does_not(): void
    {
        $schema = self::decode('{
            "type": "object",
            "properties": {
                "email": {"type": "string", "x-gdpr-sensitive": "email"},
                "name": {"type": "string", "x-gdpr-sensitive": true}
            }
        }');

        $byPath = [];
        foreach (SensitivePaths::of($schema) as $sp) {
            $byPath[$sp->path] = $sp->category;
        }

        self::assertSame('email', $byPath['email']);
        self::assertSame('', $byPath['name']);
    }

    public function test_root_mark_yields_empty_path(): void
    {
        $schema = self::decode('{"type": "string", "x-gdpr-sensitive": true}');

        $paths = SensitivePaths::of($schema);

        self::assertCount(1, $paths);
        self::assertSame('', $paths[0]->path);
    }

    public function test_non_marking_shapes_are_ignored(): void
    {
        // false, empty string, a number — none of these mark the property (validation-neutral).
        $schema = self::decode('{
            "type": "object",
            "properties": {
                "a": {"type": "string", "x-gdpr-sensitive": false},
                "b": {"type": "string", "x-gdpr-sensitive": ""},
                "c": {"type": "string", "x-gdpr-sensitive": 1},
                "d": {"type": "string"}
            }
        }');

        self::assertSame([], SensitivePaths::of($schema));
    }

    /**
     * @param  list<SensitivePath>  $paths
     * @return list<string>
     */
    private static function paths(array $paths): array
    {
        return array_map(static fn (SensitivePath $p): string => $p->path, $paths);
    }

    /**
     * @return array<string, mixed>
     */
    private static function decode(string $json): array
    {
        /** @var array<string, mixed> $decoded */
        $decoded = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        return $decoded;
    }
}
