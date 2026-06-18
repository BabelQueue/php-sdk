<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

use RuntimeException;

/**
 * In-memory {@see SchemaProvider}, suitable for tests and for embedding schemas in code. It
 * is read-only after construction.
 */
final class MapProvider implements SchemaProvider
{
    /**
     * @param  array<string, array<string, mixed>>  $schemas  urn => decoded JSON Schema
     */
    public function __construct(private readonly array $schemas)
    {
    }

    /**
     * Build a MapProvider from URN => raw JSON Schema strings, decoding each.
     *
     * @param  array<string, string>  $raw
     */
    public static function fromJson(array $raw): self
    {
        $schemas = [];
        foreach ($raw as $urn => $json) {
            $decoded = json_decode($json, true);
            if (! is_array($decoded)) {
                throw new RuntimeException("schema: invalid JSON schema for [{$urn}].");
            }
            /** @var array<string, mixed> $decoded */
            $schemas[$urn] = $decoded;
        }

        return new self($schemas);
    }

    public function schemaFor(string $urn): ?array
    {
        $schema = $this->schemas[$urn] ?? null;

        return is_array($schema) ? $schema : null;
    }
}
