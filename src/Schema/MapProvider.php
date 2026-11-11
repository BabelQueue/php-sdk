<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

use RuntimeException;

/**
 * In-memory {@see SchemaProvider}, suitable for tests and for embedding schemas in code. It
 * is read-only after construction.
 *
 * Registering a schema reports every keyword outside the validator's supported subset through
 * {@see KeywordWarnings} (once per occurrence, at construction); {@see warnings()} returns them.
 */
final class MapProvider implements SchemaProvider
{
    /** @var array<string, list<array{pointer: string, keyword: string}>> urn => warnings reported at registration */
    private array $warnings = [];

    /**
     * @param  array<string, array<string, mixed>>  $schemas  urn => decoded JSON Schema
     * @param  (callable(string, string): void)|null  $onWarning  fn(string $message, string $pointer); defaults to error_log()
     */
    public function __construct(private readonly array $schemas, ?callable $onWarning = null)
    {
        foreach ($this->schemas as $urn => $schema) {
            $found = KeywordWarnings::report((string) $urn, $schema, $onWarning);
            if ($found !== []) {
                $this->warnings[(string) $urn] = $found;
            }
        }
    }

    /**
     * The unsupported-keyword warnings each URN's schema produced at registration (only URNs that
     * produced at least one): `urn => list<['pointer' => ..., 'keyword' => ...]>`.
     *
     * @return array<string, list<array{pointer: string, keyword: string}>>
     */
    public function warnings(): array
    {
        return $this->warnings;
    }

    /**
     * Build a MapProvider from URN => raw JSON Schema strings, decoding each.
     *
     * @param  array<string, string>  $raw
     * @param  (callable(string, string): void)|null  $onWarning  see {@see __construct()}
     */
    public static function fromJson(array $raw, ?callable $onWarning = null): self
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

        return new self($schemas, $onWarning);
    }

    public function schemaFor(string $urn): ?array
    {
        $schema = $this->schemas[$urn] ?? null;

        return is_array($schema) ? $schema : null;
    }
}
