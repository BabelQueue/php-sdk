<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

/**
 * A source of per-URN `data` schemas (ADR-0024). Given a message URN, it returns the decoded
 * JSON Schema for that URN's `data` block, or null when no schema is registered — in which
 * case the caller skips validation (the feature is opt-in).
 *
 * The reference {@see MapProvider} is in-memory; {@see DirProvider} reads a
 * babelqueue-registry `registry.json`. A production provider (a service client, an embedded
 * bundle) implements the same single method. This is the PHP mirror of the Go
 * `schema.Provider` interface.
 */
interface SchemaProvider
{
    /**
     * The decoded JSON Schema registered for $urn, or null when none is registered.
     *
     * @return array<string, mixed>|null
     */
    public function schemaFor(string $urn): ?array;
}
