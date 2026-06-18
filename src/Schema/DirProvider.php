<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

use RuntimeException;

/**
 * Reads schemas from a babelqueue-registry manifest (`registry.json`): a list of
 * `{urn, schema}` entries mapping each URN to a Draft-07 schema file for its `data` block.
 * This is the bridge that makes the registry's governed schemas enforceable at runtime.
 *
 * The manifest is read once in the constructor; schema files are read and decoded lazily and
 * cached. A URN that is not in the manifest returns null (skip validation); a URN whose
 * schema file is missing or unreadable throws (a configuration/IO error → the consumer
 * redelivers until it is fixed). The PHP mirror of the Go `schema.DirProvider`.
 */
final class DirProvider implements SchemaProvider
{
    private string $dir;

    /** @var array<string, string> urn => schema file path (relative to the manifest dir) */
    private array $files = [];

    /** @var array<string, array<string, mixed>> lazily-decoded schema cache */
    private array $cache = [];

    public function __construct(string $manifestPath)
    {
        $raw = @file_get_contents($manifestPath);
        if ($raw === false) {
            throw new RuntimeException("schema: cannot read registry manifest [{$manifestPath}].");
        }
        $manifest = json_decode($raw, true);
        if (! is_array($manifest)) {
            throw new RuntimeException("schema: invalid registry manifest [{$manifestPath}].");
        }

        $this->dir = \dirname($manifestPath);

        $entries = is_array($manifest['schemas'] ?? null) ? $manifest['schemas'] : [];
        foreach ($entries as $entry) {
            if (! is_array($entry)) {
                continue;
            }
            $urn = is_string($entry['urn'] ?? null) ? $entry['urn'] : '';
            $file = is_string($entry['schema'] ?? null) ? $entry['schema'] : '';
            if ($urn === '' || $file === '') {
                continue;
            }
            $this->files[$urn] = $file;
        }
    }

    public function schemaFor(string $urn): ?array
    {
        if (isset($this->cache[$urn])) {
            return $this->cache[$urn];
        }
        if (! isset($this->files[$urn])) {
            return null;
        }

        $file = $this->files[$urn];
        $path = self::isAbsolute($file) ? $file : $this->dir . DIRECTORY_SEPARATOR . $file;

        $raw = @file_get_contents($path);
        if ($raw === false) {
            throw new RuntimeException("schema: cannot read schema for [{$urn}] ({$file}).");
        }
        $decoded = json_decode($raw, true);
        if (! is_array($decoded)) {
            throw new RuntimeException("schema: invalid schema for [{$urn}] ({$file}).");
        }

        /** @var array<string, mixed> $decoded */
        $this->cache[$urn] = $decoded;

        return $decoded;
    }

    private static function isAbsolute(string $path): bool
    {
        return $path !== '' && ($path[0] === '/' || preg_match('#^[A-Za-z]:[\\\\/]#', $path) === 1);
    }
}
