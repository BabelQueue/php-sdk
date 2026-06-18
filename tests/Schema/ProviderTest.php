<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Schema;

use BabelQueue\Schema\DirProvider;
use BabelQueue\Schema\MapProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ProviderTest extends TestCase
{
    public function test_map_provider_from_json(): void
    {
        $p = MapProvider::fromJson([
            'urn:babel:orders:created' => '{"type":"object","required":["order_id"]}',
        ]);

        self::assertNotNull($p->schemaFor('urn:babel:orders:created'));
        self::assertNull($p->schemaFor('urn:babel:unknown'));
    }

    public function test_map_provider_from_json_rejects_invalid(): void
    {
        $this->expectException(RuntimeException::class);
        MapProvider::fromJson(['u' => 'not json']);
    }

    public function test_dir_provider_reads_registry_lazily(): void
    {
        $dir = $this->tempDir();
        mkdir($dir . '/schemas', 0o777, true);
        file_put_contents($dir . '/schemas/orders.json', '{"type":"object","required":["order_id"],"properties":{"order_id":{"type":"integer"}}}');
        // the empty-urn entry is ignored on load
        file_put_contents($dir . '/registry.json', '{"schemas":[{"urn":"urn:babel:orders:created","schema":"schemas/orders.json"},{"urn":"","schema":"x"}]}');

        $p = new DirProvider($dir . '/registry.json');
        // call twice: the second hits the cache
        self::assertNotNull($p->schemaFor('urn:babel:orders:created'));
        self::assertNotNull($p->schemaFor('urn:babel:orders:created'));
        self::assertNull($p->schemaFor('urn:babel:unknown'));
    }

    public function test_dir_provider_missing_manifest_throws(): void
    {
        $this->expectException(RuntimeException::class);
        new DirProvider($this->tempDir() . '/nope.json');
    }

    public function test_dir_provider_invalid_manifest_throws(): void
    {
        $dir = $this->tempDir();
        mkdir($dir, 0o777, true);
        file_put_contents($dir . '/registry.json', 'not json');

        $this->expectException(RuntimeException::class);
        new DirProvider($dir . '/registry.json');
    }

    public function test_dir_provider_missing_schema_file_throws(): void
    {
        $dir = $this->tempDir();
        mkdir($dir, 0o777, true);
        file_put_contents($dir . '/registry.json', '{"schemas":[{"urn":"u","schema":"missing.json"}]}');

        $p = new DirProvider($dir . '/registry.json');
        $this->expectException(RuntimeException::class);
        $p->schemaFor('u');
    }

    private function tempDir(): string
    {
        return sys_get_temp_dir() . '/bqschema_' . bin2hex(random_bytes(6));
    }
}
