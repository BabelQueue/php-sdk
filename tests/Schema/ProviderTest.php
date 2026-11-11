<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Schema;

use BabelQueue\Codec\EnvelopeCodec;
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

    public function test_map_provider_reports_unsupported_keywords_on_registration(): void
    {
        $sunk = [];
        $p = new MapProvider(
            [
                'urn:babel:orders:created' => ['type' => 'object', 'properties' => ['e' => ['type' => 'string', 'format' => 'email']]],
                'urn:babel:clean' => ['type' => 'object'],
            ],
            static function (string $message, string $pointer) use (&$sunk): void {
                $sunk[] = [$message, $pointer];
            },
        );

        self::assertSame(
            ['urn:babel:orders:created' => [['pointer' => '/properties/e', 'keyword' => 'format']]],
            $p->warnings(),
        );
        self::assertCount(1, $sunk);
        self::assertSame('/properties/e', $sunk[0][1]);
        self::assertStringContainsString('urn:babel:orders:created', $sunk[0][0]);
        self::assertStringContainsString('format', $sunk[0][0]);
    }

    public function test_map_provider_from_json_passes_the_sink_through(): void
    {
        $sunk = 0;
        $p = MapProvider::fromJson(['urn:a' => '{"type":"object","minProperties":1}'], static function () use (&$sunk): void {
            $sunk++;
        });

        self::assertSame(1, $sunk);
        self::assertSame([['pointer' => '', 'keyword' => 'minProperties']], $p->warnings()['urn:a']);
    }

    public function test_dir_provider_reports_once_when_the_schema_is_first_loaded(): void
    {
        $dir = $this->tempDir();
        mkdir($dir, 0o777, true);
        file_put_contents($dir . '/o.json', '{"type":"object","properties":{"n":{"type":"integer","maximum":5}}}');
        file_put_contents($dir . '/registry.json', '{"schemas":[{"urn":"urn:o","schema":"o.json"}]}');

        $sunk = 0;
        $p = new DirProvider($dir . '/registry.json', static function () use (&$sunk): void {
            $sunk++;
        });
        self::assertSame([], $p->warnings());
        self::assertSame(0, $sunk);

        $p->schemaFor('urn:o');
        $p->schemaFor('urn:o'); // cached: no second report

        self::assertSame(1, $sunk);
        self::assertSame(['urn:o' => [['pointer' => '/properties/n', 'keyword' => 'maximum']]], $p->warnings());
    }

    public function test_warnings_fall_back_to_the_codec_warning_handler(): void
    {
        $viaCodec = [];
        $viaCall = 0;
        EnvelopeCodec::setWarningHandler(static function (string $message, string $pointer) use (&$viaCodec): void {
            $viaCodec[] = $pointer;
        });

        try {
            self::assertNotNull(EnvelopeCodec::warningHandler());

            // no per-call sink: the codec's handler receives it
            new MapProvider(['urn:a' => ['type' => 'object', 'properties' => ['e' => ['format' => 'email']]]]);
            self::assertSame(['/properties/e'], $viaCodec);

            // a per-call sink wins over the codec's handler
            new MapProvider(['urn:a' => ['type' => 'object', 'pattern' => 'x']], static function () use (&$viaCall): void {
                $viaCall++;
            });
            self::assertSame(1, $viaCall);
            self::assertSame(['/properties/e'], $viaCodec);
        } finally {
            EnvelopeCodec::setWarningHandler(null);
        }

        self::assertNull(EnvelopeCodec::warningHandler());
    }
}
