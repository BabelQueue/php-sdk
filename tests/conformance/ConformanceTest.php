<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Conformance;

use BabelQueue\Codec\EnvelopeCodec;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Runs the shared cross-SDK conformance suite (vendored under tests/conformance/).
 * The same manifest + fixtures are run by every BabelQueue SDK; passing here proves
 * this SDK reads/writes the canonical envelope identically to the others.
 */
final class ConformanceTest extends TestCase
{
    private static function suiteDir(): string
    {
        return __DIR__ . '/../conformance';
    }

    /**
     * @return array<string, array{0: array<string, mixed>}>
     */
    public static function cases(): array
    {
        $manifest = json_decode(
            (string) file_get_contents(self::suiteDir() . '/manifest.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );

        $provided = [];
        foreach ($manifest['cases'] as $case) {
            $provided[$case['name']] = [$case];
        }

        return $provided;
    }

    /**
     * @param  array<string, mixed>  $case
     */
    #[DataProvider('cases')]
    public function test_case(array $case): void
    {
        $raw = (string) file_get_contents(self::suiteDir() . '/' . $case['file']);
        $envelope = EnvelopeCodec::decode($raw);

        $this->assertNotSame([], $envelope, 'fixture must decode');

        if ($case['valid'] === false) {
            $this->assertFalse(
                EnvelopeCodec::accepts($envelope),
                $case['name'] . ' must be rejected: ' . ($case['reason'] ?? ''),
            );

            return;
        }

        $expect = $case['expect'];

        $this->assertTrue(EnvelopeCodec::accepts($envelope), $case['name'] . ' must be accepted');
        $this->assertSame($expect['urn'], EnvelopeCodec::urn($envelope));
        $this->assertSame($expect['attempts'], $envelope['attempts']);
        $this->assertSame($expect['lang'], $envelope['meta']['lang']);
        $this->assertSame($expect['schema_version'], $envelope['meta']['schema_version']);

        if (isset($expect['data'])) {
            $this->assertEquals($expect['data'], $envelope['data']);
        }

        if (isset($expect['dead_letter'])) {
            foreach ($expect['dead_letter'] as $key => $value) {
                $this->assertSame($value, $envelope['dead_letter'][$key]);
            }
        }

        // Per-message fields must be present (not asserted by value).
        $this->assertArrayHasKey('id', $envelope['meta']);
        $this->assertNotSame('', (string) $envelope['trace_id']);
    }
}
