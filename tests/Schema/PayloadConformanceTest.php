<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Schema;

use BabelQueue\Schema\PayloadValidator;
use PHPUnit\Framework\TestCase;

/**
 * Runs the shared cross-SDK payload-schema cases (ADR-0024) from the vendored conformance
 * suite: this validator must agree with the Go and Python ones on each case's `valid` flag.
 */
final class PayloadConformanceTest extends TestCase
{
    public function test_payload_cases_match_across_sdks(): void
    {
        $path = __DIR__ . '/../conformance/manifest.json';
        if (! is_file($path)) {
            self::markTestSkipped('vendored conformance suite not present');
        }

        /** @var array<string, mixed> $manifest */
        $manifest = (array) json_decode((string) file_get_contents($path), true, 512, JSON_THROW_ON_ERROR);
        $section = $manifest['payload_schema'] ?? null;
        if (! is_array($section) || ! is_array($section['schema'] ?? null) || ! is_array($section['cases'] ?? null)) {
            self::markTestSkipped('manifest has no payload_schema section');
        }

        /** @var array<string, mixed> $schema */
        $schema = $section['schema'];
        /** @var array<int, mixed> $cases */
        $cases = $section['cases'];
        self::assertNotEmpty($cases);

        foreach ($cases as $case) {
            self::assertIsArray($case);
            /** @var array<string, mixed> $data */
            $data = is_array($case['data'] ?? null) ? $case['data'] : [];
            $valid = PayloadValidator::check($schema, $data) === null;
            self::assertSame((bool) $case['valid'], $valid, 'case ' . (string) $case['name']);
        }
    }
}
