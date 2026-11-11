<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Schema;

use BabelQueue\Schema\KeywordWarnings;
use BabelQueue\Schema\MapProvider;
use BabelQueue\Schema\PayloadValidator;
use BabelQueue\Tests\Support\ConformanceResults;
use PHPUnit\Framework\TestCase;

/**
 * Runs the shared cross-SDK payload-schema cases (ADR-0024) from the vendored conformance
 * suite: this validator must agree with the Go and Python ones on each case's `valid` flag.
 */
final class PayloadConformanceTest extends TestCase
{
    public function test_payload_cases_match_across_sdks(): void
    {
        /** @var array<string, mixed> $schema */
        $schema = ConformanceResults::manifest()['payload_schema']['schema'];

        ConformanceResults::each('payload_schema', static function (array $case) use ($schema): void {
            /** @var array<string, mixed> $data */
            $data = is_array($case['data'] ?? null) ? $case['data'] : [];
            $valid = PayloadValidator::check($schema, $data) === null;
            self::assertSame((bool) $case['valid'], $valid, 'case ' . (string) $case['name']);
        });
    }

    /**
     * The `payload_schema_keywords` section (conformance 1.10.0, K-17): union `type`, numeric equality
     * in `const` / `enum` and the registration warning for keywords outside the subset. Each case
     * carries its own schema: register it, compare the warnings as a SET of (pointer, keyword), then
     * validate `data` and compare `valid`.
     */
    public function test_payload_schema_keywords_cases(): void
    {
        ConformanceResults::each('payload_schema_keywords', static function (array $case): void {
            $name = (string) $case['name'];
            /** @var array<string, mixed> $schema */
            $schema = $case['schema'];

            $sunk = [];
            $provider = new MapProvider(
                ['urn:babel:conformance:keywords' => $schema],
                static function (string $message, string $pointer) use (&$sunk): void {
                    $sunk[] = $pointer;
                },
            );

            $pairs = static function (array $warnings): array {
                $set = array_map(static fn (array $w): string => $w['pointer'] . "\0" . $w['keyword'], $warnings);
                sort($set);

                return $set;
            };
            $expected = $pairs((array) ($case['warnings'] ?? []));
            $got = $pairs($provider->warnings()['urn:babel:conformance:keywords'] ?? []);

            self::assertSame($expected, $got, 'case ' . $name . ': warnings');
            self::assertCount(count($expected), $sunk, 'case ' . $name . ': one sink call per warning');
            self::assertSame(
                $expected,
                $pairs(KeywordWarnings::report('urn:babel:conformance:keywords', $schema, static function (): void {
                })),
                'case ' . $name . ': report() returns the same set',
            );

            /** @var array<string, mixed> $data */
            $data = is_array($case['data']) ? $case['data'] : [];
            $valid = PayloadValidator::check($schema, $data) === null;
            self::assertSame((bool) $case['valid'], $valid, 'case ' . $name);
        });
    }
}
