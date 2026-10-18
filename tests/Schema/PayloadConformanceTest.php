<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Schema;

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
}
