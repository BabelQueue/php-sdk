<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Tests\Support\ConformanceResults;
use BabelQueue\Transport\KafkaBodyClassifier;
use PHPUnit\Framework\TestCase;

/**
 * Body classification outside Kafka (ADR-0036 decision 4): the `identity_verdict` section. Its case set is
 * every `kafka_poison` body with `no_identity` mapped to `poison`, so the PHP strict decoder verdict
 * ({@see KafkaBodyClassifier}, which applies the ADR-0032 section 6 identity rule on top of the codec's
 * lenient decode) is the same for every binding: `ok` reaches a handler, `poison` takes the poison route.
 */
final class IdentityVerdictConformanceTest extends TestCase
{
    public function test_identity_verdict_matches_the_golden_cases(): void
    {
        ConformanceResults::each('identity_verdict', function (array $case): void {
            $verdict = KafkaBodyClassifier::classify((string) $case['body']);
            $verdict = $verdict === KafkaBodyClassifier::OK ? 'ok' : 'poison'; // no_identity and poison share one fate

            $this->assertSame($case['expect'], $verdict, (string) $case['name']);
        });
    }
}
