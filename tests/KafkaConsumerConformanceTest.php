<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\KafkaConsumer;
use BabelQueue\Transport\KafkaConsumerClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * Apache Kafka (§6) consume-side conformance: the `kafka` block's `attempts_reconciliation` cases.
 * The PHP consumer takes the `bq-attempts` header as authoritative (Kafka has no native delivery
 * count), falling back to the body's `attempts` only when the header is absent — NOT a max (the
 * header overrides even when lower), matching the runtime SDKs (Go/Node/Java/Python/.NET).
 */
final class KafkaConsumerConformanceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_attempts_reconciliation_matches_the_golden_cases(): void
    {
        $manifest = json_decode(
            (string) file_get_contents(__DIR__ . '/conformance/manifest.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        $cases = $manifest['kafka']['attempts_reconciliation']['cases'];

        foreach ($cases as $case) {
            $payload = (string) json_encode([
                'job' => 'urn:babel:orders:created',
                'trace_id' => 'trace-1',
                'data' => [],
                'meta' => ['id' => 'm', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1],
                'attempts' => $case['body_attempts'],
            ]);

            // header_attempts: null means the bq-attempts header is absent.
            $headers = $case['header_attempts'] === null ? [] : ['bq-attempts' => (string) $case['header_attempts']];

            $client = Mockery::mock(KafkaConsumerClient::class);
            $client->shouldReceive('receive')->once()->andReturn(['payload' => $payload, 'headers' => $headers]);

            $message = (new KafkaConsumer($client))->receive();

            $this->assertNotNull($message, $case['name']);
            $this->assertSame($case['expected_attempts'], $message->attempts(), $case['name']);
        }
    }
}
