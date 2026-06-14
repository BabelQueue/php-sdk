<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * Apache Pulsar (§5) consume-side conformance: the `pulsar` block's `attempts_reconciliation`
 * cases. The PHP consumer reconciles `attempts = max(body.attempts, redeliveryCount)` on receive,
 * matching the native-consumer SDKs (.NET/Java/Node) and the Transport+App SDKs (Python/Go).
 */
final class PulsarConsumerConformanceTest extends TestCase
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
        $cases = $manifest['pulsar']['attempts_reconciliation']['cases'];

        foreach ($cases as $case) {
            $payload = (string) json_encode([
                'job' => 'urn:babel:orders:created',
                'trace_id' => 'trace-1',
                'data' => [],
                'meta' => ['id' => 'm', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1],
                'attempts' => $case['body_attempts'],
            ]);

            $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
            $client->shouldReceive('receive')->once()->andReturn([
                'messageId' => 'm',
                'payload' => $payload,
                'properties' => [],
                'redeliveryCount' => $case['redelivery_count'],
            ]);

            $message = (new PulsarConsumer($client))->receive();

            $this->assertNotNull($message, $case['name']);
            $this->assertSame($case['expected_attempts'], $message->attempts(), $case['name']);
        }
    }
}
