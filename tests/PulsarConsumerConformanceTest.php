<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use BabelQueue\Tests\Support\ConformanceResults;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * Apache Pulsar (§5) consume-side conformance: the `pulsar` block's `attempts_reconciliation`
 * cases. The PHP consumer reconciles `attempts` (bq-attempts, else body, floored by redeliveryCount) on receive,
 * matching the native-consumer SDKs (.NET/Java/Node) and the Transport+App SDKs (Python/Go).
 */
final class PulsarConsumerConformanceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_attempts_reconciliation_matches_the_golden_cases(): void
    {
        ConformanceResults::each('pulsar', function (array $case): void {
            $payload = (string) json_encode([
                'job' => 'urn:babel:orders:created',
                'trace_id' => 'trace-1',
                'data' => new \stdClass(), // canonical `{}`: a list `[]` is a wrong-typed member, i.e. poison
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

            $message = (new PulsarConsumer($client, subscriptionType: 'Shared'))->receive();

            $this->assertNotNull($message, $case['name']);
            $this->assertSame($case['expected_attempts'], $message->attempts(), $case['name']);
        }, 'attempts_reconciliation');
    }

    /**
     * §5.7 consumer-side schema-version gate: the manifest's `pulsar.schema_version_gate` cases. The
     * body is always the valid fixture envelope, so a `reject` verdict can only come from the property
     * gate (the body is left undecoded and the routing URN is the reserved sentinel).
     */
    public function test_schema_version_gate_matches_the_golden_cases(): void
    {
        $gate = ConformanceResults::manifest()['pulsar']['schema_version_gate'];
        $body = (string) file_get_contents(__DIR__ . '/conformance/' . $gate['fixture']);
        $fixtureUrn = (string) json_decode($body, true, 512, JSON_THROW_ON_ERROR)['job'];

        ConformanceResults::each('pulsar', function (array $case) use ($gate, $body, $fixtureUrn): void {
            $absent = ($case['absent'] ?? false) === true;
            $properties = $absent ? [] : [$gate['property'] => (string) $case['value']];
            $label = $absent ? 'absent' : bin2hex((string) $case['value']);

            $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
            $client->shouldReceive('receive')->once()->andReturn([
                'messageId' => 'm',
                'payload' => $body,
                'properties' => $properties,
                'redeliveryCount' => 0,
            ]);

            $message = (new PulsarConsumer($client, subscriptionType: 'Shared'))->receive();

            $this->assertNotNull($message, $label);
            if ($case['expect'] === 'decode') {
                $this->assertSame($fixtureUrn, $message->getUrn(), $label);
                $this->assertArrayHasKey('job', $message->envelope(), $label);
            } else {
                $this->assertSame(PulsarConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX . $case['value'], $message->getUrn(), $label);
                $this->assertSame($body, $message->envelope()['raw'], $label);
                $this->assertArrayNotHasKey('job', $message->envelope(), $label);
            }
        }, 'schema_version_gate');
    }
}
