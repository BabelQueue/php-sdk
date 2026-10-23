<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\KafkaConsumer;
use BabelQueue\Transport\KafkaConsumerClient;
use BabelQueue\Tests\Support\ConformanceResults;
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
        ConformanceResults::each('kafka', function (array $case): void {
            $payload = (string) json_encode([
                'job' => 'urn:babel:orders:created',
                'trace_id' => 'trace-1',
                // `data` is a JSON object on the wire; a PHP `[]` would encode as `[]` and be quarantined as poison.
                'data' => new \stdClass(),
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
        }, 'attempts_reconciliation');
    }

    /**
     * §6.7 consumer-side schema-version gate: the manifest's `kafka.schema_version_gate` cases. The
     * body is always the valid fixture envelope, so a `reject` verdict can only come from the header
     * gate (the body is left undecoded and the routing URN is the reserved sentinel).
     */
    public function test_schema_version_gate_matches_the_golden_cases(): void
    {
        $gate = ConformanceResults::manifest()['kafka']['schema_version_gate'];
        $body = (string) file_get_contents(__DIR__ . '/conformance/' . $gate['fixture']);
        $fixtureUrn = (string) json_decode($body, true, 512, JSON_THROW_ON_ERROR)['job'];

        ConformanceResults::each('kafka', function (array $case) use ($gate, $body, $fixtureUrn): void {
            $absent = ($case['absent'] ?? false) === true;
            // A JSON null value is a header present with a null value, which §6.7 treats as absent.
            $headers = $absent ? [] : [$gate['property'] => $case['value'] === null ? null : (string) $case['value']];
            $label = $absent ? 'absent' : ($case['value'] === null ? 'null' : bin2hex((string) $case['value']));

            $client = Mockery::mock(KafkaConsumerClient::class);
            $client->shouldReceive('receive')->once()->andReturn(['payload' => $body, 'headers' => $headers]);

            $message = (new KafkaConsumer($client))->receive();

            $this->assertNotNull($message, $label);
            if ($case['expect'] === 'decode') {
                $this->assertSame($fixtureUrn, $message->getUrn(), $label);
                $this->assertArrayHasKey('job', $message->envelope(), $label);
            } else {
                $this->assertSame(KafkaConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX . $case['value'], $message->getUrn(), $label);
                $this->assertSame($body, $message->envelope()['raw'], $label);
                $this->assertArrayNotHasKey('job', $message->envelope(), $label);
            }
        }, 'schema_version_gate');
    }
}
