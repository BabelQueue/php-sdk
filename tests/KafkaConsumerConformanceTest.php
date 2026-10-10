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

    /**
     * §6.7 consumer-side schema-version gate: the manifest's `kafka.schema_version_gate` cases. The
     * body is always the valid fixture envelope, so a `reject` verdict can only come from the header
     * gate (the body is left undecoded and the routing URN is the reserved sentinel).
     */
    public function test_schema_version_gate_matches_the_golden_cases(): void
    {
        $manifest = json_decode(
            (string) file_get_contents(__DIR__ . '/conformance/manifest.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        $gate = $manifest['kafka']['schema_version_gate'];
        $body = (string) file_get_contents(__DIR__ . '/conformance/' . $gate['fixture']);
        $fixtureUrn = (string) json_decode($body, true, 512, JSON_THROW_ON_ERROR)['job'];

        foreach ($gate['cases'] as $case) {
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
        }
    }
}
