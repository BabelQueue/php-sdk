<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\SqsClient;
use BabelQueue\Transport\SqsTransport;
use BabelQueue\Tests\Support\ConformanceResults;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * Amazon SQS binding conformance against the vendored canonical suite's `sqs` block.
 *
 * The `php-sdk` ships the produce-side `SqsTransport`, so it satisfies
 * `attribute_projection`. The consume-side `attempts_reconciliation` is exercised by
 * the framework-less runtime SDKs (Go/Python/Node/Java/.NET); the Laravel drop-in
 * driver surfaces the broker's native count instead (exempt per the manifest note).
 */
final class SqsConformanceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_attribute_projection_matches_the_golden(): void
    {
        ConformanceResults::each('sqs', function (array $projection): void {
            $body = (string) file_get_contents(__DIR__ . '/conformance/' . $projection['envelope_file']);

            $captured = null;
            $client = Mockery::mock(SqsClient::class);
            $client->shouldReceive('sendMessage')->once()->with(
                Mockery::on(function (array $args) use (&$captured): bool {
                    $captured = $args;

                    return true;
                }),
            );

            (new SqsTransport($client, 'https://sqs.eu-central-1.amazonaws.com/123456789012/orders'))->publish($body);

            $attributes = $captured['MessageAttributes'];
            $want = $projection['message_attributes'];

            $this->assertCount(count($want), $attributes);
            foreach ($want as $key => $expected) {
                $this->assertArrayHasKey($key, $attributes, $key);
                $this->assertSame($expected['DataType'], $attributes[$key]['DataType'], $key);
                $this->assertSame($expected['StringValue'], $attributes[$key]['StringValue'], $key);
            }
        }, 'attribute_projection');
    }

    /**
     * Consume-side cases (`attempts_reconciliation`, `schema_version_gate`, broker-bindings.md §3.6/§3.7) belong to
     * a consumer. The php-sdk core is framework-agnostic and publish-only (ADR-0010); `SqsTransport` only
     * publishes and consumption runs in the framework workers (Laravel/Symfony), where the cases are
     * evaluated. They are therefore recorded as per-case `skip`s citing the ADRs (user decision 2026-10-18),
     * not as gaps. The test pins the recorded status and the citation, and that no core consumer exists:
     * adding one means the skips must be replaced by real evaluation.
     */
    public function test_consume_side_cases_are_skipped_with_an_adr(): void
    {
        self::assertFalse(class_exists('BabelQueue\\Transport\\SqsConsumer'), 'a core consumer would implement §3.6/§3.7: evaluate the cases instead of skipping them');
        self::assertFalse(method_exists(SqsTransport::class, 'receive'), 'SqsTransport::receive would implement §3.6/§3.7: evaluate the cases instead of skipping them');

        ConformanceResults::skipAll('sqs', 'SQS consumption is out of scope for the framework-agnostic core (ADR-0010); it runs in the framework workers', 'attempts_reconciliation');
        ConformanceResults::skipAll('sqs', 'SQS consumption is out of scope for the framework-agnostic core (ADR-0010); it runs in the framework workers', 'schema_version_gate');

        foreach (['attempts_reconciliation', 'schema_version_gate'] as $sub) {
            $ids = array_keys(ConformanceResults::cases('sqs', $sub));
            self::assertNotEmpty($ids);
            foreach ($ids as $id) {
                $recorded = ConformanceResults::recorded()['sqs'][(string) $id] ?? null;
                self::assertSame('skip', $recorded['status'] ?? null, 'sqs/' . $id);
                self::assertMatchesRegularExpression('/ADR-\d{4}/', $recorded['reason'] ?? '', 'sqs/' . $id . ' cites an ADR');
                self::assertArrayNotHasKey('sqs/' . $id, ConformanceResults::knownGaps(), 'a skipped case is not a known gap');
            }
        }
    }
}
