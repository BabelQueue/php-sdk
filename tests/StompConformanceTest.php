<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\StompClient;
use BabelQueue\Transport\StompTransport;
use BabelQueue\Tests\Support\ConformanceResults;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * Apache ActiveMQ Artemis (§7) binding conformance for the PHP STOMP producer against the
 * vendored canonical suite's `artemis` block.
 *
 * The STOMP path projects the §7 `bq_` application properties (as STOMP headers, which Artemis
 * maps to AMQP application-properties) and `correlation-id` = `trace_id`. The `x-opt-jms-type`
 * *annotation* is NOT settable from STOMP (custom STOMP headers become AMQP application-properties,
 * not annotations), so it is exempt here — every Artemis consumer routes on the body's `job` URN
 * for a STOMP-produced message. The body stays byte-identical (locked by the shared `cases`).
 */
final class StompConformanceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_property_projection_matches_the_golden(): void
    {
        ConformanceResults::each('artemis', function (array $projection): void {
            $body = (string) file_get_contents(__DIR__ . '/conformance/' . $projection['envelope_file']);

            $captured = null;
            $client = Mockery::mock(StompClient::class);
            $client->shouldReceive('send')->once()->with(
                Mockery::any(),
                $body,
                Mockery::on(function (array $headers) use (&$captured): bool {
                    $captured = $headers;

                    return true;
                }),
            );

            (new StompTransport($client, 'orders'))->publish($body);

            // The string-valued bq_ properties — the part the STOMP framing can carry.
            foreach ($projection['properties'] as $key => $want) {
                $this->assertSame($want, $captured[$key], $key);
            }
            // trace_id rides STOMP correlation-id (→ AMQP correlation-id).
            $this->assertSame($projection['correlation_id'], $captured['correlation-id']);
        }, 'property_projection');
    }

    /**
     * Consume-side cases (`attempts_reconciliation`, `schema_version_gate`, broker-bindings.md §7.5/§7.7) belong to
     * a consumer. The php-sdk core is framework-agnostic and publish-only (ADR-0010, ADR-0018); `StompTransport` only
     * publishes and consumption runs in the framework workers (Laravel/Symfony), where the cases are
     * evaluated. They are therefore recorded as per-case `skip`s citing the ADRs (user decision 2026-10-18),
     * not as gaps. The test pins the recorded status and the citation, and that no core consumer exists:
     * adding one means the skips must be replaced by real evaluation.
     */
    public function test_consume_side_cases_are_skipped_with_an_adr(): void
    {
        self::assertFalse(class_exists('BabelQueue\\Transport\\StompConsumer'), 'a core consumer would implement §7.5/§7.7: evaluate the cases instead of skipping them');
        self::assertFalse(method_exists(StompTransport::class, 'receive'), 'StompTransport::receive would implement §7.5/§7.7: evaluate the cases instead of skipping them');

        ConformanceResults::skipAll('artemis', 'Artemis (STOMP) consumption is out of scope for the framework-agnostic core (ADR-0010, ADR-0018); it runs in the framework workers', 'attempts_reconciliation');
        ConformanceResults::skipAll('artemis', 'Artemis (STOMP) consumption is out of scope for the framework-agnostic core (ADR-0010, ADR-0018); it runs in the framework workers', 'schema_version_gate');

        foreach (['attempts_reconciliation', 'schema_version_gate'] as $sub) {
            $ids = array_keys(ConformanceResults::cases('artemis', $sub));
            self::assertNotEmpty($ids);
            foreach ($ids as $id) {
                $recorded = ConformanceResults::recorded()['artemis'][(string) $id] ?? null;
                self::assertSame('skip', $recorded['status'] ?? null, 'artemis/' . $id);
                self::assertMatchesRegularExpression('/ADR-\d{4}/', $recorded['reason'] ?? '', 'artemis/' . $id . ' cites an ADR');
                self::assertArrayNotHasKey('artemis/' . $id, ConformanceResults::knownGaps(), 'a skipped case is not a known gap');
            }
        }
    }
}
