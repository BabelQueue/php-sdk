<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\StompClient;
use BabelQueue\Transport\StompTransport;
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
        $manifest = json_decode(
            (string) file_get_contents(__DIR__ . '/conformance/manifest.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        $projection = $manifest['artemis']['property_projection'];
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
    }
}
