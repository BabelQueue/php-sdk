<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\PulsarTransport;
use BabelQueue\Transport\PulsarWebSocketClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * Apache Pulsar (§5) binding conformance for the PHP producer against the vendored canonical
 * suite's `pulsar` block. The `php-sdk` ships the produce-side `PulsarTransport` (WebSocket path),
 * so it satisfies the `property_projection` (the `bq-` native-property set). The consume-side
 * `attempts_reconciliation` is exercised by the runtime SDKs (Go/Node/Java/Python/.NET).
 */
final class PulsarConformanceTest extends TestCase
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
        $projection = $manifest['pulsar']['property_projection'];
        $body = (string) file_get_contents(__DIR__ . '/conformance/' . $projection['envelope_file']);

        $captured = null;
        $client = Mockery::mock(PulsarWebSocketClient::class);
        $client->shouldReceive('publish')->once()->with(
            Mockery::any(),
            $body,
            Mockery::on(function (array $properties) use (&$captured): bool {
                $captured = $properties;

                return true;
            }),
        );

        (new PulsarTransport($client, 'orders'))->publish($body);

        foreach ($projection['properties'] as $key => $want) {
            $this->assertSame($want, $captured[$key], $key);
        }
    }
}
