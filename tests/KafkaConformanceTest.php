<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * Apache Kafka (§6) binding conformance for the PHP producer against the vendored canonical
 * suite's `kafka` block. The `php-sdk` ships the produce-side `KafkaTransport`, so it satisfies
 * the `property_projection` (the `bq-` header set). The consume-side
 * `attempts_reconciliation` is exercised by the runtime SDKs (Go/Node/Java/Python/.NET).
 */
final class KafkaConformanceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_header_projection_matches_the_golden(): void
    {
        $manifest = json_decode(
            (string) file_get_contents(__DIR__ . '/conformance/manifest.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        $projection = $manifest['kafka']['property_projection'];
        $body = (string) file_get_contents(__DIR__ . '/conformance/' . $projection['envelope_file']);

        $captured = null;
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            Mockery::any(),
            $body,
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured = $headers;

                return true;
            }),
            Mockery::any(),
        );

        (new KafkaTransport($producer, 'orders'))->publish($body);

        foreach ($projection['headers'] as $key => $want) {
            $this->assertSame($want, $captured[$key], $key);
        }
    }
}
