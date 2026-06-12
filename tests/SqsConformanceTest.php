<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\SqsClient;
use BabelQueue\Transport\SqsTransport;
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
        $manifest = json_decode(
            (string) file_get_contents(__DIR__ . '/conformance/manifest.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        $projection = $manifest['sqs']['attribute_projection'];
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
    }
}
