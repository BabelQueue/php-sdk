<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\PulsarTransport;
use BabelQueue\Transport\PulsarWebSocketClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * The framework-less Pulsar producer (WebSocket path): the message value is the canonical
 * envelope, the queue maps to `persistent://<tenant>/<namespace>/<queue>`, and the §5 `bq-`
 * properties (string→string) let a non-PHP consumer route on `bq-job` without decoding the body.
 * Decoupled from the WebSocket library behind the {@see PulsarWebSocketClient} seam (GR-7 intact).
 */
final class PulsarTransportTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        .'"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        .'"schema_version":1,"created_at":1749132727000},"attempts":0}';

    public function test_publish_targets_the_topic_and_projects_bq_properties(): void
    {
        $captured = [];
        $client = Mockery::mock(PulsarWebSocketClient::class);
        $client->shouldReceive('publish')->once()->with(
            'persistent://public/default/orders',
            self::ENVELOPE,
            Mockery::on(function (array $properties) use (&$captured): bool {
                $captured['properties'] = $properties;

                return true;
            }),
        );

        $id = (new PulsarTransport($client, 'orders'))->publish(self::ENVELOPE);

        $this->assertSame('msg-1', $id);
        $this->assertSame('urn:babel:orders:created', $captured['properties']['bq-job']);
        $this->assertSame('trace-1', $captured['properties']['bq-trace-id']);
        $this->assertSame('msg-1', $captured['properties']['bq-message-id']);
        $this->assertSame('1', $captured['properties']['bq-schema-version']);
        $this->assertSame('php', $captured['properties']['bq-source-lang']);
        $this->assertSame('0', $captured['properties']['bq-attempts']);
    }

    public function test_publish_uses_default_queue_tenant_and_namespace(): void
    {
        $client = Mockery::mock(PulsarWebSocketClient::class);
        $client->shouldReceive('publish')->once()->with(
            'persistent://acme/jobs/default',
            Mockery::any(),
            Mockery::any(),
        );

        (new PulsarTransport($client, 'default', 'acme', 'jobs'))->publish(self::ENVELOPE);

        $this->addToAssertionCount(1);
    }

    public function test_publish_overrides_queue_and_returns_null_without_meta_id(): void
    {
        $envelope = '{"job":"urn:babel:x","trace_id":"","data":{},"meta":{"queue":"q",'
            .'"lang":"php","schema_version":1},"attempts":3}';

        $captured = [];
        $client = Mockery::mock(PulsarWebSocketClient::class);
        $client->shouldReceive('publish')->once()->with(
            'persistent://public/default/other',
            $envelope,
            Mockery::on(function (array $properties) use (&$captured): bool {
                $captured['properties'] = $properties;

                return true;
            }),
        );

        $id = (new PulsarTransport($client, 'orders'))->publish($envelope, 'other');

        $this->assertNull($id);
        $this->assertSame('3', $captured['properties']['bq-attempts']);
        $this->assertArrayNotHasKey('bq-trace-id', $captured['properties']); // empty trace_id → no property
    }
}
