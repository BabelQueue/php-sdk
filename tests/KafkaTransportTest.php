<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * The framework-less Kafka producer: the record value is the canonical envelope, the timestamp
 * mirrors meta.created_at, and the §6 `bq-` headers (UTF-8 strings) let a non-PHP consumer route
 * on `bq-job` without decoding the body. Decoupled from `ext-rdkafka` behind the KafkaProducer seam.
 */
final class KafkaTransportTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        .'"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        .'"schema_version":1,"created_at":1749132727000},"attempts":0}';

    public function test_publish_produces_value_timestamp_and_bq_headers(): void
    {
        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders',
            self::ENVELOPE,
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured['headers'] = $headers;

                return true;
            }),
            1749132727000,
        );

        $id = (new KafkaTransport($producer, 'orders'))->publish(self::ENVELOPE);

        $this->assertSame('msg-1', $id);
        $this->assertSame('urn:babel:orders:created', $captured['headers']['bq-job']);
        $this->assertSame('trace-1', $captured['headers']['bq-trace-id']);
        $this->assertSame('msg-1', $captured['headers']['bq-message-id']);
        $this->assertSame('1', $captured['headers']['bq-schema-version']);
        $this->assertSame('php', $captured['headers']['bq-source-lang']);
        $this->assertSame('0', $captured['headers']['bq-attempts']);
    }

    public function test_publish_applies_default_topic_and_prefix(): void
    {
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with('app.orders', Mockery::any(), Mockery::any(), Mockery::any());

        (new KafkaTransport($producer, 'orders', 'app.'))->publish(self::ENVELOPE);

        $this->addToAssertionCount(1);
    }

    public function test_publish_overrides_topic_and_returns_null_without_meta_id(): void
    {
        $envelope = '{"job":"urn:babel:x","trace_id":"","data":{},"meta":{"queue":"q",'
            .'"lang":"php","schema_version":1},"attempts":3}';

        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'other',
            $envelope,
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured['headers'] = $headers;

                return true;
            }),
            null,   // no created_at → broker time
        );

        $id = (new KafkaTransport($producer, 'orders'))->publish($envelope, 'other');

        $this->assertNull($id);
        $this->assertSame('3', $captured['headers']['bq-attempts']);
        $this->assertArrayNotHasKey('bq-trace-id', $captured['headers']); // empty trace_id → no header
    }
}
