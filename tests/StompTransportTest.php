<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\StompClient;
use BabelQueue\Transport\StompTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * The framework-less Apache Artemis producer over STOMP: the envelope body plus the §7 STOMP
 * header projection (`correlation-id` = trace_id, `content-type`, the `bq_` application
 * properties) that survive Artemis's STOMP↔AMQP/JMS bridge so a non-PHP consumer routes (on the
 * body's URN) and traces without decoding the body first.
 */
final class StompTransportTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        .'"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        .'"schema_version":1,"created_at":1749132727000},"attempts":0}';

    public function test_publish_sends_body_with_correlation_and_bq_headers(): void
    {
        $captured = null;
        $client = Mockery::mock(StompClient::class);
        $client->shouldReceive('send')->once()->with(
            'orders',
            self::ENVELOPE,
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured = $headers;

                return true;
            }),
        );

        $id = (new StompTransport($client, 'orders'))->publish(self::ENVELOPE);

        $this->assertSame('msg-1', $id);
        $this->assertSame('application/json', $captured['content-type']);
        $this->assertSame('trace-1', $captured['correlation-id']);
        $this->assertSame('1', $captured['bq_schema_version']);
        $this->assertSame('php', $captured['bq_source_lang']);
        $this->assertSame('0', $captured['bq_attempts']);
        $this->assertSame('babelqueue', $captured['bq_app_id']);
    }

    public function test_publish_applies_the_default_queue_and_destination_prefix(): void
    {
        $client = Mockery::mock(StompClient::class);
        $client->shouldReceive('send')->once()->with('jms.queue.orders', Mockery::any(), Mockery::any());

        (new StompTransport($client, 'orders', 'jms.queue.'))->publish(self::ENVELOPE);

        $this->addToAssertionCount(1);
    }

    public function test_publish_overrides_queue_and_omits_correlation_without_trace(): void
    {
        $envelope = '{"job":"urn:babel:x","trace_id":"","data":{},"meta":{"queue":"q",'
            .'"lang":"php","schema_version":1,"created_at":1},"attempts":2}';

        $captured = null;
        $client = Mockery::mock(StompClient::class);
        $client->shouldReceive('send')->once()->with(
            'other',
            $envelope,
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured = $headers;

                return true;
            }),
        );

        $id = (new StompTransport($client, 'orders'))->publish($envelope, 'other');

        $this->assertNull($id);                                 // no meta.id → null
        $this->assertSame('2', $captured['bq_attempts']);       // attempts taken from the body
        $this->assertArrayNotHasKey('correlation-id', $captured); // empty trace_id → no header
    }
}
