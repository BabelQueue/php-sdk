<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Contracts\HeaderPublisher;
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

    public function test_it_is_a_header_publisher(): void
    {
        $this->assertInstanceOf(HeaderPublisher::class, new StompTransport(Mockery::mock(StompClient::class)));
    }

    /**
     * ADR-0028 / R1-O3: the rider is a STOMP custom header (which Artemis bridges to an AMQP 1.0
     * application-property and a JMS string property of the same name, §7.3) beside the §7 projection,
     * and never enters the frame body (GR-1).
     */
    public function test_publish_with_headers_carries_the_traceparent_as_a_stomp_header_and_not_in_the_body(): void
    {
        $captured = null;
        $body = null;
        $client = Mockery::mock(StompClient::class);
        $client->shouldReceive('send')->once()->with(
            'orders',
            Mockery::on(function (string $frameBody) use (&$body): bool {
                $body = $frameBody;

                return true;
            }),
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured = $headers;

                return true;
            }),
        );

        $traceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
        $id = (new StompTransport($client, 'orders'))->publishWithHeaders(
            self::ENVELOPE,
            ['traceparent' => $traceparent, 'tracestate' => 'vendor=1'],
        );

        $this->assertSame('msg-1', $id);
        $this->assertSame($traceparent, $captured['traceparent']);
        $this->assertSame('vendor=1', $captured['tracestate']);
        // The §7 projection is untouched.
        $this->assertSame('application/json', $captured['content-type']);
        $this->assertSame('trace-1', $captured['correlation-id']);
        $this->assertSame('1', $captured['bq_schema_version']);
        $this->assertSame('php', $captured['bq_source_lang']);
        $this->assertSame('0', $captured['bq_attempts']);
        $this->assertSame('babelqueue', $captured['bq_app_id']);
        // The frame body is the envelope verbatim and has no rider (GR-1).
        $this->assertSame(self::ENVELOPE, $body);
        $this->assertStringNotContainsString('traceparent', (string) $body);
    }

    public function test_publish_with_headers_lets_the_projection_win_a_key_collision_and_drops_blank_riders(): void
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

        (new StompTransport($client, 'orders'))->publishWithHeaders(
            self::ENVELOPE,
            ['bq_attempts' => '99', 'correlation-id' => 'evil', 'x-empty' => '', 'x-kept' => 'yes'],
        );

        $this->assertSame('0', $captured['bq_attempts']);
        $this->assertSame('trace-1', $captured['correlation-id']);
        $this->assertSame('yes', $captured['x-kept']);
        $this->assertArrayNotHasKey('x-empty', $captured);
    }

    public function test_publish_with_no_headers_is_exactly_a_plain_publish(): void
    {
        $sent = [];
        $client = Mockery::mock(StompClient::class);
        $client->shouldReceive('send')->twice()->andReturnUsing(
            function (string $destination, string $body, array $headers) use (&$sent): void {
                $sent[] = [$destination, $body, $headers];
            },
        );

        $transport = new StompTransport($client, 'orders');
        $transport->publish(self::ENVELOPE);
        $transport->publishWithHeaders(self::ENVELOPE, []);

        $this->assertCount(2, $sent);
        $this->assertSame($sent[0], $sent[1]);
    }

    /**
     * Regression: the `bq_*` family (so `bq_delay`, which §7.3 allows only for a requested delay) and the frame-level
     * / Artemis-meaningful headers are never taken from the carrier.
     */
    public function test_publish_with_headers_never_takes_bq_properties_or_frame_headers_from_the_carrier(): void
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

        (new StompTransport($client, 'orders'))->publishWithHeaders(self::ENVELOPE, [
            'bq_delay' => '60000', 'bq_custom' => 'x', 'BQ_Other' => 'x', '_AMQ_SCHED_DELIVERY' => '1', 'JMSType' => 'evil',
            'destination' => '/queue/other', 'content-length' => '1', 'receipt' => 'r', 'transaction' => 't',
            'traceparent' => '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01', 'x-kept' => 'yes',
        ]);

        foreach (['bq_delay', 'bq_custom', 'BQ_Other', '_AMQ_SCHED_DELIVERY', 'JMSType', 'destination', 'content-length', 'receipt', 'transaction'] as $name) {
            $this->assertArrayNotHasKey($name, $captured, $name . ' must not come from the carrier');
        }
        $this->assertSame('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01', $captured['traceparent']);
        $this->assertSame('yes', $captured['x-kept']);
        $this->assertSame('0', $captured['bq_attempts']);
    }
}
