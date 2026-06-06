<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\AmqpTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;

/**
 * The framework-less RabbitMQ producer: a durable queue, a persistent message,
 * and the contract AMQP properties (type/correlation_id/message_id + x-headers)
 * so a non-PHP consumer can route and trace without decoding the body.
 */
final class AmqpTransportTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        .'"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        .'"schema_version":1,"created_at":1749132727000},"attempts":0}';

    public function test_publish_declares_durable_queue_and_maps_contract_properties(): void
    {
        $captured = null;

        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('queue_declare')->once()->with('orders', false, true, false, false);
        $channel->shouldReceive('basic_publish')->once()->with(
            Mockery::on(function (AMQPMessage $message) use (&$captured): bool {
                $captured = $message;

                return true;
            }),
            '',
            'orders',
        );

        $id = (new AmqpTransport($channel, 'default'))->publish(self::ENVELOPE, 'orders');

        $this->assertSame('msg-1', $id);
        $this->assertInstanceOf(AMQPMessage::class, $captured);
        $this->assertSame(self::ENVELOPE, $captured->getBody());
        $this->assertSame('urn:babel:orders:created', $captured->get('type'));
        $this->assertSame('trace-1', $captured->get('correlation_id'));
        $this->assertSame('msg-1', $captured->get('message_id'));
        $this->assertSame('application/json', $captured->get('content_type'));
        $this->assertSame(AMQPMessage::DELIVERY_MODE_PERSISTENT, $captured->get('delivery_mode'));

        $headers = $captured->get('application_headers')->getNativeData();
        $this->assertSame(1, $headers['x-schema-version']);
        $this->assertSame('php', $headers['x-source-lang']);
        $this->assertSame(0, $headers['x-attempts']);
    }

    public function test_publish_uses_the_default_queue_when_none_is_given(): void
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('queue_declare')->once()->with('default', false, true, false, false);
        $channel->shouldReceive('basic_publish')->once()->with(
            Mockery::type(AMQPMessage::class),
            '',
            'default',
        );

        (new AmqpTransport($channel))->publish(self::ENVELOPE);
    }
}
