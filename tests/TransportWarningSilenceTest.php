<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\Transport;
use BabelQueue\Transport\AmqpTransport;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaTransport;
use BabelQueue\Transport\PulsarTransport;
use BabelQueue\Transport\PulsarWebSocketClient;
use BabelQueue\Transport\SqsClient;
use BabelQueue\Transport\SqsTransport;
use BabelQueue\Transport\StompClient;
use BabelQueue\Transport\StompTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;

/**
 * The produce-side transports decode the payload only to project their transport headers and then
 * publish it verbatim — so a forbidden key in a caller-built payload must not raise the decode-side
 * "dropped from the decoded envelope" warning (nothing was dropped from what goes on the wire).
 */
final class TransportWarningSilenceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const PAYLOAD = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        . '"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        . '"schema_version":1,"created_at":1749132727000,"ts":1},"attempts":0,"timestamp":1}';

    /** @var list<string> */
    private array $warnings = [];

    protected function setUp(): void
    {
        $this->warnings = [];
        EnvelopeCodec::setWarningHandler(function (string $message): void {
            $this->warnings[] = $message;
        });
    }

    protected function tearDown(): void
    {
        EnvelopeCodec::setWarningHandler(null);
    }

    private function assertPublishedSilently(Transport $transport, ?string &$published): void
    {
        $transport->publish(self::PAYLOAD, 'orders');

        $this->assertSame([], $this->warnings);
        $this->assertSame(self::PAYLOAD, $published);
    }

    public function test_sqs_transport_publishes_without_a_warning(): void
    {
        $published = null;
        $client = Mockery::mock(SqsClient::class);
        $client->shouldReceive('sendMessage')->once()->with(Mockery::on(function (array $args) use (&$published): bool {
            $published = $args['MessageBody'];

            return true;
        }));

        $this->assertPublishedSilently(new SqsTransport($client, 'https://sqs.example/123/orders'), $published);
    }

    public function test_kafka_transport_publishes_without_a_warning(): void
    {
        $published = null;
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders',
            Mockery::on(function (string $payload) use (&$published): bool {
                $published = $payload;

                return true;
            }),
            Mockery::any(),
            Mockery::any(),
        );

        $this->assertPublishedSilently(new KafkaTransport($producer), $published);
    }

    public function test_pulsar_transport_publishes_without_a_warning(): void
    {
        $published = null;
        $client = Mockery::mock(PulsarWebSocketClient::class);
        $client->shouldReceive('publish')->once()->with(
            Mockery::any(),
            Mockery::on(function (string $payload) use (&$published): bool {
                $published = $payload;

                return true;
            }),
            Mockery::any(),
        );

        $this->assertPublishedSilently(new PulsarTransport($client), $published);
    }

    public function test_stomp_transport_publishes_without_a_warning(): void
    {
        $published = null;
        $client = Mockery::mock(StompClient::class);
        $client->shouldReceive('send')->once()->with(
            Mockery::any(),
            Mockery::on(function (string $body) use (&$published): bool {
                $published = $body;

                return true;
            }),
            Mockery::any(),
        );

        $this->assertPublishedSilently(new StompTransport($client), $published);
    }

    public function test_amqp_transport_publishes_without_a_warning(): void
    {
        $published = null;
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('queue_declare')->once();
        $channel->shouldReceive('basic_publish')->once()->with(
            Mockery::on(function (AMQPMessage $message) use (&$published): bool {
                $published = $message->getBody();

                return true;
            }),
            '',
            'orders',
        );

        $this->assertPublishedSilently(new AmqpTransport($channel), $published);
    }
}
