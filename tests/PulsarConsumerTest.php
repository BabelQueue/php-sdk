<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarMessage;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The framework-less Pulsar consumer (WebSocket consumer API): receive decodes the envelope and
 * reconciles §5 attempts; the consume loop acknowledges on success and releases (redelivers) when
 * the handler throws. Decoupled from the WebSocket library behind the seam (GR-7 intact).
 */
final class PulsarConsumerTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        . '"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        . '"schema_version":1,"created_at":1749132727000},"attempts":0}';

    /** @return array{messageId: string, payload: string, properties: array<string,string>, redeliveryCount: int} */
    private function raw(string $payload = self::ENVELOPE, int $redelivery = 0): array
    {
        return ['messageId' => 'mid-1', 'payload' => $payload, 'properties' => [], 'redeliveryCount' => $redelivery];
    }

    public function test_receive_decodes_the_envelope_and_exposes_the_inbound_view(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw());

        $message = (new PulsarConsumer($client))->receive();

        $this->assertInstanceOf(PulsarMessage::class, $message);
        $this->assertSame('urn:babel:orders:created', $message->getUrn());
        $this->assertSame('trace-1', $message->getTraceId());
        $this->assertSame(['order_id' => 1042], $message->getData());
        $this->assertSame('php', $message->getMeta()['lang']);
        $this->assertSame('mid-1', $message->messageId());
        $this->assertSame(0, $message->attempts());
        $this->assertSame('urn:babel:orders:created', $message->envelope()['job']);
    }

    public function test_receive_reconciles_attempts_to_the_redelivery_count(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(redelivery: 3));

        $message = (new PulsarConsumer($client))->receive();

        $this->assertSame(3, $message->attempts()); // max(body 0, redelivery 3)
    }

    public function test_receive_returns_null_when_no_message(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturnNull();

        $this->assertNull((new PulsarConsumer($client))->receive());
    }

    public function test_consume_acknowledges_after_a_successful_handler(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw());
        $client->shouldReceive('acknowledge')->once()->with('mid-1');

        $handled = null;
        $calls = 0;
        (new PulsarConsumer($client))->consume(
            function (PulsarMessage $m) use (&$handled): void {
                $handled = $m->getUrn();
            },
            function () use (&$calls): bool {
                return $calls++ >= 1; // run exactly one iteration
            },
        );

        $this->assertSame('urn:babel:orders:created', $handled);
    }

    public function test_consume_releases_when_the_handler_throws(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw());
        $client->shouldReceive('negativeAcknowledge')->once()->with('mid-1');
        $client->shouldNotReceive('acknowledge');

        $calls = 0;
        (new PulsarConsumer($client))->consume(
            function (PulsarMessage $m): void {
                throw new RuntimeException('boom');
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->addToAssertionCount(1);
    }

    public function test_consume_skips_idle_receives(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturnNull();
        $client->shouldNotReceive('acknowledge');

        $handlerCalls = 0;
        $calls = 0;
        (new PulsarConsumer($client))->consume(
            function (PulsarMessage $m) use (&$handlerCalls): void {
                $handlerCalls++;
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->assertSame(0, $handlerCalls); // null receive → handler never called
    }
}
