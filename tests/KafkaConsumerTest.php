<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\KafkaConsumer;
use BabelQueue\Transport\KafkaConsumerClient;
use BabelQueue\Transport\KafkaMessage;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The framework-less Kafka consumer (ext-rdkafka): receive decodes the record and reconciles §6
 * attempts (the `bq-attempts` header is authoritative); the consume loop commits the offset on
 * success (process-then-commit) and leaves it uncommitted when the handler throws. Decoupled from
 * `ext-rdkafka` behind the seam.
 */
final class KafkaConsumerTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        . '"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        . '"schema_version":1,"created_at":1749132727000},"attempts":0}';

    /**
     * @param  array<string, string>  $headers
     * @return array{payload: string, headers: array<string, string>}
     */
    private function raw(array $headers = [], string $payload = self::ENVELOPE): array
    {
        return ['payload' => $payload, 'headers' => $headers];
    }

    public function test_receive_decodes_the_envelope_and_exposes_the_inbound_view(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '0']));

        $message = (new KafkaConsumer($client))->receive();

        $this->assertInstanceOf(KafkaMessage::class, $message);
        $this->assertSame('urn:babel:orders:created', $message->getUrn());
        $this->assertSame('trace-1', $message->getTraceId());
        $this->assertSame(['order_id' => 1042], $message->getData());
        $this->assertSame('php', $message->getMeta()['lang']);
        $this->assertSame('urn:babel:orders:created', $message->envelope()['job']);
    }

    public function test_receive_uses_the_authoritative_header_attempts_over_the_body(): void
    {
        // body attempts 0, header 4 → header wins.
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '4']));

        $this->assertSame(4, (new KafkaConsumer($client))->receive()->attempts());
    }

    public function test_receive_falls_back_to_the_body_attempts_without_a_header(): void
    {
        $body = '{"job":"urn:babel:x","trace_id":"t","data":{},"meta":{"id":"m","queue":"q",'
            . '"lang":"php","schema_version":1,"created_at":1},"attempts":3}';

        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw([], $body)); // no bq-attempts header

        $this->assertSame(3, (new KafkaConsumer($client))->receive()->attempts());
    }

    public function test_receive_returns_null_when_no_record(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturnNull();

        $this->assertNull((new KafkaConsumer($client))->receive());
    }

    public function test_receive_surfaces_the_raw_bq_headers_for_the_retry_machinery(): void
    {
        // The §6.4/§6.5 retry-topic machinery reads bq-original-topic off the message to route a
        // retry record back to its work topic across hops.
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw(['bq-attempts' => '2', 'bq-original-topic' => 'orders']),
        );

        $message = (new KafkaConsumer($client))->receive();

        $this->assertInstanceOf(KafkaMessage::class, $message);
        $this->assertSame('orders', $message->header('bq-original-topic'));
        $this->assertSame('2', $message->headers()['bq-attempts']);
        $this->assertNull($message->header('bq-missing'));
    }

    public function test_consume_commits_after_a_successful_handler(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '0']));
        $client->shouldReceive('commit')->once();

        $handled = null;
        $calls = 0;
        (new KafkaConsumer($client))->consume(
            function (KafkaMessage $m) use (&$handled): void {
                $handled = $m->getUrn();
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->assertSame('urn:babel:orders:created', $handled);
    }

    public function test_consume_does_not_commit_when_the_handler_throws(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '0']));
        $client->shouldNotReceive('commit');

        $calls = 0;
        (new KafkaConsumer($client))->consume(
            function (KafkaMessage $m): void {
                throw new RuntimeException('boom');
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->addToAssertionCount(1);
    }

    public function test_consume_skips_idle_polls(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturnNull();
        $client->shouldNotReceive('commit');

        $handlerCalls = 0;
        $calls = 0;
        (new KafkaConsumer($client))->consume(
            function (KafkaMessage $m) use (&$handlerCalls): void {
                $handlerCalls++;
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->assertSame(0, $handlerCalls);
    }
}
