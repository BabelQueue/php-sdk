<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Consume;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\Transport;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\PulsarMessage;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The dead-letter publisher: it enriches the envelope with the additive `dead_letter` block and
 * publishes it to `<queue>.dlq` via any Transport (the body stays byte-identical).
 */
final class DeadLetterPublisherTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    /** The raw body of the last publishAndCapture() call. */
    private string $lastPayload = '';

    /** @param array<string,mixed> $meta */
    private function message(array $meta, int $attempts = 0): ConsumedMessage
    {
        $envelope = [
            'job' => 'urn:babel:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 7],
            'meta' => $meta,
            'attempts' => $attempts,
        ];

        $message = Mockery::mock(ConsumedMessage::class);
        $message->shouldReceive('envelope')->andReturn($envelope);
        $message->shouldReceive('getMeta')->andReturn($meta);
        $message->shouldReceive('attempts')->andReturn($attempts);

        return $message;
    }

    public function test_publishes_an_annotated_envelope_to_the_dlq(): void
    {
        $captured = null;
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->with(
            Mockery::on(function (string $payload) use (&$captured): bool {
                $captured = json_decode($payload, true);

                return true;
            }),
            'orders.dlq',
        )->andReturn(null);

        $message = $this->message(['id' => 'msg-1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1], 3);
        (new DeadLetterPublisher($transport))->publish($message, 'failed', new RuntimeException('boom'));

        $this->assertSame('urn:babel:orders:created', $captured['job']); // body preserved
        $this->assertSame(['order_id' => 7], $captured['data']);
        $dl = $captured['dead_letter'];
        $this->assertSame('failed', $dl['reason']);
        $this->assertSame('boom', $dl['error']);
        $this->assertSame(RuntimeException::class, $dl['exception']);
        $this->assertSame('orders', $dl['original_queue']);
        $this->assertSame(3, $dl['attempts']);
        $this->assertSame('php', $dl['lang']);
    }

    public function test_falls_back_to_the_default_queue_when_meta_queue_is_missing(): void
    {
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->with(Mockery::any(), 'default.dlq')->andReturn(null);

        (new DeadLetterPublisher($transport))->publish($this->message(['lang' => 'php']), 'unknown_urn', null);
        $this->addToAssertionCount(1);
    }

    public function test_honours_a_custom_suffix(): void
    {
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->with(Mockery::any(), 'orders.deadletter')->andReturn(null);

        (new DeadLetterPublisher($transport, '.deadletter'))
            ->publish($this->message(['queue' => 'orders']), 'poison', null);
        $this->addToAssertionCount(1);
    }

    /** @return array<string, mixed>|null the decoded DLQ body published to $dlq */
    private function publishAndCapture(ConsumedMessage $message, string $dlq = 'default.dlq'): ?array
    {
        $captured = null;
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->with(
            Mockery::on(function (string $payload) use (&$captured): bool {
                $captured = json_decode($payload, true);
                $this->lastPayload = $payload;

                return true;
            }),
            $dlq,
        )->andReturn(null);

        (new DeadLetterPublisher($transport))->publish($message, 'poison', null);

        return $captured;
    }

    public function test_a_poison_list_data_message_keeps_its_original_body_in_the_dlq(): void
    {
        $raw = '{"job":"urn:babel:orders:created","trace_id":"trace-9","data":[1,2],'
            . '"meta":{"id":"msg-9","queue":"orders","schema_version":1},"attempts":0}';
        $message = new PulsarMessage(EnvelopeCodec::decode($raw, static function (): void {}), 'mid-1', $raw);

        $captured = $this->publishAndCapture($message);

        $this->assertSame('urn:babel:orders:created', $captured['job']);
        $this->assertSame('trace-9', $captured['trace_id']);
        $this->assertSame([1, 2], $captured['data']);
        $this->assertSame('msg-9', $captured['meta']['id']);
        $this->assertSame('poison', $captured['dead_letter']['reason']);
        $this->assertTrue(array_is_list($captured['data']));
        $this->assertStringContainsString('"data":[1,2]', $this->lastPayload);
    }

    public function test_a_valid_object_data_with_index_like_keys_is_not_poison_and_keeps_its_shape(): void
    {
        $raw = '{"job":"urn:babel:orders:created","trace_id":"trace-8","data":{"0":"a","1":"b"},'
            . '"meta":{"id":"msg-8","queue":"orders","schema_version":1},"attempts":0}';
        $message = new PulsarMessage(EnvelopeCodec::decode($raw, static function (): void {}), 'mid-2', $raw);

        $captured = $this->publishAndCapture($message, 'orders.dlq');

        $this->assertSame('urn:babel:orders:created', $captured['job']);
        $this->assertStringContainsString('"data":{"0":"a","1":"b"}', $this->lastPayload);
    }

    public function test_a_malformed_body_is_dead_lettered_as_a_raw_string(): void
    {
        $raw = '{not json';
        $message = new KafkaMessage(EnvelopeCodec::decode($raw, static function (): void {}), [], $raw);

        $captured = $this->publishAndCapture($message);

        $this->assertSame($raw, $captured['raw']);
        $this->assertSame('poison', $captured['dead_letter']['reason']);
    }

    public function test_a_decodable_message_with_a_raw_body_dead_letters_its_envelope(): void
    {
        $raw = '{"job":"urn:babel:orders:created","trace_id":"trace-1","data":{"order_id":7},'
            . '"meta":{"id":"msg-1","queue":"orders","schema_version":1,"ts":1},"attempts":2}';
        $message = new KafkaMessage(EnvelopeCodec::decode($raw, static function (): void {}), [], $raw);

        $captured = $this->publishAndCapture($message, 'orders.dlq');

        $this->assertSame(['order_id' => 7], $captured['data']);
        $this->assertArrayNotHasKey('ts', $captured['meta']); // the decoded envelope, not the raw body
        $this->assertSame('poison', $captured['dead_letter']['reason']);
    }
}
