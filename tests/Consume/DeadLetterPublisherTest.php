<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Consume;

use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\Transport;
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
}
