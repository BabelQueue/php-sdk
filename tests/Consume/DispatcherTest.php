<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Consume;

use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\Dispatcher;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\Transport;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The framework-less consume runtime: URN dispatch, the four on_unknown_urn strategies, and the
 * max-attempts dead-letter cap — driven through the consumer loop's ack-on-return / redeliver-on-
 * throw contract.
 */
final class DispatcherTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private function message(string $urn, int $attempts = 0, string $queue = 'orders'): ConsumedMessage
    {
        return new FakeConsumedMessage([
            'job' => $urn,
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 7],
            'meta' => ['id' => 'msg-1', 'queue' => $queue, 'lang' => 'php', 'schema_version' => 1],
            'attempts' => $attempts,
        ], $attempts);
    }

    public function test_dispatches_to_the_registered_handler(): void
    {
        $seen = null;
        $dispatcher = (new Dispatcher())->on('urn:babel:orders:created', function (ConsumedMessage $m) use (&$seen): void {
            $seen = $m->getData()['order_id'];
        });

        $dispatcher($this->message('urn:babel:orders:created'));

        $this->assertSame(7, $seen);
    }

    public function test_rethrows_a_handler_failure_to_redeliver(): void
    {
        $dispatcher = (new Dispatcher())->on('urn:babel:orders:created', function (): void {
            throw new RuntimeException('boom');
        });

        $this->expectException(RuntimeException::class);
        $dispatcher($this->message('urn:babel:orders:created'));
    }

    public function test_on_retry_hook_receives_the_backoff_delay_and_the_failure_is_rethrown(): void
    {
        $calls = [];
        $boom = new RuntimeException('boom');
        $dispatcher = (new Dispatcher(
            maxAttempts: 0, // no cap: attempts 2 would otherwise be exhausted (default 3) and dropped
            retryBackoff: new \BabelQueue\Consume\RetryBackoff(retryBackoff: 2, multiplier: 3),
            onRetry: function (ConsumedMessage $m, int $delay, \Throwable $e) use (&$calls): void {
                $calls[] = [$m->attempts(), $delay, $e->getMessage()];
            },
        ))->on('urn:babel:orders:created', function () use ($boom): void {
            throw $boom;
        });

        try {
            $dispatcher($this->message('urn:babel:orders:created', 2));
            $this->fail('the handler failure must be rethrown');
        } catch (RuntimeException $e) {
            $this->assertSame($boom, $e);
        }

        // attempts 2 -> n=3 -> 2 * 3^2 = 18 s
        $this->assertSame([[2, 18, 'boom']], $calls);
    }

    public function test_on_retry_hook_is_not_called_for_a_dead_lettered_failure_nor_a_success(): void
    {
        $calls = 0;
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->andReturn(null);

        $dispatcher = (new Dispatcher(
            'fail',
            3,
            new DeadLetterPublisher($transport),
            new \BabelQueue\Consume\RetryBackoff(retryBackoff: 1),
            function () use (&$calls): void {
                $calls++;
            },
        ))->on('urn:babel:orders:created', function (ConsumedMessage $m): void {
            if ($m->getData()['order_id'] === 7 && $m->attempts() >= 0) {
                throw new RuntimeException('boom');
            }
        })->on('urn:babel:ok', static function (): void {
        });

        $dispatcher($this->message('urn:babel:orders:created', 2)); // attempts + 1 >= 3 -> dead-lettered
        $dispatcher($this->message('urn:babel:ok'));                  // success

        $this->assertSame(0, $calls);
    }

    public function test_an_unbounded_backoff_yields_php_int_max_which_the_sqs_release_clamps(): void
    {
        // multiplier > 1 with no max: d(n) overflows to INF after enough attempts.
        $dispatcher = new Dispatcher(retryBackoff: new \BabelQueue\Consume\RetryBackoff(retryBackoff: 1, multiplier: 10));
        $delay = $dispatcher->retryDelaySeconds($this->message('urn:babel:x', 400));

        $this->assertSame(PHP_INT_MAX, $delay);
    }

    public function test_dead_letters_a_handler_failure_at_the_attempt_cap(): void
    {
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->with(
            Mockery::on(function (string $payload): bool {
                $env = json_decode($payload, true);

                return $env['dead_letter']['reason'] === 'failed'
                    && $env['dead_letter']['original_queue'] === 'orders';
            }),
            'orders.dlq',
        )->andReturn(null);

        $dispatcher = (new Dispatcher('fail', 3, new DeadLetterPublisher($transport)))
            ->on('urn:babel:orders:created', function (): void {
                throw new RuntimeException('boom');
            });

        // attempts 2 → 2 + 1 >= 3 → dead-lettered instead of rethrown (returns normally, i.e. ack).
        $dispatcher($this->message('urn:babel:orders:created', 2));
        $this->addToAssertionCount(1);
    }

    public function test_unknown_urn_fail_throws(): void
    {
        $this->expectException(UnknownUrnException::class);
        (new Dispatcher('fail'))($this->message('urn:babel:orders:unmapped'));
    }

    public function test_unknown_urn_release_throws(): void
    {
        $this->expectException(UnknownUrnException::class);
        (new Dispatcher('release'))($this->message('urn:babel:orders:unmapped'));
    }

    public function test_unknown_urn_delete_acks_and_drops(): void
    {
        (new Dispatcher('delete'))($this->message('urn:babel:orders:unmapped'));
        $this->addToAssertionCount(1); // no throw, no publish
    }

    public function test_unknown_urn_dead_letter_publishes_then_acks(): void
    {
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->with(
            Mockery::on(function (string $payload): bool {
                return json_decode($payload, true)['dead_letter']['reason'] === 'unknown_urn';
            }),
            'orders.dlq',
        )->andReturn(null);

        (new Dispatcher('dead_letter', 0, new DeadLetterPublisher($transport)))($this->message('urn:babel:x'));
        $this->addToAssertionCount(1);
    }

    public function test_unknown_urn_dead_letter_degrades_to_delete_without_a_publisher(): void
    {
        // dead_letter strategy but no publisher → degrade to delete (no throw, nothing published), visibly:
        // the drop is reported to the onDrop hook (ADR-0036 decision 3).
        $dropped = [];
        (new Dispatcher('dead_letter', onDrop: function (ConsumedMessage $m, string $why) use (&$dropped): void {
            $dropped[] = $m->getUrn() . ' | ' . $why;
        }))($this->message('urn:babel:x'));

        $this->assertCount(1, $dropped);
        $this->assertStringContainsString('urn:babel:x', $dropped[0]);
    }

    public function test_plugs_into_a_consumer_consume_loop_and_acks(): void
    {
        // End-to-end: the Dispatcher is the callable a real consumer's consume() loop drives —
        // it dispatches the decoded message to the handler, and the loop acks on normal return.
        $payload = '{"job":"urn:babel:orders:created","trace_id":"t","data":{"order_id":7},'
            . '"meta":{"id":"m","queue":"orders","lang":"php","schema_version":1,"created_at":1},"attempts":0}';

        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            ['messageId' => 'm', 'payload' => $payload, 'properties' => [], 'redeliveryCount' => 0],
        );
        $client->shouldReceive('acknowledge')->once()->with('m');

        $seen = null;
        $dispatcher = (new Dispatcher())->on('urn:babel:orders:created', function (ConsumedMessage $m) use (&$seen): void {
            $seen = $m->getData()['order_id'];
        });

        $calls = 0;
        (new PulsarConsumer($client, subscriptionType: 'Shared'))->consume($dispatcher, function () use (&$calls): bool {
            return $calls++ >= 1;
        });

        $this->assertSame(7, $seen);
    }
}

final class FakeConsumedMessage implements ConsumedMessage
{
    /** @param array<string, mixed> $envelope */
    public function __construct(private array $envelope, private int $attempts = 0)
    {
    }

    public function getUrn(): string
    {
        return is_string($this->envelope['job'] ?? null) ? $this->envelope['job'] : '';
    }

    public function getTraceId(): string
    {
        return is_string($this->envelope['trace_id'] ?? null) ? $this->envelope['trace_id'] : '';
    }

    /** @return array<string, mixed> */
    public function getData(): array
    {
        return is_array($this->envelope['data'] ?? null) ? $this->envelope['data'] : [];
    }

    /** @return array<string, mixed> */
    public function getMeta(): array
    {
        return is_array($this->envelope['meta'] ?? null) ? $this->envelope['meta'] : [];
    }

    public function attempts(): int
    {
        return $this->attempts;
    }

    /** @return array<string, mixed> */
    public function envelope(): array
    {
        return $this->envelope;
    }
}
