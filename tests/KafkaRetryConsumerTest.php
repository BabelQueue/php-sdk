<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaRetryConsumer;
use BabelQueue\Transport\KafkaRetryConsumerClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * The §6.4 re-injection consumer: it reads a `<workTopic>.retry.<delayMs>` record, waits the
 * `bq-delay` tier cooperatively (heartbeating the seam to keep group membership during a wait that
 * could exceed `max.poll.interval.ms`), then re-injects the decoded envelope into `bq-original-topic`
 * and commits. `bq-attempts` is NOT re-incremented (the router already bumped it). The wait is driven
 * by injected `$now`/`$sleep` seams — no real sleeping in tests.
 */
final class KafkaRetryConsumerTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        . '"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        . '"schema_version":1,"created_at":1749132727000},"attempts":1}';

    /**
     * @param  array<string, string>  $headers
     * @return array{payload: string, headers: array<string, string>}
     */
    private function raw(array $headers, string $payload = self::ENVELOPE): array
    {
        return ['payload' => $payload, 'headers' => $headers];
    }

    public function test_waits_the_delay_then_reinjects_into_the_original_topic_and_commits(): void
    {
        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw(['bq-delay' => '5000', 'bq-original-topic' => 'orders']),
        );
        $client->shouldReceive('poll')->with(0)->atLeast()->once();
        $client->shouldReceive('commit')->once();

        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders',
            Mockery::on(function (string $payload) use (&$captured): bool {
                $captured['payload'] = $payload;

                return true;
            }),
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured['headers'] = $headers;

                return true;
            }),
            Mockery::type('int'),
        );

        // Virtual clock advancing 1000ms per read; assert it slept the full 5000ms.
        $clock = 0;
        $slept = 0;
        $now = function () use (&$clock): int {
            return $clock;
        };
        $sleep = function (int $ms) use (&$clock, &$slept): void {
            $clock += $ms;
            $slept += $ms;
        };

        $calls = 0;
        (new KafkaRetryConsumer($client, $producer, $now, $sleep))->consume(
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->assertSame(5000, $slept); // waited the full bq-delay
        // The re-injected record is back on the work topic, so the re-injection headers are dropped;
        // the target topic is asserted via the produce() topic argument ('orders') above.
        $this->assertArrayNotHasKey('bq-original-topic', $captured['headers']);
        // re-injected record keeps the current bq-attempts (not re-incremented) and drops bq-delay.
        $this->assertSame('1', $captured['headers']['bq-attempts']);
        $this->assertArrayNotHasKey('bq-delay', $captured['headers']);
        $this->assertSame(1, json_decode($captured['payload'], true)['attempts']);
        $this->assertSame('trace-1', $captured['headers']['bq-trace-id']); // GR-4 preserved
    }

    public function test_heartbeats_repeatedly_across_a_long_wait(): void
    {
        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw(['bq-delay' => '3000', 'bq-original-topic' => 'orders']),
        );
        // 3000ms / 1000ms heartbeat cadence → 3 keep-alive polls during the wait.
        $client->shouldReceive('poll')->with(0)->times(3);
        $client->shouldReceive('commit')->once();

        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once();

        $clock = 0;
        $now = function () use (&$clock): int {
            return $clock;
        };
        $sleep = function (int $ms) use (&$clock): void {
            $clock += $ms;
        };

        $calls = 0;
        (new KafkaRetryConsumer($client, $producer, $now, $sleep))->consume(
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->addToAssertionCount(1);
    }

    public function test_reinjects_immediately_with_no_delay_header(): void
    {
        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-original-topic' => 'orders']));
        $client->shouldNotReceive('poll'); // no wait → no heartbeat
        $client->shouldReceive('commit')->once();

        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with('orders', Mockery::any(), Mockery::any(), Mockery::type('int'));

        $calls = 0;
        (new KafkaRetryConsumer($client, $producer))->consume(
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->addToAssertionCount(1);
    }

    public function test_falls_back_to_meta_queue_when_no_original_topic_header(): void
    {
        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-delay' => '0'])); // no bq-original-topic
        $client->shouldReceive('commit')->once();

        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with('orders', Mockery::any(), Mockery::any(), Mockery::type('int'));

        $calls = 0;
        (new KafkaRetryConsumer($client, $producer))->consume(
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->addToAssertionCount(1);
    }

    public function test_skips_idle_receives(): void
    {
        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturnNull();
        $client->shouldNotReceive('commit');

        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldNotReceive('produce');

        $calls = 0;
        (new KafkaRetryConsumer($client, $producer))->consume(
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->addToAssertionCount(1);
    }

    public function test_default_now_and_sleep_run_a_tiny_real_delay(): void
    {
        // Exercise the real microtime/usleep defaults (no injected seams) on a tiny 5ms delay, so the
        // default $now/$sleep closures (the production path) are covered with a negligible real wait.
        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw(['bq-delay' => '5', 'bq-original-topic' => 'orders']),
        );
        $client->shouldReceive('poll')->with(0)->atLeast()->once();
        $client->shouldReceive('commit')->once();

        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once();

        $calls = 0;
        (new KafkaRetryConsumer($client, $producer))->consume(
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->addToAssertionCount(1);
    }
}
