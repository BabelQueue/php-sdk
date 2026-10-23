<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaRetryConsumer;
use BabelQueue\Transport\KafkaRetryConsumerClient;
use BabelQueue\Transport\KeyedKafkaProducer;
use BabelQueue\Transport\PausableKafkaRetryConsumerClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * The §6.4 re-injection consumer: it reads a `<workTopic>.retry.<n>` record, waits until
 * `record.timestamp + bq-delay` (clamped into `[now, now + bq-delay]`, ADR-0033 A6) cooperatively
 * (heartbeating the seam to keep group membership, pausing the assignment when the client can), then
 * re-injects the record byte-for-byte into `bq-original-topic` (A7: only `bq-delay` and
 * `bq-original-topic` are dropped) and commits after the write. The wait is driven by injected
 * `$now`/`$sleep` seams — no real sleeping in tests.
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

        $this->assertSame(5000, $slept); // no record timestamp: waited the full bq-delay from now
        // Byte-for-byte: the value is the received record, not a re-encode.
        $this->assertSame(self::ENVELOPE, $captured['payload']);
        $this->assertArrayNotHasKey('bq-original-topic', $captured['headers']);
        $this->assertArrayNotHasKey('bq-delay', $captured['headers']);
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

    public function test_reinjects_an_undecodable_record_verbatim(): void
    {
        $poison = '{"job":"urn:babel:orders:created","trace_id":"trace-1","data":[1,2],'
            . '"meta":{"id":"msg-1","queue":"orders","schema_version":1},"attempts":1}';

        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw([
            'bq-original-topic' => 'orders',
            'bq-job' => 'urn:babel:orders:created',
            'bq-attempts' => '2',
        ], $poison));
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

        EnvelopeCodec::setWarningHandler(static function (): void {});
        try {
            $calls = 0;
            (new KafkaRetryConsumer($client, $producer))->consume(
                function () use (&$calls): bool {
                    return $calls++ >= 1;
                },
            );
        } finally {
            EnvelopeCodec::setWarningHandler(null);
        }

        $this->assertSame($poison, $captured['payload']); // not re-encoded from the empty envelope
        $this->assertSame(['bq-job' => 'urn:babel:orders:created', 'bq-attempts' => '2'], $captured['headers']);
    }

    public function test_reinjects_value_key_and_other_headers_byte_for_byte_dropping_only_the_two_routing_headers(): void
    {
        $headers = [
            'bq-delay' => '5000',
            'bq-original-topic' => 'orders',
            'bq-attempts' => '2',
            'bq-trace-id' => 'trace-1',
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
            'x-app' => 'kept',
        ];
        // Deliberately not canonical JSON (spaces, key order): a decode/encode would change it.
        $payload = '{ "job" : "urn:babel:orders:created", "attempts" : 2, "trace_id":"trace-1" }';

        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            ['payload' => $payload, 'headers' => $headers, 'key' => "key\x00-1", 'timestamp_ms' => 0],
        );
        $client->shouldReceive('commit')->once();

        $sent = [];
        $producer = Mockery::mock(KafkaProducer::class, KeyedKafkaProducer::class);
        $producer->shouldReceive('produceKeyed')->once()->andReturnUsing(
            static function (string $topic, string $body, array $h, ?int $ts, ?string $key) use (&$sent): void {
                $sent = [$topic, $body, $h, $key];
            },
        );

        $calls = 0;
        (new KafkaRetryConsumer($client, $producer, static fn (): int => 10_000, static function (int $ms): void {}))->consume(
            static function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->assertSame('orders', $sent[0]);
        $this->assertSame($payload, $sent[1]);
        $this->assertSame("key\x00-1", $sent[3]);
        $this->assertSame([
            'bq-attempts' => '2',
            'bq-trace-id' => 'trace-1',
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
            'x-app' => 'kept',
        ], $sent[2]);
    }

    public function test_the_wait_is_computed_from_the_record_timestamp(): void
    {
        // Record appended 2000ms ago on a 5000ms tier: only the remaining 3000ms is waited.
        $this->assertSame(3000, $this->waitedMs(['bq-delay' => '5000', 'bq-original-topic' => 'orders'], 8_000, 10_000));
    }

    public function test_a_record_older_than_its_tier_is_moved_without_waiting(): void
    {
        $this->assertSame(0, $this->waitedMs(['bq-delay' => '5000', 'bq-original-topic' => 'orders'], 1_000, 10_000));
    }

    public function test_a_future_timestamp_never_waits_longer_than_the_tier(): void
    {
        // Clock skew: timestamp 60s ahead. The wait is clamped to now + tier_ms, not 65s.
        $this->assertSame(5000, $this->waitedMs(['bq-delay' => '5000', 'bq-original-topic' => 'orders'], 70_000, 10_000));
    }

    public function test_the_assignment_is_paused_for_the_wait_and_resumed_before_the_commit(): void
    {
        $events = [];
        $client = Mockery::mock(PausableKafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            ['payload' => self::ENVELOPE, 'headers' => ['bq-delay' => '2000', 'bq-original-topic' => 'orders'], 'timestamp_ms' => 10_000],
        );
        $client->shouldReceive('pause')->once()->andReturnUsing(static function () use (&$events): void {
            $events[] = 'pause';
        });
        $client->shouldReceive('poll')->with(0)->atLeast()->once()->andReturnUsing(static function () use (&$events): void {
            $events[] = 'poll';
        });
        $client->shouldReceive('resume')->once()->andReturnUsing(static function () use (&$events): void {
            $events[] = 'resume';
        });
        $client->shouldReceive('commit')->once()->andReturnUsing(static function () use (&$events): void {
            $events[] = 'commit';
        });

        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->andReturnUsing(static function () use (&$events): void {
            $events[] = 'produce';
        });

        $clock = 10_000;
        $calls = 0;
        (new KafkaRetryConsumer($client, $producer, static function () use (&$clock): int {
            return $clock;
        }, static function (int $ms) use (&$clock): void {
            $clock += $ms;
        }))->consume(static function () use (&$calls): bool {
            return $calls++ >= 1;
        });

        $this->assertSame('pause', $events[0]);
        $this->assertSame(['resume', 'produce', 'commit'], array_slice($events, -3), 'ack (produce) happens before the commit');
    }

    public function test_the_offset_is_not_committed_when_the_write_fails(): void
    {
        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-original-topic' => 'orders']));
        $client->shouldNotReceive('commit');

        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->andThrow(new \RuntimeException('broker down'));

        $this->expectException(\RuntimeException::class);
        (new KafkaRetryConsumer($client, $producer))->consume(static fn (): bool => false);
    }

    /**
     * Runs one record and returns the total ms slept on a virtual clock.
     *
     * @param  array<string, string>  $headers
     */
    private function waitedMs(array $headers, int $timestampMs, int $nowMs): int
    {
        $client = Mockery::mock(KafkaRetryConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            ['payload' => self::ENVELOPE, 'headers' => $headers, 'timestamp_ms' => $timestampMs],
        );
        $client->shouldReceive('poll')->with(0)->zeroOrMoreTimes();
        $client->shouldReceive('commit')->once();

        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once();

        $clock = $nowMs;
        $slept = 0;
        $calls = 0;
        (new KafkaRetryConsumer($client, $producer, static function () use (&$clock): int {
            return $clock;
        }, static function (int $ms) use (&$clock, &$slept): void {
            $clock += $ms;
            $slept += $ms;
        }))->consume(static function () use (&$calls): bool {
            return $calls++ >= 1;
        });

        return $slept;
    }
}
