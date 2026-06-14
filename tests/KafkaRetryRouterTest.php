<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaRetryRouter;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The §6.4/§6.5 retry-topic router: a failed work-topic record is republished to the next tiered
 * delay topic `<workTopic>.retry.<delayMs>` with `bq-attempts` incremented (and `bq-delay` /
 * `bq-original-topic` re-injection headers), or — once attempts are exhausted — dead-lettered to
 * `<workTopic>.dlq` with the additive `dead_letter` block. Decoupled from `ext-rdkafka` behind the
 * KafkaProducer seam.
 */
final class KafkaRetryRouterTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    /**
     * @param  array<string, mixed>  $overrides
     * @param  array<string, string>  $headers
     */
    private function message(array $overrides = [], array $headers = []): KafkaMessage
    {
        $envelope = array_merge([
            'job' => 'urn:babel:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 1042],
            'meta' => ['id' => 'msg-1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1749132727000],
            'attempts' => 0,
        ], $overrides);

        return new KafkaMessage($envelope, $headers);
    }

    public function test_routes_a_first_failure_to_the_first_tier_with_incremented_attempts(): void
    {
        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders.retry.5000',
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

        $retried = (new KafkaRetryRouter($producer))->route($this->message());

        $this->assertTrue($retried);

        // attempts bumped to 1 in both the header and the body.
        $this->assertSame('1', $captured['headers']['bq-attempts']);
        $this->assertSame(1, json_decode($captured['payload'], true)['attempts']);

        // re-injection headers.
        $this->assertSame('5000', $captured['headers']['bq-delay']);
        $this->assertSame('orders', $captured['headers']['bq-original-topic']);

        // the rest of the §6 projection is intact; trace_id preserved byte-for-byte (GR-4).
        $this->assertSame('urn:babel:orders:created', $captured['headers']['bq-job']);
        $this->assertSame('trace-1', $captured['headers']['bq-trace-id']);
        $this->assertSame('msg-1', $captured['headers']['bq-message-id']);
        $this->assertSame('1', $captured['headers']['bq-schema-version']);
        $this->assertSame('php', $captured['headers']['bq-source-lang']);
    }

    public function test_picks_the_tier_for_the_next_attempt(): void
    {
        // attempts 1 → next 2 → tier index min(2-1, 3) = 1 → 30000.
        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders.retry.30000',
            Mockery::any(),
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured['headers'] = $headers;

                return true;
            }),
            Mockery::type('int'),
        );

        (new KafkaRetryRouter($producer))->route($this->message(['attempts' => 1]));

        $this->assertSame('2', $captured['headers']['bq-attempts']);
        $this->assertSame('30000', $captured['headers']['bq-delay']);
    }

    public function test_clamps_to_the_largest_tier_past_the_last_index(): void
    {
        // tiers [1000, 3000]; attempts 5 → next 6 → index min(6-1, 1) = 1 → 3000.
        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders.retry.3000',
            Mockery::any(),
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured['headers'] = $headers;

                return true;
            }),
            Mockery::type('int'),
        );

        // maxAttempts high enough that next (6) does not exhaust.
        (new KafkaRetryRouter($producer, [1000, 3000], 100))->route($this->message(['attempts' => 5]));

        $this->assertSame('3000', $captured['headers']['bq-delay']);
    }

    public function test_dead_letters_when_attempts_are_exhausted(): void
    {
        // maxAttempts 5; attempts 4 → next 5 → 5 >= 5 → DLQ, not retry.
        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders.dlq',
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

        $retried = (new KafkaRetryRouter($producer))->route(
            $this->message(['attempts' => 4]),
            new RuntimeException('boom'),
        );

        $this->assertFalse($retried); // exhausted

        $env = json_decode($captured['payload'], true);
        $this->assertSame('failed', $env['dead_letter']['reason']);
        $this->assertSame('boom', $env['dead_letter']['error']);
        $this->assertSame(RuntimeException::class, $env['dead_letter']['exception']);
        $this->assertSame('orders', $env['dead_letter']['original_queue']);
        $this->assertSame(5, $env['dead_letter']['attempts']);
        $this->assertSame('php', $env['dead_letter']['lang']);

        // the DLQ record still carries the §6 bq- headers (attempts at the exhausted count).
        $this->assertSame('5', $captured['headers']['bq-attempts']);
        $this->assertSame('trace-1', $captured['headers']['bq-trace-id']);
        $this->assertArrayNotHasKey('bq-delay', $captured['headers']); // DLQ is terminal, no delay
    }

    public function test_prefers_the_original_topic_header_across_retry_hops(): void
    {
        // A record already in the retry chain carries bq-original-topic; the router routes back to it
        // (with the prefix already baked in), not to <prefix><meta.queue>.
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'app.orders.retry.5000',
            Mockery::any(),
            Mockery::on(function (array $headers): bool {
                return $headers['bq-original-topic'] === 'app.orders';
            }),
            Mockery::type('int'),
        );

        (new KafkaRetryRouter($producer, topicPrefix: 'app.'))->route(
            $this->message(headers: ['bq-original-topic' => 'app.orders']),
        );

        $this->addToAssertionCount(1);
    }

    public function test_applies_the_topic_prefix_to_the_work_topic(): void
    {
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'app.orders.retry.5000',
            Mockery::any(),
            Mockery::on(function (array $headers): bool {
                return $headers['bq-original-topic'] === 'app.orders';
            }),
            Mockery::type('int'),
        );

        (new KafkaRetryRouter($producer, topicPrefix: 'app.'))->route($this->message());

        $this->addToAssertionCount(1);
    }

    public function test_falls_back_to_default_queue_when_meta_queue_is_absent(): void
    {
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'default.retry.5000',
            Mockery::any(),
            Mockery::any(),
            Mockery::type('int'),
        );

        (new KafkaRetryRouter($producer))->route($this->message(['meta' => ['id' => 'm', 'schema_version' => 1]]));

        $this->addToAssertionCount(1);
    }

    public function test_honours_custom_infix_suffix_and_tiers(): void
    {
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders-retry-2500',
            Mockery::any(),
            Mockery::on(function (array $headers): bool {
                return $headers['bq-delay'] === '2500';
            }),
            Mockery::type('int'),
        );

        (new KafkaRetryRouter($producer, [2500], 5, '-retry-', '-dead'))->route($this->message());

        $this->addToAssertionCount(1);
    }
}
