<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Consume;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\Dispatcher;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\Transport;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarMessage;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

/**
 * Every message terminates (ADR-0036 decision 3): the Dispatcher's cap, its poison route, the
 * counted `dead_letter.attempts`, the publisher-less drop on Pulsar and the never-drop rule on Kafka.
 */
final class DispatcherTerminationTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public const URN = 'urn:babel:orders:created';

    private const BODY = '{"job":"urn:babel:orders:created","trace_id":"t-1","data":{"order_id":7},'
        . '"meta":{"id":"m-1","queue":"orders","lang":"php","schema_version":1,"created_at":1},"attempts":0}';

    /** @var list<array{0: string, 1: string}> payload, queue */
    private array $published = [];

    /** @var list<string> */
    private array $dropped = [];

    protected function setUp(): void
    {
        EnvelopeCodec::setWarningHandler(static function (): void {
        });
    }

    protected function tearDown(): void
    {
        EnvelopeCodec::setWarningHandler(null);
    }

    private function publisher(): DeadLetterPublisher
    {
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->andReturnUsing(function (string $payload, ?string $queue): ?string {
            $this->published[] = [$payload, (string) $queue];

            return null;
        });

        return new DeadLetterPublisher($transport);
    }

    private function onDrop(): \Closure
    {
        return function (ConsumedMessage $m, string $why): void {
            $this->dropped[] = $why;
        };
    }

    /**
     * A Pulsar delivery through the real consumer.
     *
     * @param  array<string, string>  $properties
     * @param  array<string, mixed>  $frame  extra frame keys (e.g. `topic`)
     */
    private function pulsar(string $body = self::BODY, int $redelivery = 0, array $properties = [], ?string $queue = 'orders', array $frame = []): PulsarMessage
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            ['messageId' => 'mid-1', 'payload' => $body, 'properties' => $properties, 'redeliveryCount' => $redelivery] + $frame,
        );
        $message = (new PulsarConsumer($client, null, $queue, subscriptionType: 'Shared'))->receive();
        $this->assertNotNull($message);

        return $message;
    }

    private function kafka(string $body = self::BODY, int $attempts = 0, ?string $topic = 'orders'): KafkaMessage
    {
        $envelope = EnvelopeCodec::decode($body);
        $envelope['attempts'] = $attempts;

        return new KafkaMessage($envelope, [], $body, null, null, $topic);
    }

    private function failing(Dispatcher $dispatcher): Dispatcher
    {
        return $dispatcher->on(self::URN, static function (): void {
            throw new RuntimeException('boom');
        });
    }

    /** @return array<string, mixed> */
    private function record(int $index = 0): array
    {
        return json_decode($this->published[$index][0], true, 512, JSON_THROW_ON_ERROR);
    }

    // -- poison route ---------------------------------------------------------------------------------

    public function test_a_pulsar_poison_body_goes_to_the_consumers_queue_dlq_not_default(): void
    {
        $dispatcher = (new Dispatcher(deadLetters: $this->publisher()))
            ->on(self::URN, static fn () => self::fail('a poison body must never reach a handler'));

        $dispatcher($this->pulsar('{not json'));

        $this->assertCount(1, $this->published);
        $this->assertSame('orders.dlq', $this->published[0][1]);
        $record = $this->record();
        $this->assertSame('{not json', $record['raw']);
        $this->assertSame('poison', $record['dead_letter']['reason']);
        $this->assertSame('orders', $record['dead_letter']['original_queue']);
    }

    public function test_the_queue_comes_from_the_frames_topic_when_the_consumer_was_not_given_one(): void
    {
        $dispatcher = new Dispatcher(deadLetters: $this->publisher());

        $dispatcher($this->pulsar('{}', 0, [], null, ['topic' => 'persistent://public/default/invoices']));

        $this->assertSame('invoices.dlq', $this->published[0][1]);
        $this->assertSame('invoices', $this->record()['dead_letter']['original_queue']);
    }

    public function test_an_explicit_consumer_queue_wins_over_the_frames_topic(): void
    {
        $dispatcher = new Dispatcher(deadLetters: $this->publisher());

        $dispatcher($this->pulsar('{}', 0, [], 'orders', ['topic' => 'persistent://public/default/invoices']));

        $this->assertSame('orders.dlq', $this->published[0][1]);
    }

    /** A valid envelope with no `meta.queue` (a foreign producer), through a consumer that was not told its queue. */
    private const NO_META_QUEUE = '{"job":"urn:babel:orders:created","trace_id":"t-1","data":{},"attempts":0}';

    public function test_default_dlq_is_never_written_an_unnameable_queue_is_handled_like_no_publisher_on_pulsar(): void
    {
        // Pulsar, queue unknown: a poison body under `dead_letter` is acknowledged, dropped and logged.
        (new Dispatcher('dead_letter', 3, $this->publisher(), onDrop: $this->onDrop()))($this->pulsar('{not json', 0, [], null));
        // ... an unknown URN under `dead_letter` likewise (the envelope has no meta.queue either).
        (new Dispatcher('dead_letter', 3, $this->publisher(), onDrop: $this->onDrop()))($this->pulsar(self::NO_META_QUEUE, 0, [], null));
        // ... a poison body under `fail` is retried, then dropped at the cap (the bounded path of "no publisher").
        $fail = new Dispatcher('fail', 3, $this->publisher(), onDrop: $this->onDrop());
        try {
            $fail($this->pulsar('{not json', 0, [], null));
            $this->fail('delivery 1 is retried');
        } catch (UnknownUrnException) {
            $this->addToAssertionCount(1);
        }
        $fail($this->pulsar('{not json', 2, [], null));
        // ... and a failing handler at the cap.
        $this->failing(new Dispatcher('fail', 3, $this->publisher(), onDrop: $this->onDrop()))($this->pulsar(self::NO_META_QUEUE, 2, [], null));

        $this->assertSame([], $this->published, 'nothing is written to default.dlq');
        $this->assertCount(4, $this->dropped);
    }

    public function test_an_unnameable_queue_on_kafka_with_a_publisher_drops_and_logs_and_never_publishes(): void
    {
        // ADR-0036 decision 3: a publisher is set but the topic is unknown: section 6.5 degrade (commit, drop, log).
        // A rethrow would redeliver a record that can never be addressed and wedge the partition.
        $poison = new KafkaMessage(['attempts' => 0], [], '{not json');
        $noMeta = new KafkaMessage(EnvelopeCodec::decode(self::NO_META_QUEUE), [], self::NO_META_QUEUE);

        foreach ([$poison, $noMeta] as $message) {
            (new Dispatcher('dead_letter', 3, $this->publisher(), onDrop: $this->onDrop()))($message);
        }

        $this->assertSame([], $this->published, 'never default.dlq, never <meta.queue>.dlq');
        $this->assertCount(2, $this->dropped);
    }

    public function test_an_unnameable_queue_on_kafka_without_any_publisher_still_rethrows(): void
    {
        $poison = new KafkaMessage(['attempts' => 0], [], '{not json');
        $noMeta = new KafkaMessage(EnvelopeCodec::decode(self::NO_META_QUEUE), [], self::NO_META_QUEUE);

        foreach ([$poison, $noMeta] as $message) {
            try {
                (new Dispatcher('dead_letter', 3, onDrop: $this->onDrop()))($message);
                $this->fail('without a publisher Kafka rethrows');
            } catch (UnknownUrnException) {
                $this->addToAssertionCount(1);
            }
        }

        $this->assertSame([], $this->dropped);
    }

    public function test_the_drop_is_for_pulsar_only_an_unknown_message_type_keeps_rethrowing(): void
    {
        $message = new class implements ConsumedMessage {
            public function getUrn(): string
            {
                return DispatcherTerminationTest::URN;
            }

            public function getTraceId(): string
            {
                return '';
            }

            public function getData(): array
            {
                return [];
            }

            public function getMeta(): array
            {
                return ['queue' => 'orders'];
            }

            public function attempts(): int
            {
                return 2; // counted 3 = the cap
            }

            public function envelope(): array
            {
                return ['job' => DispatcherTerminationTest::URN, 'meta' => ['queue' => 'orders'], 'attempts' => 2];
            }
        };

        try {
            $this->failing(new Dispatcher(onDrop: $this->onDrop()))($message);
            $this->fail('a message type the Dispatcher does not know is never dropped on the cap');
        } catch (RuntimeException) {
            $this->assertSame([], $this->dropped);
        }

        // ... but the dead_letter strategy keeps degrading to delete (acked, now logged) as configured.
        (new Dispatcher('dead_letter', onDrop: $this->onDrop()))(new class implements ConsumedMessage {
            public function getUrn(): string
            {
                return 'urn:x';
            }

            public function getTraceId(): string
            {
                return '';
            }

            public function getData(): array
            {
                return [];
            }

            public function getMeta(): array
            {
                return [];
            }

            public function attempts(): int
            {
                return 0;
            }

            public function envelope(): array
            {
                return ['job' => 'urn:x'];
            }
        });
        $this->assertCount(1, $this->dropped);
    }

    public function test_a_poison_body_counts_only_bq_attempts_and_the_redelivery_count_not_a_body_attempts(): void
    {
        // Strictly poison (wrong-typed trace_id) yet decodable: the body says attempts 7.
        $body = '{"job":"urn:babel:orders:created","trace_id":5,"attempts":7}';
        $message = $this->pulsar($body);

        $this->assertSame(0, $message->attempts());

        try {
            (new Dispatcher('fail', 3, onDrop: $this->onDrop()))($message);
            $this->fail('delivery 1 (counted 1 < 3) is retried, not dropped');
        } catch (UnknownUrnException) {
            $this->assertSame([], $this->dropped);
        }

        // bq-attempts and the redelivery count still count.
        $this->assertSame(2, $this->pulsar($body, 0, ['bq-attempts' => '2'])->attempts());
        $this->assertSame(4, $this->pulsar($body, 4)->attempts());
    }

    public function test_an_identity_less_object_and_a_wrong_typed_member_are_poison_with_their_bytes_kept(): void
    {
        $dispatcher = new Dispatcher(deadLetters: $this->publisher());

        $wrongTyped = str_replace('"attempts":0', '"attempts":"3"', self::BODY);
        $dispatcher($this->pulsar('{"trace_id":"t","data":{}}'));
        $dispatcher($this->pulsar($wrongTyped));

        $this->assertCount(2, $this->published);
        $this->assertSame('poison', $this->record(0)['dead_letter']['reason']);
        $this->assertSame('t', $this->record(0)['trace_id']);
        $this->assertSame('poison', $this->record(1)['dead_letter']['reason']);
        $this->assertStringContainsString('"attempts":"3"', $this->published[1][0], 'the original bytes are spliced, not re-encoded');
    }

    public function test_a_poison_body_is_not_retried_when_a_publisher_is_set(): void
    {
        $dispatcher = new Dispatcher('release', 5, $this->publisher());

        $dispatcher($this->pulsar('{not json', 0)); // returns (ack), no throw, on its first delivery

        $this->assertCount(1, $this->published);
    }

    public function test_a_poison_body_without_a_publisher_takes_the_bounded_fail_path(): void
    {
        foreach (['fail', 'release'] as $strategy) {
            $dispatcher = new Dispatcher($strategy, 3, onDrop: $this->onDrop());

            foreach ([0, 1] as $redelivery) {
                try {
                    $dispatcher($this->pulsar('{not json', $redelivery));
                    $this->fail('delivery ' . ($redelivery + 1) . ' must be retried');
                } catch (UnknownUrnException) {
                    $this->addToAssertionCount(1);
                }
            }

            $dispatcher($this->pulsar('{not json', 2)); // 3rd delivery: acknowledged and dropped
        }

        $this->assertCount(2, $this->dropped);
        $this->assertSame([], $this->published);
    }

    public function test_poison_with_delete_drops_and_logs(): void
    {
        (new Dispatcher('delete', 3, $this->publisher(), onDrop: $this->onDrop()))($this->pulsar('{not json'));

        $this->assertSame([], $this->published);
        $this->assertCount(1, $this->dropped);
    }

    public function test_poison_with_dead_letter_and_no_publisher_drops_and_logs_on_pulsar_but_rethrows_on_kafka(): void
    {
        $dispatcher = new Dispatcher('dead_letter', onDrop: $this->onDrop());

        $dispatcher($this->pulsar('{not json'));
        $this->assertCount(1, $this->dropped);

        $this->expectException(UnknownUrnException::class);
        $dispatcher(new KafkaMessage(['attempts' => 0], [], '{not json'));
    }

    // -- cap, counted dead_letter.attempts ------------------------------------------------------------

    public function test_the_default_cap_is_three_and_the_dlq_record_carries_the_counted_attempts(): void
    {
        $dispatcher = $this->failing(new Dispatcher(deadLetters: $this->publisher()));

        foreach ([0, 1] as $redelivery) {
            try {
                $dispatcher($this->pulsar(self::BODY, $redelivery));
                $this->fail('delivery ' . ($redelivery + 1) . ' must be rethrown');
            } catch (RuntimeException) {
                $this->assertSame([], $this->published);
            }
        }

        $dispatcher($this->pulsar(self::BODY, 2)); // 3rd delivery

        $this->assertCount(1, $this->published);
        $this->assertSame('orders.dlq', $this->published[0][1]);
        $this->assertSame('failed', $this->record()['dead_letter']['reason']);
        $this->assertSame(3, $this->record()['dead_letter']['attempts']);
    }

    public function test_without_a_publisher_the_third_delivery_is_acknowledged_dropped_and_not_rethrown(): void
    {
        $dispatcher = $this->failing(new Dispatcher(onDrop: $this->onDrop()));

        $dispatcher($this->pulsar(self::BODY, 2)); // would throw before: no publisher -> unbounded redelivery

        $this->assertCount(1, $this->dropped);
        $this->assertSame([], $this->published);
    }

    public function test_the_default_drop_is_logged_through_error_log_without_the_body(): void
    {
        $file = tempnam(sys_get_temp_dir(), 'bq-log');
        $previous = ini_set('error_log', (string) $file);

        try {
            $this->failing(new Dispatcher())($this->pulsar(self::BODY, 2));
        } finally {
            ini_set('error_log', (string) $previous);
        }

        $log = (string) file_get_contents((string) $file);
        unlink((string) $file);
        $this->assertStringContainsString('[babelqueue] dropped a message (id m-1, delivery 3)', $log);
        $this->assertStringNotContainsString('order_id', $log);
    }

    public function test_unknown_urn_fail_and_release_are_bounded_by_the_cap(): void
    {
        foreach (['fail', 'release'] as $strategy) {
            $this->published = [];
            $dispatcher = new Dispatcher($strategy, 3, $this->publisher());

            try {
                $dispatcher($this->pulsar(self::BODY, 1));
                $this->fail('delivery 2 must be retried');
            } catch (UnknownUrnException) {
                $this->assertSame([], $this->published);
            }

            $dispatcher($this->pulsar(self::BODY, 2));

            $this->assertCount(1, $this->published, $strategy);
            $this->assertSame('unknown_urn', $this->record()['dead_letter']['reason']);
            $this->assertSame(3, $this->record()['dead_letter']['attempts']);
        }
    }

    public function test_a_direct_dead_letter_keeps_the_delivered_attempts(): void
    {
        (new Dispatcher('dead_letter', 3, $this->publisher()))($this->pulsar(self::BODY, 1));

        $this->assertSame('unknown_urn', $this->record()['dead_letter']['reason']);
        $this->assertSame(1, $this->record()['dead_letter']['attempts'], 'unchanged: informational, not counted');
    }

    public function test_max_attempts_zero_means_no_cap(): void
    {
        $dispatcher = $this->failing(new Dispatcher('fail', 0, $this->publisher(), onDrop: $this->onDrop()));

        $this->expectException(RuntimeException::class);
        try {
            $dispatcher($this->pulsar(self::BODY, 500));
        } finally {
            $this->assertSame([], $this->published);
            $this->assertSame([], $this->dropped);
        }
    }

    public function test_unknown_urn_dead_letter_without_a_publisher_logs_and_acks_on_pulsar(): void
    {
        (new Dispatcher('dead_letter', onDrop: $this->onDrop()))($this->pulsar());

        $this->assertCount(1, $this->dropped);
    }

    public function test_unknown_urn_delete_stays_a_silent_ack_and_drop(): void
    {
        (new Dispatcher('delete', onDrop: $this->onDrop()))($this->pulsar());

        $this->assertSame([], $this->dropped);
        $this->assertSame([], $this->published);
    }

    // -- version gate (ADR-0037 decision 1, Pulsar §5.7) ---------------------------------------------------

    /** @return array<string, string> */
    private function gated(string $attempts = '0'): array
    {
        return ['bq-schema-version' => '2', 'bq-attempts' => $attempts];
    }

    public function test_a_version_gated_pulsar_delivery_is_written_raw_to_the_consumers_queue_dlq(): void
    {
        $dispatcher = new Dispatcher('dead_letter', 3, $this->publisher());

        $dispatcher($this->pulsar(self::BODY, 0, $this->gated()));

        $this->assertCount(1, $this->published);
        $this->assertSame('orders.dlq', $this->published[0][1]);
        // The raw record: the exact bytes, no `dead_letter` block, no ADR-0035 wrapping.
        $this->assertSame(self::BODY, $this->published[0][0]);
    }

    public function test_a_version_gated_delivery_is_settled_on_the_first_delivery_whatever_the_attempts_say(): void
    {
        foreach (['fail', 'release', 'dead_letter'] as $strategy) {
            $this->published = [];
            $dispatcher = (new Dispatcher($strategy, 3, $this->publisher()))
                ->on(self::URN, static fn () => self::fail('a version-gated delivery must never reach a handler'));

            // Well below the cap, and far above it: the outcome does not depend on the attempts.
            $dispatcher($this->pulsar(self::BODY, 0, $this->gated('0')));
            $dispatcher($this->pulsar(self::BODY, 40, $this->gated('7')));

            $this->assertCount(2, $this->published, $strategy);
            $this->assertSame([self::BODY, self::BODY], [$this->published[0][0], $this->published[1][0]], $strategy);
        }
    }

    public function test_a_version_gated_delivery_is_acked_dropped_and_logged_without_a_publisher(): void
    {
        foreach (['fail', 'release', 'dead_letter'] as $strategy) {
            $this->dropped = [];

            (new Dispatcher($strategy, 3, onDrop: $this->onDrop()))($this->pulsar(self::BODY, 5, $this->gated('7')));

            $this->assertCount(1, $this->dropped, $strategy);
            $this->assertStringNotContainsString('order_id', $this->dropped[0], 'a drop never carries the body');
        }
    }

    public function test_a_version_gated_delivery_under_delete_is_a_silent_ack_even_with_a_publisher(): void
    {
        (new Dispatcher('delete', 3, $this->publisher(), onDrop: $this->onDrop()))($this->pulsar(self::BODY, 0, $this->gated()));

        $this->assertSame([], $this->published);
        $this->assertSame([], $this->dropped);
    }

    public function test_a_version_gated_delivery_whose_queue_is_unknown_is_acked_dropped_and_logged(): void
    {
        (new Dispatcher('fail', 3, $this->publisher(), onDrop: $this->onDrop()))($this->pulsar(self::BODY, 0, $this->gated(), null));

        $this->assertSame([], $this->published, 'nothing is ever written to default.dlq');
        $this->assertCount(1, $this->dropped);
    }

    public function test_a_failed_raw_write_of_a_version_gated_delivery_throws_and_never_retries_or_drops(): void
    {
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->andThrow(new RuntimeException('broker down'));
        $retried = 0;

        $dispatcher = new Dispatcher('fail', 3, new DeadLetterPublisher($transport), onRetry: function () use (&$retried): void {
            $retried++;
        }, onDrop: $this->onDrop());

        try {
            $dispatcher($this->pulsar(self::BODY, 0, $this->gated()));
            $this->fail('the failed write must surface so the delivery stays unsettled');
        } catch (\BabelQueue\Consume\DeadLetterWriteFailed) {
            $this->assertSame(0, $retried);
            $this->assertSame([], $this->dropped);
        }
    }

    public function test_a_version_gated_kafka_record_keeps_the_unknown_urn_path(): void
    {
        $message = new KafkaMessage([], [], self::BODY, PulsarConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX . '2', null, 'orders');

        $this->expectException(UnknownUrnException::class);
        (new Dispatcher('fail', 3))($message);
    }

    // -- Kafka: the Dispatcher never drops ------------------------------------------------------------

    public function test_on_kafka_a_failing_handler_at_the_cap_without_a_publisher_rethrows_and_never_drops(): void
    {
        $dispatcher = $this->failing(new Dispatcher(onDrop: $this->onDrop()));

        try {
            $dispatcher($this->kafka(self::BODY, 2));
            $this->fail('the exception must be rethrown so the retry router or the consumer decides');
        } catch (RuntimeException) {
            $this->assertSame([], $this->dropped);
            $this->assertSame([], $this->published);
        }
    }

    public function test_on_kafka_unknown_urn_fail_and_release_at_the_cap_without_a_publisher_rethrow(): void
    {
        foreach (['fail', 'release', 'dead_letter'] as $strategy) {
            try {
                (new Dispatcher($strategy, 3, onDrop: $this->onDrop()))($this->kafka(self::BODY, 2));
                $this->fail($strategy . ' must rethrow on Kafka');
            } catch (UnknownUrnException) {
                $this->assertSame([], $this->dropped);
            }
        }
    }

    public function test_on_kafka_the_cap_with_a_publisher_dead_letters_to_the_topic_dlq_with_counted_attempts(): void
    {
        $dispatcher = $this->failing(new Dispatcher(deadLetters: $this->publisher()));

        $dispatcher($this->kafka(self::BODY, 2));

        $this->assertSame('orders.dlq', $this->published[0][1]);
        $this->assertSame('failed', $this->record()['dead_letter']['reason']);
        $this->assertSame(3, $this->record()['dead_letter']['attempts']);
    }

    public function test_on_kafka_the_dlq_is_named_by_the_consumers_topic_never_by_meta_queue(): void
    {
        // meta.queue says "orders" (or "default"), the record was read from "billing": billing.dlq, only.
        foreach ([self::BODY, str_replace('"queue":"orders"', '"queue":"default"', self::BODY)] as $body) {
            $this->published = [];
            $this->failing(new Dispatcher(deadLetters: $this->publisher()))($this->kafka($body, 2, 'billing'));

            $this->assertSame(['billing.dlq'], array_column($this->published, 1));
            $this->assertSame($this->record()['meta']['queue'], $this->record()['dead_letter']['original_queue']);
        }
    }

    public function test_on_kafka_poison_is_dead_lettered_to_the_topic_dlq(): void
    {
        $dispatcher = new Dispatcher('dead_letter', 3, $this->publisher());
        $dispatcher(new KafkaMessage(['attempts' => 0], [], '{not json', null, null, 'billing'));

        $this->assertSame(['billing.dlq'], array_column($this->published, 1));
        $this->assertSame('poison', $this->record()['dead_letter']['reason']);
    }

    public function test_on_kafka_an_unknown_topic_with_a_publisher_drops_and_logs_even_with_meta_queue(): void
    {
        // A failing handler at the cap, an unknown-URN fail at the cap, both with a publisher and no topic.
        foreach ([null, ''] as $topic) {
            $this->failing(new Dispatcher(deadLetters: $this->publisher(), onDrop: $this->onDrop()))($this->kafka(self::BODY, 2, $topic));
            (new Dispatcher('fail', 3, $this->publisher(), onDrop: $this->onDrop()))($this->kafka(self::BODY, 2, $topic));
        }

        $this->assertSame([], $this->published, 'meta.queue must not name the DLQ');
        $this->assertCount(4, $this->dropped, 'committed, dropped and logged: never a rethrow that wedges the partition');
    }

    public function test_on_kafka_an_unknown_topic_below_the_cap_is_still_a_normal_retry(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->failing(new Dispatcher(deadLetters: $this->publisher(), onDrop: $this->onDrop()))($this->kafka(self::BODY, 0, null));
    }

    public function test_on_kafka_an_unknown_topic_without_a_publisher_still_rethrows_at_the_cap(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->failing(new Dispatcher(onDrop: $this->onDrop()))($this->kafka(self::BODY, 2, null));
    }

    public function test_kafka_message_exposes_the_source_topic_as_its_queue(): void
    {
        $this->assertSame('billing', (new KafkaMessage(['attempts' => 0], [], '{}', null, null, 'billing'))->queue());
        $this->assertNull((new KafkaMessage(['attempts' => 0], [], '{}'))->queue());
        $this->assertNull((new KafkaMessage(['attempts' => 0], [], '{}', null, null, ''))->queue());
    }

    public function test_on_kafka_below_the_cap_the_failure_is_rethrown_as_before(): void
    {
        $dispatcher = $this->failing(new Dispatcher(deadLetters: $this->publisher()));

        $this->expectException(RuntimeException::class);
        $dispatcher($this->kafka(self::BODY, 1));
    }

    public function test_max_attempts_zero_leaves_the_cap_to_the_kafka_retry_router(): void
    {
        $dispatcher = $this->failing(new Dispatcher('fail', 0, $this->publisher()));

        try {
            $dispatcher($this->kafka(self::BODY, 9));
            $this->fail('rethrown');
        } catch (Throwable) {
            $this->assertSame([], $this->published);
        }
    }

    // -- DeadLetterPublisher --------------------------------------------------------------------------

    public function test_the_target_is_the_callers_queue_and_meta_queue_only_fills_original_queue(): void
    {
        $publisher = $this->publisher();
        $withMeta = $this->pulsar(self::BODY);
        $noMeta = new PulsarMessage(['job' => self::URN, 'attempts' => 1], 'mid-2');

        $publisher->publish($withMeta, 'failed', null, 5, 'other');
        $publisher->publish($noMeta, 'failed', null, null, 'other');
        $publisher->publish($withMeta, 'failed', null, 5); // no caller queue: the message's own meta.queue

        $this->assertSame(['other.dlq', 'other.dlq', 'orders.dlq'], array_column($this->published, 1));
        $this->assertSame('orders', $this->record(0)['dead_letter']['original_queue']);
        $this->assertSame('other', $this->record(1)['dead_letter']['original_queue']);
        $this->assertSame(5, $this->record(0)['dead_letter']['attempts']);
        $this->assertSame(1, $this->record(1)['dead_letter']['attempts'], 'no explicit attempts: the delivered value');
    }

    public function test_a_producers_meta_queue_never_picks_the_dlq_on_pulsar(): void
    {
        // EnvelopeCodec::make() defaults meta.queue to "default"; a producer may also have used any other name.
        foreach (['default', 'billing'] as $metaQueue) {
            $this->published = [];
            $body = str_replace('"queue":"orders"', '"queue":"' . $metaQueue . '"', self::BODY);
            $dispatcher = $this->failing(new Dispatcher(deadLetters: $this->publisher()));

            $dispatcher($this->pulsar($body, 2)); // consumer queue `orders`, third delivery

            $this->assertSame('orders.dlq', $this->published[0][1], $metaQueue);
            $this->assertSame($metaQueue, $this->record()['dead_letter']['original_queue'], $metaQueue);
        }

        // ... and the unknown-URN strategy likewise.
        $this->published = [];
        (new Dispatcher('dead_letter', 3, $this->publisher()))($this->pulsar(str_replace('"queue":"orders"', '"queue":"default"', str_replace(self::URN, 'urn:babel:other', self::BODY))));
        $this->assertSame('orders.dlq', $this->published[0][1]);
    }

    public function test_a_pulsar_consumer_with_an_unknown_queue_does_not_fall_back_to_the_producers_meta_queue(): void
    {
        $dispatcher = $this->failing(new Dispatcher(deadLetters: $this->publisher(), onDrop: $this->onDrop()));

        $dispatcher($this->pulsar(self::BODY, 2, [], null)); // meta.queue is orders, but the consumer's queue is unknown

        $this->assertSame([], $this->published);
        $this->assertCount(1, $this->dropped);
    }
}
