<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\Dispatcher;
use BabelQueue\Contracts\Transport;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Transport\KafkaConsumer;
use BabelQueue\Transport\KafkaConsumerClient;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaSubscriptionDescriptor;
use BabelQueue\Transport\KeyedKafkaProducer;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\Attributes\DataProvider;
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
     * @return array{payload: string, headers: array<string, string>, topic?: string}
     */
    private function raw(array $headers = [], string $payload = self::ENVELOPE, ?string $topic = 'orders'): array
    {
        return ['payload' => $payload, 'headers' => $headers] + ($topic === null ? [] : ['topic' => $topic]);
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

    public function test_receive_carries_the_raw_body_for_the_dead_letter_path(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '0']));

        $this->assertSame(self::ENVELOPE, (new KafkaConsumer($client))->receive()->rawBody());
    }

    // -- bq-schema-version gate (§6.7) ---------------------------------------

    public function test_a_missing_or_blank_schema_version_decodes_normally(): void
    {
        // blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r), identical in every SDK
        $blanks = ['', '  ', "\x0B", "\f", " \t\n\x0B\f\r"];
        $cases = [[]];
        foreach ($blanks as $blank) {
            $cases[] = ['bq-schema-version' => $blank];
        }

        foreach ($cases as $headers) {
            $client = Mockery::mock(KafkaConsumerClient::class);
            $client->shouldReceive('receive')->once()->andReturn($this->raw($headers));

            $message = (new KafkaConsumer($client))->receive();

            $this->assertSame('urn:babel:orders:created', $message->getUrn());
            $this->assertSame('urn:babel:orders:created', $message->envelope()['job']);
        }
    }

    public function test_a_supported_schema_version_is_delivered(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-schema-version' => '1', 'bq-attempts' => '3']));

        $message = (new KafkaConsumer($client))->receive();

        $this->assertSame('urn:babel:orders:created', $message->getUrn());
        $this->assertSame(3, $message->attempts());
    }

    /** @return array<string, array{string}> */
    public static function unsupportedSchemaVersions(): array
    {
        return ['2' => ['2'], 'non-numeric' => ['x'], 'leading zero' => ['01'], 'padded' => [' 1'], 'decimal' => ['1.0'], 'negative' => ['-1'],
            // not ASCII whitespace, hence not blank
            'nbsp' => ["\u{00A0}"], 'U+001C' => ["\u{001C}"], 'U+0085' => ["\u{0085}"], 'BOM' => ["\u{FEFF}"], 'ideographic space' => ["\u{3000}"], 'NUL' => ["\0"]];
    }

    #[DataProvider('unsupportedSchemaVersions')]
    public function test_an_unsupported_schema_version_is_gated_before_decode(string $value): void
    {
        // The body is not JSON: had it been decoded the routing URN would be empty (and the envelope
        // empty). The sentinel URN and the untouched raw body prove the gate ran first.
        $garbage = '{not json';

        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw(['bq-schema-version' => $value, 'bq-job' => 'urn:babel:orders:created', 'bq-attempts' => '2'], $garbage),
        );

        $message = (new KafkaConsumer($client))->receive();

        $this->assertSame(KafkaConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX . $value, $message->getUrn());
        $this->assertSame($garbage, $message->rawBody());
        $this->assertSame(['raw' => $garbage, 'attempts' => 2], $message->envelope());
        $this->assertSame(2, $message->attempts());
    }

    public function test_a_gated_message_never_reaches_a_handler_and_stays_uncommitted(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw(['bq-schema-version' => '2', 'bq-job' => 'urn:babel:orders:created'], '{not json'),
        );
        $client->shouldNotReceive('commit');

        $handled = false;
        $dispatcher = (new Dispatcher())->on('urn:babel:orders:created', function () use (&$handled): void {
            $handled = true;
        });

        $thrown = null;
        $calls = 0;
        (new KafkaConsumer($client))->consume(function (KafkaMessage $m) use ($dispatcher, &$thrown): void {
            try {
                $dispatcher($m);
            } catch (UnknownUrnException $e) {
                $thrown = $e;

                throw $e; // consume() leaves the record uncommitted on a throwing handler
            }
        }, function () use (&$calls): bool {
            return $calls++ >= 1;
        });

        $this->assertFalse($handled);
        $this->assertNotNull($thrown);
        $this->assertStringContainsString('babelqueue:unsupported-schema-version:2', $thrown->getMessage());
    }

    public function test_a_gated_message_is_dead_lettered_with_its_original_bytes(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-schema-version' => 'x']));
        $message = (new KafkaConsumer($client))->receive();

        $published = [];
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->andReturnUsing(function (string $payload, string $queue) use (&$published): void {
            $published = [json_decode($payload, true), $queue, $payload];
        });

        // The consumer's topic names the DLQ (`meta.queue` is unknown here, the gate never decodes the body).
        (new Dispatcher('dead_letter', 0, new DeadLetterPublisher($transport)))($message);

        // Without a topic (and no Kafka-side way to derive one) a publisher cannot name `<queue>.dlq`: nothing is written to
        // `default.dlq`, and the record is dropped and logged (ADR-0036 decision 3), not rethrown.
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-schema-version' => 'x'], self::ENVELOPE, null));
        $topicless = (new KafkaConsumer($client))->receive();
        $dropped = 0;
        (new Dispatcher('dead_letter', 0, new DeadLetterPublisher(Mockery::mock(Transport::class)), onDrop: static function () use (&$dropped): void {
            $dropped++;
        }))($topicless);
        $this->assertSame(1, $dropped);

        $this->assertSame(self::ENVELOPE, $published[0]['raw']);
        $this->assertSame('unknown_urn', $published[0]['dead_letter']['reason']);
        // The sentinel URN is an in-process marker: the dead-letter record never carries it.
        $this->assertStringNotContainsString('"job":"babelqueue:unsupported-schema-version', $published[2]);
        $this->assertArrayNotHasKey('job', $published[0]);
        $this->assertSame('orders.dlq', $published[1]);
    }
    // -- poison / no_identity -> <topic>.dlq with reason poison (ADR-0034 §10) ----------

    /**
     * @return array<string, array{0: string}>
     */
    public static function unusableBodies(): array
    {
        return [
            'invalid json' => ['{not json'],
            'empty body' => [''],
            'trailing data' => ['{"job":"urn:babel:orders:created"} x'],
            'not an object' => ['[1,2,3]'],
            'wrong-typed member' => ['{"job":"urn:babel:orders:created","attempts":"3"}'],
            'wrong-typed nested meta' => ['{"job":"urn:babel:orders:created","meta":{"id":7}}'],
            'no identity (empty object)' => ['{}'],
            'no identity (only data)' => ['{"data":{"a":1}}'],
            'no identity (literal null)' => ['null'],
            'blank identity' => ['{"job":"  ","data":{}}'],
        ];
    }

    /**
     * A recording keyed producer + a commit log, so ordering (DLQ write before commit) is asserted.
     *
     * @param  list<string>  $log
     * @return array{0: KeyedKafkaProducer, 1: object}
     */
    private function recordingProducer(array &$log): array
    {
        $recorder = new class($log) implements KeyedKafkaProducer {
            /** @var list<array{topic: string, payload: string, headers: array<string, string>, key: ?string}> */
            public array $records = [];

            /** @param list<string> $log */
            public function __construct(private array &$log)
            {
            }

            public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void
            {
                $this->produceKeyed($topic, $payload, $headers, $timestampMs, null);
            }

            public function produceKeyed(string $topic, string $payload, array $headers, ?int $timestampMs, ?string $key): void
            {
                $this->log[] = 'produce';
                $this->records[] = ['topic' => $topic, 'payload' => $payload, 'headers' => $headers, 'key' => $key];
            }
        };

        return [$recorder, $recorder];
    }

    #[DataProvider('unusableBodies')]
    public function test_an_unusable_body_is_dead_lettered_as_poison_then_committed_and_never_handled(string $body): void
    {
        $log = [];
        [$producer, $recorder] = $this->recordingProducer($log);

        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn([
            'payload' => $body,
            'headers' => ['bq-attempts' => '2', 'traceparent' => '00-aa-bb-01'],
            'key' => 'order-7',
            'topic' => 'orders',
        ]);
        $client->shouldReceive('commit')->once()->andReturnUsing(function () use (&$log): void {
            $log[] = 'commit';
        });

        $handled = false;
        $consumer = new KafkaConsumer($client, $producer);
        $stops = 0;
        $consumer->consume(static function () use (&$handled): void {
            $handled = true;
        }, static function () use (&$stops): bool {
            return $stops++ >= 1;
        });

        $this->assertFalse($handled, 'the handler must not be called for an unusable body');
        $this->assertSame(['produce', 'commit'], $log, 'DLQ write first, offset commit after');
        $this->assertCount(1, $recorder->records);

        $record = $recorder->records[0];
        $this->assertSame('orders.dlq', $record['topic']);
        $this->assertSame('order-7', $record['key']);
        $this->assertSame(['bq-attempts' => '2', 'traceparent' => '00-aa-bb-01'], $record['headers']);

        $dlq = json_decode($record['payload'], true);
        $this->assertSame('poison', $dlq['dead_letter']['reason']);
        $this->assertSame('orders', $dlq['dead_letter']['original_queue']);
        $this->assertSame(2, $dlq['dead_letter']['attempts']);
        $this->assertSame('php', $dlq['dead_letter']['lang']);
    }

    public function test_a_poison_object_body_keeps_its_members_and_a_non_object_is_wrapped_as_raw(): void
    {
        $log = [];
        [$producer, $recorder] = $this->recordingProducer($log);
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->twice()->andReturn(
            $this->raw([], '{"data":{"a":1},"trace_id":"t-9"}'),
            $this->raw([], '{broken'),
        );
        $client->shouldReceive('commit')->twice();

        $consumer = new KafkaConsumer($client, $producer);
        $this->assertNull($consumer->receive());
        $this->assertNull($consumer->receive());

        $first = json_decode($recorder->records[0]['payload'], true);
        $this->assertSame(['a' => 1], $first['data']);
        $this->assertSame('t-9', $first['trace_id']);
        $this->assertArrayNotHasKey('raw', $first);

        $second = json_decode($recorder->records[1]['payload'], true);
        $this->assertSame('{broken', $second['raw']);
        $this->assertSame('poison', $second['dead_letter']['reason']);
    }

    /**
     * @return array<string, array{0: string, 1: bool}> body, whether it is carried as `raw` (else spliced as an object)
     */
    public static function unencodableBodies(): array
    {
        return [
            'invalid UTF-8' => ["\xff\xfe{bad", true],
            'invalid UTF-8 inside an object' => ["{\"job\":\"urn:babel:a:b\",\"data\":\"\xff\"}", true],
            'overflowing number (INF)' => ['{"job":"urn:babel:a:b","attempts":"x","data":{"n":1e400}}', false],
            'overflowing number without identity' => ['{"data":{"n":1e400}}', false],
        ];
    }

    #[DataProvider('unencodableBodies')]
    public function test_an_unencodable_poison_body_is_dead_lettered_byte_exact_and_committed_without_throwing(string $body, bool $wrapped): void
    {
        $log = [];
        [$producer, $recorder] = $this->recordingProducer($log);
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw([], $body));
        $client->shouldReceive('commit')->once()->andReturnUsing(function () use (&$log): void {
            $log[] = 'commit';
        });

        $this->assertNull((new KafkaConsumer($client, $producer))->receive());

        $this->assertSame(['produce', 'commit'], $log, 'written to the DLQ and committed: the partition must not wedge');
        $payload = $recorder->records[0]['payload'];
        $this->assertSame('orders.dlq', $recorder->records[0]['topic']);

        $dlq = json_decode($payload, true);
        $this->assertIsArray($dlq, 'the DLQ record is valid JSON');
        $this->assertSame('poison', $dlq['dead_letter']['reason']);

        if ($wrapped) {
            $this->assertSame('base64', $dlq['raw_encoding'] ?? null);
            $this->assertSame($body, base64_decode($dlq['raw'], true), 'every original byte is recoverable');
        } else {
            $this->assertStringContainsString(substr($body, 1, -1), $payload, 'the original bytes are carried verbatim');
        }
    }

    public function test_a_poison_copy_preserves_empty_objects_and_big_integers(): void
    {
        $log = [];
        [$producer, $recorder] = $this->recordingProducer($log);
        $body = '{"job":"","data":{},"meta":{"ext":{},"big":12345678901234567890}}';
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw([], $body));
        $client->shouldReceive('commit')->once();

        (new KafkaConsumer($client, $producer))->receive();

        $payload = $recorder->records[0]['payload'];
        $this->assertStringContainsString('"data":{}', $payload, '{} must not become []');
        $this->assertStringContainsString('"ext":{}', $payload);
        $this->assertStringContainsString('"big":12345678901234567890', $payload, 'integers beyond int64 must stay exact');
        $this->assertSame('poison', json_decode($payload, false)->dead_letter->reason);
    }

    public function test_the_dlq_topic_is_the_clients_topic_never_the_body_queue(): void
    {
        $log = [];
        [$producer, $recorder] = $this->recordingProducer($log);
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw([], '{"meta":{"queue":"billing"}}', 'shipping'),
        );
        $client->shouldReceive('commit')->once();

        (new KafkaConsumer($client, $producer))->receive();

        $this->assertSame(['shipping.dlq'], array_column($recorder->records, 'topic'));
    }

    /**
     * @param  list<string>|null  $subscribed  null = a client that is not a {@see KafkaSubscriptionDescriptor}
     * @return array{0: KafkaConsumerClient, 1: object, 2: object}  client, recorder, producer
     */
    private function poisonSetup(?array $subscribed, ?string $frameTopic): array
    {
        $log = [];
        [$producer, $recorder] = $this->recordingProducer($log);
        $frame = $this->raw([], '{"meta":{"queue":"billing"}}', $frameTopic);
        $client = $subscribed === null
            ? Mockery::mock(KafkaConsumerClient::class)
            : Mockery::mock(KafkaConsumerClient::class, KafkaSubscriptionDescriptor::class);
        $client->shouldReceive('receive')->once()->andReturn($frame);
        $client->shouldReceive('commit')->once(); // always committed: never a wedged partition
        if ($subscribed !== null) {
            $client->shouldReceive('subscribedTopics')->andReturn($subscribed);
        }

        return [$client, $recorder, $producer];
    }

    public function test_a_poison_record_without_any_topic_is_committed_dropped_and_logged_never_a_default_dlq(): void
    {
        foreach ([[null, null], [null, ''], [['a', 'b'], null], [[], null]] as [$subscribed, $frameTopic]) {
            [$client, $recorder, $producer] = $this->poisonSetup($subscribed, $frameTopic);
            $logged = '';
            $previous = ini_set('error_log', $file = tempnam(sys_get_temp_dir(), 'bqlog'));

            try {
                $this->assertNull((new KafkaConsumer($client, $producer))->receive(), 'no throw: the partition must not wedge');
                $logged = (string) file_get_contents($file);
            } finally {
                ini_set('error_log', (string) $previous);
                unlink($file);
            }

            $this->assertSame([], $recorder->records, 'neither billing.dlq (meta.queue) nor default.dlq');
            $this->assertStringContainsString('poison Kafka record was dropped', $logged);
        }
    }

    public function test_the_dlq_topic_falls_back_to_the_constructor_topic_then_a_single_subscribed_topic(): void
    {
        // the constructor's topic
        [$client, $recorder, $producer] = $this->poisonSetup(null, null);
        (new KafkaConsumer($client, $producer, '.dlq', 'shipping'))->receive();
        $this->assertSame(['shipping.dlq'], array_column($recorder->records, 'topic'));

        // a single subscribed topic of the client
        [$client, $recorder, $producer] = $this->poisonSetup(['invoices'], null);
        (new KafkaConsumer($client, $producer))->receive();
        $this->assertSame(['invoices.dlq'], array_column($recorder->records, 'topic'));

        // the frame wins over both
        [$client, $recorder, $producer] = $this->poisonSetup(['invoices'], 'orders');
        (new KafkaConsumer($client, $producer, '.dlq', 'shipping'))->receive();
        $this->assertSame(['orders.dlq'], array_column($recorder->records, 'topic'));
    }

    public function test_the_message_queue_follows_the_same_derivation(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class, KafkaSubscriptionDescriptor::class);
        $client->shouldReceive('receive')->twice()->andReturn($this->raw([], self::ENVELOPE, null), $this->raw([], self::ENVELOPE, null));
        $client->shouldReceive('subscribedTopics')->andReturn(['orders', 'billing'], ['orders']);
        $consumer = new KafkaConsumer($client);

        $this->assertNull($consumer->receive()?->queue(), 'two subscribed topics: ambiguous');
        $this->assertSame('orders', $consumer->receive()?->queue());
    }

    public function test_receive_carries_the_source_topic_as_the_queue(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->twice()->andReturn(
            $this->raw([], self::ENVELOPE, 'shipping'),
            $this->raw([], self::ENVELOPE, null),
        );
        $consumer = new KafkaConsumer($client);

        $this->assertSame('shipping', $consumer->receive()?->queue());
        $this->assertNull($consumer->receive()?->queue());
    }

    public function test_an_unusable_body_without_a_dlq_producer_is_committed_and_dropped(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw([], '{not json'));
        $client->shouldReceive('commit')->once();

        $this->assertNull((new KafkaConsumer($client))->receive());
    }

    public function test_a_failing_dlq_write_leaves_the_poison_record_uncommitted(): void
    {
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->andThrow(new RuntimeException('broker down'));

        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw([], '{not json'));
        $client->shouldNotReceive('commit');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('broker down');

        (new KafkaConsumer($client, $producer))->receive();
    }

    public function test_a_valid_envelope_is_delivered_and_never_dead_lettered_when_a_dlq_producer_is_set(): void
    {
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldNotReceive('produce');

        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw());
        $client->shouldNotReceive('commit');

        $message = (new KafkaConsumer($client, $producer))->receive();

        $this->assertSame('urn:babel:orders:created', $message?->getUrn());
    }

    public function test_the_version_gate_still_wins_over_the_poison_check(): void
    {
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldNotReceive('produce');

        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-schema-version' => '2'], '{not json'));
        $client->shouldNotReceive('commit');

        $message = (new KafkaConsumer($client, $producer))->receive();

        $this->assertNotNull($message);
        $this->assertSame(KafkaConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX . '2', $message->getUrn());
    }
}
