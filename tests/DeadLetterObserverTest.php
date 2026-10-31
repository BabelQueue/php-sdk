<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\DeadLetterWriteFailed;
use BabelQueue\Contracts\DeadLetterObserver;
use BabelQueue\Contracts\Transport;
use BabelQueue\Tests\Support\EnvelopeMessage;
use BabelQueue\Transport\KafkaConsumer;
use BabelQueue\Transport\KafkaConsumerClient;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaRetryRouter;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The dead-letter observer seam (ADR-0039 section 5): each of the three PHP write primitives tells the
 * observer once, after a confirmed write, with the source queue, the DLQ and the reason; never after a
 * failed write; and an observer that throws changes nothing about the settle outcome. The Kafka paths run
 * against fake producers and clients, so `ext-rdkafka` is not needed.
 */
final class DeadLetterObserverTest extends TestCase
{
    private string $errorLog = '';

    private string|false $previousErrorLog = false;

    protected function setUp(): void
    {
        // A throwing observer is logged with error_log(); keep that out of the test output and assert on it.
        $this->errorLog = (string) tempnam(sys_get_temp_dir(), 'bq-observer');
        $this->previousErrorLog = ini_set('error_log', $this->errorLog);
    }

    protected function tearDown(): void
    {
        if ($this->previousErrorLog !== false) {
            ini_set('error_log', $this->previousErrorLog);
        }
        @unlink($this->errorLog);
    }

    // -- Consume\DeadLetterPublisher::publish / publishRaw -----------------------------------------

    public function test_publish_notifies_once_with_queue_dlq_and_reason_after_the_write(): void
    {
        $observer = new RecordingObserver();
        $transport = new RecordingTransport();

        (new DeadLetterPublisher($transport, observer: $observer))->publish($this->message(), 'unknown_urn', null, null, 'orders');

        self::assertSame(['orders.dlq'], $transport->queues);
        self::assertSame([['orders', 'orders.dlq', 'unknown_urn']], $observer->events);
    }

    public function test_publish_uses_the_message_queue_and_the_configured_suffix(): void
    {
        $observer = new RecordingObserver();

        (new DeadLetterPublisher(new RecordingTransport(), '-dead', $observer))->publish($this->message(), 'failed', new RuntimeException('boom'));

        self::assertSame([['orders', 'orders-dead', 'failed']], $observer->events);
    }

    public function test_publish_does_not_notify_when_the_write_fails(): void
    {
        $observer = new RecordingObserver();
        $publisher = new DeadLetterPublisher(new FailingTransport(), observer: $observer);

        try {
            $publisher->publish($this->message(), 'failed', null, null, 'orders');
            self::fail('expected the write failure');
        } catch (DeadLetterWriteFailed) {
        }

        self::assertSame([], $observer->events);
    }

    public function test_publish_does_not_notify_when_no_queue_can_be_named(): void
    {
        $observer = new RecordingObserver();
        $message = new EnvelopeMessage(['job' => 'urn:babel:x', 'trace_id' => 't', 'data' => [], 'meta' => [], 'attempts' => 0]);

        try {
            (new DeadLetterPublisher(new RecordingTransport(), observer: $observer))->publish($message, 'failed', null);
            self::fail('expected no queue to be nameable');
        } catch (\BabelQueue\Exceptions\BabelQueueException) {
        }

        self::assertSame([], $observer->events);
    }

    public function test_publish_raw_notifies_once_with_an_empty_reason(): void
    {
        $observer = new RecordingObserver();
        $transport = new RecordingTransport();

        (new DeadLetterPublisher($transport, observer: $observer))->publishRaw($this->message(), '{"job":"x","meta":{"schema_version":2}}', 'orders');

        self::assertSame(['{"job":"x","meta":{"schema_version":2}}'], $transport->payloads); // byte for byte
        self::assertSame([['orders', 'orders.dlq', '']], $observer->events);
    }

    public function test_publish_raw_does_not_notify_when_the_write_fails(): void
    {
        $observer = new RecordingObserver();

        try {
            (new DeadLetterPublisher(new FailingTransport(), observer: $observer))->publishRaw($this->message(), '{}', 'orders');
            self::fail('expected the write failure');
        } catch (DeadLetterWriteFailed) {
        }

        self::assertSame([], $observer->events);
    }

    public function test_a_throwing_observer_does_not_change_the_outcome_of_publish_or_publish_raw(): void
    {
        $observer = new ThrowingObserver();
        $transport = new RecordingTransport();
        $publisher = new DeadLetterPublisher($transport, observer: $observer);

        $publisher->publish($this->message(), 'failed', null, null, 'orders'); // does not throw
        $publisher->publishRaw($this->message(), '{}', 'orders');

        self::assertSame(2, $observer->calls);
        self::assertCount(2, $transport->payloads, 'both records were written');
        self::assertStringContainsString('The dead-letter observer failed and was ignored', (string) file_get_contents($this->errorLog));
    }

    public function test_a_publisher_without_an_observer_behaves_as_before(): void
    {
        $transport = new RecordingTransport();

        (new DeadLetterPublisher($transport))->publish($this->message(), 'failed', null, null, 'orders');

        self::assertSame(['orders.dlq'], $transport->queues);
    }

    // -- Transport\KafkaRetryRouter::deadLetter (attempts exhausted) -------------------------------

    public function test_the_retry_router_notifies_once_when_attempts_are_exhausted(): void
    {
        $observer = new RecordingObserver();
        $producer = new RecordingKafkaProducer();

        $retried = (new KafkaRetryRouter($producer, observer: $observer))->route($this->kafkaMessage(attempts: 4), new RuntimeException('boom'));

        self::assertFalse($retried);
        self::assertSame(['orders.dlq'], $producer->topics);
        self::assertSame([['orders', 'orders.dlq', 'failed']], $observer->events);
    }

    public function test_the_retry_router_reports_meta_queue_without_the_topic_prefix_but_the_real_dlq_topic(): void
    {
        $observer = new RecordingObserver();

        (new KafkaRetryRouter(new RecordingKafkaProducer(), dlqSuffix: '-dead', topicPrefix: 'app.', observer: $observer))
            ->route($this->kafkaMessage(attempts: 4, headers: ['bq-original-topic' => 'app.orders']));

        // `queue` is meta.queue, as on the spans and the other metrics; the DLQ is the real topic.
        self::assertSame([['orders', 'app.orders-dead', 'failed']], $observer->events);
    }

    public function test_the_retry_router_does_not_notify_for_a_retry_or_a_requested_delay(): void
    {
        $observer = new RecordingObserver();
        $router = new KafkaRetryRouter(new RecordingKafkaProducer(), observer: $observer);

        self::assertTrue($router->route($this->kafkaMessage(attempts: 0)));
        self::assertTrue($router->route($this->kafkaMessage(attempts: 0), null, 5000));

        self::assertSame([], $observer->events);
    }

    public function test_the_retry_router_does_not_notify_when_the_dlq_write_fails(): void
    {
        $observer = new RecordingObserver();
        $router = new KafkaRetryRouter(new RecordingKafkaProducer(fail: true), observer: $observer);

        try {
            $router->route($this->kafkaMessage(attempts: 4));
            self::fail('expected the producer failure');
        } catch (RuntimeException) {
        }

        self::assertSame([], $observer->events);
    }

    public function test_a_throwing_observer_does_not_change_the_retry_router_result(): void
    {
        $observer = new ThrowingObserver();
        $producer = new RecordingKafkaProducer();

        $retried = (new KafkaRetryRouter($producer, observer: $observer))->route($this->kafkaMessage(attempts: 4));

        self::assertFalse($retried, 'still reported as dead-lettered');
        self::assertSame(['orders.dlq'], $producer->topics);
        self::assertSame(1, $observer->calls);
        self::assertStringContainsString('The dead-letter observer failed and was ignored', (string) file_get_contents($this->errorLog));
    }

    // -- Transport\KafkaConsumer::quarantine (poison) ----------------------------------------------

    public function test_the_kafka_consumer_notifies_once_for_a_quarantined_poison_record(): void
    {
        $observer = new RecordingObserver();
        $producer = new RecordingKafkaProducer();
        $client = new ScriptedKafkaClient([['payload' => 'not json', 'headers' => [], 'topic' => 'orders']]);

        $received = (new KafkaConsumer($client, $producer, observer: $observer))->receive();

        self::assertNull($received);
        self::assertSame(['orders.dlq'], $producer->topics);
        self::assertSame(1, $client->commits, 'the offset is committed after the DLQ write');
        self::assertSame([['orders', 'orders.dlq', 'poison']], $observer->events);
    }

    public function test_the_kafka_consumer_does_not_notify_when_the_dlq_write_fails_and_does_not_commit(): void
    {
        $observer = new RecordingObserver();
        $client = new ScriptedKafkaClient([['payload' => 'not json', 'headers' => [], 'topic' => 'orders']]);
        $consumer = new KafkaConsumer($client, new RecordingKafkaProducer(fail: true), observer: $observer);

        try {
            $consumer->receive();
            self::fail('expected the producer failure');
        } catch (RuntimeException) {
        }

        self::assertSame([], $observer->events);
        self::assertSame(0, $client->commits);
    }

    public function test_the_kafka_consumer_does_not_notify_when_the_record_is_dropped_without_a_dlq(): void
    {
        $observer = new RecordingObserver();
        $client = new ScriptedKafkaClient([['payload' => 'not json', 'headers' => [], 'topic' => 'orders']]);

        // No dead-letter producer: DLQ writing is off, the record is committed and dropped.
        self::assertNull((new KafkaConsumer($client, null, observer: $observer))->receive());

        self::assertSame([], $observer->events);
        self::assertSame(1, $client->commits);
    }

    public function test_the_kafka_consumer_does_not_notify_when_the_topic_is_unknown(): void
    {
        $observer = new RecordingObserver();
        $producer = new RecordingKafkaProducer();
        $client = new ScriptedKafkaClient([['payload' => 'not json', 'headers' => []]]); // no topic anywhere

        self::assertNull((new KafkaConsumer($client, $producer, observer: $observer))->receive());

        self::assertSame([], $producer->topics, 'nothing was written');
        self::assertSame([], $observer->events);
        self::assertSame(1, $client->commits);
    }

    public function test_the_kafka_consumer_does_not_notify_for_a_valid_record(): void
    {
        $observer = new RecordingObserver();
        $producer = new RecordingKafkaProducer();
        $valid = '{"job":"urn:babel:orders:created","trace_id":"t","data":{},"meta":{"id":"i","queue":"orders","lang":"php","schema_version":1,"created_at":1},"attempts":0}';
        $client = new ScriptedKafkaClient([['payload' => $valid, 'headers' => [], 'topic' => 'orders']]);

        self::assertInstanceOf(KafkaMessage::class, (new KafkaConsumer($client, $producer, observer: $observer))->receive());

        self::assertSame([], $observer->events);
        self::assertSame([], $producer->topics);
    }

    public function test_a_throwing_observer_does_not_stop_the_kafka_consumer_from_committing(): void
    {
        $observer = new ThrowingObserver();
        $producer = new RecordingKafkaProducer();
        $client = new ScriptedKafkaClient([['payload' => 'not json', 'headers' => [], 'topic' => 'orders']]);

        self::assertNull((new KafkaConsumer($client, $producer, observer: $observer))->receive()); // does not throw

        self::assertSame(['orders.dlq'], $producer->topics);
        self::assertSame(1, $observer->calls);
        self::assertSame(1, $client->commits, 'the offset is committed regardless of the observer');
        self::assertStringContainsString('The dead-letter observer failed and was ignored', (string) file_get_contents($this->errorLog));
    }

    // -- helpers -----------------------------------------------------------------------------------

    private function message(): EnvelopeMessage
    {
        return new EnvelopeMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 7],
            'meta' => ['id' => 'msg-1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1],
            'attempts' => 0,
        ]);
    }

    /**
     * @param  array<string, string>  $headers
     */
    private function kafkaMessage(int $attempts = 0, array $headers = []): KafkaMessage
    {
        return new KafkaMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 1042],
            'meta' => ['id' => 'msg-1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1749132727000],
            'attempts' => $attempts,
        ], $headers);
    }
}

/** Records every event the observer is told about. */
final class RecordingObserver implements DeadLetterObserver
{
    /** @var list<array{string, string, string}> */
    public array $events = [];

    public function deadLettered(string $queue, string $deadLetterQueue, string $reason): void
    {
        $this->events[] = [$queue, $deadLetterQueue, $reason];
    }
}

/** An observer that always throws. */
final class ThrowingObserver implements DeadLetterObserver
{
    public int $calls = 0;

    public function deadLettered(string $queue, string $deadLetterQueue, string $reason): void
    {
        ++$this->calls;

        throw new RuntimeException('observer failed');
    }
}

/** A {@see Transport} that records what it was given. */
final class RecordingTransport implements Transport
{
    /** @var list<string> */
    public array $payloads = [];

    /** @var list<string|null> */
    public array $queues = [];

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $this->payloads[] = $payload;
        $this->queues[] = $queue;

        return null;
    }
}

/** A {@see Transport} whose every write fails. */
final class FailingTransport implements Transport
{
    public function publish(string $payload, ?string $queue = null): ?string
    {
        throw new RuntimeException('broker down');
    }
}

/** A fake {@see KafkaProducer} (no `ext-rdkafka`) that records the topics it was given, or fails. */
final class RecordingKafkaProducer implements KafkaProducer
{
    /** @var list<string> */
    public array $topics = [];

    public function __construct(private readonly bool $fail = false)
    {
    }

    public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void
    {
        if ($this->fail) {
            throw new RuntimeException('broker down');
        }

        $this->topics[] = $topic;
    }
}

/** A fake {@see KafkaConsumerClient}: hands out the scripted frames, then null, and counts commits. */
final class ScriptedKafkaClient implements KafkaConsumerClient
{
    public int $commits = 0;

    /**
     * @param  list<array{payload: string, headers: array<string, string>, key?: string|null, topic?: string|null}>  $frames
     */
    public function __construct(private array $frames)
    {
    }

    public function receive(): ?array
    {
        return array_shift($this->frames);
    }

    public function commit(): void
    {
        ++$this->commits;
    }
}
