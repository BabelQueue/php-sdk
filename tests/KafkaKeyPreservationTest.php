<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\Dispatcher;
use BabelQueue\Contracts\HasRecordKey;
use BabelQueue\Transport\KafkaConsumer;
use BabelQueue\Transport\KafkaConsumerClient;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaRetryConsumer;
use BabelQueue\Transport\KafkaRetryConsumerClient;
use BabelQueue\Transport\KafkaRetryRouter;
use BabelQueue\Transport\KafkaTransport;
use BabelQueue\Transport\KeyedKafkaProducer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The original Kafka record key survives every SDK-owned write: retry-topic republish, retry→work
 * re-inject, envelope DLQ and raw (poison / gate-rejected) DLQ. A null key stays null, an empty key
 * stays empty, and a key is never synthesised.
 */
final class KafkaKeyPreservationTest extends TestCase
{
    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        . '"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        . '"schema_version":1,"created_at":1749132727000},"attempts":0}';

    /**
     * @return array<string, array{0: string|null}>
     */
    public static function keys(): array
    {
        return ['keyed' => ['order-1042'], 'empty key' => [''], 'null key' => [null]];
    }

    private function keyedProducer(): RecordingKeyedProducer
    {
        return new RecordingKeyedProducer();
    }

    private function message(?string $key, int $attempts = 0): KafkaMessage
    {
        $envelope = EnvelopeCodec::decode(self::ENVELOPE, static function (): void {});
        $envelope['attempts'] = $attempts;

        return new KafkaMessage($envelope, [], self::ENVELOPE, null, $key);
    }

    /**
     * Asserts the single produced record carries $key exactly: a non-null key via the keyed seam, a
     * null key as a plain keyless produce (never a synthesised key).
     *
     * @param  array<int, array{topic: string, key: string|null, keyed: bool}>  $records
     */
    private function assertCarriesKey(array $records, string $topic, ?string $key): void
    {
        $this->assertCount(1, $records);
        $this->assertSame($topic, $records[0]['topic']);
        $this->assertSame($key, $records[0]['key']);
        $this->assertSame($key !== null, $records[0]['keyed']);
    }

    #[DataProvider('keys')]
    public function test_the_retry_republish_keeps_the_key(?string $key): void
    {
        $producer = $this->keyedProducer();

        (new KafkaRetryRouter($producer))->route($this->message($key));

        $this->assertCarriesKey($producer->records, 'orders.retry.1', $key);
    }

    #[DataProvider('keys')]
    public function test_the_envelope_dead_letter_keeps_the_key(?string $key): void
    {
        $producer = $this->keyedProducer();

        (new KafkaRetryRouter($producer, maxAttempts: 1))->route($this->message($key));

        $this->assertCarriesKey($producer->records, 'orders.dlq', $key);
    }

    #[DataProvider('keys')]
    public function test_the_retry_to_work_reinject_keeps_the_key(?string $key): void
    {
        $producer = $this->keyedProducer();
        $client = new ScriptedRetryClient(['payload' => self::ENVELOPE, 'headers' => ['bq-original-topic' => 'orders', 'bq-attempts' => '1'], 'key' => $key]);

        (new KafkaRetryConsumer($client, $producer, static fn (): int => 0, static function (): void {}))
            ->consume(static fn (): bool => self::stopAfter($client));

        $this->assertCarriesKey($producer->records, 'orders', $key);
        $this->assertSame(1, $client->commits);
    }

    #[DataProvider('keys')]
    public function test_a_poison_reinject_keeps_the_key(?string $key): void
    {
        $producer = $this->keyedProducer();
        $client = new ScriptedRetryClient(['payload' => 'not json', 'headers' => ['bq-original-topic' => 'orders'], 'key' => $key]);

        (new KafkaRetryConsumer($client, $producer, static fn (): int => 0, static function (): void {}))
            ->consume(static fn (): bool => self::stopAfter($client));

        $this->assertCarriesKey($producer->records, 'orders', $key);
    }

    public function test_a_retry_record_without_a_key_entry_stays_keyless(): void
    {
        $producer = $this->keyedProducer();
        $client = new ScriptedRetryClient(['payload' => self::ENVELOPE, 'headers' => ['bq-original-topic' => 'orders']]);

        (new KafkaRetryConsumer($client, $producer, static fn (): int => 0, static function (): void {}))
            ->consume(static fn (): bool => self::stopAfter($client));

        $this->assertCarriesKey($producer->records, 'orders', null);
    }

    #[DataProvider('keys')]
    public function test_the_consumer_surfaces_the_record_key(?string $key): void
    {
        $client = new class($key) implements KafkaConsumerClient {
            public function __construct(private readonly ?string $key)
            {
            }

            public function receive(): ?array
            {
                return ['payload' => KafkaKeyPreservationTest::ENVELOPE_JSON, 'headers' => [], 'key' => $this->key];
            }

            public function commit(): void
            {
            }
        };

        $message = (new KafkaConsumer($client))->receive();

        $this->assertInstanceOf(HasRecordKey::class, $message);
        $this->assertSame($key, $message->recordKey());
    }

    public function test_a_consumer_client_without_a_key_entry_yields_a_null_key(): void
    {
        $client = new class implements KafkaConsumerClient {
            public function receive(): ?array
            {
                return ['payload' => KafkaKeyPreservationTest::ENVELOPE_JSON, 'headers' => []];
            }

            public function commit(): void
            {
            }
        };

        $this->assertNull((new KafkaConsumer($client))->receive()?->recordKey());
    }

    #[DataProvider('keys')]
    public function test_a_gate_rejected_record_keeps_its_key_to_the_dlq(?string $key): void
    {
        $client = new class($key) implements KafkaConsumerClient {
            public function __construct(private readonly ?string $key)
            {
            }

            public function receive(): ?array
            {
                return ['payload' => KafkaKeyPreservationTest::ENVELOPE_JSON, 'headers' => ['bq-schema-version' => 'x'], 'key' => $this->key];
            }

            public function commit(): void
            {
            }
        };
        $producer = $this->keyedProducer();

        $message = (new KafkaConsumer($client))->receive();
        $this->assertNotNull($message);
        // The gate-rejected envelope has no meta.queue: the queue is the caller's (the consumer's topic).
        (new DeadLetterPublisher(new KafkaTransport($producer)))->publish($message, 'unknown_urn', null, null, 'orders');

        $this->assertCarriesKey($producer->records, 'orders.dlq', $key);
    }

    #[DataProvider('keys')]
    public function test_a_poison_record_keeps_its_key_to_the_dlq(?string $key): void
    {
        $producer = $this->keyedProducer();
        $raw = '{not json';
        $message = new KafkaMessage(EnvelopeCodec::decode($raw, static function (): void {}), [], $raw, null, $key);

        (new DeadLetterPublisher(new KafkaTransport($producer)))->publish($message, 'poison', null, null, 'orders');

        $this->assertCarriesKey($producer->records, 'orders.dlq', $key);
    }

    public function test_a_producer_without_key_support_keeps_todays_behaviour(): void
    {
        $producer = new RecordingPlainProducer();

        (new KafkaRetryRouter($producer))->route($this->message('order-1042'));

        $this->assertSame(['orders.retry.1'], $producer->topics);
    }

    public function test_a_plain_publish_is_keyless(): void
    {
        $producer = $this->keyedProducer();

        (new KafkaTransport($producer))->publish(self::ENVELOPE, 'orders');

        $this->assertCarriesKey($producer->records, 'orders', null);
    }

    public const ENVELOPE_JSON = self::ENVELOPE;

    public static function stopAfter(ScriptedRetryClient $client): bool
    {
        return $client->received;
    }
}

final class RecordingKeyedProducer implements KeyedKafkaProducer
{
    /** @var array<int, array{topic: string, key: string|null, keyed: bool}> */
    public array $records = [];

    public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void
    {
        $this->records[] = ['topic' => $topic, 'key' => null, 'keyed' => false];
    }

    public function produceKeyed(string $topic, string $payload, array $headers, ?int $timestampMs, ?string $key): void
    {
        $this->records[] = ['topic' => $topic, 'key' => $key, 'keyed' => true];
    }
}

final class RecordingPlainProducer implements KafkaProducer
{
    /** @var list<string> */
    public array $topics = [];

    public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void
    {
        $this->topics[] = $topic;
    }
}

final class ScriptedRetryClient implements KafkaRetryConsumerClient
{
    public bool $received = false;

    public int $commits = 0;

    /**
     * @param  array{payload: string, headers: array<string, string>, key?: string|null}  $record
     */
    public function __construct(private readonly array $record)
    {
    }

    public function receive(): ?array
    {
        $this->received = true;

        return $this->record;
    }

    public function poll(int $timeoutMs): void
    {
    }

    public function commit(): void
    {
        $this->commits++;
    }
}
