<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Redrive\KafkaRedriveIO;
use BabelQueue\Redrive\Redrive;
use BabelQueue\Redrive\RedriveOptions;
use BabelQueue\Tests\Support\RdKafkaConsumerAdapter;
use BabelQueue\Tests\Support\RdKafkaProducerAdapter;
use BabelQueue\Transport\KafkaConsumer;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\KafkaRetryRouter;
use BabelQueue\Transport\KafkaTransport;
use BabelQueue\Transport\KeyedKafkaProducer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * Live Apache Kafka tests (`ext-rdkafka`, ADR-0019) — the record key survives the SDK-owned
 * retry -> DLQ -> redrive chain **as the broker stores it**, and a failed restore leaves the whole DLQ
 * batch to be delivered again. Skipped unless `BQ_INTEGRATION=1` and `BQ_KAFKA_BROKERS` are set (the CI
 * `integration-live` job sets them); excluded from the unit job via `--exclude-group integration`.
 */
#[Group('integration')]
final class KafkaLiveIntegrationTest extends TestCase
{
    private string $brokers = '';

    private string $id = '';

    protected function setUp(): void
    {
        $brokers = getenv('BQ_KAFKA_BROKERS');
        if (getenv('BQ_INTEGRATION') !== '1' || $brokers === false || $brokers === '') {
            self::markTestSkipped('live Kafka test: set BQ_INTEGRATION=1 and BQ_KAFKA_BROKERS=host:port');
        }
        if (! extension_loaded('rdkafka')) {
            self::fail('BQ_INTEGRATION is set but ext-rdkafka is not loaded');
        }

        $this->brokers = $brokers;
        $this->id = bin2hex(random_bytes(4));
    }

    /** @return array<string, array{0: string|null}> */
    public static function keys(): array
    {
        return ['keyed' => ['order-1042'], 'empty key' => [''], 'null key' => [null]];
    }

    private function envelope(string $queue, string $messageId): string
    {
        return '{"job":"urn:babel:orders:created","trace_id":"trace-' . $messageId . '","data":{"order_id":1042},'
            . '"meta":{"id":"' . $messageId . '","queue":"' . $queue . '","lang":"php","schema_version":1,'
            . '"created_at":' . (int) round(microtime(true) * 1000) . '},"attempts":0}';
    }

    /**
     * Every record currently in $topic, oldest first, read with a throw-away consumer group.
     *
     * @return list<array{payload: string, headers: array<string, string>, key: string|null}>
     */
    private function readAll(string $topic): array
    {
        $client = new RdKafkaConsumerAdapter($this->brokers, "bq-php-it-{$this->id}-read-" . bin2hex(random_bytes(3)), $topic, 3000);
        $records = [];
        while (($record = $client->receive()) !== null) {
            $records[] = ['payload' => $record['payload'], 'headers' => $record['headers'], 'key' => $record['key'] ?? null];
        }

        return $records;
    }

    #[DataProvider('keys')]
    public function testKeySurvivesRetryDlqAndRedriveAsTheBrokerStoresIt(?string $key): void
    {
        $work = "bq-php-it-{$this->id}-orders";
        $producer = new RdKafkaProducerAdapter($this->brokers);

        // 1. produce a keyed record to the work topic
        (new KafkaTransport($producer))->publishKeyed($this->envelope($work, "msg-{$this->id}"), $work, $key);

        // 2. the work-topic handler fails: the record is routed to the first retry topic
        $router = new KafkaRetryRouter($producer, [1], 2);
        $workConsumer = new KafkaConsumer(new RdKafkaConsumerAdapter($this->brokers, "bq-php-it-{$this->id}-work", $work));
        $message = $this->receiveMessage($workConsumer);
        self::assertSame($key, $message->recordKey(), 'work topic');
        self::assertTrue($router->route($message, new RuntimeException('boom')), 'first failure is retried');

        // 3. the retry-topic record fails again: attempts are exhausted and it is dead-lettered
        $retryConsumer = new KafkaConsumer(new RdKafkaConsumerAdapter($this->brokers, "bq-php-it-{$this->id}-retry", "{$work}.retry.1"));
        $retried = $this->receiveMessage($retryConsumer);
        self::assertSame($key, $retried->recordKey(), 'retry topic');
        self::assertSame(1, $retried->attempts());
        self::assertFalse($router->route($retried, new RuntimeException('boom again')), 'exhausted: dead-lettered');

        // 4. the DLQ record carries the same key
        $dlq = $this->readAll("{$work}.dlq");
        self::assertCount(1, $dlq);
        self::assertSame($key, $dlq[0]['key'], 'dlq topic');

        // 5. redrive: the record goes back to the work topic with the key untouched
        $client = new RdKafkaConsumerAdapter($this->brokers, "bq-php-it-{$this->id}-redrive", null, 3000);
        $result = Redrive::run(new KafkaRedriveIO($client, $producer), "{$work}.dlq");
        self::assertSame(1, $result->redriven);

        // 6. read back from the broker: the original and the redriven record, same key byte for byte
        $records = $this->readAll($work);
        self::assertCount(2, $records);
        foreach ($records as $i => $record) {
            self::assertSame($key, $record['key'], "work topic record {$i}");
        }
        self::assertSame('0', $records[1]['headers']['bq-attempts'], 'the redriven record restarts at attempts 0');
    }

    public function testFailedRestoreRewindsTheBatchAndLosesNothing(): void
    {
        $dlq = "bq-php-it-{$this->id}-restore.dlq";
        $producer = new RdKafkaProducerAdapter($this->brokers);
        $keys = ['k0', '', null];
        $transport = new KafkaTransport($producer);
        foreach ($keys as $i => $key) {
            $transport->publishKeyed($this->envelope('bq-php-it-' . $this->id . '-restore', "msg-{$i}"), $dlq, $key);
        }

        // a producer that refuses every write to the DLQ topic: nothing can be restored
        $refusing = new class ($producer, $dlq) implements KeyedKafkaProducer {
            public function __construct(private readonly KeyedKafkaProducer $inner, private readonly string $deniedTopic)
            {
            }

            public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void
            {
                $this->produceKeyed($topic, $payload, $headers, $timestampMs, null);
            }

            public function produceKeyed(string $topic, string $payload, array $headers, ?int $timestampMs, ?string $key): void
            {
                if ($topic === $this->deniedTopic) {
                    throw new RuntimeException("write to {$topic} refused");
                }
                $this->inner->produceKeyed($topic, $payload, $headers, $timestampMs, $key);
            }
        };

        $client = new RdKafkaConsumerAdapter($this->brokers, "bq-php-it-{$this->id}-restore-group", null, 3000);
        $nothingSelected = new RedriveOptions(select: static fn (array $envelope): bool => false);

        try {
            Redrive::run(new KafkaRedriveIO($client, $refusing), $dlq, $nothingSelected);
            self::fail('the refused restore must surface');
        } catch (RuntimeException $e) {
            self::assertStringContainsString("write to {$dlq} refused", $e->getMessage());
        }

        // the same consumer was rewound: a second run (the DLQ accepts writes again) sees the whole batch
        $result = Redrive::run(new KafkaRedriveIO($client, $producer), $dlq, $nothingSelected);
        self::assertSame(3, $result->skipped);

        // 3 originals + 3 verbatim restores, in order, with every key as the broker stores it
        $records = $this->readAll($dlq);
        self::assertCount(6, $records);
        self::assertSame(
            ['k0', '', null, 'k0', '', null],
            array_map(static fn (array $r): ?string => $r['key'], $records),
        );
        for ($i = 0; $i < 3; $i++) {
            self::assertSame($records[$i]['payload'], $records[$i + 3]['payload'], 'restore is verbatim');
            self::assertSame($records[$i]['headers'], $records[$i + 3]['headers']);
        }
    }

    private function receiveMessage(KafkaConsumer $consumer): KafkaMessage
    {
        for ($attempt = 0; $attempt < 6; $attempt++) {
            $message = $consumer->receive();
            if ($message !== null) {
                return $message;
            }
        }

        self::fail('no record arrived from Kafka in time');
    }
}
