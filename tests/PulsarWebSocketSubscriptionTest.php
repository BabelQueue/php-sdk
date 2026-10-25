<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\Dispatcher;
use BabelQueue\Contracts\Transport;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarConsumerDescriptor;
use BabelQueue\Transport\PulsarServiceEndpoint;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use BabelQueue\Transport\PulsarWebSocketSubscription;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The SDK's own Pulsar WebSocket consumer client, against the loopback mini broker
 * (tests/Support/pulsar_ws_server.php): plain `new PulsarConsumer($subscription)` needs nothing more and the
 * `maxAttempts` cap holds on an Exclusive subscription (ADR-0036 decision 1).
 */
final class PulsarWebSocketSubscriptionTest extends TestCase
{
    private const TOPIC = 'persistent://acme/ns1/orders';

    private const BODY = '{"job":"urn:babel:orders:created","trace_id":"t-1","data":{"order_id":7},'
        . '"meta":{"id":"m-1","queue":"orders","lang":"php","schema_version":1,"created_at":1},"attempts":0}';

    /** @var resource|null */
    private $process = null;

    private string $record = '';

    private string $seed = '';

    protected function setUp(): void
    {
        EnvelopeCodec::setWarningHandler(static function (): void {
        });
    }

    protected function tearDown(): void
    {
        EnvelopeCodec::setWarningHandler(null);
        if ($this->process !== null) {
            proc_terminate($this->process);
            proc_close($this->process);
            $this->process = null;
        }
        foreach ([$this->record, $this->record . '.consumer', $this->seed] as $file) {
            if ($file !== '' && is_file($file)) {
                unlink($file);
            }
        }
    }

    private function startServer(string $subscriptionType = 'Exclusive', bool $seeded = true): string
    {
        $base = sys_get_temp_dir() . '/bq-pulsar-sub-' . bin2hex(random_bytes(6));
        $this->record = $base . '.jsonl';
        $this->seed = $base . '.seed';
        file_put_contents($this->seed, $seeded ? json_encode(['payload' => self::BODY, 'properties' => ['bq-job' => 'urn:babel:orders:created']]) . "\n" : '');

        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/Support/pulsar_ws_server.php', $this->record, 'ok', $subscriptionType, $this->seed],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        $this->assertIsResource($process);
        $this->process = $process;
        $port = fgets($pipes[1]);
        if ($port === false || (int) $port === 0) {
            $this->markTestSkipped('cannot start the loopback Pulsar stand-in: ' . stream_get_contents($pipes[2]));
        }

        return 'http://127.0.0.1:' . (int) $port;
    }

    /** @return list<array<string, mixed>> */
    private function lines(string $file): array
    {
        return is_file($file)
            ? array_map(static fn (string $l): array => json_decode($l, true, 512, JSON_THROW_ON_ERROR), array_values(array_filter(explode("\n", (string) file_get_contents($file)))))
            : [];
    }

    /** @return list<string> */
    private function consumerEvents(): array
    {
        return array_map(static fn (array $l): string => (string) $l['event'], $this->lines($this->record . '.consumer'));
    }

    private function subscription(string $url, string $type = 'Exclusive', ?string $token = null): PulsarWebSocketSubscription
    {
        return new PulsarWebSocketSubscription($url, self::TOPIC, 'my-sub', $type, $token, 0.3, 5.0);
    }

    public function test_it_is_a_consumer_client_a_descriptor_and_a_service_endpoint(): void
    {
        $sub = new PulsarWebSocketSubscription('https://pulsar.example:8443/', self::TOPIC, 'my-sub', 'key_shared', 'tok');

        $this->assertInstanceOf(PulsarWebSocketConsumerClient::class, $sub);
        $this->assertInstanceOf(PulsarConsumerDescriptor::class, $sub);
        $this->assertInstanceOf(PulsarServiceEndpoint::class, $sub);
        $this->assertSame(self::TOPIC, $sub->topic());
        $this->assertSame('Key_Shared', $sub->subscriptionType());
        $this->assertSame('https://pulsar.example:8443/', $sub->serviceUrl());
        $this->assertSame('tok', $sub->authToken());
    }

    public function test_the_consumer_path_is_the_subscription_endpoint(): void
    {
        $sub = new PulsarWebSocketSubscription('wss://gw/pulsar', 'persistent://t/n/a b', 'sub 1', 'Shared', null, 1.0, 10.0, 500);

        $this->assertSame('/pulsar/ws/v2/consumer/persistent/t/n/a%20b/sub%201?subscriptionType=Shared&receiverQueueSize=500', $sub->consumerPath());
    }

    public function test_invalid_arguments_are_refused(): void
    {
        foreach ([
            ['tcp://h:1', self::TOPIC, 's', 'Exclusive'],
            ['http://h:1', 'orders', 's', 'Exclusive'],
            ['http://h:1', self::TOPIC, '', 'Exclusive'],
            ['http://h:1', self::TOPIC, 's', 'Durable'],
        ] as [$url, $topic, $name, $type]) {
            try {
                new PulsarWebSocketSubscription($url, $topic, $name, $type);
                $this->fail('must be refused: ' . $url . ' ' . $topic . ' ' . $type);
            } catch (\InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function test_it_receives_decodes_and_acknowledges_a_message(): void
    {
        $sub = $this->subscription($this->startServer(), 'Exclusive', 'tok-9');

        $frame = $sub->receive();

        $this->assertNotNull($frame);
        $this->assertSame(self::BODY, $frame['payload'], 'the base64 payload is decoded');
        $this->assertSame(['bq-job' => 'urn:babel:orders:created'], $frame['properties']);
        $this->assertSame(0, $frame['redeliveryCount']);
        $this->assertSame(self::TOPIC, $frame['topic']);

        $sub->acknowledge($frame['messageId']);
        $this->assertNull($sub->receive(), 'nothing else arrives within the read window');

        $open = $this->lines($this->record . '.consumer')[0];
        $this->assertSame('/ws/v2/consumer/persistent/acme/ns1/orders/my-sub?subscriptionType=Exclusive', $open['path']);
        $this->assertSame('Bearer tok-9', $open['authorization']);
        $this->assertSame(['open', 'ack'], $this->consumerEvents());
        $sub->close();
    }

    public function test_a_negative_acknowledgement_redelivers_and_only_shared_advances_the_count(): void
    {
        foreach (['Exclusive' => 0, 'Failover' => 0, 'Shared' => 1, 'Key_Shared' => 1] as $type => $expected) {
            $this->tearDown();
            $sub = $this->subscription($this->startServer($type), $type);

            $first = $sub->receive();
            $this->assertNotNull($first);
            $sub->negativeAcknowledge($first['messageId']);
            $again = $sub->receive();

            $this->assertNotNull($again, $type);
            $this->assertSame($expected, $again['redeliveryCount'], $type);
            $this->assertContains('nack', $this->consumerEvents(), $type);
            $sub->close();
        }
    }

    public function test_acknowledging_without_a_connection_throws(): void
    {
        $this->expectException(BabelQueueException::class);

        (new PulsarWebSocketSubscription('http://127.0.0.1:1', self::TOPIC, 's'))->acknowledge('x');
    }

    public function test_an_unreachable_service_throws_from_receive(): void
    {
        $this->expectException(BabelQueueException::class);
        $this->expectExceptionMessage('Cannot connect');

        (new PulsarWebSocketSubscription('http://127.0.0.1:1', self::TOPIC, 's', 'Exclusive', null, 0.1, 1.0))->receive();
    }

    public function test_plain_new_pulsar_consumer_holds_the_cap_on_an_exclusive_subscription(): void
    {
        $sub = $this->subscription($this->startServer('Exclusive'), 'Exclusive', 'tok-2');

        $dlq = [];
        $calls = 0;
        $dispatcher = (new Dispatcher(deadLetters: new DeadLetterPublisher(new class($dlq) implements Transport {
            /** @param list<array{0: string, 1: string}> $dlq */
            public function __construct(private array &$dlq)
            {
            }

            public function publish(string $payload, ?string $queue = null): ?string
            {
                $this->dlq[] = [$payload, (string) $queue];

                return null;
            }
        })))->on('urn:babel:orders:created', function () use (&$calls): void {
            $calls++;

            throw new RuntimeException('boom');
        });

        $consumer = new PulsarConsumer($sub); // nothing else: no producer, no type, no URL, no tenant
        $polls = 0;
        $consumer->consume($dispatcher, function () use (&$dlq, &$polls): bool {
            return $dlq !== [] || ++$polls > 60;
        });

        $this->assertSame(3, $calls, 'the handler ran exactly maxAttempts times');
        $this->assertCount(1, $dlq);
        $this->assertSame('orders.dlq', $dlq[0][1]);
        $record = json_decode($dlq[0][0], true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('failed', $record['dead_letter']['reason']);
        $this->assertSame(3, $record['dead_letter']['attempts']);

        $this->assertNotContains('nack', $this->consumerEvents(), 'never a nack: it would not advance the count on Exclusive');
        $producerFrames = $this->lines($this->record);
        $this->assertCount(2, $producerFrames, 'two re-publishes: bq-attempts 1 and 2');
        $this->assertSame('/ws/v2/producer/persistent/acme/ns1/orders', $producerFrames[0]['path']);
        $this->assertSame('Bearer tok-2', $producerFrames[0]['authorization'], 'the derived producer carries the subscription token');
        $this->assertSame(['1', '2'], array_map(static fn (array $f): string => $f['frame']['properties']['bq-attempts'], $producerFrames));
        $sub->close();
    }
}
