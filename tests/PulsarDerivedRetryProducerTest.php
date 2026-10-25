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
use BabelQueue\Transport\PulsarWebSocketProducer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * `new PulsarConsumer($client)` must not break on a client that only reads (ADR-0036 decision 1, user decision
 * 2026-10-25): the retry producer is derived from the client's service URL, the producer endpoint being the same host
 * (`/ws/v2/producer/persistent/<tenant>/<namespace>/<topic>`, tenant and namespace from the consumer's real topic).
 * The WebSocket exchange is proven against a loopback stand-in server (tests/Support/pulsar_ws_server.php).
 */
final class PulsarDerivedRetryProducerTest extends TestCase
{
    private const BODY = '{"job":"urn:babel:orders:created","trace_id":"t-1","data":{"order_id":7},'
        . '"meta":{"id":"m-1","queue":"orders","lang":"php","schema_version":1,"created_at":1},"attempts":0}';

    /** @var resource|null */
    private $process = null;

    private string $record = '';

    private int $port = 0;

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
        if ($this->record !== '' && is_file($this->record)) {
            unlink($this->record);
        }
    }

    private function startServer(string $mode = 'ok'): string
    {
        $this->record = sys_get_temp_dir() . '/bq-pulsar-ws-' . bin2hex(random_bytes(6)) . '.jsonl';
        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/Support/pulsar_ws_server.php', $this->record, $mode],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        $this->assertIsResource($process);
        $this->process = $process;
        $port = fgets($pipes[1]);
        if ($port === false || (int) $port === 0) {
            $this->markTestSkipped('cannot start the loopback Pulsar WebSocket stand-in: ' . stream_get_contents($pipes[2]));
        }
        $this->port = (int) $port;

        return 'http://127.0.0.1:' . $this->port;
    }

    /** @return list<array{path: string, authorization: ?string, frame: array<string, mixed>}> */
    private function frames(): array
    {
        if (! is_file($this->record)) {
            return [];
        }

        return array_map(
            static fn (string $line): array => json_decode($line, true, 512, JSON_THROW_ON_ERROR),
            array_values(array_filter(explode("\n", (string) file_get_contents($this->record)))),
        );
    }

    // -- the producer ---------------------------------------------------------------------------------

    public function test_the_producer_publishes_to_the_derived_endpoint_with_base64_payload_and_properties(): void
    {
        $url = $this->startServer();

        (new PulsarWebSocketProducer($url, 'secret-token'))->publish(
            'persistent://acme/ns1/orders',
            self::BODY,
            ['bq-job' => 'urn:babel:orders:created', 'bq-attempts' => '2'],
        );

        $frames = $this->frames();
        $this->assertCount(1, $frames);
        $this->assertSame('/ws/v2/producer/persistent/acme/ns1/orders', $frames[0]['path']);
        $this->assertSame('Bearer secret-token', $frames[0]['authorization']);
        $this->assertSame(self::BODY, base64_decode($frames[0]['frame']['payload'], true));
        $this->assertSame(['bq-job' => 'urn:babel:orders:created', 'bq-attempts' => '2'], $frames[0]['frame']['properties']);
        $this->assertArrayNotHasKey('eventTime', $frames[0]['frame']);
    }

    public function test_the_producer_answers_a_ping_and_reads_a_fragmented_reply(): void
    {
        $url = $this->startServer('ping_fragment');

        (new PulsarWebSocketProducer($url))->publish('persistent://public/default/orders', self::BODY, []);

        $this->assertCount(1, $this->frames());
    }

    #[DataProvider('failingServers')]
    public function test_a_failed_publish_throws_and_is_never_silent(string $mode, string $message): void
    {
        $url = $this->startServer($mode);

        $this->expectException(BabelQueueException::class);
        $this->expectExceptionMessage($message);

        (new PulsarWebSocketProducer($url, null, 3.0))->publish('persistent://public/default/orders', self::BODY, []);
    }

    /** @return iterable<string, array{0: string, 1: string}> */
    public static function failingServers(): iterable
    {
        yield 'a non-ok result' => ['reject', 'Topic not found'];
        yield 'a refused upgrade' => ['refuse', 'upgrade was refused'];
        yield 'a wrong Sec-WebSocket-Accept' => ['bad_accept', 'Sec-WebSocket-Accept'];
        yield 'a close before the ack' => ['close', 'closed the connection'];
    }

    public function test_an_unreachable_service_throws(): void
    {
        $this->expectException(BabelQueueException::class);
        $this->expectExceptionMessage('Cannot connect');

        (new PulsarWebSocketProducer('http://127.0.0.1:1', null, 1.0))->publish('persistent://public/default/orders', self::BODY, []);
    }

    public function test_the_producer_needs_a_full_topic_and_a_ws_or_http_url(): void
    {
        try {
            (new PulsarWebSocketProducer('ws://127.0.0.1:9'))->publish('orders', self::BODY, []);
            $this->fail('a local name has no tenant: it must be refused, not guessed');
        } catch (BabelQueueException $e) {
            $this->assertStringContainsString('not a full topic name', $e->getMessage());
        }

        $this->expectException(\InvalidArgumentException::class);
        new PulsarWebSocketProducer('tcp://host:1');
    }

    public function test_the_endpoint_path_keeps_a_proxy_prefix_and_encodes_the_topic(): void
    {
        $producer = new PulsarWebSocketProducer('https://gw.example/pulsar/');

        $this->assertSame(
            '/pulsar/ws/v2/producer/persistent/t/ns/a%20b',
            $producer->producerPath('persistent://t/ns/a b'),
        );
        $this->assertSame(
            '/pulsar/ws/v2/producer/non-persistent/t/ns/x',
            $producer->producerPath('non-persistent://t/ns/x'),
        );
    }

    // -- the consumer derives it ----------------------------------------------------------------------

    public function test_a_client_that_only_reads_but_reports_its_service_url_gets_a_derived_retry_producer(): void
    {
        $url = $this->startServer();
        $client = new EndpointConsumerClient($url, 'Exclusive', 'persistent://acme/ns1/orders');
        $client->seed(self::BODY);

        $consumer = new PulsarConsumer($client); // no retryProducer, no subscriptionType: nothing breaks
        $message = $consumer->receive();
        $this->assertNotNull($message);

        $consumer->retry($message);

        $frames = $this->frames();
        $this->assertCount(1, $frames);
        $this->assertSame('/ws/v2/producer/persistent/acme/ns1/orders', $frames[0]['path'], 'tenant/namespace come from the consumer\'s real topic');
        $this->assertSame('1', $frames[0]['frame']['properties']['bq-attempts']);
        $this->assertSame(['mid-1'], $client->acked);
        $this->assertSame([], $client->nacked);
    }

    public function test_on_exclusive_the_default_setup_reaches_the_cap_and_dead_letters(): void
    {
        $url = $this->startServer();
        $client = new EndpointConsumerClient($url, 'Exclusive', 'persistent://acme/ns1/orders', $this->record);
        $client->seed(self::BODY);

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

        $polls = 0;
        (new PulsarConsumer($client))->consume($dispatcher, function () use ($client, &$polls): bool {
            return $client->isDrained() || ++$polls > 30;
        });

        $this->assertSame(3, $calls, 'the cap holds on an Exclusive subscription with the default setup');
        $this->assertCount(1, $dlq);
        $this->assertSame('orders.dlq', $dlq[0][1]);
        $this->assertSame([], $client->nacked, 'never a nack: a nack does not advance the count on Exclusive');
        $this->assertSame(['1', '2'], array_map(static fn (array $f): string => $f['frame']['properties']['bq-attempts'], $this->frames()));
    }

    public function test_an_explicit_retry_producer_wins_over_the_derived_one(): void
    {
        $url = $this->startServer();
        $client = new EndpointConsumerClient($url, 'Exclusive', 'persistent://acme/ns1/orders');
        $client->seed(self::BODY);
        $explicit = new class implements \BabelQueue\Transport\PulsarWebSocketClient {
            /** @var list<string> */
            public array $topics = [];

            public function publish(string $topic, string $payload, array $properties): void
            {
                $this->topics[] = $topic;
            }
        };

        $consumer = new PulsarConsumer($client, null, null, $explicit);
        $message = $consumer->receive();
        $this->assertNotNull($message);
        $consumer->retry($message);

        $this->assertSame(['persistent://acme/ns1/orders'], $explicit->topics);
        $this->assertSame([], $this->frames(), 'the derived producer was not used');
    }

    public function test_an_explicit_service_url_derives_the_producer_for_a_plain_consumer_client(): void
    {
        $url = $this->startServer();
        $client = new EndpointConsumerClient($url, 'Exclusive', 'persistent://acme/ns1/orders', null, false);
        $client->seed(self::BODY);

        $consumer = new PulsarConsumer($client, serviceUrl: $url);
        $message = $consumer->receive();
        $this->assertNotNull($message);
        $consumer->retry($message);

        $this->assertCount(1, $this->frames());
    }

    public function test_a_failing_derived_publish_nacks_the_original(): void
    {
        $client = new EndpointConsumerClient('http://127.0.0.1:1', 'Exclusive', 'persistent://acme/ns1/orders');
        $client->seed(self::BODY);
        $consumer = new PulsarConsumer($client);
        $message = $consumer->receive();
        $this->assertNotNull($message);

        $log = ini_set('error_log', '/dev/null');
        try {
            $consumer->retry($message);
        } finally {
            ini_set('error_log', (string) $log);
        }

        $this->assertSame(['mid-1'], $client->nacked, 'the original is not lost: it is negatively acknowledged');
        $this->assertSame([], $client->acked);
    }

    public function test_a_local_topic_name_is_not_guessed_into_a_tenant_even_with_a_service_url(): void
    {
        $url = $this->startServer();
        $client = new EndpointConsumerClient($url, 'Exclusive', 'orders'); // local name only

        try {
            new PulsarConsumer($client);
            $this->fail('the retry could only throw: refused at setup');
        } catch (BabelQueueException $e) {
            $this->assertStringContainsString('full topic', $e->getMessage());
        }

        $this->assertSame([], $this->frames(), 'nothing was published to a guessed tenant');
    }

    public function test_the_service_auth_token_is_sent_by_the_derived_producer(): void
    {
        $url = $this->startServer();
        $client = new EndpointConsumerClient($url, 'Exclusive', 'persistent://acme/ns1/orders', null, false);
        $client->seed(self::BODY);

        $consumer = new PulsarConsumer($client, serviceUrl: $url, serviceAuthToken: 'tok-1');
        $message = $consumer->receive();
        $this->assertNotNull($message);
        $consumer->retry($message);

        $this->assertSame('Bearer tok-1', $this->frames()[0]['authorization']);
    }

    public function test_fail_closed_remains_only_when_nothing_can_be_derived(): void
    {
        // A service URL the client does not know (empty), no producer, type unknown/Exclusive: still a setup error.
        foreach (['', null] as $unknownUrl) {
            try {
                new PulsarConsumer(new EndpointConsumerClient($unknownUrl ?? '', 'Exclusive', 'persistent://a/b/orders'));
                $this->fail('an unknown service URL cannot derive a producer');
            } catch (BabelQueueException $e) {
                $this->assertStringContainsString(PulsarServiceEndpoint::class, $e->getMessage());
            }
        }

        // ... and a declared Shared subscription is still allowed to nack.
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer(new EndpointConsumerClient('', 'Shared', 'persistent://a/b/orders')));
    }
}

/** A consumer client that reports its service URL (and optionally its topic/subscription) but cannot publish. */
final class EndpointConsumerClient implements PulsarWebSocketConsumerClient, PulsarConsumerDescriptor, PulsarServiceEndpoint
{
    /** @var list<array{messageId: string, payload: string, properties: array<string, string>, redeliveryCount: int}> */
    private array $queue = [];

    /** @var array<string, array{messageId: string, payload: string, properties: array<string, string>, redeliveryCount: int}> */
    private array $inFlight = [];

    private int $next = 1;

    private int $consumedFrames = 0;

    /** @var list<string> */
    public array $acked = [];

    /** @var list<string> */
    public array $nacked = [];

    /**
     * @param  string|null  $recordFile  a stand-in server's record: frames published to it come back as new deliveries
     *         (the broker side of the loop), as on a real Pulsar topic
     */
    public function __construct(
        private readonly string $serviceUrl,
        private readonly string $subscription,
        private readonly string $topic,
        private readonly ?string $recordFile = null,
        private readonly bool $reportsUrl = true,
    ) {
    }

    public function seed(string $payload): void
    {
        $this->queue[] = ['messageId' => 'mid-' . $this->next++, 'payload' => $payload, 'properties' => [], 'redeliveryCount' => 0];
    }

    public function isDrained(): bool
    {
        $this->pullPublished();

        return $this->queue === [] && $this->inFlight === [];
    }

    private function pullPublished(): void
    {
        if ($this->recordFile === null || ! is_file($this->recordFile)) {
            return;
        }

        $lines = array_values(array_filter(explode("\n", (string) file_get_contents($this->recordFile))));
        for (; $this->consumedFrames < count($lines); $this->consumedFrames++) {
            $entry = json_decode($lines[$this->consumedFrames], true);
            $this->queue[] = [
                'messageId' => 'mid-' . $this->next++,
                'payload' => (string) base64_decode($entry['frame']['payload'], true),
                'properties' => $entry['frame']['properties'],
                'redeliveryCount' => 0, // Exclusive: a redelivery would not advance it anyway
            ];
        }
    }

    public function receive(): ?array
    {
        $this->pullPublished();
        $frame = array_shift($this->queue);
        if ($frame === null) {
            return null;
        }
        $this->inFlight[$frame['messageId']] = $frame;

        return $frame;
    }

    public function acknowledge(string $messageId): void
    {
        $this->acked[] = $messageId;
        unset($this->inFlight[$messageId]);
    }

    public function negativeAcknowledge(string $messageId): void
    {
        $this->nacked[] = $messageId;
        $frame = $this->inFlight[$messageId];
        unset($this->inFlight[$messageId]);
        $this->queue[] = $frame; // Exclusive / Failover: the redelivery count is NOT advanced
    }

    public function topic(): string
    {
        return $this->topic;
    }

    public function subscriptionType(): ?string
    {
        return $this->subscription;
    }

    public function serviceUrl(): string
    {
        return $this->reportsUrl ? $this->serviceUrl : '';
    }

    public function authToken(): ?string
    {
        return null;
    }
}
