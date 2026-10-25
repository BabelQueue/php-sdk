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
use BabelQueue\Transport\PulsarWebSocketClient;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * ADR-0036 decision 1: the bounded retry is a re-publish (ack + `bq-attempts` + 1 to the same topic), so the
 * `maxAttempts` cap is reached whatever the subscription type. A negative acknowledgement advances Pulsar's
 * redelivery count on `Shared` / `Key_Shared` only; on `Exclusive` / `Failover` it does not.
 */
final class PulsarRetryRepublishTest extends TestCase
{
    private const BODY = '{"job":"urn:babel:orders:created","trace_id":"t-1","data":{"order_id":7},'
        . '"meta":{"id":"m-1","queue":"orders","lang":"php","schema_version":1,"created_at":1},"attempts":0}';

    protected function setUp(): void
    {
        EnvelopeCodec::setWarningHandler(static function (): void {
        });
    }

    protected function tearDown(): void
    {
        EnvelopeCodec::setWarningHandler(null);
    }

    /**
     * @param  list<array{0: string, 1: string}>  $dlq  filled with [payload, queue]
     */
    private function dlqPublisher(array &$dlq): DeadLetterPublisher
    {
        return new DeadLetterPublisher(new class($dlq) implements Transport {
            /** @param list<array{0: string, 1: string}> $dlq */
            public function __construct(private array &$dlq)
            {
            }

            public function publish(string $payload, ?string $queue = null): ?string
            {
                $this->dlq[] = [$payload, (string) $queue];

                return null;
            }
        });
    }

    private function consume(FakePulsarBroker $broker, Dispatcher $dispatcher, bool $withProducer = true, int $maxPolls = 50): void
    {
        // The broker is both the consumer and the producer seam, so it is the derived retry producer; the
        // consumer-only view hides the producer to exercise the nack-only fallback.
        $consumer = $withProducer
            ? new PulsarConsumer($broker, null, 'orders', topicFromFrame: true)
            : new PulsarConsumer(new ConsumerOnlyView($broker), null, 'orders', null, 'public', 'default', 'Shared');
        $polls = 0;
        $consumer->consume($dispatcher, function () use ($broker, &$polls, $maxPolls): bool {
            return $broker->isDrained() || ++$polls > $maxPolls;
        });
    }

    private function alwaysFailing(Dispatcher $dispatcher, int &$calls): Dispatcher
    {
        return $dispatcher->on('urn:babel:orders:created', function () use (&$calls): void {
            $calls++;

            throw new RuntimeException('boom');
        });
    }

    /** @return iterable<string, array{0: bool}> */
    public static function subscriptionTypes(): iterable
    {
        yield 'Exclusive / Failover (nack does not advance the count)' => [false];
        yield 'Shared / Key_Shared (nack advances the count)' => [true];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('subscriptionTypes')]
    public function test_a_failing_handler_reaches_the_cap_and_is_dead_lettered_on_every_subscription_type(bool $nackAdvancesCount): void
    {
        $broker = new FakePulsarBroker($nackAdvancesCount);
        $broker->seed(self::BODY);
        $dlq = [];
        $calls = 0;

        $this->consume($broker, $this->alwaysFailing(new Dispatcher(deadLetters: $this->dlqPublisher($dlq)), $calls));

        $this->assertSame(3, $calls, 'the handler runs exactly maxAttempts (3) times');
        $this->assertCount(1, $dlq);
        $this->assertSame('orders.dlq', $dlq[0][1]);
        $record = json_decode($dlq[0][0], true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('failed', $record['dead_letter']['reason']);
        $this->assertSame(3, $record['dead_letter']['attempts']);
        $this->assertTrue($broker->isDrained());
        $this->assertSame([], $broker->nacked, 'the retry never needed a negative acknowledgement');

        // two re-publishes to the same topic, bq-attempts 1 then 2, the body's attempts in sync
        $this->assertCount(2, $broker->published);
        $this->assertSame('persistent://public/default/orders', $broker->published[0]['topic']);
        $this->assertSame(['1', '2'], array_map(static fn (array $p): string => $p['properties']['bq-attempts'], $broker->published));
        $this->assertSame([1, 2], array_map(static fn (array $p): int => json_decode($p['payload'], true)['attempts'], $broker->published));
    }

    public function test_a_nack_only_retry_on_a_mis_declared_subscription_loops_past_the_cap(): void
    {
        // Why the constructor fails closed: a nack-only retry on what is really an Exclusive/Failover subscription
        // loops past the cap. Declaring Shared without a producer is the caller's explicit claim.
        $broker = new FakePulsarBroker(false);
        $broker->seed(self::BODY);
        $dlq = [];
        $calls = 0;

        $this->consume($broker, $this->alwaysFailing(new Dispatcher(deadLetters: $this->dlqPublisher($dlq)), $calls), false, 10);

        $this->assertGreaterThan(3, $calls);
        $this->assertSame([], $dlq);
    }

    public function test_without_a_publisher_the_message_is_acknowledged_and_dropped_at_the_cap_on_exclusive(): void
    {
        $broker = new FakePulsarBroker(false);
        $broker->seed(self::BODY);
        $calls = 0;
        $dropped = 0;

        $this->consume($broker, $this->alwaysFailing(new Dispatcher(onDrop: function () use (&$dropped): void {
            $dropped++;
        }), $calls));

        $this->assertSame(3, $calls);
        $this->assertSame(1, $dropped);
        $this->assertTrue($broker->isDrained());
    }

    public function test_a_poison_body_is_republished_with_its_exact_bytes_and_an_advancing_count(): void
    {
        $broker = new FakePulsarBroker(false);
        $broker->seed('{not json', ['bq-job' => 'urn:babel:orders:created']);
        $dropped = 0;
        $dispatcher = new Dispatcher('fail', 3, onDrop: function () use (&$dropped): void {
            $dropped++;
        });

        $this->consume($broker, $dispatcher);

        $this->assertCount(2, $broker->published);
        foreach ($broker->published as $copy) {
            $this->assertSame('{not json', $copy['payload'], 'the poison bytes are kept');
            $this->assertSame('urn:babel:orders:created', $copy['properties']['bq-job'], 'the original properties are carried');
        }
        $this->assertSame(['1', '2'], array_map(static fn (array $p): string => $p['properties']['bq-attempts'], $broker->published));
        $this->assertSame(1, $dropped, 'dropped at the third delivery');
        $this->assertTrue($broker->isDrained());
    }

    public function test_a_failed_republish_negatively_acknowledges_and_does_not_acknowledge(): void
    {
        $broker = new FakePulsarBroker(false);
        $broker->seed(self::BODY);
        $broker->failPublish = true;
        $calls = 0;
        $consumer = new PulsarConsumer($broker, null, 'orders', $broker, topicFromFrame: true);
        $dispatcher = $this->alwaysFailing(new Dispatcher(), $calls);

        $file = tempnam(sys_get_temp_dir(), 'bq-log');
        $previous = ini_set('error_log', (string) $file);
        try {
            $message = $consumer->receive();
            $this->assertNotNull($message);
            try {
                $dispatcher($message);
            } catch (RuntimeException) {
                $consumer->retry($message);
            }
        } finally {
            ini_set('error_log', (string) $previous);
        }

        $this->assertSame(['mid-1'], $broker->nacked);
        $this->assertSame([], $broker->acked);
        $this->assertStringContainsString('re-publish failed', (string) file_get_contents((string) $file));
        unlink((string) $file);
    }

    public function test_a_version_gated_delivery_keeps_its_gate_property_and_bytes_on_retry(): void
    {
        $broker = new FakePulsarBroker(false);
        $broker->seed(self::BODY, ['bq-schema-version' => '2']);

        $consumer = new PulsarConsumer($broker, null, 'orders', $broker, topicFromFrame: true);
        $message = $consumer->receive();
        $this->assertNotNull($message);
        $consumer->retry($message);

        $this->assertSame(self::BODY, $broker->published[0]['payload']);
        $this->assertSame('2', $broker->published[0]['properties']['bq-schema-version']);
        $this->assertSame('1', $broker->published[0]['properties']['bq-attempts']);
    }

    public function test_the_consumer_derives_the_producer_and_the_topic_from_its_own_client(): void
    {
        $broker = new FakePulsarBroker(false, 'persistent://acme/ns1/orders', 'Exclusive');
        $broker->seed(self::BODY);
        $dlq = [];
        $calls = 0;

        // No queue:, no retryProducer: both come from the client (it reads the topic and can publish).
        $consumer = new PulsarConsumer($broker, topicFromFrame: true);
        $dispatcher = $this->alwaysFailing(new Dispatcher(deadLetters: $this->dlqPublisher($dlq)), $calls);
        $polls = 0;
        $consumer->consume($dispatcher, function () use ($broker, &$polls): bool {
            return $broker->isDrained() || ++$polls > 50;
        });

        $this->assertSame(3, $calls);
        $this->assertSame('persistent://acme/ns1/orders', $broker->published[0]['topic'], 'the full topic of the client is used as-is');
        $this->assertSame('orders.dlq', $dlq[0][1], 'the queue is the local name of the client topic');
    }

    public function test_a_local_topic_name_is_completed_with_the_tenant_and_namespace(): void
    {
        $broker = new FakePulsarBroker(false, 'orders', 'Failover');
        $broker->frameTopic = null; // a local name only: the tenant and namespace must then be given
        $broker->seed(self::BODY);
        $consumer = new PulsarConsumer($broker, null, null, null, 'acme', 'ns1', topicFromFrame: true);

        $message = $consumer->receive();
        $this->assertNotNull($message);
        $this->assertSame('orders', $message->queue());
        $consumer->retry($message);

        $this->assertSame('persistent://acme/ns1/orders', $broker->published[0]['topic']);
    }

    public function test_explicit_arguments_win_over_the_derived_ones(): void
    {
        $broker = new FakePulsarBroker(false, 'persistent://acme/ns1/orders', 'Exclusive');
        $other = new FakePulsarBroker(false);
        $broker->seed(self::BODY);

        $consumer = new PulsarConsumer($broker, null, 'invoices', $other, topicFromFrame: true);
        $message = $consumer->receive();
        $this->assertNotNull($message);
        $this->assertSame('invoices', $message->queue());
        $consumer->retry($message);

        // `queue:` names the DLQ; the copy goes back to the topic the consumer really reads.
        $this->assertSame([], $broker->published);
        $this->assertSame('persistent://acme/ns1/orders', $other->published[0]['topic']);
    }

    public function test_an_exclusive_or_failover_subscription_without_a_retry_producer_is_a_setup_error(): void
    {
        foreach (['Exclusive', 'Failover', 'failover'] as $type) {
            try {
                new PulsarConsumer(new ConsumerOnlyView(new FakePulsarBroker(false, 'orders', $type)));
                $this->fail($type . ' without a producer must throw');
            } catch (BabelQueueException $e) {
                $this->assertStringContainsString('retry producer', $e->getMessage());
            }
        }

        // The type can also be passed when the client does not report it.
        $silent = new ConsumerOnlyView(new FakePulsarBroker(false));
        try {
            new PulsarConsumer($silent, null, 'orders', null, 'public', 'default', 'Exclusive');
            $this->fail('an explicit Exclusive without a producer must throw');
        } catch (BabelQueueException) {
            $this->addToAssertionCount(1);
        }

        // ... and an explicit or derived producer satisfies it.
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer($silent, null, 'orders', new FakePulsarBroker(false), 'public', 'default', 'Exclusive'));
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer(new FakePulsarBroker(false, 'orders', 'Exclusive'), topicFromFrame: true));
    }

    public function test_a_known_shared_subscription_without_a_producer_keeps_the_nack_path(): void
    {
        foreach (['Shared', 'Key_Shared', 'key_shared'] as $type) {
            $broker = new FakePulsarBroker(true, 'orders', $type);
            $broker->seed(self::BODY);
            $consumer = new PulsarConsumer(new ConsumerOnlyView($broker));
            $message = $consumer->receive();
            $this->assertNotNull($message);

            $consumer->retry($message);

            $this->assertSame(['mid-1'], $broker->nacked, $type);
            $this->assertSame([], $broker->published, $type);
        }
    }

    public function test_an_unknown_subscription_type_is_treated_as_exclusive_and_fails_closed(): void
    {
        // The client reports no type (null), nothing is passed, and no producer can be derived.
        try {
            new PulsarConsumer(new ConsumerOnlyView(new FakePulsarBroker(true, 'orders', null)));
            $this->fail('an unknown type without a producer must throw');
        } catch (BabelQueueException $e) {
            $this->assertStringContainsString('unknown (treated as Exclusive)', $e->getMessage());
            $this->assertStringContainsString('Shared / Key_Shared', $e->getMessage(), 'the message names the way out');
        }

        // A client that is not a descriptor at all is the same (the common legacy adapter).
        $bare = new class implements PulsarWebSocketConsumerClient {
            public function receive(): ?array
            {
                return null;
            }

            public function acknowledge(string $messageId): void
            {
            }

            public function negativeAcknowledge(string $messageId): void
            {
            }
        };
        try {
            new PulsarConsumer($bare, null, 'orders');
            $this->fail('a legacy adapter with no producer must throw');
        } catch (BabelQueueException) {
            $this->addToAssertionCount(1);
        }

        // Each way out works: declare Shared, or give / derive a producer.
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer($bare, null, 'orders', null, 'public', 'default', 'Shared'));
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer($bare, null, 'orders', new FakePulsarBroker(false), topicFromFrame: true));
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer(new FakePulsarBroker(false), topicFromFrame: true));
    }

    public function test_an_unknown_subscription_type_name_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new PulsarConsumer(new FakePulsarBroker(false), null, 'orders', null, 'public', 'default', 'Durable');
    }

    public function test_a_retry_whose_topic_is_unknown_fails_loudly_instead_of_nacking_unless_shared(): void
    {
        foreach (['Exclusive', null] as $type) { // unknown counts as Exclusive
            $broker = new FakePulsarBroker(false, null, $type); // can publish, but nobody knows the topic
            $broker->frameTopic = null;
            $broker->seed(self::BODY);
            $consumer = new PulsarConsumer($broker, topicFromFrame: true);
            $message = $consumer->receive();
            $this->assertNotNull($message);

            try {
                $consumer->retry($message);
                $this->fail('a retry with no topic must throw for ' . ($type ?? 'unknown'));
            } catch (BabelQueueException) {
                $this->assertSame([], $broker->nacked);
            }
        }

        // Shared: the nack path is fine without a topic.
        $shared = new FakePulsarBroker(true, null, 'Shared');
        $shared->frameTopic = null;
        $shared->seed(self::BODY);
        $consumer = new PulsarConsumer($shared);
        $message = $consumer->receive();
        $this->assertNotNull($message);
        $consumer->retry($message);
        $this->assertSame(['mid-1'], $shared->nacked);
    }

    public function test_a_full_frame_topic_in_another_tenant_is_where_the_copy_goes(): void
    {
        // An adapter that is not a descriptor, whose frames carry the full topic of a non-default tenant/namespace.
        $broker = new FakePulsarBroker(false);
        $broker->frameTopic = 'persistent://acme/prod/orders';
        $broker->seed(self::BODY);
        $consumer = new PulsarConsumer($broker, topicFromFrame: true); // no tenant:, no namespace:, no queue:
        $message = $consumer->receive();
        $this->assertNotNull($message);
        $this->assertSame('persistent://acme/prod/orders', $message->topic());

        $consumer->retry($message);

        $this->assertSame('persistent://acme/prod/orders', $broker->published[0]['topic'], 'never persistent://public/default/...');
        $this->assertSame(['mid-1'], $broker->acked);
    }

    public function test_a_producer_that_cannot_know_where_to_publish_is_refused_at_setup(): void
    {
        // A derived producer, an Exclusive-ish subscription, but a LOCAL topic name and no tenant / namespace and no
        // statement that frames carry the full topic: the first retry could only throw, so the constructor does.
        foreach ([
            static fn () => new PulsarConsumer(new FakePulsarBroker(false, 'orders', 'Exclusive')),
            static fn () => new PulsarConsumer(new FakePulsarBroker(false), null, 'orders'),
            static fn () => new PulsarConsumer(new FakePulsarBroker(false), null, 'orders', null, 'acme'),
        ] as $build) {
            try {
                $build();
                $this->fail('must be refused at setup');
            } catch (BabelQueueException $e) {
                $this->assertStringContainsString('topicFromFrame', $e->getMessage());
            }
        }

        // Each way out: a full descriptor topic, queue + tenant + namespace, or the explicit frame-topic declaration.
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer(new FakePulsarBroker(false, 'persistent://acme/ns/orders', 'Exclusive')));
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer(new FakePulsarBroker(false), null, 'orders', null, 'acme', 'ns'));
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer(new FakePulsarBroker(false), topicFromFrame: true));
        // ... and a known Shared subscription never needs the topic (a nack retries).
        $this->assertInstanceOf(PulsarConsumer::class, new PulsarConsumer(new FakePulsarBroker(true, 'orders', 'Shared')));
    }

    public function test_a_local_name_without_tenant_and_namespace_is_never_guessed(): void
    {
        $broker = new FakePulsarBroker(false);
        $broker->frameTopic = null;
        $broker->seed(self::BODY);
        $consumer = new PulsarConsumer($broker, null, 'orders', topicFromFrame: true); // Exclusive-ish (unknown type), producer derived

        $message = $consumer->receive();
        $this->assertNotNull($message);

        try {
            $consumer->retry($message);
            $this->fail('the copy must not go to a guessed public/default');
        } catch (BabelQueueException $e) {
            $this->assertStringContainsString('tenant', $e->getMessage());
        }
        $this->assertSame([], $broker->published);
        $this->assertSame([], $broker->acked, 'the original is not acknowledged');
    }

    public function test_a_failed_dead_letter_write_nacks_and_does_not_re_publish_a_retry_copy(): void
    {
        $broker = new FakePulsarBroker(false);
        $broker->seed(self::BODY);
        $calls = 0;
        $failing = new DeadLetterPublisher(new class implements Transport {
            public function publish(string $payload, ?string $queue = null): ?string
            {
                throw new RuntimeException('dlq down');
            }
        });
        $dispatcher = $this->alwaysFailing(new Dispatcher('fail', 1, $failing), $calls); // cap 1: the first failure dead-letters

        $consumer = new PulsarConsumer($broker, null, 'orders', topicFromFrame: true);
        $polls = 0;
        $consumer->consume($dispatcher, function () use (&$polls): bool {
            return ++$polls > 1; // one delivery, then stop
        });

        $this->assertSame([], $broker->published, 'no retry copy for a failed DLQ write');
        $this->assertSame([], $broker->acked);
        $this->assertSame(['mid-1'], $broker->nacked, 'the original is negatively acknowledged so the write is tried again');
    }

    public function test_the_incoming_bq_job_route_is_kept_on_the_retry_copy_and_on_the_dead_letter_record(): void
    {
        // bq-job routes to urn:a although the body says urn:b (section 5.2 allows it).
        $broker = new FakePulsarBroker(false);
        $broker->seed(str_replace('urn:babel:orders:created', 'urn:babel:orders:refunded', self::BODY), ['bq-job' => 'urn:babel:orders:created']);

        $written = [];
        $wsProducer = new class($written) implements PulsarWebSocketClient {
            /** @param list<array{topic: string, payload: string, properties: array<string, string>}> $written */
            public function __construct(private array &$written)
            {
            }

            public function publish(string $topic, string $payload, array $properties): void
            {
                $this->written[] = ['topic' => $topic, 'payload' => $payload, 'properties' => $properties];
            }
        };
        $dlq = new DeadLetterPublisher(new \BabelQueue\Transport\PulsarTransport($wsProducer));
        $calls = 0;
        $dispatcher = $this->alwaysFailing(new Dispatcher(deadLetters: $dlq), $calls);

        $this->consume($broker, $dispatcher);

        $this->assertSame(3, $calls);
        foreach ($broker->published as $copy) {
            $this->assertSame('urn:babel:orders:created', $copy['properties']['bq-job'], 'the retry copy keeps the route');
            $this->assertSame('urn:babel:orders:refunded', json_decode($copy['payload'], true)['job'], 'the body is not rewritten');
        }
        $this->assertCount(1, $written);
        $this->assertSame('public', explode('/', $written[0]['topic'])[2]);
        $this->assertStringEndsWith('/orders.dlq', $written[0]['topic']);
        $this->assertSame('urn:babel:orders:created', $written[0]['properties']['bq-job'], 'the DLQ record keeps the incoming route');
        $this->assertSame((string) json_decode($written[0]['payload'], true)['attempts'], $written[0]['properties']['bq-attempts'], 'bq-attempts follows the record body, not the incoming property');
    }

    public function test_the_consume_loop_acks_a_success_without_publishing(): void
    {
        $broker = new FakePulsarBroker(false);
        $broker->seed(self::BODY);
        $seen = 0;

        $this->consume($broker, (new Dispatcher())->on('urn:babel:orders:created', function () use (&$seen): void {
            $seen++;
        }));

        $this->assertSame(1, $seen);
        $this->assertSame([], $broker->published);
        $this->assertSame(['mid-1'], $broker->acked);
    }
}

/**
 * An in-memory Pulsar: a queue of frames plus the producer seam. `$nackAdvancesCount` selects the subscription
 * semantics: true = Shared / Key_Shared (a nack redelivers with redeliveryCount + 1), false = Exclusive / Failover
 * (a nack redelivers the same frame with the same redeliveryCount).
 */
final class FakePulsarBroker implements PulsarWebSocketConsumerClient, PulsarWebSocketClient, PulsarConsumerDescriptor
{
    /** @var list<array{messageId: string, payload: string, properties: array<string, string>, redeliveryCount: int}> */
    private array $queue = [];

    /** @var array<string, array{messageId: string, payload: string, properties: array<string, string>, redeliveryCount: int}> */
    private array $inFlight = [];

    private int $next = 1;

    /** @var list<array{topic: string, payload: string, properties: array<string, string>}> */
    public array $published = [];

    /** @var list<string> */
    public array $acked = [];

    /** @var list<string> */
    public array $nacked = [];

    public bool $failPublish = false;

    /** The `topic` key of every frame (a client adapter that adds the full topic); null = frames carry none. */
    public ?string $frameTopic = 'persistent://public/default/orders';

    public function __construct(
        private readonly bool $nackAdvancesCount,
        private readonly ?string $topic = null,
        private readonly ?string $subscription = null,
    ) {
    }

    public function topic(): string
    {
        return $this->topic ?? '';
    }

    public function subscriptionType(): ?string
    {
        return $this->subscription;
    }

    /** @param array<string, string> $properties */
    public function seed(string $payload, array $properties = []): void
    {
        $this->queue[] = ['messageId' => 'mid-' . $this->next++, 'payload' => $payload, 'properties' => $properties, 'redeliveryCount' => 0];
    }

    public function isDrained(): bool
    {
        return $this->queue === [] && $this->inFlight === [];
    }

    public function receive(): ?array
    {
        $frame = array_shift($this->queue);
        if ($frame === null) {
            return null;
        }

        $this->inFlight[$frame['messageId']] = $frame;

        return $this->frameTopic === null ? $frame : $frame + ['topic' => $this->frameTopic];
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
        if ($this->nackAdvancesCount) {
            $frame['redeliveryCount']++;
        }
        $this->queue[] = $frame;
    }

    public function publish(string $topic, string $payload, array $properties): void
    {
        if ($this->failPublish) {
            throw new RuntimeException('broker unavailable');
        }

        $this->published[] = ['topic' => $topic, 'payload' => $payload, 'properties' => $properties];
        $this->queue[] = ['messageId' => 'mid-' . $this->next++, 'payload' => $payload, 'properties' => $properties, 'redeliveryCount' => 0];
    }
}

/** Only the consumer side (and the descriptor) of a broker: no producer seam, so nothing can be derived. */
final class ConsumerOnlyView implements PulsarWebSocketConsumerClient, PulsarConsumerDescriptor
{
    public function __construct(private readonly FakePulsarBroker $broker)
    {
    }

    public function receive(): ?array
    {
        return $this->broker->receive();
    }

    public function acknowledge(string $messageId): void
    {
        $this->broker->acknowledge($messageId);
    }

    public function negativeAcknowledge(string $messageId): void
    {
        $this->broker->negativeAcknowledge($messageId);
    }

    public function topic(): string
    {
        return $this->broker->topic();
    }

    public function subscriptionType(): ?string
    {
        return $this->broker->subscriptionType();
    }
}
