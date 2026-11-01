<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Otel;

use ArrayObject;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\Transport;
use BabelQueue\Otel\Metrics;
use BabelQueue\Otel\Tracing;
use OpenTelemetry\API\Metrics\MeterInterface;
use OpenTelemetry\API\Trace\TracerInterface;
use OpenTelemetry\SDK\Metrics\Data\Histogram;
use OpenTelemetry\SDK\Metrics\Data\Metric;
use OpenTelemetry\SDK\Metrics\Data\Sum;
use OpenTelemetry\SDK\Metrics\MeterProvider;
use OpenTelemetry\SDK\Metrics\MetricExporter\InMemoryExporter;
use OpenTelemetry\SDK\Metrics\MetricReader\ExportingReader;
use OpenTelemetry\SDK\Trace\TracerProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The standard metric set (ADR-0039): the instruments, the attribute set with its cardinality limit, and the
 * wrapper recording rules, read back through an in-memory metric reader.
 */
final class MetricsTest extends TestCase
{
    private const TRACE_ID = '7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b';

    /** Attributes that must never be a metric attribute (ADR-0039 section 2). */
    private const FORBIDDEN = [
        'babelqueue.urn',
        'messaging.message.id',
        'messaging.message.conversation_id',
        'babelqueue.attempts',
        'babelqueue.schema_version',
        'messaging.consumer.group.name',
        'dead_letter.reason',
        'trace_id',
    ];

    /** @var ArrayObject<int, Metric> */
    private ArrayObject $storage;

    private ExportingReader $reader;

    private MeterInterface $meter;

    private TracerInterface $tracer;

    protected function setUp(): void
    {
        /** @var ArrayObject<int, Metric> $storage */
        $storage = new ArrayObject();
        $this->storage = $storage;
        $this->reader = new ExportingReader(new InMemoryExporter($this->storage));
        $this->meter = MeterProvider::builder()->addReader($this->reader)->build()->getMeter('test', null, Tracing::SCHEMA_URL);
        $this->tracer = (new TracerProvider())->getTracer('test');
    }

    public function test_the_five_instruments_have_the_adr_0039_names_units_and_kinds(): void
    {
        $metrics = new Metrics($this->meter, 'kafka');
        $metrics->consumed('orders', 1);
        $metrics->processed('orders', 0.2);
        $metrics->sent('orders');
        $metrics->deadLettered('orders', 'orders.dlq', 'failed');

        $byName = [];
        foreach ($this->collect() as $metric) {
            $byName[$metric->name] = $metric;
        }

        self::assertEqualsCanonicalizing(
            ['messaging.process.duration', 'messaging.client.sent.messages', 'messaging.client.consumed.messages', 'babelqueue.retries', 'babelqueue.dlq.messages'],
            array_keys($byName),
        );
        self::assertSame('s', $byName['messaging.process.duration']->unit);
        self::assertInstanceOf(Histogram::class, $byName['messaging.process.duration']->data);
        foreach (['messaging.client.sent.messages', 'messaging.client.consumed.messages', 'babelqueue.retries', 'babelqueue.dlq.messages'] as $counter) {
            self::assertSame('{message}', $byName[$counter]->unit, $counter);
            self::assertInstanceOf(Sum::class, $byName[$counter]->data, $counter);
        }

        // The semconv advisory bucket boundaries reach the histogram.
        $histogram = $byName['messaging.process.duration']->data;
        self::assertInstanceOf(Histogram::class, $histogram);
        foreach ($histogram->dataPoints as $point) {
            self::assertSame([0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10], $point->explicitBounds);
        }
    }

    public function test_the_static_producer_path_reuses_one_instance_per_meter_and_system(): void
    {
        $first = Metrics::shared($this->meter, 'kafka');

        self::assertSame($first, Metrics::shared($this->meter, 'kafka'));
        self::assertNotSame($first, Metrics::shared($this->meter, 'rabbitmq'));
        self::assertSame(Metrics::shared($this->meter, null), Metrics::shared($this->meter, ''));
        // A publish through the shared instance still records one data point per attempt.
        Tracing::publish($this->tracer, new MetricsFakeTransport(), 'urn:babel:orders:created', [], 'orders', system: 'kafka', meter: $this->meter);
        Tracing::publish($this->tracer, new MetricsFakeTransport(), 'urn:babel:orders:created', [], 'orders', system: 'kafka', meter: $this->meter);
        self::assertSame(2, $this->points()['messaging.client.sent.messages'][0]['value']);
    }

    public function test_the_consumer_wrapper_records_consumed_retries_and_duration_and_leaves_the_result_alone(): void
    {
        $ran = 0;
        $handler = Tracing::wrap($this->tracer, function (ConsumedMessage $message) use (&$ran): void {
            ++$ran;
        }, system: 'kafka', consumerGroup: 'billing', meter: $this->meter);

        $handler($this->message(attempts: 0));
        $handler($this->message(attempts: 2));

        self::assertSame(2, $ran);
        $points = $this->points();
        self::assertSame(2, $points['messaging.client.consumed.messages'][0]['value']);
        self::assertSame(1, $points['babelqueue.retries'][0]['value']); // only the delivery with attempts > 0
        self::assertSame(2, $points['messaging.process.duration'][0]['count']);
        self::assertGreaterThanOrEqual(0.0, $points['messaging.process.duration'][0]['sum']);
        self::assertSame(
            ['messaging.system' => 'kafka', 'messaging.operation.name' => 'process', 'messaging.destination.name' => 'orders'],
            $points['messaging.client.consumed.messages'][0]['attributes'],
        );
        self::assertArrayNotHasKey('babelqueue.dlq.messages', $points);
        self::assertArrayNotHasKey('messaging.client.sent.messages', $points);
    }

    public function test_a_failing_handler_is_timed_with_error_type_and_the_error_is_rethrown_unchanged(): void
    {
        $boom = new RuntimeException('boom');
        $handler = Tracing::wrap($this->tracer, static function (ConsumedMessage $message) use ($boom): void {
            throw $boom;
        }, meter: $this->meter);

        try {
            $handler($this->message(attempts: 2));
            self::fail('expected the handler error');
        } catch (RuntimeException $e) {
            self::assertSame($boom, $e);
        }

        $points = $this->points();
        self::assertSame(RuntimeException::class, $points['messaging.process.duration'][0]['attributes']['error.type']);
        // The delivery was received: consumed.messages and retries carry no error.type.
        self::assertArrayNotHasKey('error.type', $points['messaging.client.consumed.messages'][0]['attributes']);
        self::assertArrayNotHasKey('error.type', $points['babelqueue.retries'][0]['attributes']);
        self::assertSame('babelqueue', $points['messaging.process.duration'][0]['attributes']['messaging.system']);
    }

    public function test_no_destination_attribute_when_the_message_has_no_queue(): void
    {
        $message = new MetricsFakeMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => self::TRACE_ID,
            'data' => [],
            'meta' => ['id' => 'm1', 'schema_version' => 1],
            'attempts' => 0,
        ]);

        Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {}, meter: $this->meter)($message);

        $points = $this->points();
        self::assertSame(
            ['messaging.system' => 'babelqueue', 'messaging.operation.name' => 'process'],
            $points['messaging.client.consumed.messages'][0]['attributes'],
        );
    }

    public function test_without_a_meter_nothing_is_recorded(): void
    {
        Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {}, system: 'kafka')($this->message());
        Tracing::publish($this->tracer, new MetricsFakeTransport(), 'urn:babel:orders:created', [], 'orders', system: 'kafka');

        self::assertSame([], $this->points());
    }

    public function test_publish_counts_every_attempt_with_error_type_on_failure_and_returns_the_transport_id(): void
    {
        $id = Tracing::publish($this->tracer, new MetricsFakeTransport(), 'urn:babel:orders:created', [], 'orders', system: 'rabbitmq', meter: $this->meter);
        self::assertSame('msg-123', $id);

        $failing = new class implements Transport {
            public function publish(string $payload, ?string $queue = null): ?string
            {
                throw new RuntimeException('transport down');
            }
        };
        try {
            Tracing::publish($this->tracer, $failing, 'urn:babel:orders:created', [], 'orders', system: 'rabbitmq', meter: $this->meter);
            self::fail('expected the transport error');
        } catch (RuntimeException $e) {
            self::assertSame('transport down', $e->getMessage());
        }

        $points = $this->points();
        $sent = $points['messaging.client.sent.messages'];
        self::assertCount(2, $sent); // one data point without and one with error.type
        $byError = [];
        foreach ($sent as $point) {
            $byError[$point['attributes']['error.type'] ?? ''] = $point;
        }
        self::assertSame(1, $byError['']['value']);
        self::assertSame(1, $byError[RuntimeException::class]['value']);
        self::assertSame(
            ['messaging.system' => 'rabbitmq', 'messaging.operation.name' => 'send', 'messaging.destination.name' => 'orders'],
            $byError['']['attributes'],
        );
    }

    public function test_the_dead_letter_observer_counts_one_per_call_for_the_source_queue_only(): void
    {
        $metrics = new Metrics($this->meter, 'redis');

        $metrics->deadLettered('orders', 'orders.dlq', 'failed');
        $metrics->deadLettered('', 'orders.dlq', 'poison');

        $points = $this->points()['babelqueue.dlq.messages'];
        self::assertCount(2, $points);
        $attributes = array_map(static fn (array $point): array => $point['attributes'], $points);
        self::assertContains(['messaging.system' => 'redis', 'messaging.operation.name' => 'process', 'messaging.destination.name' => 'orders'], $attributes);
        self::assertContains(['messaging.system' => 'redis', 'messaging.operation.name' => 'process'], $attributes);
    }

    public function test_no_high_cardinality_attribute_is_ever_a_metric_attribute(): void
    {
        $handler = Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {
            throw new RuntimeException('boom');
        }, system: 'kafka', consumerGroup: 'billing', meter: $this->meter);
        try {
            $handler($this->message(attempts: 3));
        } catch (RuntimeException) {
        }
        Tracing::publish($this->tracer, new MetricsFakeTransport(), 'urn:babel:orders:created', [], 'orders', system: 'kafka', meter: $this->meter);
        (new Metrics($this->meter, 'kafka'))->deadLettered('orders', 'orders.dlq', 'failed');

        $seen = 0;
        foreach ($this->points() as $name => $points) {
            foreach ($points as $point) {
                ++$seen;
                foreach (self::FORBIDDEN as $key) {
                    self::assertArrayNotHasKey($key, $point['attributes'], $name . ' must not carry ' . $key);
                }
            }
        }
        self::assertGreaterThan(0, $seen);
    }

    /**
     * @return array<string, list<array{value: int|float, count: int, sum: float|int, attributes: array<string, mixed>}>>
     */
    private function points(): array
    {
        $out = [];
        foreach ($this->collect() as $metric) {
            $data = $metric->data;
            if ($data instanceof Sum) {
                foreach ($data->dataPoints as $point) {
                    $out[$metric->name][] = ['value' => $point->value, 'count' => 0, 'sum' => 0, 'attributes' => $point->attributes->toArray()];
                }
            } elseif ($data instanceof Histogram) {
                foreach ($data->dataPoints as $point) {
                    $out[$metric->name][] = ['value' => 0, 'count' => $point->count, 'sum' => $point->sum, 'attributes' => $point->attributes->toArray()];
                }
            }
        }

        return $out;
    }

    /**
     * @return list<Metric>
     */
    private function collect(): array
    {
        $this->reader->collect();

        return array_values($this->storage->getArrayCopy());
    }

    private function message(int $attempts = 0): ConsumedMessage
    {
        return new MetricsFakeMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => self::TRACE_ID,
            'data' => ['order_id' => 1],
            'meta' => ['id' => 'm1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1],
            'attempts' => $attempts,
        ]);
    }
}

/**
 * A minimal {@see ConsumedMessage} double.
 */
final class MetricsFakeMessage implements ConsumedMessage
{
    /** @param array<string, mixed> $envelope */
    public function __construct(private array $envelope)
    {
    }

    public function getUrn(): string
    {
        return is_string($this->envelope['job'] ?? null) ? $this->envelope['job'] : '';
    }

    public function getTraceId(): string
    {
        return is_string($this->envelope['trace_id'] ?? null) ? $this->envelope['trace_id'] : '';
    }

    /** @return array<string, mixed> */
    public function getData(): array
    {
        return is_array($this->envelope['data'] ?? null) ? $this->envelope['data'] : [];
    }

    /** @return array<string, mixed> */
    public function getMeta(): array
    {
        return is_array($this->envelope['meta'] ?? null) ? $this->envelope['meta'] : [];
    }

    public function attempts(): int
    {
        return is_int($this->envelope['attempts'] ?? null) ? $this->envelope['attempts'] : 0;
    }

    /** @return array<string, mixed> */
    public function envelope(): array
    {
        return $this->envelope;
    }
}

/**
 * A {@see Transport} that accepts everything and returns a fixed id.
 */
final class MetricsFakeTransport implements Transport
{
    public function publish(string $payload, ?string $queue = null): ?string
    {
        return 'msg-123';
    }
}
