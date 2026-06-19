<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Otel;

use ArrayObject;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\Transport;
use BabelQueue\Otel\Tracing;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\API\Trace\TracerInterface;
use OpenTelemetry\SDK\Trace\ImmutableSpan;
use OpenTelemetry\SDK\Trace\SpanExporter\InMemoryExporter;
use OpenTelemetry\SDK\Trace\SpanProcessor\SimpleSpanProcessor;
use OpenTelemetry\SDK\Trace\TracerProvider;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The optional OpenTelemetry tracing facade (ADR-0025): the trace_id <-> TraceId mapping and
 * the consumer/producer spans, verified against an in-memory span exporter.
 */
final class TracingTest extends TestCase
{
    private const TRACE_ID = '7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b';

    /** @var ArrayObject<int, ImmutableSpan> */
    private ArrayObject $storage;

    private TracerProvider $provider;

    private TracerInterface $tracer;

    protected function setUp(): void
    {
        /** @var ArrayObject<int, ImmutableSpan> $storage */
        $storage = new ArrayObject();
        $this->storage = $storage;
        $this->provider = new TracerProvider(new SimpleSpanProcessor(new InMemoryExporter($this->storage)));
        $this->tracer = $this->provider->getTracer('test');
    }

    protected function tearDown(): void
    {
        $this->provider->shutdown();
    }

    public function test_trace_id_round_trips_a_uuid(): void
    {
        $hex = Tracing::traceIdOf(self::TRACE_ID);
        self::assertMatchesRegularExpression('/^[0-9a-f]{32}$/', $hex);
        self::assertSame(self::TRACE_ID, Tracing::uuidOf($hex));
    }

    /**
     * @return array<string, array{string}>
     */
    public static function nonUuidProvider(): array
    {
        return [
            'plain string' => ['not-a-uuid'],
            '32 non-hex chars' => [str_repeat('z', 32)],
        ];
    }

    #[DataProvider('nonUuidProvider')]
    public function test_non_uuid_trace_id_is_hashed_to_a_valid_id(string $traceId): void
    {
        $hex = Tracing::traceIdOf($traceId);
        self::assertMatchesRegularExpression('/^[0-9a-f]{32}$/', $hex);
        self::assertSame($hex, Tracing::traceIdOf($traceId)); // deterministic
        self::assertNotSame(Tracing::traceIdOf(self::TRACE_ID), $hex);
    }

    public function test_wrap_emits_a_consumer_span_in_the_trace_id_trace(): void
    {
        $called = false;
        $handler = Tracing::wrap($this->tracer, function (ConsumedMessage $message) use (&$called): void {
            $called = true;
        });

        $handler($this->message());

        self::assertTrue($called);
        $span = $this->firstSpan();
        self::assertSame('process urn:babel:orders:created', $span->getName());
        self::assertSame(SpanKind::KIND_CONSUMER, $span->getKind());
        self::assertSame(Tracing::traceIdOf(self::TRACE_ID), $span->getContext()->getTraceId());

        $attributes = $span->getAttributes()->toArray();
        self::assertSame('babelqueue', $attributes['messaging.system']);
        self::assertSame(self::TRACE_ID, $attributes['messaging.message.conversation_id']);
        self::assertSame('orders', $attributes['messaging.destination.name']);
        self::assertSame('m1', $attributes['messaging.message.id']);
    }

    public function test_wrap_records_handler_error_and_rethrows(): void
    {
        $handler = Tracing::wrap($this->tracer, function (ConsumedMessage $message): void {
            throw new RuntimeException('boom');
        });

        try {
            $handler($this->message());
            self::fail('expected a RuntimeException');
        } catch (RuntimeException $e) {
            self::assertSame('boom', $e->getMessage());
        }

        $span = $this->firstSpan();
        self::assertSame(StatusCode::STATUS_ERROR, $span->getStatus()->getCode());
        self::assertNotEmpty($span->getEvents()); // the recorded exception
    }

    public function test_publish_emits_a_producer_span_and_stamps_trace_id(): void
    {
        $transport = new RecordingTransport();

        $id = Tracing::publish($this->tracer, $transport, 'urn:babel:orders:created', ['order_id' => 7]);

        self::assertSame('msg-123', $id);
        $span = $this->firstSpan();
        self::assertSame(SpanKind::KIND_PRODUCER, $span->getKind());

        self::assertNotNull($transport->lastPayload);
        $envelope = json_decode($transport->lastPayload, true);
        self::assertIsArray($envelope);
        self::assertIsArray($envelope['meta'] ?? null);
        // the span's message id is the envelope's own id (meta.id), like every other SDK
        self::assertSame($envelope['meta']['id'], $span->getAttributes()->toArray()['messaging.message.id'] ?? null);
        // the published trace_id encodes the producer span's trace, so a consumer recovers it
        self::assertSame(Tracing::uuidOf($span->getContext()->getTraceId()), $envelope['trace_id']);
        self::assertSame($span->getContext()->getTraceId(), Tracing::traceIdOf((string) $envelope['trace_id']));
    }

    public function test_publish_records_a_failing_transport_and_rethrows(): void
    {
        $transport = new class implements Transport {
            public function publish(string $payload, ?string $queue = null): ?string
            {
                throw new RuntimeException('transport down');
            }
        };

        try {
            Tracing::publish($this->tracer, $transport, 'urn:babel:orders:created');
            self::fail('expected a RuntimeException');
        } catch (RuntimeException $e) {
            self::assertSame('transport down', $e->getMessage());
        }

        $span = $this->firstSpan();
        self::assertSame(SpanKind::KIND_PRODUCER, $span->getKind());
        self::assertSame(StatusCode::STATUS_ERROR, $span->getStatus()->getCode());
    }

    private function firstSpan(): ImmutableSpan
    {
        $span = $this->storage[0] ?? null;
        self::assertInstanceOf(ImmutableSpan::class, $span);

        return $span;
    }

    private function message(): ConsumedMessage
    {
        return new OtelFakeMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => self::TRACE_ID,
            'data' => ['order_id' => 1],
            'meta' => ['id' => 'm1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1],
            'attempts' => 0,
        ]);
    }
}

/**
 * Records the last published payload so the test can assert the stamped trace_id.
 */
final class RecordingTransport implements Transport
{
    public ?string $lastPayload = null;

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $this->lastPayload = $payload;

        return 'msg-123';
    }
}

/**
 * Minimal {@see ConsumedMessage} test double.
 */
final class OtelFakeMessage implements ConsumedMessage
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
