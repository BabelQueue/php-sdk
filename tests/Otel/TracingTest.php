<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Otel;

use ArrayObject;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasHeaders;
use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Contracts\Transport;
use BabelQueue\Otel\Tracing;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\API\Trace\TracerInterface;
use OpenTelemetry\Context\Context;
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

    // -- ADR-0028: W3C traceparent transport-header propagation (v0.2) -----------------------------

    public function test_inject_traceparent_round_trips_an_active_span_context(): void
    {
        $span = $this->tracer->spanBuilder('publish')->startSpan();
        $context = Context::getCurrent()->withContextValue($span);

        $headers = Tracing::injectTraceparent($context);

        self::assertArrayHasKey('traceparent', $headers);
        // The injected traceparent encodes the producer span's trace + span id (W3C format).
        $expected = sprintf('00-%s-%s-01', $span->getContext()->getTraceId(), $span->getContext()->getSpanId());
        self::assertSame($expected, $headers['traceparent']);

        // Extracting it back yields a valid remote parent in the same trace.
        $parent = Tracing::remoteParentFromHeaders($headers);
        self::assertNotNull($parent);
        $parentContext = Span::fromContext($parent)->getContext();
        self::assertTrue($parentContext->isValid());
        self::assertSame($span->getContext()->getTraceId(), $parentContext->getTraceId());
        self::assertSame($span->getContext()->getSpanId(), $parentContext->getSpanId());

        $span->end();
    }

    public function test_inject_traceparent_is_empty_without_an_active_span(): void
    {
        // No span on the root context ⇒ nothing to inject ⇒ a no-trace publish stays header-free.
        self::assertSame([], Tracing::injectTraceparent(Context::getRoot()));
    }

    public function test_wrap_starts_a_true_child_of_the_producer_span_when_a_traceparent_is_carried(): void
    {
        // Producer span → its W3C traceparent, the exact header a transport would carry.
        $producer = $this->tracer->spanBuilder('publish')->setSpanKind(SpanKind::KIND_PRODUCER)->startSpan();
        $headers = Tracing::injectTraceparent(Context::getCurrent()->withContextValue($producer));
        $producer->end();

        $handler = Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {});
        // The consumed message carries the producer's traceparent out of band (HasHeaders).
        $handler($this->message($headers));

        $consumer = $this->lastSpan();
        self::assertSame(SpanKind::KIND_CONSUMER, $consumer->getKind());
        // True cross-hop parent-child linkage: the consumer span's parent IS the producer span.
        self::assertSame($producer->getContext()->getTraceId(), $consumer->getContext()->getTraceId());
        self::assertSame($producer->getContext()->getSpanId(), $consumer->getParentContext()->getSpanId());
        self::assertTrue($consumer->getParentContext()->isRemote());
    }

    public function test_wrap_falls_back_to_the_trace_id_parent_when_no_traceparent_is_carried(): void
    {
        $handler = Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {});

        // No traceparent header ⇒ v0.1 behaviour: a remote parent derived from the envelope trace_id.
        $handler($this->message([]));

        $span = $this->lastSpan();
        self::assertSame(Tracing::traceIdOf(self::TRACE_ID), $span->getContext()->getTraceId());
        self::assertTrue($span->getParentContext()->isValid());
    }

    public function test_wrap_falls_back_when_the_traceparent_is_malformed(): void
    {
        $handler = Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {});

        // A malformed traceparent extracts to an invalid span context ⇒ v0.1 trace_id fallback.
        $handler($this->message(['traceparent' => 'not-a-valid-traceparent']));

        $span = $this->lastSpan();
        self::assertSame(Tracing::traceIdOf(self::TRACE_ID), $span->getContext()->getTraceId());
    }

    public function test_wrap_ignores_headers_on_a_message_without_the_has_headers_seam(): void
    {
        $handler = Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {});

        // A plain ConsumedMessage (no HasHeaders) can't surface a traceparent ⇒ v0.1 fallback.
        $handler(new OtelFakeMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => self::TRACE_ID,
            'data' => [],
            'meta' => ['id' => 'm1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1],
            'attempts' => 0,
        ]));

        self::assertSame(Tracing::traceIdOf(self::TRACE_ID), $this->lastSpan()->getContext()->getTraceId());
    }

    public function test_publish_injects_a_traceparent_when_the_transport_carries_headers(): void
    {
        $transport = new RecordingHeaderTransport();

        $id = Tracing::publish($this->tracer, $transport, 'urn:babel:orders:created', ['order_id' => 7]);

        self::assertSame('hdr-1', $id);
        $span = $this->firstSpan();

        // The transport received the header path with a W3C traceparent encoding the producer span.
        self::assertNotNull($transport->lastHeaders);
        $expected = sprintf('00-%s-%s-01', $span->getContext()->getTraceId(), $span->getContext()->getSpanId());
        self::assertSame($expected, $transport->lastHeaders['traceparent'] ?? null);

        // GR-1 / GR-4: the body is the frozen envelope and still stamps trace_id (belt-and-braces).
        self::assertNotNull($transport->lastPayload);
        $envelope = json_decode($transport->lastPayload, true);
        self::assertIsArray($envelope);
        self::assertSame(Tracing::uuidOf($span->getContext()->getTraceId()), $envelope['trace_id']);
        // The header rides out of band — never inside the envelope.
        self::assertArrayNotHasKey('traceparent', $envelope);
    }

    public function test_publish_degrades_to_a_plain_publish_for_a_non_header_transport(): void
    {
        // A transport that does not implement HeaderPublisher: no error, just v0.1 trace_id.
        $transport = new RecordingTransport();

        $id = Tracing::publish($this->tracer, $transport, 'urn:babel:orders:created');

        self::assertSame('msg-123', $id);
        self::assertNotNull($transport->lastPayload);
        $envelope = json_decode($transport->lastPayload, true);
        self::assertIsArray($envelope);
        self::assertSame(
            Tracing::uuidOf($this->firstSpan()->getContext()->getTraceId()),
            $envelope['trace_id'],
        );
    }

    private function firstSpan(): ImmutableSpan
    {
        $span = $this->storage[0] ?? null;
        self::assertInstanceOf(ImmutableSpan::class, $span);

        return $span;
    }

    private function lastSpan(): ImmutableSpan
    {
        $span = $this->storage[count($this->storage) - 1] ?? null;
        self::assertInstanceOf(ImmutableSpan::class, $span);

        return $span;
    }

    /**
     * @param  array<string, string>  $headers  out-of-band transport headers carried with the message
     */
    private function message(array $headers = []): ConsumedMessage
    {
        return new OtelFakeHeaderMessage(
            [
                'job' => 'urn:babel:orders:created',
                'trace_id' => self::TRACE_ID,
                'data' => ['order_id' => 1],
                'meta' => ['id' => 'm1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1],
                'attempts' => 0,
            ],
            $headers,
        );
    }
}

/**
 * Records the last published payload so the test can assert the stamped trace_id. A plain
 * {@see Transport} (no {@see HeaderPublisher}), so {@see Tracing::publish()} degrades to a plain
 * publish — the v0.1 path.
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
 * A {@see HeaderPublisher} recording both the payload and the out-of-band headers, so the test can
 * assert the carried W3C traceparent (ADR-0028) lands beside the unchanged body.
 */
final class RecordingHeaderTransport implements HeaderPublisher
{
    public ?string $lastPayload = null;

    /** @var array<string, string>|null */
    public ?array $lastHeaders = null;

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $this->lastPayload = $payload;
        $this->lastHeaders = [];

        return 'plain-1';
    }

    public function publishWithHeaders(string $payload, array $headers, ?string $queue = null): ?string
    {
        $this->lastPayload = $payload;
        $this->lastHeaders = $headers;

        return 'hdr-1';
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

/**
 * A {@see ConsumedMessage} that also surfaces out-of-band transport headers ({@see HasHeaders}) —
 * the consume-side seam through which a carried W3C traceparent reaches {@see Tracing::wrap()}.
 */
final class OtelFakeHeaderMessage implements ConsumedMessage, HasHeaders
{
    /**
     * @param  array<string, mixed>  $envelope
     * @param  array<string, string>  $headers
     */
    public function __construct(private array $envelope, private array $headers = [])
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

    /** @return array<string, string> */
    public function headers(): array
    {
        return $this->headers;
    }
}
