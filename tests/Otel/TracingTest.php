<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Otel;

use ArrayObject;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasHeaders;
use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Contracts\Transport;
use BabelQueue\Otel\Tracing;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaTransport;
use BabelQueue\Transport\StompClient;
use BabelQueue\Transport\StompTransport;
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
use Psr\Log\AbstractLogger;
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
        // ADR-0038: the span name carries the queue, never the URN (which moves to babelqueue.urn).
        self::assertSame('process orders', $span->getName());
        self::assertSame(SpanKind::KIND_CONSUMER, $span->getKind());
        self::assertSame(Tracing::traceIdOf(self::TRACE_ID), $span->getContext()->getTraceId());

        $attributes = $span->getAttributes()->toArray();
        self::assertSame('babelqueue', $attributes['messaging.system']);
        self::assertSame(self::TRACE_ID, $attributes['messaging.message.conversation_id']);
        self::assertSame('orders', $attributes['messaging.destination.name']);
        self::assertSame('m1', $attributes['messaging.message.id']);
        self::assertSame('urn:babel:orders:created', $attributes['babelqueue.urn']);
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
        $expected = sprintf('00-%s-%s-%02x', $span->getContext()->getTraceId(), $span->getContext()->getSpanId(), $span->getContext()->getTraceFlags());
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
        $expected = sprintf('00-%s-%s-%02x', $span->getContext()->getTraceId(), $span->getContext()->getSpanId(), $span->getContext()->getTraceFlags());
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

    // -- ADR-0038: span shape ----------------------------------------------------------------------

    public function test_semconv_pin_is_exposed(): void
    {
        self::assertSame('1.44.0', Tracing::SEMCONV_VERSION);
        self::assertSame('https://opentelemetry.io/schemas/1.44.0', Tracing::SCHEMA_URL);
    }

    public function test_consumer_span_carries_the_adr_0038_attribute_set_and_the_transition_keys(): void
    {
        $handler = Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {}, system: 'kafka', consumerGroup: 'billing');
        $handler($this->message());

        $span = $this->firstSpan();
        self::assertSame('process orders', $span->getName());
        self::assertAttributes(
            [
                'messaging.system' => 'kafka',
                'messaging.operation.type' => 'process',
                'messaging.operation.name' => 'process',
                'messaging.message.conversation_id' => self::TRACE_ID,
                'babelqueue.urn' => 'urn:babel:orders:created',
                'babelqueue.schema_version' => 1,
                'babelqueue.attempts' => 0,
                'messaging.babelqueue.attempts' => 0,
                'messaging.operation' => 'process',
                'messaging.destination.name' => 'orders',
                'messaging.message.id' => 'm1',
                'messaging.consumer.group.name' => 'billing',
            ],
            $span->getAttributes()->toArray(),
        );
    }

    public function test_consumer_without_options_keeps_the_deprecated_default_system_and_writes_no_group(): void
    {
        Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {}, system: '')($this->message());

        $attributes = $this->firstSpan()->getAttributes()->toArray();
        self::assertSame('babelqueue', $attributes['messaging.system']);
        self::assertArrayNotHasKey('messaging.consumer.group.name', $attributes);
    }

    public function test_consumer_span_without_a_queue_is_the_bare_operation_and_has_no_destination(): void
    {
        $message = new OtelFakeMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => self::TRACE_ID,
            'data' => [],
            'meta' => ['id' => 'm1', 'queue' => " \t ", 'schema_version' => 1],
            'attempts' => 2,
        ]);

        Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {})($message);

        $span = $this->firstSpan();
        self::assertSame('process', $span->getName());
        $attributes = $span->getAttributes()->toArray();
        self::assertArrayNotHasKey('messaging.destination.name', $attributes);
        self::assertSame('urn:babel:orders:created', $attributes['babelqueue.urn']);
        self::assertSame(2, $attributes['babelqueue.attempts']);
    }

    public function test_consumer_failure_sets_error_type_status_and_rethrows_unchanged(): void
    {
        $boom = new RuntimeException('boom');
        $handler = Tracing::wrap($this->tracer, static function (ConsumedMessage $message) use ($boom): void {
            throw $boom;
        });

        try {
            $handler($this->message());
            self::fail('expected the handler error');
        } catch (RuntimeException $e) {
            self::assertSame($boom, $e);
        }

        $span = $this->firstSpan();
        self::assertSame(StatusCode::STATUS_ERROR, $span->getStatus()->getCode());
        self::assertSame('boom', $span->getStatus()->getDescription());
        self::assertSame(RuntimeException::class, $span->getAttributes()->toArray()['error.type']);
        self::assertSame('exception', $span->getEvents()[0]->getName());
    }

    public function test_a_successful_span_has_no_error_type(): void
    {
        Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {})($this->message());

        self::assertArrayNotHasKey('error.type', $this->firstSpan()->getAttributes()->toArray());
        self::assertSame(StatusCode::STATUS_UNSET, $this->firstSpan()->getStatus()->getCode());
    }

    public function test_producer_span_is_send_queue_with_the_adr_0038_attribute_set(): void
    {
        $transport = new RecordingTransport();

        Tracing::publish($this->tracer, $transport, 'urn:babel:orders:created', ['order_id' => 7], 'orders', system: 'rabbitmq');

        self::assertNotNull($transport->lastPayload);
        $envelope = json_decode($transport->lastPayload, true);
        self::assertIsArray($envelope);
        self::assertIsArray($envelope['meta'] ?? null);

        $span = $this->firstSpan();
        self::assertSame('send orders', $span->getName());
        self::assertSame(SpanKind::KIND_PRODUCER, $span->getKind());
        self::assertAttributes(
            [
                'messaging.system' => 'rabbitmq',
                'messaging.operation.type' => 'send',
                'messaging.operation.name' => 'send',
                'babelqueue.urn' => 'urn:babel:orders:created',
                'babelqueue.schema_version' => 1,
                'messaging.operation' => 'publish',
                'messaging.destination.name' => 'orders',
                'messaging.message.id' => $envelope['meta']['id'],
                'messaging.message.conversation_id' => $envelope['trace_id'],
            ],
            $span->getAttributes()->toArray(),
        );
    }

    public function test_producer_span_with_a_blank_queue_is_the_bare_send_without_destination(): void
    {
        Tracing::publish($this->tracer, new RecordingTransport(), 'urn:babel:orders:created', [], '  ');

        $span = $this->firstSpan();
        self::assertSame('send', $span->getName());
        self::assertArrayNotHasKey('messaging.destination.name', $span->getAttributes()->toArray());
    }

    public function test_producer_failure_after_the_envelope_is_built_keeps_the_message_identity_and_sets_error_type(): void
    {
        $transport = new class implements Transport {
            public function publish(string $payload, ?string $queue = null): ?string
            {
                throw new RuntimeException('transport down');
            }
        };

        try {
            Tracing::publish($this->tracer, $transport, 'urn:babel:orders:created', [], 'orders');
            self::fail('expected a RuntimeException');
        } catch (RuntimeException) {
        }

        $attributes = $this->firstSpan()->getAttributes()->toArray();
        self::assertSame(RuntimeException::class, $attributes['error.type']);
        self::assertNotSame('', $attributes['messaging.message.id'] ?? '');
        self::assertNotSame('', $attributes['messaging.message.conversation_id'] ?? '');
    }

    public function test_producer_failure_before_the_envelope_is_built_has_no_message_identity(): void
    {
        $transport = new RecordingTransport();

        try {
            // Invalid UTF-8 in the data: encoding fails, so no envelope ever reaches the transport.
            Tracing::publish($this->tracer, $transport, 'urn:babel:orders:created', ['bad' => "\xB1"], 'orders');
            self::fail('expected the encode to fail');
        } catch (\JsonException) {
        }

        self::assertNull($transport->lastPayload);
        $attributes = $this->firstSpan()->getAttributes()->toArray();
        self::assertArrayNotHasKey('messaging.message.id', $attributes);
        self::assertArrayNotHasKey('messaging.message.conversation_id', $attributes);
        self::assertSame(\JsonException::class, $attributes['error.type']);
    }

    // -- ADR-0038 section 7: the non-UUID trace_id mapping -----------------------------------------

    public function test_a_uuid_with_a_trailing_newline_is_hashed_not_decoded(): void
    {
        // PCRE `$` matches before a trailing "\n"; the mapping is anchored with `\z`, like the other five SDKs.
        self::assertSame('4e72d2e2f40df8529256d33c375dfe8c', Tracing::traceIdOf(self::TRACE_ID . "\n"));
        self::assertSame('2092f9b3cbf4e482796b67febc62c85e', Tracing::traceIdOf(str_replace('-', '', self::TRACE_ID) . "\n"));
        self::assertSame(str_replace('-', '', self::TRACE_ID), Tracing::traceIdOf(self::TRACE_ID));
    }

    public function test_hashing_a_non_uuid_warns_once_per_distinct_value_when_a_logger_is_given(): void
    {
        $logger = new RecordingLogger();
        $value = 'order-' . bin2hex(random_bytes(6));

        Tracing::traceIdOf($value, $logger);
        Tracing::traceIdOf($value, $logger);
        Tracing::traceIdOf(self::TRACE_ID, $logger); // a UUID is not hashed: no warning
        Tracing::traceIdOf($value . 'x', $logger);

        self::assertCount(2, $logger->warnings);
        self::assertSame($value, $logger->warnings[0]['trace_id']);
        // Without a logger it is silent and still deterministic.
        self::assertSame(Tracing::traceIdOf($value), Tracing::traceIdOf($value));
    }

    public function test_wrap_warns_through_the_logger_for_a_non_uuid_trace_id_and_forwards_it_unchanged(): void
    {
        $logger = new RecordingLogger();
        $traceId = 'req/' . bin2hex(random_bytes(6));
        $message = new OtelFakeMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => $traceId,
            'data' => [],
            'meta' => ['id' => 'm1', 'queue' => 'orders', 'schema_version' => 1],
            'attempts' => 0,
        ]);

        Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {}, logger: $logger)($message);

        self::assertCount(1, $logger->warnings);
        $span = $this->firstSpan();
        self::assertSame($traceId, $span->getAttributes()->toArray()['messaging.message.conversation_id']);
        self::assertSame(Tracing::traceIdOf($traceId), $span->getContext()->getTraceId());
    }

    // -- ADR-0028 finer W3C validity: upper-case hex is ignored ------------------------------------

    public function test_remote_parent_from_headers_ignores_an_upper_case_flags_field(): void
    {
        $header = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0A';

        // The open-telemetry/api propagator alone accepts this header; the structure check rejects it.
        self::assertNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => $header]));
        self::assertNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => strtoupper($header)]));
        self::assertNotNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0a']));
    }

    public function test_remote_parent_from_headers_rejects_a_trailing_newline_and_invalid_ids(): void
    {
        $valid = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

        self::assertNotNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => $valid]));
        self::assertNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => $valid . "\n"]));
        self::assertNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => 'ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01']));
        self::assertNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => '00-00000000000000000000000000000000-00f067aa0ba902b7-01']));
        self::assertNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => '00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01']));
        self::assertNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => '00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01']));
        self::assertNull(Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => '00-4bf92f3577b34da6a3ce929d0e0e4736-00F067AA0BA902B7-01']));
    }

    // -- R1-O3: traceparent rides the Kafka and STOMP producers end to end -------------------------

    public function test_publish_through_the_kafka_transport_propagates_a_traceparent_that_wrap_picks_up(): void
    {
        $producer = new CapturingKafkaProducer();

        Tracing::publish($this->tracer, new KafkaTransport($producer), 'urn:babel:orders:created', ['order_id' => 7], 'orders', system: 'kafka');
        $producerSpan = $this->firstSpan();

        self::assertNotNull($producer->payload);
        self::assertArrayHasKey('traceparent', $producer->headers);
        self::assertSame(
            sprintf('00-%s-%s-%02x', $producerSpan->getContext()->getTraceId(), $producerSpan->getContext()->getSpanId(), $producerSpan->getContext()->getTraceFlags()),
            $producer->headers['traceparent'],
        );
        self::assertStringNotContainsString('traceparent', $producer->payload);
        self::assertSame('urn:babel:orders:created', $producer->headers['bq-job']);

        // The consume side: the record headers reach wrap() through KafkaMessage (HasHeaders).
        $envelope = json_decode($producer->payload, true);
        self::assertIsArray($envelope);
        $received = new KafkaMessage($envelope, $producer->headers, $producer->payload, null, null, 'orders');
        Tracing::wrap($this->tracer, static function (ConsumedMessage $message): void {}, system: 'kafka', consumerGroup: 'billing')($received);

        $consumer = $this->lastSpan();
        self::assertSame('process orders', $consumer->getName());
        self::assertSame($producerSpan->getContext()->getSpanId(), $consumer->getParentContext()->getSpanId());
        self::assertTrue($consumer->getParentContext()->isRemote());
    }

    public function test_publish_through_the_stomp_transport_propagates_a_traceparent_header(): void
    {
        $client = new CapturingStompClient();

        Tracing::publish($this->tracer, new StompTransport($client), 'urn:babel:orders:created', ['order_id' => 7], 'orders', system: 'activemq');
        $producerSpan = $this->firstSpan();

        self::assertSame('orders', $client->destination);
        self::assertSame(
            sprintf('00-%s-%s-%02x', $producerSpan->getContext()->getTraceId(), $producerSpan->getContext()->getSpanId(), $producerSpan->getContext()->getTraceFlags()),
            $client->headers['traceparent'] ?? null,
        );
        self::assertNotNull($client->body);
        self::assertStringNotContainsString('traceparent', $client->body);
        self::assertSame('babelqueue', $client->headers['bq_app_id']);
    }

    /**
     * Same keys and the same typed values, in any order.
     *
     * @param  array<string, mixed>  $expected
     * @param  array<string, mixed>  $actual
     */
    private static function assertAttributes(array $expected, array $actual): void
    {
        ksort($expected);
        ksort($actual);
        self::assertSame($expected, $actual);
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
 * A PSR-3 logger that keeps the warnings (message and context) it was given.
 */
final class RecordingLogger extends AbstractLogger
{
    /** @var list<array<string, mixed>> the context of every warning */
    public array $warnings = [];

    public function log($level, string|\Stringable $message, array $context = []): void
    {
        if ($level === 'warning') {
            $this->warnings[] = $context;
        }
    }
}

/**
 * A {@see KafkaProducer} that remembers the last record it was given.
 */
final class CapturingKafkaProducer implements KafkaProducer
{
    public ?string $payload = null;

    /** @var array<string, string> */
    public array $headers = [];

    public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void
    {
        $this->payload = $payload;
        $this->headers = $headers;
    }
}

/**
 * A {@see StompClient} that remembers the last frame it was given.
 */
final class CapturingStompClient implements StompClient
{
    public ?string $destination = null;

    public ?string $body = null;

    /** @var array<string, string> */
    public array $headers = [];

    public function send(string $destination, string $body, array $headers): void
    {
        $this->destination = $destination;
        $this->body = $body;
        $this->headers = $headers;
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
