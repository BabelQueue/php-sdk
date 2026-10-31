<?php

declare(strict_types=1);

namespace BabelQueue\Otel;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasHeaders;
use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Contracts\Transport;
use BabelQueue\Support\Headers;
use OpenTelemetry\API\Metrics\MeterInterface;
use OpenTelemetry\API\Trace\Propagation\TraceContextPropagator;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanContext;
use OpenTelemetry\API\Trace\SpanInterface;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\API\Trace\TraceFlags;
use OpenTelemetry\API\Trace\TracerInterface;
use OpenTelemetry\Context\Context;
use OpenTelemetry\Context\ContextInterface;
use Psr\Log\LoggerInterface;
use Throwable;

/**
 * Optional OpenTelemetry tracing for a babelqueue producer or consumer — the PHP mirror of the
 * Go `babelqueue-go/otel`, Python `babelqueue.otel` and Node `@babelqueue/core/otel` modules.
 *
 * It emits a CONSUMER span per handled message and a PRODUCER span per publish. Cross-hop trace
 * propagation works at two layered levels:
 *
 *  - **trace_id ↔ TraceId** (ADR-0025, v0.1): the envelope's `trace_id` — a UUID — maps 1:1 to a
 *    32-hex OTel trace id, so every hop that shares a `trace_id` shares one OTel trace
 *    (correlation + per-hop timing) with **zero** wire/transport change.
 *  - **W3C `traceparent`** (ADR-0028, v0.2): the producer also injects the active span context as
 *    a `traceparent` (and `tracestate`) **transport header** beside the frozen envelope, never in
 *    it (GR-1), so the consumer starts its span as a **true child** of the producer span — real
 *    cross-hop parent-child linkage. This rides the out-of-band {@see HeaderPublisher} /
 *    {@see HasHeaders} seam (the same seam as the replay-bypass marker, ADR-0027) and is available
 *    on any transport that carries headers. With no `traceparent` present it falls back to the v0.1
 *    `trace_id` behaviour — a strict, **backward-compatible** upgrade (no regression).
 *
 * The wire envelope is untouched (GR-1) and the dependency-light core never requires
 * OpenTelemetry: `open-telemetry/api` is an **optional** (`suggest`) dependency and this class is
 * only loaded when you wire a tracer. The W3C inject/extract uses OTel's own
 * {@see TraceContextPropagator}, which lives in `open-telemetry/api` — so GR-7 holds: no extra
 * dependency beyond the already-optional API package.
 *
 *     $dispatch->on('urn:babel:orders:created', Tracing::wrap($tracer, $handler)); // consumer
 *     Tracing::publish($tracer, $transport, 'urn:babel:orders:created', $data);    // producer
 *
 * **Span shape** (ADR-0038, OpenTelemetry semantic conventions {@see self::SEMCONV_VERSION}): a producer span is
 * `send <queue>` (kind PRODUCER) and a consumer span `process <queue>` (kind CONSUMER); the URN is never in the
 * span name but in `babelqueue.urn`. With no known queue the name is the bare `send` / `process`. Both take
 * the optional named arguments `system:` (the `messaging.system` value, e.g. `'kafka'`; absent or empty keeps the
 * **deprecated** default `babelqueue`), `meter:` (an OpenTelemetry meter to record the ADR-0039 metrics, see
 * {@see Metrics}) and, on the consumer, `consumerGroup:` (`messaging.consumer.group.name`). Create the tracer
 * (and the meter) with {@see self::SCHEMA_URL} to pin the semantic-conventions release.
 *
 * **Dashboard-breaking change (ADR-0038 section 5).** The span names, `messaging.system` (when `system:` is
 * passed) and the producer's `messaging.destination.name` (the URN before, the queue now) changed with no
 * overlap. The deprecated `messaging.operation` and `messaging.babelqueue.attempts` keys are dual-emitted for
 * this MINOR and removed in the next.
 */
final class Tracing
{
    /**
     * The OpenTelemetry semantic-conventions release this module targets (ADR-0038 section 1).
     */
    public const SEMCONV_VERSION = '1.44.0';

    /**
     * The schema URL of {@see self::SEMCONV_VERSION}: pass it as the schema URL when creating the tracer or
     * the meter, e.g. `$provider->getTracer('babelqueue', null, Tracing::SCHEMA_URL)`.
     */
    public const SCHEMA_URL = 'https://opentelemetry.io/schemas/1.44.0';

    /** The `messaging.system` value when no `system` is given. Deprecated default (ADR-0038 section 5). */
    private const DEFAULT_SYSTEM = 'babelqueue';
    /** The error.type fallback when no type name can be obtained (ADR-0038 section 6). */
    private const OTHER_ERROR_TYPE = '_OTHER';
    /** How many distinct hashed trace ids are remembered, so a hostile producer cannot grow the set without bound. */
    private const WARNED_LIMIT = 1024;
    private const INVALID_TRACE_ID = '00000000000000000000000000000000';
    private const INVALID_SPAN_ID = '0000000000000000';
    /** W3C Trace Context structure, lower-case hex only (ADR-0028). `\A…\z` so a trailing newline is not accepted. */
    private const TRACEPARENT_PATTERN = '/\A[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}(-.*)?\z/s';

    /** @var array<string, true> trace ids that were already warned about (once per distinct value) */
    private static array $warnedTraceIds = [];

    /**
     * The W3C out-of-band transport headers that carry Trace Context across a hop (ADR-0028). They
     * ride beside the frozen envelope on the transport's per-message metadata channel — the same
     * seam as the replay-bypass marker (ADR-0027) — so a consumer starts its span as a true child
     * of the producer's span, not merely share the `trace_id`-derived trace. `traceparent` /
     * `tracestate` are exactly the W3C wire format, so a babelqueue header interoperates with any
     * OTel SDK or W3C-compliant peer.
     */
    public const HEADER_TRACEPARENT = TraceContextPropagator::TRACEPARENT;
    public const HEADER_TRACESTATE = TraceContextPropagator::TRACESTATE;

    /**
     * Map an envelope `trace_id` to a deterministic 32-hex OTel trace id: a value that is 32 hex digits once the
     * hyphens are removed (case-insensitive, not all zero) maps to those bytes; any other string is hashed (SHA-256
     * of its raw UTF-8 bytes, first 16 bytes), with no trimming. The inverse of {@see self::uuidOf()} for the UUID
     * case. The match is anchored with `\z`, so a UUID with a trailing newline is hashed, like the other SDKs (GR-5).
     *
     * A hashed value is a non-UUID `trace_id` (message-envelope.md section 5.1): it is preserved and forwarded
     * unchanged everywhere else, only this mapping hashes it. With a `$logger` it logs one warning per distinct
     * value (rate-limited, ADR-0038 section 7).
     */
    public static function traceIdOf(string $traceId, ?LoggerInterface $logger = null): string
    {
        $hex = strtolower(str_replace('-', '', $traceId));
        if (preg_match('/\A[0-9a-f]{32}\z/', $hex) === 1 && $hex !== self::INVALID_TRACE_ID) {
            return $hex;
        }

        if ($logger !== null) {
            self::warnHashed($traceId, $logger);
        }

        return substr(hash('sha256', $traceId), 0, 32);
    }

    /**
     * Format a 32-hex OTel trace id as a canonical UUID string — the form a producer stamps
     * into the message's `trace_id` so a consumer can recover the same trace id via
     * {@see self::traceIdOf()}.
     */
    public static function uuidOf(string $traceIdHex): string
    {
        $hex = substr(str_pad(strtolower(str_replace('-', '', $traceIdHex)), 32, '0', STR_PAD_LEFT), 0, 32);

        return sprintf(
            '%s-%s-%s-%s-%s',
            substr($hex, 0, 8),
            substr($hex, 8, 4),
            substr($hex, 12, 4),
            substr($hex, 16, 4),
            substr($hex, 20, 12),
        );
    }

    /**
     * Wrap a consume handler so each message emits a CONSUMER span `process <queue>` (`process` when the message has
     * no queue), recording the handler's error/status. Mirrors the
     * {@see \BabelQueue\Schema\SchemaValidated::wrap()} / {@see \BabelQueue\Idempotency\Idempotent::wrap()}
     * shape and composes with the consume runtime's ack-on-return / redeliver-on-throw contract.
     *
     * **Parent selection** (ADR-0028): when the producer carried a W3C `traceparent` on the
     * transport — surfaced when the consumed message implements {@see HasHeaders} — the span is
     * started as a true **child** of the producer's span (real cross-hop parent-child linkage with
     * per-hop span timing). With no `traceparent` present (a header-less or non-`HasHeaders`
     * message, or a malformed value) it falls back to the v0.1 behaviour: a remote parent derived
     * from the envelope's `trace_id` (ADR-0025 Option 1), which shares the trace but not the exact
     * span link. So enabling propagation is a strict, backward-compatible upgrade — no regression
     * for messages produced without it.
     *
     * **Attributes** (ADR-0038 section 4): `messaging.system`, `messaging.operation.type` / `.name`, the destination
     * (`meta.queue`, when not blank), `messaging.message.id`, `messaging.message.conversation_id` (the `trace_id`
     * byte for byte), `messaging.consumer.group.name` (when `$consumerGroup` is given), `babelqueue.urn`,
     * `babelqueue.schema_version`, `babelqueue.attempts`, and `error.type` on a failed handler.
     *
     * @param  callable(ConsumedMessage): void  $handler
     * @param  string|null  $system  the `messaging.system` value (`kafka`, `rabbitmq`, ...); null or empty keeps the deprecated `babelqueue`
     * @param  string|null  $consumerGroup  the `messaging.consumer.group.name` value (Kafka); null or empty = not written
     * @param  MeterInterface|null  $meter  records the ADR-0039 consumer metrics when given; with none nothing is recorded
     * @param  LoggerInterface|null  $logger  gets one warning per distinct non-UUID `trace_id` that the trace mapping hashes
     * @return callable(ConsumedMessage): void
     */
    public static function wrap(
        TracerInterface $tracer,
        callable $handler,
        ?string $system = null,
        ?string $consumerGroup = null,
        ?MeterInterface $meter = null,
        ?LoggerInterface $logger = null,
    ): callable {
        $system = self::systemOf($system);
        $metrics = $meter !== null ? new Metrics($meter, $system) : null;

        return static function (ConsumedMessage $message) use ($tracer, $handler, $system, $consumerGroup, $metrics, $logger): void {
            $meta = $message->getMeta();
            $destination = self::destinationOf(self::stringMeta($meta, 'queue'));
            $attempts = $message->attempts();
            // Prefer a true remote parent from a carried W3C traceparent (v0.2); else fall back to
            // the trace_id-derived parent (v0.1). A header-less / malformed traceparent yields null.
            $parent = self::remoteParentFromMessage($message)
                ?? self::traceIdParentContext($message->getTraceId(), $logger);

            $attributes = [
                'messaging.system' => $system,
                'messaging.operation.type' => 'process',
                'messaging.operation.name' => 'process',
                'messaging.message.conversation_id' => $message->getTraceId(),
                'babelqueue.urn' => $message->getUrn(),
                'babelqueue.schema_version' => self::schemaVersionOf($meta),
                'babelqueue.attempts' => $attempts,
                // Deprecated keys, dual-emitted for this MINOR only (ADR-0038 section 5).
                'messaging.operation' => 'process',
                'messaging.babelqueue.attempts' => $attempts,
            ];
            if ($destination !== null) {
                $attributes['messaging.destination.name'] = $destination;
            }
            $messageId = self::stringMeta($meta, 'id');
            if ($messageId !== '') {
                $attributes['messaging.message.id'] = $messageId;
            }
            if ($consumerGroup !== null && $consumerGroup !== '') {
                $attributes['messaging.consumer.group.name'] = $consumerGroup;
            }

            $metrics?->consumed($destination ?? '', $attempts);

            $span = $tracer->spanBuilder($destination !== null ? 'process ' . $destination : 'process')
                ->setParent($parent)
                ->setSpanKind(SpanKind::KIND_CONSUMER)
                ->setAttributes($attributes)
                ->startSpan();
            $scope = $span->activate();

            $errorType = null;
            $started = hrtime(true);
            try {
                $handler($message);
            } catch (Throwable $e) {
                $errorType = self::errorTypeOf($e);
                self::recordFailure($span, $e, $errorType);

                throw $e;
            } finally {
                $seconds = (hrtime(true) - $started) / 1e9;
                $scope->detach();
                $span->end();
                $metrics?->processed($destination ?? '', $seconds, $errorType);
            }
        };
    }

    /**
     * Publish under a PRODUCER span `send <queue>` (`send` when the queue is blank), propagating the trace
     * downstream two ways (ADR-0028):
     *
     *  - It injects the active span context as a W3C `traceparent` (and `tracestate`) onto the
     *    outgoing transport headers, so a consumer can start its span as a true **child** of this
     *    producer span — real cross-hop parent-child linkage. The header rides beside the frozen
     *    envelope, never in it (GR-1), via {@see HeaderPublisher::publishWithHeaders()}; if the
     *    transport does not implement {@see HeaderPublisher} (or carries no usable header) it
     *    degrades to a plain {@see Transport::publish()} (the `traceparent` is simply not
     *    propagated — no error).
     *  - It also carries the active trace's id into the built envelope's `trace_id` (the v0.1
     *    behaviour), so even a consumer that ignores the header — or a transport that drops it —
     *    still recovers the same trace (correlation without exact span linkage). `trace_id` is
     *    preserved end-to-end (GR-4).
     *
     * **Attributes** (ADR-0038 section 4): `messaging.system`, `messaging.operation.type` / `.name`, the destination
     * (`$queue`, when not blank), `messaging.message.id` and `messaging.message.conversation_id` (of the produced
     * envelope, once it is built), `babelqueue.urn`, `babelqueue.schema_version`, and `error.type` on a failure.
     *
     * Encodes and publishes through the given {@see Transport}; returns the transport's message id.
     *
     * @param  array<string, mixed>  $data
     * @param  string|null  $system  the `messaging.system` value (`kafka`, `rabbitmq`, ...); null or empty keeps the deprecated `babelqueue`
     * @param  MeterInterface|null  $meter  records `messaging.client.sent.messages` when given; with none nothing is recorded
     */
    public static function publish(
        TracerInterface $tracer,
        Transport $transport,
        string $urn,
        array $data = [],
        string $queue = 'default',
        ?string $system = null,
        ?MeterInterface $meter = null,
    ): ?string {
        $system = self::systemOf($system);
        // The instruments are created once per meter and system, not on every publish (ADR-0039 section 3).
        $metrics = $meter !== null ? Metrics::shared($meter, $system) : null;
        // The producer knows its destination before the envelope is built: `make` stores `$queue` as `meta.queue`.
        $destination = self::destinationOf($queue);

        $attributes = [
            'messaging.system' => $system,
            'messaging.operation.type' => 'send',
            'messaging.operation.name' => 'send',
            'babelqueue.urn' => $urn,
            'babelqueue.schema_version' => EnvelopeCodec::SCHEMA_VERSION,
            // Deprecated key, dual-emitted for this MINOR only (ADR-0038 section 5).
            'messaging.operation' => 'publish',
        ];
        if ($destination !== null) {
            $attributes['messaging.destination.name'] = $destination;
        }

        $span = $tracer->spanBuilder($destination !== null ? 'send ' . $destination : 'send')
            ->setSpanKind(SpanKind::KIND_PRODUCER)
            ->setAttributes($attributes)
            ->startSpan();
        $scope = $span->activate();

        $errorType = null;
        try {
            $traceId = self::uuidOf($span->getContext()->getTraceId());
            $envelope = EnvelopeCodec::make($urn, $data, $queue, $traceId);
            $encoded = EnvelopeCodec::encode($envelope);

            // The envelope exists from here on, so its identity is on the span even if the publish fails.
            $messageId = self::stringMeta($envelope['meta'], 'id');
            if ($messageId !== '') {
                $span->setAttribute('messaging.message.id', $messageId);
            }
            $span->setAttribute('messaging.message.conversation_id', $traceId);

            $headers = self::injectTraceparent(Context::getCurrent());
            if ($headers !== [] && $transport instanceof HeaderPublisher) {
                return $transport->publishWithHeaders($encoded, $headers, $queue);
            }

            return $transport->publish($encoded, $queue);
        } catch (Throwable $e) {
            $errorType = self::errorTypeOf($e);
            self::recordFailure($span, $e, $errorType);

            throw $e;
        } finally {
            $scope->detach();
            $span->end();
            $metrics?->sent($destination ?? '', $errorType);
        }
    }

    /**
     * Write the active span context (in `$context`) as W3C `traceparent` (and `tracestate`) into a
     * fresh header map, returning it. The producer half: the result is handed to a
     * {@see HeaderPublisher} so the consumer can reconstruct the remote parent. With no valid span
     * context the propagator writes nothing and the map stays empty (so a no-trace publish stays
     * header-free). Uses OTel's own {@see TraceContextPropagator} over a plain-array carrier —
     * exactly the W3C wire format, GR-7 intact.
     *
     * @return array<string, string>
     */
    public static function injectTraceparent(ContextInterface $context): array
    {
        $carrier = [];
        TraceContextPropagator::getInstance()->inject($carrier, null, $context);

        /** @var array<string, string> $carrier */
        return $carrier;
    }

    /**
     * Extract a W3C `traceparent` from out-of-band `$headers` and return a context carrying the
     * resulting remote parent span context, or null when no valid `traceparent` is present.
     *
     * The consumer half of true cross-hop parent-child linkage: a span whose parent is the returned
     * context is a child of the producer's span (remote parent). Null signals the caller to fall
     * back to the v0.1 `trace_id`-derived parent (ADR-0025 Option 1). A header-less or malformed
     * `traceparent` yields null.
     *
     * The header is first checked against the W3C structure in **lower-case hex only**
     * (`{@see self::TRACEPARENT_PATTERN}`), because the `open-telemetry/api` propagator accepts an upper-case
     * flags field (`…-0A`) that the conformance suite (`traceparent` section) requires to be ignored; a
     * header that does not match yields null. The propagator then rejects version `ff` and an all-zero
     * trace or parent id itself.
     *
     * @param  array<string, string>  $headers
     */
    public static function remoteParentFromHeaders(array $headers): ?ContextInterface
    {
        if (! isset($headers[self::HEADER_TRACEPARENT]) || preg_match(self::TRACEPARENT_PATTERN, $headers[self::HEADER_TRACEPARENT]) !== 1) {
            return null;
        }

        $extracted = TraceContextPropagator::getInstance()->extract($headers, null, Context::getRoot());
        if (! Span::fromContext($extracted)->getContext()->isValid()) {
            return null;
        }

        return $extracted;
    }

    /**
     * The remote-parent context from a consumed message's carried headers, or null when the message
     * surfaces no headers ({@see HasHeaders}) or carries no valid `traceparent`.
     */
    private static function remoteParentFromMessage(ConsumedMessage $message): ?ContextInterface
    {
        if (! $message instanceof HasHeaders) {
            return null;
        }

        return self::remoteParentFromHeaders(Headers::sanitize($message->headers()));
    }

    /**
     * A context carrying a remote parent in the `trace_id`-derived trace, so a span started
     * from it lands in that trace (cross-hop correlation). The parent span id is derived
     * deterministically (and non-zero) so the context is valid.
     */
    private static function traceIdParentContext(string $traceId, ?LoggerInterface $logger = null): ContextInterface
    {
        $spanContext = SpanContext::createFromRemoteParent(
            self::traceIdOf($traceId, $logger),
            self::spanIdOf($traceId),
            TraceFlags::SAMPLED,
        );

        return Context::getCurrent()->withContextValue(Span::wrap($spanContext));
    }

    private static function spanIdOf(string $traceId): string
    {
        $spanId = substr(hash('sha256', 'babelqueue-span:' . $traceId), 0, 16);

        return $spanId === self::INVALID_SPAN_ID ? '0000000000000001' : $spanId;
    }

    /**
     * The `messaging.system` value: `$system`, or the deprecated `babelqueue` default when it is null or empty
     * (ADR-0038 section 5). Any other value is written as given, never validated.
     *
     * @internal shared with {@see Metrics}
     */
    public static function systemOf(?string $system): string
    {
        return $system === null || $system === '' ? self::DEFAULT_SYSTEM : $system;
    }

    /**
     * The `messaging.destination.name` for `$queue`, or null when it is empty or blank under the ASCII whitespace
     * set (no destination: the span name is the bare operation and the attribute is not written, ADR-0038 section 4).
     *
     * @internal shared with {@see Metrics}
     */
    public static function destinationOf(string $queue): ?string
    {
        return trim($queue, EnvelopeCodec::BLANK_CHARS) === '' ? null : $queue;
    }

    /**
     * The `error.type` value: the error's class name (ADR-0038 section 6). An anonymous class name is cut at its
     * NUL byte (`class@anonymous`); `_OTHER` if no name can be obtained.
     */
    private static function errorTypeOf(Throwable $e): string
    {
        $name = explode("\0", $e::class, 2)[0];

        return $name !== '' ? $name : self::OTHER_ERROR_TYPE;
    }

    /**
     * Record a failure on `$span`: the exception event, status Error carrying the message, and `error.type`
     * (ADR-0038 section 6). The caller rethrows the error unchanged.
     */
    private static function recordFailure(SpanInterface $span, Throwable $e, string $errorType): void
    {
        $span->recordException($e);
        $span->setStatus(StatusCode::STATUS_ERROR, $e->getMessage());
        $span->setAttribute('error.type', $errorType);
    }

    /**
     * `meta.schema_version` as an int; the supported version when it is absent or not a number.
     *
     * @param  array<string, mixed>  $meta
     */
    private static function schemaVersionOf(array $meta): int
    {
        $version = $meta['schema_version'] ?? null;
        if (is_int($version)) {
            return $version;
        }

        return is_string($version) && preg_match('/\A[0-9]+\z/', $version) === 1 ? (int) $version : EnvelopeCodec::SCHEMA_VERSION;
    }

    /**
     * Log (once per distinct value, bounded) that a non-UUID `trace_id` was hashed for the OTel mapping
     * (message-envelope.md section 5.1). The value is cut to 64 bytes so a hostile producer cannot flood the log.
     */
    private static function warnHashed(string $traceId, LoggerInterface $logger): void
    {
        $key = hash('sha256', $traceId);
        if (isset(self::$warnedTraceIds[$key])) {
            return;
        }
        if (count(self::$warnedTraceIds) >= self::WARNED_LIMIT) {
            self::$warnedTraceIds = [];
        }
        self::$warnedTraceIds[$key] = true;

        $logger->warning(
            'BabelQueue trace_id is not a UUID: it is hashed to derive the OpenTelemetry trace id and is forwarded unchanged.',
            ['trace_id' => substr($traceId, 0, 64)],
        );
    }

    /**
     * @param  array<string, mixed>  $meta
     */
    private static function stringMeta(array $meta, string $key): string
    {
        return isset($meta[$key]) && is_string($meta[$key]) ? $meta[$key] : '';
    }
}
