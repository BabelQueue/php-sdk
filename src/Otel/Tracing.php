<?php

declare(strict_types=1);

namespace BabelQueue\Otel;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasHeaders;
use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Contracts\Transport;
use BabelQueue\Support\Headers;
use OpenTelemetry\API\Trace\Propagation\TraceContextPropagator;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanContext;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\API\Trace\TraceFlags;
use OpenTelemetry\API\Trace\TracerInterface;
use OpenTelemetry\Context\Context;
use OpenTelemetry\Context\ContextInterface;
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
 */
final class Tracing
{
    private const SYSTEM = 'babelqueue';
    private const INVALID_TRACE_ID = '00000000000000000000000000000000';
    private const INVALID_SPAN_ID = '0000000000000000';

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
     * Map an envelope `trace_id` to a deterministic 32-hex OTel trace id: a UUID maps to its
     * hex bytes; any other string is hashed (SHA-256, first 16 bytes). The inverse of
     * {@see self::uuidOf()} for the UUID case.
     */
    public static function traceIdOf(string $traceId): string
    {
        $hex = strtolower(str_replace('-', '', $traceId));
        if (preg_match('/^[0-9a-f]{32}$/', $hex) === 1 && $hex !== self::INVALID_TRACE_ID) {
            return $hex;
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
     * Wrap a consume handler so each message emits a CONSUMER span `process <urn>`, recording the
     * handler's error/status. Mirrors the
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
     * @param  callable(ConsumedMessage): void  $handler
     * @return callable(ConsumedMessage): void
     */
    public static function wrap(TracerInterface $tracer, callable $handler): callable
    {
        return static function (ConsumedMessage $message) use ($tracer, $handler): void {
            $meta = $message->getMeta();
            // Prefer a true remote parent from a carried W3C traceparent (v0.2); else fall back to
            // the trace_id-derived parent (v0.1). A header-less / malformed traceparent yields null.
            $parent = self::remoteParentFromMessage($message)
                ?? self::traceIdParentContext($message->getTraceId());

            $span = $tracer->spanBuilder('process ' . $message->getUrn())
                ->setParent($parent)
                ->setSpanKind(SpanKind::KIND_CONSUMER)
                ->setAttributes([
                    'messaging.system' => self::SYSTEM,
                    'messaging.operation' => 'process',
                    'messaging.destination.name' => self::stringMeta($meta, 'queue'),
                    'messaging.message.id' => self::stringMeta($meta, 'id'),
                    'messaging.message.conversation_id' => $message->getTraceId(),
                    'messaging.babelqueue.attempts' => $message->attempts(),
                ])
                ->startSpan();
            $scope = $span->activate();

            try {
                $handler($message);
            } catch (Throwable $e) {
                $span->recordException($e);
                $span->setStatus(StatusCode::STATUS_ERROR, $e->getMessage());

                throw $e;
            } finally {
                $scope->detach();
                $span->end();
            }
        };
    }

    /**
     * Publish under a PRODUCER span `publish <urn>`, propagating the trace downstream two ways
     * (ADR-0028):
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
     * Encodes and publishes through the given {@see Transport}; returns the transport's message id.
     *
     * @param  array<string, mixed>  $data
     */
    public static function publish(
        TracerInterface $tracer,
        Transport $transport,
        string $urn,
        array $data = [],
        string $queue = 'default',
    ): ?string {
        $span = $tracer->spanBuilder('publish ' . $urn)
            ->setSpanKind(SpanKind::KIND_PRODUCER)
            ->setAttributes([
                'messaging.system' => self::SYSTEM,
                'messaging.operation' => 'publish',
                'messaging.destination.name' => $urn,
            ])
            ->startSpan();
        $scope = $span->activate();

        try {
            $traceId = self::uuidOf($span->getContext()->getTraceId());
            $envelope = EnvelopeCodec::make($urn, $data, $queue, $traceId);
            $encoded = EnvelopeCodec::encode($envelope);

            $headers = self::injectTraceparent(Context::getCurrent());
            if ($headers !== [] && $transport instanceof HeaderPublisher) {
                $result = $transport->publishWithHeaders($encoded, $headers, $queue);
            } else {
                $result = $transport->publish($encoded, $queue);
            }
            $span->setAttribute('messaging.message.id', self::stringMeta($envelope['meta'], 'id'));

            return $result;
        } catch (Throwable $e) {
            $span->recordException($e);
            $span->setStatus(StatusCode::STATUS_ERROR, $e->getMessage());

            throw $e;
        } finally {
            $scope->detach();
            $span->end();
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
     * `traceparent` yields null. The propagator validates the W3C format itself, so a malformed
     * value extracts to an invalid span context (→ null).
     *
     * @param  array<string, string>  $headers
     */
    public static function remoteParentFromHeaders(array $headers): ?ContextInterface
    {
        if (! isset($headers[self::HEADER_TRACEPARENT]) || $headers[self::HEADER_TRACEPARENT] === '') {
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
    private static function traceIdParentContext(string $traceId): ContextInterface
    {
        $spanContext = SpanContext::createFromRemoteParent(
            self::traceIdOf($traceId),
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
     * @param  array<string, mixed>  $meta
     */
    private static function stringMeta(array $meta, string $key): string
    {
        return isset($meta[$key]) && is_string($meta[$key]) ? $meta[$key] : '';
    }
}
