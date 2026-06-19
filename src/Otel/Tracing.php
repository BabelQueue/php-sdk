<?php

declare(strict_types=1);

namespace BabelQueue\Otel;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\Transport;
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
 * Optional OpenTelemetry tracing for a babelqueue producer or consumer (ADR-0025) — the PHP
 * mirror of the Go `babelqueue-go/otel`, Python `babelqueue.otel` and Node
 * `@babelqueue/core/otel` modules.
 *
 * It emits a CONSUMER span per handled message and a PRODUCER span per publish, correlating
 * them across every hop and SDK through the envelope's `trace_id` — a UUID, which maps 1:1 to
 * a 32-hex OTel trace id. The wire envelope is untouched (GR-1) and the dependency-light core
 * never requires OpenTelemetry: `open-telemetry/api` is an **optional** (`suggest`) dependency
 * and this class is only loaded when you wire a tracer.
 *
 *     $dispatch->on('urn:babel:orders:created', Tracing::wrap($tracer, $handler)); // consumer
 *     Tracing::publish($tracer, $transport, 'urn:babel:orders:created', $data);    // producer
 *
 * Every hop that shares a `trace_id` shares one OTel trace. Exact cross-hop *span*
 * parent-child linkage (W3C `traceparent` as a transport header) is a documented follow-up.
 */
final class Tracing
{
    private const SYSTEM = 'babelqueue';
    private const INVALID_TRACE_ID = '00000000000000000000000000000000';
    private const INVALID_SPAN_ID = '0000000000000000';

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
     * Wrap a consume handler so each message emits a CONSUMER span `process <urn>` in the OTel
     * trace derived from its `trace_id`, recording the handler's error/status. Mirrors the
     * {@see \BabelQueue\Schema\SchemaValidated::wrap()} / {@see \BabelQueue\Idempotency\Idempotent::wrap()}
     * shape and composes with the consume runtime's ack-on-return / redeliver-on-throw contract.
     *
     * @param  callable(ConsumedMessage): void  $handler
     * @return callable(ConsumedMessage): void
     */
    public static function wrap(TracerInterface $tracer, callable $handler): callable
    {
        return static function (ConsumedMessage $message) use ($tracer, $handler): void {
            $meta = $message->getMeta();
            $span = $tracer->spanBuilder('process ' . $message->getUrn())
                ->setParent(self::parentContext($message->getTraceId()))
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
     * Publish under a PRODUCER span `publish <urn>`, carrying the active trace's id into the
     * built envelope's `trace_id` so the downstream consumer recovers the same trace. Encodes
     * and publishes through the given {@see Transport}; returns the transport's message id.
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
            $result = $transport->publish(EnvelopeCodec::encode($envelope), $queue);
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
     * A context carrying a remote parent in the `trace_id`-derived trace, so a span started
     * from it lands in that trace (cross-hop correlation). The parent span id is derived
     * deterministically (and non-zero) so the context is valid.
     */
    private static function parentContext(string $traceId): ContextInterface
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
