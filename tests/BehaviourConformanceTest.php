<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use ArrayObject;
use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\DeadLetter\PoisonBody;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Idempotency\Idempotent;
use BabelQueue\Idempotency\InMemoryStore;
use BabelQueue\Otel\Metrics;
use BabelQueue\Otel\Tracing;
use BabelQueue\Tests\Support\ConformanceResults;
use BabelQueue\Tests\Support\EnvelopeMessage;
use BabelQueue\Transport\AmqpTransport;
use BabelQueue\Consume\RetryBackoff;
use BabelQueue\Transport\KafkaBodyClassifier;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KeyedKafkaProducer;
use BabelQueue\Transport\KafkaRetryRouter;
use BabelQueue\Transport\RedisTransport;
use BabelQueue\Transport\SqsClient;
use BabelQueue\Transport\SqsTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use OpenTelemetry\API\Metrics\MeterInterface;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\SDK\Metrics\Data\Histogram;
use OpenTelemetry\SDK\Metrics\Data\Metric;
use OpenTelemetry\SDK\Metrics\Data\Sum;
use OpenTelemetry\SDK\Metrics\MeterProvider;
use OpenTelemetry\SDK\Metrics\MetricExporter\InMemoryExporter as MetricMemoryExporter;
use OpenTelemetry\SDK\Metrics\MetricReader\ExportingReader;
use OpenTelemetry\SDK\Trace\ImmutableSpan;
use OpenTelemetry\SDK\Trace\SpanExporter\InMemoryExporter as SpanMemoryExporter;
use OpenTelemetry\SDK\Trace\SpanProcessor\SimpleSpanProcessor;
use OpenTelemetry\SDK\Trace\TracerProvider;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Predis\ClientInterface;

/**
 * Runs the broker-free sections of the vendored cross-SDK conformance manifest and records every
 * case for `results.json` (ADR-0032): `cases`, `roundtrip`, `data_shape`, `forbidden_keys`,
 * `extension_write`, `encode_reject`, `payload_schema_unicode`, `trim`, `job_type`, `idempotency`,
 * `traceparent`, `span-shape`, `trace-id`, `metrics`, `redis`, `redis_layout`, `rabbitmq`, `delay`, `backoff`, `retry_backoff`, `redis_delay` and `kafka_poison`.
 *
 * Re-encoded output is parsed with objects kept as stdClass so the comparison is
 * type-strict ({} is not [], 1 is not 1.0 or true); object key order is ignored.
 *
 * `delay`, `backoff`, `retry_backoff`, `redis_delay` and `kafka_poison` run against the real SQS / Kafka
 * router / Redis producer / backoff code. The consumer-side `redis_layout` blocks run in
 * `babelqueue/laravel` (this package has no Redis consumer; see the skip in
 * {@see self::test_redis_layout_consumer_blocks_belong_to_the_framework_adapter()}).
 */
final class BehaviourConformanceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const DIR = __DIR__ . '/conformance';

    /** A minimal valid envelope the delay evaluator publishes. */
    private const DELAY_BODY = '{"job":"urn:acme:orders:created","trace_id":"t","data":{"order_id":1},"meta":{"id":"i","queue":"orders","lang":"php","schema_version":1,"created_at":1},"attempts":0}';

    protected function tearDown(): void
    {
        EnvelopeCodec::setWarningHandler(null);
    }

    public function test_decode_cases(): void
    {
        ConformanceResults::each('cases', function (array $case): void {
            $name = (string) $case['name'];
            $envelope = EnvelopeCodec::decode($this->fixture($case));
            self::assertSame((bool) $case['valid'], EnvelopeCodec::accepts($envelope), $name . ': verdict');

            if (! $case['valid']) {
                return;
            }

            /** @var array<string, mixed> $expect */
            $expect = $case['expect'];
            self::assertSame($expect['urn'], EnvelopeCodec::urn($envelope), $name . ': urn');
            self::assertSame($expect['attempts'], $envelope['attempts'], $name . ': attempts');
            self::assertSame($expect['lang'], $envelope['meta']['lang'] ?? null, $name . ': meta.lang');
            self::assertSame($expect['schema_version'], $envelope['meta']['schema_version'] ?? null, $name . ': meta.schema_version');
            self::assertSame(self::canonical($expect['data']), self::canonical($envelope['data']), $name . ': data');

            foreach ((array) ($expect['dead_letter'] ?? []) as $field => $value) {
                self::assertSame($value, $envelope['dead_letter'][$field] ?? null, $name . ': dead_letter.' . $field);
            }
        });
    }

    public function test_roundtrip_cases(): void
    {
        ConformanceResults::each('roundtrip', function (\stdClass $case): void {
            self::assertSame([], $this->roundtripViolations($case, $this->fixture($case)), (string) $case->name);
        }, objects: true);
    }

    public function test_data_shape_cases(): void
    {
        ConformanceResults::each('data_shape', function (array $case): void {
            $name = (string) $case['name'];

            if ($case['mode'] === 'encode') {
                /** @var array<string, mixed> $data */
                $data = (array) $case['data'];
                $json = EnvelopeCodec::encode(EnvelopeCodec::make((string) $case['urn'], $data, (string) $case['queue']));
                $out = self::parse($json);
                $raw = json_encode(self::pointer($out, '/data', $name), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
                self::assertSame($case['expect_encoded_data_json'], $raw, $name . ': encoded data');

                return;
            }

            self::assertSame('decode', $case['mode'], $name . ': unknown mode');
            $warnings = [];
            $envelope = EnvelopeCodec::decode($this->fixture($case), self::collector($warnings));
            self::assertSame((bool) $case['valid'], EnvelopeCodec::accepts($envelope), $name . ': verdict');
        });
    }

    public function test_forbidden_key_cases(): void
    {
        ConformanceResults::each('forbidden_keys', function (\stdClass $case): void {
            self::assertSame('warn', $case->expect, $case->name . ': only warn is defined for R0');
            self::assertSame([], $this->forbiddenKeyViolations($case, $this->fixture($case)), (string) $case->name);
        }, objects: true);
    }

    public function test_extension_write_cases(): void
    {
        ConformanceResults::each('extension_write', function (\stdClass $case): void {
            $name = (string) $case->name;
            /** @var array<string, mixed> $meta */
            $meta = get_object_vars($case->meta);
            /** @var array<string, mixed> $data */
            $data = (array) $case->data;

            $envelope = EnvelopeCodec::make((string) $case->urn, $data, (string) $case->queue, meta: $meta);
            $out = self::parse(EnvelopeCodec::encode($envelope));
            self::assertSame([], self::pointerViolations($out, $case->expect_envelope, $name . ' (encode)'), $name);

            // Re-emit chain: decode -> attempts+1 -> encode.
            $warnings = [];
            $decoded = EnvelopeCodec::decode(EnvelopeCodec::encode($envelope), self::collector($warnings));
            self::assertSame([], $warnings, $name . ': decode must not warn');
            self::assertIsInt($decoded['attempts']);
            $decoded['attempts']++;
            $again = self::parse(EnvelopeCodec::encode($decoded));
            self::assertSame([], self::pointerViolations($again, $case->expect_envelope, $name . ' (re-emit)'), $name);
            self::assertSame(1, self::pointer($again, '/attempts', $name), $name . ': attempts');
        }, objects: true);
    }

    public function test_encode_reject_cases(): void
    {
        ConformanceResults::each('encode_reject', function (\stdClass $case): void {
            $name = (string) $case->name;
            self::assertSame('reject', $case->expect, $name);

            // A repeated key cannot be expressed through make(meta: [...]) (one map per call): ADR-0031
            // lets a write API that takes one map per call skip the `meta_entries` form.
            if (isset($case->meta_entries)) {
                ConformanceResults::skipCase('ADR-0031: make(meta: [...]) takes one map per call, so a repeated meta key cannot be expressed');
            }

            $produced = null;

            try {
                /** @var array<string, mixed> $meta */
                $meta = isset($case->meta) ? get_object_vars($case->meta) : [];
                /** @var array<string, mixed> $data */
                $data = (array) $case->data;
                $envelope = EnvelopeCodec::make((string) $case->urn, $data, (string) $case->queue, meta: $meta);

                foreach (isset($case->extras_top) ? get_object_vars($case->extras_top) : [] as $key => $value) {
                    $envelope[(string) $key] = $value;
                }
                foreach (isset($case->extras_meta) ? get_object_vars($case->extras_meta) : [] as $key => $value) {
                    $envelope['meta'][(string) $key] = $value;
                }

                $produced = EnvelopeCodec::encode($envelope);
            } catch (BabelQueueException $e) {
                self::assertStringContainsString((string) $case->offending_key, $e->getMessage(), $name . ': message names the key');

                return;
            }

            self::fail($name . ': must be rejected, but produced ' . (string) $produced);
        }, objects: true);
    }

    public function test_payload_schema_unicode_cases(): void
    {
        /** @var array<string, mixed> $schema */
        $schema = ConformanceResults::manifest()['payload_schema_unicode']['schema'];

        ConformanceResults::each('payload_schema_unicode', static function (array $case) use ($schema): void {
            /** @var array<string, mixed> $data */
            $data = (array) $case['data'];
            $valid = \BabelQueue\Schema\PayloadValidator::check($schema, $data) === null;
            self::assertSame((bool) $case['valid'], $valid, 'case ' . (string) $case['name']);
        });
    }

    /**
     * ADR-0032: the ASCII trim set on `make` (urn, trace id), the blank-trace-id verdict of
     * `accepts`, and the rule that a re-emit never trims what it forwards.
     */
    public function test_trim_cases(): void
    {
        ConformanceResults::each('trim', function (\stdClass $case): void {
            $name = (string) $case->name;

            switch ($case->mode) {
                case 'make':
                    if ($case->expect === 'reject') {
                        try {
                            EnvelopeCodec::make((string) $case->urn);
                        } catch (BabelQueueException) {
                            return;
                        }
                        self::fail($name . ': make() must reject the blank urn');
                    }

                    $envelope = EnvelopeCodec::make((string) $case->urn, [], 'orders', $case->trace_id ?? null);
                    self::assertSame($case->expect->job, $envelope['job'], $name . ': job');
                    if (isset($case->expect->trace_id)) {
                        self::assertSame($case->expect->trace_id, $envelope['trace_id'], $name . ': trace_id');
                    }

                    return;

                case 'decode':
                    $this->assertDecodeVerdict($case);

                    return;

                case 'accepts':
                    $envelope = EnvelopeCodec::decode($this->fixture($case));
                    self::assertSame((bool) $case->expect_decode_ok, $envelope !== [], $name . ': decode');
                    self::assertSame((bool) $case->expect_accepts, EnvelopeCodec::accepts($envelope), $name . ': accepts');

                    return;

                case 'reemit':
                    $envelope = EnvelopeCodec::decode($this->fixture($case));
                    self::assertNotSame([], $envelope, $name . ': decode');
                    self::assertIsInt($envelope['attempts']);
                    $envelope['attempts']++;
                    $out = self::parse(EnvelopeCodec::encode($envelope));
                    self::assertSame([], self::pointerViolations($out, $case->expect_preserved, $name), $name);
                    self::assertSame($case->expect_attempts, self::pointer($out, '/attempts', $name), $name . ': attempts');

                    return;
            }

            self::fail($name . ': unknown trim mode ' . (string) $case->mode);
        }, objects: true);
    }

    /** ADR-0032: trimming is linear; a pathological run of padding must not cost quadratic time. */
    public function test_trim_is_linear_on_huge_whitespace_runs(): void
    {
        $pad = str_repeat(" \t\n\v\f\r", 33334);
        $start = hrtime(true);

        try {
            EnvelopeCodec::make($pad);
            self::fail('an all-whitespace urn must be rejected');
        } catch (BabelQueueException) {
            // expected
        }
        self::assertSame('urn:acme:a', EnvelopeCodec::make($pad . 'urn:acme:a' . $pad)['job']);

        $body = '{"job":"urn:acme:a","data":{},"attempts":0,"meta":{"id":"x","trace_id":' . json_encode($pad) . ',"schema_version":1}}';
        EnvelopeCodec::accepts(EnvelopeCodec::decode($body));

        self::assertLessThan(1.0, (hrtime(true) - $start) / 1e9, 'whitespace handling must stay well under a second');
    }

    /** Unit test for the ASCII trim set: Unicode whitespace is data, not padding (ADR-0032). */
    public function test_trim_keeps_unicode_whitespace_and_trims_the_ascii_set(): void
    {
        self::assertSame(" \t\n\v\f\r", EnvelopeCodec::BLANK_CHARS, 'exactly the ADR-0032 set: no NUL, form feed included');

        $padded = "\u{00A0}urn:acme:a\u{3000}";
        self::assertSame($padded, EnvelopeCodec::make($padded)['job'], 'U+00A0 and U+3000 are preserved');
        self::assertSame($padded, EnvelopeCodec::make(" \f" . $padded . "\v\t")['job'], 'ASCII padding stops at the first non-ASCII-whitespace byte');
        self::assertSame("\u{00A0}", EnvelopeCodec::make("\u{00A0}")['job'], 'a URN of only U+00A0 is not blank');
        self::assertSame("\0urn:acme:a", EnvelopeCodec::make("\0urn:acme:a")['job'], 'NUL is not in the trim set');

        $this->expectException(BabelQueueException::class);
        EnvelopeCodec::make("\f\v \t");
    }

    /** ADR-0032 section 6: `job` must be a string; a non-string `job` falls back to `urn` with a warning. */
    public function test_job_type_cases(): void
    {
        ConformanceResults::each('job_type', function (\stdClass $case): void {
            $this->assertDecodeVerdict($case);
        }, objects: true);
    }

    /** ADR-0022: the seen-set contract of `Idempotent::wrap`, keyed on `meta.id` verbatim. */
    public function test_idempotency_dedup_key(): void
    {
        ConformanceResults::each('idempotency', function (array $case, string $id): void {
            self::assertSame('dedup_key', $id);
            self::assertSame('meta.id', $case['key_field']);

            $envelope = EnvelopeCodec::decode($this->fixture(['file' => $case['envelope_file']]));
            self::assertSame($case['expected_key'], $envelope['meta']['id'] ?? null, 'meta.id is the dedupe key');

            $store = new InMemoryStore();
            $runs = 0;
            $handler = Idempotent::wrap($store, static function () use (&$runs): void {
                $runs++;
            });
            $handler(new EnvelopeMessage($envelope));
            $handler(new EnvelopeMessage($envelope));

            self::assertSame(1, $runs, 'a byte-identical redelivery is the same message');
            self::assertTrue($store->seen($case['expected_key']), 'the seen-set is keyed on meta.id verbatim');
        }, 'dedup_key');
    }

    public function test_idempotency_sequences(): void
    {
        ConformanceResults::each('idempotency', function (array $case): void {
            $name = (string) $case['name'];
            $store = new InMemoryStore();
            $effects = 0;

            /** @var array<string, mixed> $delivery */
            foreach ($case['deliveries'] as $index => $delivery) {
                $metaId = (string) $delivery['meta_id'];
                if (! empty($delivery['forget_before'])) {
                    $store->forget($metaId);
                }

                $ran = false;
                $handler = Idempotent::wrap($store, static function () use (&$ran, $delivery): void {
                    $ran = true;
                    if (($delivery['outcome'] ?? 'ok') === 'throw') {
                        throw new \RuntimeException('handler failed');
                    }
                });

                $envelope = EnvelopeCodec::make('urn:acme:a');
                $envelope['meta']['id'] = $metaId;

                try {
                    $handler(new EnvelopeMessage($envelope));
                } catch (\RuntimeException) {
                    // A throwing handler leaves the id unmarked; the runtime would redeliver.
                }

                self::assertSame($delivery['expect_effect'] === 'run', $ran, sprintf('%s: delivery %d effect', $name, $index));
                $effects += $ran ? 1 : 0;
            }

            self::assertSame($case['expected_effects'], $effects, $name . ': total effects');
        }, 'sequences');
    }

    /**
     * ADR-0028: consumer-side W3C traceparent extraction. A valid header yields its trace and parent
     * span ids and flags; a malformed or absent one behaves like no header and the envelope
     * `trace_id` maps 1:1 to the trace id.
     */
    public function test_traceparent_cases(): void
    {
        ConformanceResults::each('traceparent', static function (array $case): void {
            $name = (string) ($case['name'] ?? '');
            self::assertSame('extract', $case['mode'], $name . ': only extract is defined');

            $parent = $case['traceparent'] === null
                ? null
                : Tracing::remoteParentFromHeaders([Tracing::HEADER_TRACEPARENT => $case['traceparent']]);
            $expect = $case['expect'];

            if ($expect['source'] === 'traceparent') {
                self::assertNotNull($parent, $name . ': the header must be accepted');
                $spanContext = Span::fromContext($parent)->getContext();
                self::assertSame($expect['trace_id_hex'], $spanContext->getTraceId(), $name . ': trace id');
                self::assertSame($expect['parent_span_id'], $spanContext->getSpanId(), $name . ': parent span id');
                self::assertSame($expect['flags'], sprintf('%02x', $spanContext->getTraceFlags()), $name . ': flags');

                return;
            }

            self::assertSame('trace_id', $expect['source']);
            self::assertNull($parent, $name . ': a malformed or absent header is ignored');
            self::assertSame(str_replace('-', '', (string) $case['trace_id']), $expect['trace_id_hex'], $name . ': fixture consistency');
            self::assertSame($expect['trace_id_hex'], Tracing::traceIdOf((string) $case['trace_id']), $name . ': trace id from trace_id');
        });
    }

    /**
     * ADR-0038 section 4 and 5: the producer span (`send <queue>`) and the consumer span (`process <queue>`) for one
     * message, built through the real `Tracing::publish()` / `Tracing::wrap()` and read back from an in-memory span
     * exporter. Every listed attribute is exact, a table key that is not listed must be absent, an extra `babelqueue.*`
     * key is forbidden, `error.type` is asserted for presence only, and the `$produced.*` placeholders come from the
     * envelope the transport was handed.
     */
    public function test_span_shape_cases(): void
    {
        ConformanceResults::each('span-shape', function (array $case): void {
            $name = (string) ($case['name'] ?? '');
            self::assertSame('span', $case['mode'], $name . ': mode');

            /** @var ArrayObject<int, ImmutableSpan> $storage */
            $storage = new ArrayObject();
            $provider = new TracerProvider(new SimpleSpanProcessor(new SpanMemoryExporter($storage)));
            $tracer = $provider->getTracer('conformance', null, Tracing::SCHEMA_URL);
            $failing = $case['outcome'] === 'error';

            $options = [];
            if (array_key_exists('system', $case)) {
                $options['system'] = $case['system'];
            }

            $produced = null;
            $threw = false;
            if ($case['operation'] === 'send') {
                $transport = new ConformanceSpanTransport(($case['envelope_built'] ?? true) !== false && $failing);
                $arguments = ['tracer' => $tracer, 'transport' => $transport, 'urn' => $case['urn']] + $options;
                if (array_key_exists('queue', $case)) {
                    $arguments['queue'] = $case['queue'];
                }
                if (($case['envelope_built'] ?? true) === false) {
                    // Invalid UTF-8 makes the encode fail, so no envelope ever reaches the transport.
                    $arguments['data'] = ['bad' => "\xB1"];
                }

                try {
                    Tracing::publish(...$arguments);
                } catch (\Throwable) {
                    $threw = true;
                }
                if ($transport->payload !== null) {
                    $decoded = json_decode($transport->payload, true);
                    self::assertIsArray($decoded);
                    $produced = $decoded;
                }
            } else {
                $meta = ['lang' => 'php'];
                foreach (['message_id' => 'id', 'queue' => 'queue', 'schema_version' => 'schema_version'] as $input => $key) {
                    if (array_key_exists($input, $case)) {
                        $meta[$key] = $case[$input];
                    }
                }
                if (array_key_exists('consumer_group', $case)) {
                    $options['consumerGroup'] = $case['consumer_group'];
                }
                $message = new EnvelopeMessage([
                    'job' => $case['urn'],
                    'trace_id' => $case['trace_id'],
                    'data' => [],
                    'meta' => $meta,
                    'attempts' => $case['attempts'] ?? 0,
                ]);
                $handler = Tracing::wrap($tracer, static function () use ($failing): void {
                    if ($failing) {
                        throw new \RuntimeException('handler failed');
                    }
                }, ...$options);

                try {
                    $handler($message);
                } catch (\Throwable) {
                    $threw = true;
                }
            }
            $provider->forceFlush();

            self::assertSame($failing, $threw, $name . ': the failure is surfaced to the caller unchanged');
            self::assertCount(1, $storage, $name . ': exactly one span');
            $span = $storage[0];
            self::assertInstanceOf(ImmutableSpan::class, $span);
            $this->assertSpanShape($name, $span, $case['expect'], $produced);
        });
    }

    /**
     * @param  array<string, mixed>  $expect
     * @param  array<string, mixed>|null  $produced  the envelope handed to the transport (send cases)
     */
    private function assertSpanShape(string $name, ImmutableSpan $span, array $expect, ?array $produced): void
    {
        $tableKeys = [
            'messaging.system', 'messaging.operation.type', 'messaging.operation.name', 'messaging.destination.name',
            'messaging.message.id', 'messaging.message.conversation_id', 'messaging.consumer.group.name', 'babelqueue.urn',
            'babelqueue.schema_version', 'babelqueue.attempts', 'error.type', 'messaging.operation', 'messaging.babelqueue.attempts',
        ];

        // Placeholders: the runner reads the produced envelope. A blank produced queue means no destination.
        $producedMeta = is_array($produced['meta'] ?? null) ? $produced['meta'] : [];
        $producedQueue = is_string($producedMeta['queue'] ?? null) ? $producedMeta['queue'] : '';
        $queueUsable = trim($producedQueue, " \t\n\x0B\f\r") !== '';
        $resolve = static fn (mixed $value): mixed => match ($value) {
            '$produced.meta.id' => $producedMeta['id'] ?? null,
            '$produced.trace_id' => $produced['trace_id'] ?? null,
            default => $value,
        };

        $attributes = [];
        foreach ($expect['attributes'] as $key => $value) {
            if ($value === '$produced.meta.queue') {
                if ($queueUsable) {
                    $attributes[$key] = $producedQueue;
                }

                continue;
            }
            $attributes[$key] = $resolve($value);
        }

        $actual = $span->getAttributes()->toArray();
        $expectedKind = ['PRODUCER' => SpanKind::KIND_PRODUCER, 'CONSUMER' => SpanKind::KIND_CONSUMER][$expect['span_kind']];
        self::assertSame($expectedKind, $span->getKind(), $name . ': span kind');
        self::assertSame($expect['status'] === 'error' ? StatusCode::STATUS_ERROR : StatusCode::STATUS_UNSET, $span->getStatus()->getCode(), $name . ': status');

        // Name: exact, or one of the allowed names; the queue placeholder follows the produced meta.queue.
        $optional = $expect['optional_attributes'] ?? [];
        if (isset($expect['span_name'])) {
            $expectedName = rtrim(str_replace('$produced.meta.queue', $queueUsable ? $producedQueue : '', $expect['span_name']));
            self::assertSame($expectedName, $span->getName(), $name . ': span name');
        } else {
            self::assertContains($span->getName(), $expect['span_name_one_of'], $name . ': span name');
        }
        foreach ($optional as $key) {
            if (array_key_exists($key, $actual)) {
                $queue = $attributes[$key] ?? ($key === 'messaging.destination.name' ? (string) $actual[$key] : null);
                self::assertSame('send ' . $actual[$key], $span->getName(), $name . ': the name agrees with the optional ' . $key);
                self::assertSame($queue, $actual[$key], $name . ': optional ' . $key);
            } else {
                self::assertSame('send', $span->getName(), $name . ': no destination, bare name');
            }
        }

        foreach ($attributes as $key => $value) {
            self::assertArrayHasKey($key, $actual, $name . ': attribute ' . $key);
            self::assertSame($value, $actual[$key], $name . ': attribute ' . $key);
        }
        foreach ($tableKeys as $key) {
            $isErrorType = $key === 'error.type';
            if (! array_key_exists($key, $attributes) && ! in_array($key, $optional, true) && ! $isErrorType) {
                self::assertArrayNotHasKey($key, $actual, $name . ': ' . $key . ' must be absent');
            }
        }
        foreach (array_keys($actual) as $key) {
            if (str_starts_with((string) $key, 'babelqueue.')) {
                self::assertContains($key, $tableKeys, $name . ': extra babelqueue.* key ' . $key);
            }
        }

        if (($expect['error_type'] ?? null) === 'present') {
            self::assertIsString($actual['error.type'] ?? null, $name . ': error.type');
            self::assertNotSame('', $actual['error.type'], $name . ': error.type is non-empty');
        } else {
            self::assertArrayNotHasKey('error.type', $actual, $name . ': no error.type on success');
        }
    }

    /**
     * ADR-0038 section 7 and message-envelope.md sections 5.1 and 7: a non-UUID `trace_id` is preserved byte for byte
     * (decode, re-emit, downstream, `make`), and the OTel TraceID mapping decodes a 32-hex value and hashes the rest,
     * pinned to the Go reference.
     */
    public function test_trace_id_cases(): void
    {
        $silent = static function (): void {};
        $body = static fn (string $traceId): string => json_encode([
            'job' => 'urn:acme:a',
            'trace_id' => $traceId,
            'data' => new \stdClass(),
            'meta' => ['id' => 'i', 'queue' => 'q', 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1],
            'attempts' => 0,
        ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);

        ConformanceResults::each('trace-id', static function (array $case) use ($silent, $body): void {
            $name = (string) ($case['name'] ?? '');
            $expect = $case['expect'];

            switch ($case['mode']) {
                case 'map':
                    $hex = Tracing::traceIdOf($case['trace_id']);
                    self::assertSame($expect['trace_id_hex'], $hex, $name . ': trace id hex');
                    $expected = $expect['mapping'] === 'decode'
                        ? strtolower(str_replace('-', '', $case['trace_id']))
                        : substr(hash('sha256', $case['trace_id']), 0, 32);
                    self::assertSame($expected, $hex, $name . ': mapping ' . $expect['mapping']);

                    break;
                case 'decode':
                    $envelope = EnvelopeCodec::decode($body($case['trace_id']), $silent);
                    self::assertTrue($expect['decoded'], $name);
                    self::assertSame($expect['trace_id'], $envelope['trace_id'] ?? null, $name . ': decoded trace_id');

                    break;
                case 'reemit':
                    $envelope = EnvelopeCodec::decode($body($case['trace_id']), $silent);
                    self::assertIsInt($envelope['attempts']);
                    ++$envelope['attempts'];
                    $again = EnvelopeCodec::decode(EnvelopeCodec::encode($envelope), $silent);
                    self::assertSame($expect['trace_id'], $again['trace_id'] ?? null, $name . ': re-emitted trace_id');

                    break;
                case 'downstream':
                    $inbound = EnvelopeCodec::decode($body($case['trace_id']), $silent);
                    $downstream = EnvelopeCodec::make('urn:acme:b', [], 'q', (string) $inbound['trace_id']);
                    self::assertSame($expect['trace_id'], $downstream['trace_id'], $name . ': downstream trace_id');

                    break;
                case 'make':
                    $made = EnvelopeCodec::make($case['urn'], [], 'default', $case['trace_id']);
                    self::assertSame($expect['trace_id'], $made['trace_id'], $name . ': stored trace_id');
                    self::assertSame($expect['trace_id_hex'], Tracing::traceIdOf($made['trace_id']), $name . ': trace id hex of the stored value');

                    break;
                default:
                    self::fail($name . ': unknown mode ' . $case['mode']);
            }
        });
    }

    /**
     * ADR-0039: the five instruments, the data points of the consumer wrapper, the producer wrapper and the
     * dead-letter observer, read back from an in-memory metric reader. The `observer_called: false` cases carry
     * `sdk_test_required`: no event is delivered, so this runner can only report them `pass`; the rule is enforced
     * by `DeadLetterObserverTest` for the three PHP write primitives.
     */
    public function test_metrics_cases(): void
    {
        ConformanceResults::each('metrics', function (array $case): void {
            $name = (string) ($case['name'] ?? '');

            switch ($case['mode']) {
                case 'instrument':
                    $this->assertMetricInstrument($name, $case['expect']);

                    break;
                case 'publish':
                case 'consume':
                    $this->assertMetricRecords($name, $this->driveWrapper($case), $case['expect']['records'], $this->lastProducedQueue);

                    break;
                case 'dead_letter':
                    if ($case['observer_called'] === false) {
                        self::assertTrue($case['sdk_test_required'] ?? false, $name . ': an unobservable rule is enforced by the SDK tests');
                        self::assertSame([], $case['expect']['records'], $name);

                        break;
                    }
                    [$reader, $storage, $meter] = $this->metricHarness();
                    $event = $case['event'];
                    (new Metrics($meter, $case['system'] ?? null))->deadLettered($event['queue'], $event['dead_letter_queue'], $event['reason']);
                    $this->assertMetricRecords($name, $this->collectMetricRecords($reader, $storage), $case['expect']['records'], null);

                    break;
                default:
                    self::fail($name . ': unknown mode ' . $case['mode']);
            }
        });
    }

    /** The `meta.queue` of the envelope the last `publish` scenario handed to its transport. */
    private string $lastProducedQueue = '';

    /**
     * Run one `publish` or `consume` scenario through the real wrapper and return the recorded data points.
     *
     * @param  array<string, mixed>  $case
     * @return list<array{metric: string, kind: string, value: mixed, attributes: array<string, mixed>}>
     */
    private function driveWrapper(array $case): array
    {
        [$reader, $storage, $meter] = $this->metricHarness();
        $tracer = (new TracerProvider())->getTracer('conformance');
        $options = array_key_exists('system', $case) ? ['system' => $case['system']] : [];
        if (($case['meter'] ?? true) !== false) {
            $options['meter'] = $meter;
        }
        $failing = $case['outcome'] === 'error';
        $this->lastProducedQueue = '';

        if ($case['mode'] === 'publish') {
            $transport = new ConformanceSpanTransport($failing);
            $arguments = ['tracer' => $tracer, 'transport' => $transport, 'urn' => 'urn:acme:order.created'] + $options;
            if (array_key_exists('queue', $case)) {
                $arguments['queue'] = $case['queue'];
            }
            try {
                Tracing::publish(...$arguments);
            } catch (\Throwable) {
            }
            $produced = $transport->payload !== null ? json_decode($transport->payload, true) : null;
            $producedMeta = is_array($produced) && is_array($produced['meta'] ?? null) ? $produced['meta'] : [];
            $this->lastProducedQueue = is_string($producedMeta['queue'] ?? null) ? $producedMeta['queue'] : '';
        } else {
            $meta = ['id' => 'm1', 'lang' => 'php', 'schema_version' => 1];
            if (array_key_exists('queue', $case)) {
                $meta['queue'] = $case['queue'];
            }
            $message = new EnvelopeMessage([
                'job' => 'urn:acme:order.created',
                'trace_id' => '7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b',
                'data' => [],
                'meta' => $meta,
                'attempts' => $case['attempts'] ?? 0,
            ]);
            $handler = Tracing::wrap($tracer, static function () use ($failing): void {
                if ($failing) {
                    throw new \RuntimeException('handler failed');
                }
            }, ...$options);
            try {
                $handler($message);
            } catch (\Throwable) {
            }
        }

        return $this->collectMetricRecords($reader, $storage);
    }

    /**
     * One instrument: its name, kind, unit and exact attribute key set, with and without a destination and on a failure.
     *
     * @param  array<string, mixed>  $expect
     */
    private function assertMetricInstrument(string $name, array $expect): void
    {
        $metric = $expect['metric'];
        $always = $expect['attributes']['always'];
        $withDestination = $expect['attributes']['when_destination_known'];
        $onFailure = $expect['attributes']['on_failure'] ?? [];

        foreach ([false, true] as $failure) {
            if ($failure && $onFailure === []) {
                continue;
            }
            foreach ([false, true] as $destinationKnown) {
                [$reader, $storage, $meter] = $this->metricHarness();
                $tracer = (new TracerProvider())->getTracer('conformance');
                $queue = $destinationKnown ? 'orders.events' : '';

                switch ($metric) {
                    case 'messaging.client.sent.messages':
                        try {
                            Tracing::publish($tracer, new ConformanceSpanTransport($failure), 'urn:acme:a', [], $queue, system: 'kafka', meter: $meter);
                        } catch (\Throwable) {
                        }

                        break;
                    case 'babelqueue.dlq.messages':
                        (new Metrics($meter, 'kafka'))->deadLettered($queue, 'q.dlq', 'failed');

                        break;
                    default:
                        $message = new EnvelopeMessage([
                            'job' => 'urn:acme:a', 'trace_id' => 't', 'data' => [], 'meta' => ['id' => 'm', 'queue' => $queue, 'schema_version' => 1], 'attempts' => 1,
                        ]);
                        try {
                            Tracing::wrap($tracer, static function () use ($failure): void {
                                if ($failure) {
                                    throw new \RuntimeException('handler failed');
                                }
                            }, system: 'kafka', meter: $meter)($message);
                        } catch (\Throwable) {
                        }
                }

                $reader->collect();
                $found = null;
                foreach ($storage as $candidate) {
                    if ($candidate->name === $metric) {
                        $found = $candidate;
                    }
                }
                self::assertNotNull($found, $name . ': the metric exists');
                self::assertSame($expect['unit'], $found->unit, $name . ': unit');
                $expectedClass = $expect['instrument'] === 'histogram' ? Histogram::class : Sum::class;
                self::assertInstanceOf($expectedClass, $found->data, $name . ': instrument kind');

                $keys = array_merge($always, $destinationKnown ? $withDestination : [], $failure ? $onFailure : []);
                self::assertNotEmpty($found->data->dataPoints, $name . ': a data point was recorded');
                foreach ($found->data->dataPoints as $point) {
                    $actual = array_keys($point->attributes->toArray());
                    self::assertEqualsCanonicalizing($keys, $actual, $name . ': attribute key set');
                    foreach ($expect['forbidden_attributes'] as $forbidden) {
                        self::assertNotContains($forbidden, $actual, $name . ': ' . $forbidden . ' must never be a metric attribute');
                    }
                    self::assertSame($expect['operation_name'], $point->attributes->toArray()['messaging.operation.name'], $name . ': operation name');
                }
            }
        }
    }

    /**
     * Compare the recorded data points with `expect.records` as a multiset; nothing else may be recorded.
     *
     * @param  list<array{metric: string, kind: string, value: mixed, attributes: array<string, mixed>}>  $actual
     * @param  list<array<string, mixed>>  $expected
     */
    private function assertMetricRecords(string $name, array $actual, array $expected, ?string $producedQueue): void
    {
        $remaining = $actual;
        foreach ($expected as $record) {
            $attributes = [];
            foreach ($record['attributes'] as $key => $value) {
                if ($value === '$produced.meta.queue') {
                    if ($producedQueue !== null && trim($producedQueue, " \t\n\x0B\f\r") !== '') {
                        $attributes[$key] = $producedQueue;
                    }

                    continue;
                }
                $attributes[$key] = $value;
            }
            $wantsError = ($record['error_type'] ?? null) === 'present';

            $matched = null;
            foreach ($remaining as $index => $point) {
                $pointAttributes = $point['attributes'];
                if ($wantsError) {
                    if (! is_string($pointAttributes['error.type'] ?? null) || $pointAttributes['error.type'] === '') {
                        continue;
                    }
                    unset($pointAttributes['error.type']);
                }
                ksort($pointAttributes);
                $want = $attributes;
                ksort($want);
                if ($point['metric'] !== $record['metric'] || $point['kind'] !== $record['kind'] || $pointAttributes !== $want) {
                    continue;
                }
                $valueOk = $record['value'] === 'non_negative_seconds'
                    ? (is_float($point['value']) || is_int($point['value'])) && is_finite((float) $point['value']) && $point['value'] >= 0
                    : $point['value'] === $record['value'];
                if ($valueOk) {
                    $matched = $index;

                    break;
                }
            }
            self::assertNotNull($matched, $name . ': expected a data point ' . json_encode($record) . ', got ' . json_encode($actual));
            unset($remaining[$matched]);
        }
        self::assertSame([], array_values($remaining), $name . ': nothing else may be recorded');
    }

    /**
     * @return array{0: ExportingReader, 1: ArrayObject<int, Metric>, 2: MeterInterface}
     */
    private function metricHarness(): array
    {
        /** @var ArrayObject<int, Metric> $storage */
        $storage = new ArrayObject();
        $reader = new ExportingReader(new MetricMemoryExporter($storage));
        $meter = MeterProvider::builder()->addReader($reader)->build()->getMeter('conformance', null, Tracing::SCHEMA_URL);

        return [$reader, $storage, $meter];
    }

    /**
     * @param  ArrayObject<int, Metric>  $storage
     * @return list<array{metric: string, kind: string, value: mixed, attributes: array<string, mixed>}>
     */
    private function collectMetricRecords(ExportingReader $reader, ArrayObject $storage): array
    {
        $reader->collect();
        $records = [];
        foreach ($storage as $metric) {
            $data = $metric->data;
            if ($data instanceof Sum) {
                foreach ($data->dataPoints as $point) {
                    $records[] = ['metric' => $metric->name, 'kind' => 'add', 'value' => $point->value, 'attributes' => $point->attributes->toArray()];
                }
            } elseif ($data instanceof Histogram) {
                foreach ($data->dataPoints as $point) {
                    // One observation per scenario: the sum of a count-1 histogram point is the observation.
                    self::assertSame(1, $point->count, $metric->name . ': one observation per scenario');
                    $records[] = ['metric' => $metric->name, 'kind' => 'record', 'value' => $point->sum, 'attributes' => $point->attributes->toArray()];
                }
            }
        }

        return $records;
    }

    /**
     * Redis broker-bindings.md section 1.1: the Laravel-compatible key names. The transport really
     * pushes (an `eval` capture on a fake client): the ready list and the notify wake-up are the keys
     * it wrote, the delayed/reserved names come from {@see RedisTransport::keys()} (a producer never
     * touches those two structures).
     */
    public function test_redis_layout_cases(): void
    {
        ConformanceResults::each('redis_layout', static function (array $case): void {
            $queue = (string) $case['queue'];
            $captured = null;

            $client = Mockery::mock(ClientInterface::class);
            $client->shouldNotReceive('rpush');
            $client->shouldReceive('eval')->once()->andReturnUsing(static function (string $script, int $numKeys, string ...$args) use (&$captured): int {
                $captured = [$numKeys, $args];

                return 1;
            });

            (new RedisTransport($client, 'default', (string) $case['key_prefix']))->publish('{"job":"urn:acme:a"}', $queue);

            self::assertNotNull($captured, (string) $case['name'] . ': the transport must push');
            [$numKeys, $args] = $captured;
            self::assertSame(2, $numKeys, (string) $case['name'] . ': ready list + notify list');

            $keys = RedisTransport::keys($queue, (string) $case['key_prefix']);
            $actual = ['ready' => $args[0], 'delayed' => $keys['delayed'], 'reserved' => $keys['reserved'], 'notify' => $args[1]];
            $expected = $case['expect'];
            ksort($actual);
            ksort($expected);
            self::assertSame($expected, $actual, (string) $case['name']);
            self::assertSame('{"job":"urn:acme:a"}', $args[2], (string) $case['name'] . ': the payload is pushed byte for byte');
        });
    }

    /** Redis broker-bindings.md section 1: the list element is the byte-identical envelope. */
    public function test_redis_payload_identity(): void
    {
        ConformanceResults::each('redis', function (array $case): void {
            $body = $this->fixture(['file' => $case['envelope_file']]);
            $pushed = [];

            $client = Mockery::mock(ClientInterface::class);
            $client->shouldReceive('rpush')->once()->andReturnUsing(static function (string $key, array $values) use (&$pushed): int {
                $pushed = [$key, $values];

                return 1;
            });

            (new RedisTransport($client))->publish($body, 'orders');

            self::assertSame(['orders', [$body]], $pushed, 'the queue element is the envelope, byte for byte');
        }, 'payload_identity');
    }

    /** RabbitMQ broker-bindings.md section 2: native AMQP 0-9-1 property and header projection. */
    public function test_rabbitmq_property_projection(): void
    {
        ConformanceResults::each('rabbitmq', function (array $case): void {
            $body = $this->fixture(['file' => $case['envelope_file']]);
            $captured = null;

            $channel = Mockery::mock(AMQPChannel::class);
            $channel->shouldReceive('queue_declare')->once();
            $channel->shouldReceive('basic_publish')->once()->with(
                Mockery::on(static function (AMQPMessage $message) use (&$captured): bool {
                    $captured = $message;

                    return true;
                }),
                '',
                'orders',
            );

            (new AmqpTransport($channel, 'default'))->publish($body, 'orders');

            self::assertInstanceOf(AMQPMessage::class, $captured);
            self::assertSame($body, $captured->getBody(), 'the body is the byte-identical envelope');
            foreach ($case['properties'] as $property => $value) {
                self::assertSame($value, $captured->get($property), 'property ' . $property);
            }
            $headers = $captured->get('application_headers')->getNativeData();
            foreach ($case['headers'] as $header => $value) {
                self::assertSame($value, $headers[$header] ?? null, 'header ' . $header);
            }
        }, 'property_projection');
    }

    /**
     * SQS delay (broker-bindings.md section 3.4): each case runs against the real `SqsTransport` and a
     * recording client; a delay above 900 s, or any per-message delay on a FIFO queue, raises
     * `BabelQueueException` and sends nothing.
     */
    public function test_sqs_delay(): void
    {
        ConformanceResults::each('delay', function (array $case): void {
            $fifo = $case['queue_type'] === 'fifo';
            $sent = [];
            $client = Mockery::mock(SqsClient::class);
            $client->shouldReceive('sendMessage')->zeroOrMoreTimes()->andReturnUsing(static function (array $args) use (&$sent): mixed {
                $sent[] = $args;

                return null;
            });
            $transport = new SqsTransport($client, 'https://sqs.eu-central-1.amazonaws.com/123456789012/orders', $fifo, $fifo ? 'group' : null, $fifo);
            $body = self::DELAY_BODY;

            if ($case['expect'] === 'reject') {
                try {
                    $transport->publish($body, null, delaySeconds: $case['delay_seconds']);
                    self::fail('the publish must raise BabelQueueException');
                } catch (BabelQueueException) {
                    self::assertSame([], $sent, 'a rejected delay must not send anything');
                }

                return;
            }

            $transport->publish($body, null, delaySeconds: $case['delay_seconds']);
            self::assertCount(1, $sent);
            self::assertSame($case['expect']['delay_seconds_attribute'], $sent[0]['DelaySeconds'] ?? null, 'DelaySeconds');
        });
    }

    /** SQS release (broker-bindings.md section 3.5): `ChangeMessageVisibility` with the requested timeout. */
    public function test_sqs_release_visibility(): void
    {
        ConformanceResults::each('delay', function (array $case): void {
            $calls = [];
            $client = Mockery::mock(SqsClient::class);
            $client->shouldReceive('changeMessageVisibility')->once()->andReturnUsing(static function (array $args) use (&$calls): mixed {
                $calls[] = $args;

                return null;
            });
            (new SqsTransport($client, 'https://sqs.eu-central-1.amazonaws.com/123456789012/orders'))->release('receipt-handle', $case['delay_seconds']);

            self::assertCount(1, $calls);
            self::assertSame($case['expect']['visibility_timeout'], $calls[0]['VisibilityTimeout'] ?? null, 'VisibilityTimeout');
        }, 'release_visibility');
    }

    /**
     * Kafka requested-delay rule (broker-bindings.md section 6.4): the smallest tier that covers the
     * requested delay, `bq-delay` echoing the REQUESTED delay, `attempts` untouched; a delay above the
     * largest tier is rejected. Run against the real `KafkaRetryRouter`.
     */
    public function test_kafka_backoff_tiers(): void
    {
        ConformanceResults::each('backoff', function (array $case): void {
            $this->evaluateKafkaRoute($case, 0, $case['expect'], (int) $case['delay_ms']);
        });
    }

    /** Kafka release/retry republish (section 6.5): the tier follows the attempts, not a requested delay. */
    public function test_kafka_retry_republish(): void
    {
        ConformanceResults::each('backoff', function (array $case): void {
            $this->evaluateKafkaRoute($case, (int) $case['attempts'], $case['expect'], null);
        }, 'retry_republish');
    }

    /** `n = min(attempts_before, N-1) + 1`, staying on the last tier once the tiers run out; the record key is kept. */
    public function test_kafka_retry_attempt_tier(): void
    {
        ConformanceResults::each('backoff', function (array $case): void {
            $this->evaluateKafkaRoute($case, (int) $case['attempts_before'], $case['expect'], null, $case['key'] ?? null);
        }, 'retry_attempt_tier');
    }

    /** Retry topic naming (section 6.4): `<work_topic>.retry.<n>`, n 1-based. */
    public function test_kafka_topic_names(): void
    {
        ConformanceResults::each('backoff', function (array $case): void {
            $router = new KafkaRetryRouter(Mockery::mock(KafkaProducer::class));
            self::assertSame($case['expect_topic'], $router->retryTopic((string) $case['work_topic'], (int) $case['n']));
        }, 'topic_names');
    }

    /** The RECOMMENDED tier list is exposed as `DEFAULT_RETRY_TIERS_MS` (section 6.4). */
    public function test_kafka_default_tiers(): void
    {
        ConformanceResults::each('backoff', static function (array $case): void {
            self::assertSame('DEFAULT_RETRY_TIERS_MS', $case['constant']);
            self::assertSame($case['expect'], KafkaRetryRouter::DEFAULT_RETRY_TIERS_MS);
        }, 'default_tiers');
    }

    /**
     * Kafka poison classification (ADR-0034 section 10, broker-bindings.md section 6.5): the same
     * body-to-verdict table every SDK implements; `no_identity` and `poison` share one fate on Kafka.
     */
    public function test_kafka_poison_classification(): void
    {
        ConformanceResults::each('kafka_poison', static function (array $case): void {
            self::assertSame($case['expect'], KafkaBodyClassifier::classify((string) $case['body']), (string) $case['name']);
        });
    }

    /**
     * Poison DLQ record shape (ADR-0035, error-handling.md section 6): a strict JSON object is spliced
     * byte-for-byte, anything else is wrapped as `{"raw", "raw_encoding"?, "dead_letter"}`; a body that
     * is not valid UTF-8 is carried as standard base64 plus `raw_encoding: "base64"`.
     */
    public function test_poison_dlq_record_shape(): void
    {
        ConformanceResults::each('poison_dlq', static function (array $case): void {
            $name = (string) $case['name'];
            $body = isset($case['body_base64']) ? (string) base64_decode((string) $case['body_base64'], true) : (string) $case['body'];
            $expect = $case['expect'];

            $out = PoisonBody::annotate($body, 'poison', null, 'orders', 0);
            self::assertTrue(preg_match('//u', $out) === 1, $name . ': the record is valid UTF-8 (no U+FFFD substitution)');
            self::assertStringNotContainsString("\u{FFFD}", $out, $name);

            $record = json_decode($out, true, 100000, JSON_THROW_ON_ERROR);
            self::assertIsArray($record, $name);
            self::assertSame('poison', $record['dead_letter']['reason'] ?? null, $name);
            self::assertSame('orders', $record['dead_letter']['original_queue'] ?? null, $name);
            $rest = $record;
            unset($rest['dead_letter']);

            if ($expect['shape'] === 'splice_or_wrapper' && ! array_key_exists('raw', $rest)) {
                // Spliced instead of wrapped: every original literal survives and no member is added or lost.
                foreach ((array) ($expect['splice_literals'] ?? []) as $literal) {
                    self::assertStringContainsString((string) $literal, $out, $name . ': the literal is kept verbatim');
                }
                self::assertSame(
                    array_map('strval', array_merge((array) ($expect['splice_keys'] ?? []), ['dead_letter'])),
                    array_map('strval', array_keys($record)),
                    $name . ': top-level members',
                );

                return;
            }

            self::assertEquals($expect['record'], $rest, $name);
            if ($expect['shape'] === 'wrapper') {
                self::assertSame('raw', array_key_first($record), $name);
            }
        });
    }

    /**
     * Retry backoff (ADR-0034 section 9): `d(n) = min(max, base * multiplier^(n-1))`, `full` jitter
     * draws `uniform(0, d(n))`. Defaults give 0 (today's behaviour). The `RetryBackoff` value object is
     * the formula the consume path ({@see \BabelQueue\Consume\Dispatcher}) applies.
     */
    public function test_retry_backoff_formula(): void
    {
        ConformanceResults::each('retry_backoff', static function (array $case): void {
            $backoff = new RetryBackoff(
                (float) $case['retry_backoff'],
                (float) $case['retry_backoff_multiplier'],
                (float) $case['retry_backoff_max'],
                (string) $case['retry_jitter'],
            );
            $n = (int) $case['n'];

            if (array_key_exists('expect_delay_seconds', $case)) {
                self::assertEqualsWithDelta((float) $case['expect_delay_seconds'], $backoff->delaySeconds($n), 1e-9, (string) $case['name']);

                return;
            }

            $min = (float) $case['expect_delay_range_seconds']['min'];
            $max = (float) $case['expect_delay_range_seconds']['max'];
            for ($draw = 0; $draw < 50; $draw++) {
                $delay = $backoff->delaySeconds($n);
                self::assertGreaterThanOrEqual($min, $delay, (string) $case['name'] . ': jittered delay below range');
                self::assertLessThanOrEqual($max, $delay, (string) $case['name'] . ': jittered delay above range');
            }
        });
    }

    /** Redis delayed score (section 1.6): `floor(now) + ceil(delay)`, integer epoch seconds. */
    public function test_redis_delay_score(): void
    {
        ConformanceResults::each('redis_delay', static function (array $case): void {
            self::assertSame($case['expect_score'], RedisTransport::delayedScore((float) $case['now'], $case['delay']), (string) $case['name']);
        });
    }

    /**
     * The `delayed_score` behavioural block, run against the real `RedisTransport::publishDelayed()`
     * on a fake client (the producer side needs no server): one `ZADD` on the `:delayed` key, the
     * member the body byte for byte, the integer score, and no notify token.
     */
    public function test_redis_layout_delayed_score(): void
    {
        ConformanceResults::each('redis_layout', function (array $case): void {
            $step = $case['steps'][0];
            self::assertSame('push_delayed', $step['op']);

            $added = [];
            $client = Mockery::mock(ClientInterface::class);
            $client->shouldNotReceive('eval');
            $client->shouldNotReceive('rpush');
            $client->shouldReceive('zadd')->once()->andReturnUsing(static function (string $key, array $members) use (&$added): int {
                $added[] = [$key, $members];

                return 1;
            });

            $transport = new RedisTransport($client, 'default', RedisTransport::LARAVEL_KEY_PREFIX, static fn (): float => (float) $step['now']);
            $transport->publishDelayed((string) $step['body'], $step['delay'], (string) $case['queue']);

            self::assertCount(1, $added);
            self::assertSame('queues:' . $case['queue'] . ':delayed', $added[0][0]);
            $expected = $step['expect']['delayed'][0];
            self::assertSame([(string) $expected['member'] => $expected['score']], $added[0][1], 'member and integer score');
        }, 'delayed_score');
    }

    /**
     * The remaining behavioural `redis_layout` blocks exercise a Redis CONSUMER (reserve, migrate,
     * release, sweep). `php-sdk` ships a publish-only `RedisTransport` (ADR-0010: PHP consumes through
     * the framework workers), so the consumer behaviour is run by `babelqueue/laravel` against its real
     * `BabelQueueRedisQueue`. Here the blocks are recorded as skips, which is only valid while
     * `BQ_REQUIRE_REDIS` is unset (the manifest runner contract); with it set they fail loudly.
     */
    public function test_redis_layout_consumer_blocks_belong_to_the_framework_adapter(): void
    {
        $subs = ['migrate', 'reserve', 'pcall_poison', 'release', 'processing_sweep', 'processing_token', 'late_ack', 'identical_copies', 'retry_after_null', 'lrem_count', 'malformed_member', 'orphan', 'legacy_release_gate', 'legacy_dlq_gate'];

        if (getenv('BQ_REQUIRE_REDIS') !== false && getenv('BQ_REQUIRE_REDIS') !== '') {
            self::fail('BQ_REQUIRE_REDIS is set but php-sdk has no Redis consumer to run the behavioural redis_layout blocks (' . implode(', ', $subs) . '); they run in babelqueue/laravel (ADR-0033 A12, ADR-0010).');
        }

        foreach ($subs as $sub) {
            ConformanceResults::skipAll('redis_layout', 'ADR-0010/ADR-0033 A12: php-sdk has no Redis consumer; the consumer behaviour is run by babelqueue/laravel against live Redis', $sub);
        }

        $this->addToAssertionCount(1);
    }

    /**
     * Runs one router case. `$delayMs` null = the retry/release entry point (tier from the attempts),
     * otherwise the requested-delay rule. The router gets a `maxAttempts` large enough that a high
     * `attempts` still retries (the default 5 would dead-letter it).
     *
     * @param  array<string, mixed>  $case
     * @param  array<string, mixed>|string  $expect
     */
    private function evaluateKafkaRoute(array $case, int $attempts, array|string $expect, ?int $delayMs, ?string $key = null): void
    {
        $sent = [];
        $producer = Mockery::mock(KafkaProducer::class, KeyedKafkaProducer::class);
        $producer->shouldReceive('produce')->zeroOrMoreTimes()->andReturnUsing(static function (string $topic, string $payload, array $headers, ?int $timestampMs = null) use (&$sent): void {
            $sent[] = ['topic' => $topic, 'payload' => $payload, 'headers' => $headers, 'key' => null];
        });
        $producer->shouldReceive('produceKeyed')->zeroOrMoreTimes()->andReturnUsing(static function (string $topic, string $payload, array $headers, ?int $timestampMs, ?string $recordKey) use (&$sent): void {
            $sent[] = ['topic' => $topic, 'payload' => $payload, 'headers' => $headers, 'key' => $recordKey];
        });

        $router = new KafkaRetryRouter($producer, $case['retry_tiers_ms'], maxAttempts: 1000);
        $message = new KafkaMessage([
            'job' => 'urn:acme:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 1],
            'meta' => ['id' => 'msg-1', 'queue' => $case['work_topic'], 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1],
            'attempts' => $attempts,
        ], key: $key);

        if ($expect === 'reject') {
            try {
                $router->route($message, null, $delayMs);
                self::fail('the route must raise BabelQueueException');
            } catch (BabelQueueException) {
                self::assertSame([], $sent, 'a rejected route must not produce anything');
            }

            return;
        }

        self::assertTrue($router->route($message, null, $delayMs), 'the record must be retried, not dead-lettered');
        self::assertCount(1, $sent);

        $tiers = $case['retry_tiers_ms'];
        sort($tiers);
        $tierNumber = array_search($expect['tier_ms'], $tiers, true);
        self::assertNotFalse($tierNumber, 'fixture consistency: tier_ms is one of retry_tiers_ms');
        self::assertSame($case['work_topic'] . '.retry.' . ($tierNumber + 1), $sent[0]['topic'], 'tier topic');
        if (isset($expect['topic'])) {
            self::assertSame($expect['topic'], $sent[0]['topic'], 'expected topic');
        }
        foreach ($expect['headers'] ?? [] as $header => $value) {
            self::assertSame($value, $sent[0]['headers'][$header] ?? null, 'header ' . $header);
        }
        if (isset($expect['body_attempts'])) {
            self::assertSame($expect['body_attempts'], EnvelopeCodec::decode($sent[0]['payload'])['attempts'] ?? null, 'body attempts');
        }
        if (array_key_exists('key', $expect)) {
            self::assertSame($expect['key'], $sent[0]['key'], 'record key kept byte for byte');
        }
    }

    /**
     * The runner must really evaluate the optional assertion fields: on synthetic cases that
     * violate them it has to report a violation, otherwise a runner that ignores the fields
     * would be exactly as green as one that reads them.
     */
    public function test_the_runner_reports_violations_of_the_optional_fields(): void
    {
        $body = '{"job":"urn:acme:a","trace_id":"t","data":{"order_id":1},"meta":{"id":"i","queue":"q","lang":"php","schema_version":1,"created_at":1,"vendor_flag":true},"extra_top":1,"attempts":0}';

        $good = (object) [
            'name' => 'synthetic',
            'expect_attempts' => 1,
            'expect_preserved' => (object) ['/meta/vendor_flag' => true, '/extra_top' => 1],
            'expect_absent_after_reencode' => ['/meta/missing'],
        ];
        self::assertSame([], $this->roundtripViolations($good, $body), 'a satisfied case reports nothing');

        $absent = clone $good;
        $absent->expect_absent_after_reencode = ['/extra_top'];
        self::assertNotSame([], $this->roundtripViolations($absent, $body), 'expect_absent_after_reencode: a present pointer');

        $preserved = clone $good;
        $preserved->expect_preserved = (object) ['/meta/vendor_flag' => false];
        self::assertNotSame([], $this->roundtripViolations($preserved, $body), 'expect_preserved: a different value');

        $retyped = clone $good;
        $retyped->expect_preserved = (object) ['/data' => []];
        self::assertNotSame([], $this->roundtripViolations($retyped, $body), 'expect_preserved: {} is not []');

        $deadLetter = clone $good;
        $deadLetter->apply_dead_letter = (object) ['reason' => 'failed', 'original_queue' => 'orders'];
        $deadLetter->expect_dead_letter = (object) ['reason' => 'failed', 'original_queue' => 'orders'];
        self::assertSame([], $this->roundtripViolations($deadLetter, $body), 'a satisfied dead-letter case reports nothing');

        $deadLetter->expect_dead_letter = (object) ['original_queue' => 'elsewhere'];
        self::assertNotSame([], $this->roundtripViolations($deadLetter, $body), 'expect_dead_letter: a different value');

        $forbiddenBody = '{"job":"urn:acme:a","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0,"timestamp":1}';
        $forbidden = (object) [
            'name' => 'synthetic-forbidden',
            'forbidden_key' => '/timestamp',
            'expect' => 'warn',
            'expect_absent_after_reencode' => ['/timestamp'],
            'expect_preserved' => (object) ['/job' => 'urn:acme:a'],
        ];
        self::assertSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'a satisfied forbidden case reports nothing');

        $forbidden->expect_preserved = (object) ['/job' => 'urn:acme:other'];
        self::assertNotSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'forbidden_keys expect_preserved: a different value');

        $forbidden->expect_preserved = (object) [];
        $forbidden->expect_absent_after_reencode = ['/job'];
        self::assertNotSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'forbidden_keys: a present pointer');

        $forbidden->expect_absent_after_reencode = ['/timestamp'];
        $forbidden->forbidden_key = '/meta/ts';
        self::assertNotSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'forbidden_keys: no warning names the key');
    }

    public function test_encoder_never_emits_forbidden_keys(): void
    {
        $envelope = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1], 'orders');
        $envelope['timestamp'] = 1;
        $envelope['meta']['ts'] = 1;
        $envelope['meta']['source'] = 'php';
        $envelope['meta']['max_retries'] = 3;
        $envelope['meta']['attempts'] = 2;

        $this->expectException(InvalidEnvelopeException::class);
        $this->expectExceptionMessageMatches('#/timestamp, /meta/max_retries, /meta/attempts, /meta/source, /meta/ts#');

        EnvelopeCodec::encode($envelope);
    }

    /** Shared by `trim` (decode mode) and `job_type`: a decode verdict plus the urn and warning expectation. */
    private function assertDecodeVerdict(\stdClass $case): void
    {
        $name = (string) $case->name;
        $warnings = [];
        $envelope = EnvelopeCodec::decode($this->fixture($case), self::collector($warnings));

        if (! $case->valid) {
            self::assertSame([], $envelope, $name . ': decode must reject');

            return;
        }

        self::assertNotSame([], $envelope, $name . ': decode must accept');
        self::assertSame($case->expect->urn, EnvelopeCodec::urn($envelope), $name . ': urn');
        self::assertSame((bool) ($case->expect_warning ?? false), $warnings !== [], $name . ': warning');
    }

    /** Key-sorted JSON, so object key order is ignored while types stay strict. */
    private static function canonical(mixed $value): string
    {
        $sort = static function (mixed $v) use (&$sort): mixed {
            if (! is_array($v)) {
                return $v;
            }
            if (! array_is_list($v)) {
                ksort($v);
            }

            return array_map($sort, $v);
        };

        return json_encode($sort($value), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }

    /**
     * Decode -> attempts+1 [-> dead-letter] -> encode, then every assertion of the case.
     *
     * @return list<string> the violations; empty when the case holds
     */
    private function roundtripViolations(\stdClass $case, string $body): array
    {
        $name = (string) $case->name;
        $violations = [];

        $warnings = [];
        $envelope = EnvelopeCodec::decode($body, self::collector($warnings));
        if ($warnings !== []) {
            $violations[] = $name . ': decode must emit no warning, got ' . $warnings[0][0];
        }
        if (! EnvelopeCodec::accepts($envelope)) {
            return [...$violations, $name . ': decode must accept'];
        }

        self::assertIsInt($envelope['attempts']);
        $envelope['attempts']++;

        $deadLettered = isset($case->apply_dead_letter);
        if ($deadLettered) {
            $envelope = DeadLetter::annotate(
                $envelope,
                (string) $case->apply_dead_letter->reason,
                new \RuntimeException('conformance runner'),
                (string) $case->apply_dead_letter->original_queue,
                $envelope['attempts'],
            );
        }

        $out = self::parse(EnvelopeCodec::encode($envelope));

        if (isset($case->expect_attempts) && (! self::has($out, '/attempts') || self::pointer($out, '/attempts', $name) !== $case->expect_attempts)) {
            $violations[] = $name . ': /attempts must be ' . $case->expect_attempts;
        }

        $violations = [...$violations, ...self::pointerViolations($out, $case->expect_preserved ?? (object) [], $name)];
        $violations = [...$violations, ...self::absentViolations($out, (array) ($case->expect_absent_after_reencode ?? []), $name)];

        if ($deadLettered && isset($case->expect_dead_letter)) {
            if (! self::has($out, '/dead_letter')) {
                $violations[] = $name . ': /dead_letter must exist';
            } else {
                foreach (get_object_vars($case->expect_dead_letter) as $field => $expected) {
                    $pointer = '/dead_letter/' . $field;
                    if (! self::has($out, $pointer) || ! self::deepEqual($expected, self::pointer($out, $pointer, $name))) {
                        $violations[] = sprintf('%s: %s must be %s', $name, $pointer, json_encode($expected));
                    }
                }
            }
        }

        return $violations;
    }

    /**
     * Decode a forbidden-key fixture (must warn, naming the key), re-encode it, then the optional
     * `expect_preserved` / `expect_absent_after_reencode` assertions.
     *
     * @return list<string>
     */
    private function forbiddenKeyViolations(\stdClass $case, string $body): array
    {
        $name = (string) $case->name;
        $forbidden = (string) $case->forbidden_key;
        $violations = [];

        /** @var list<array{string, string}> $warnings */
        $warnings = [];
        $envelope = EnvelopeCodec::decode($body, self::collector($warnings));
        if (! EnvelopeCodec::accepts($envelope)) {
            return [$name . ': decode must succeed'];
        }

        $named = array_filter($warnings, static fn (array $w): bool => str_contains($w[0], $forbidden));
        if ($named === []) {
            $violations[] = $name . ': a warning must name ' . $forbidden;
        }

        $out = self::parse(EnvelopeCodec::encode($envelope));

        $violations = [...$violations, ...self::absentViolations($out, (array) ($case->expect_absent_after_reencode ?? []), $name)];
        $violations = [...$violations, ...self::pointerViolations($out, $case->expect_preserved ?? (object) [], $name)];

        $before = self::parse($body);
        if (self::pointer($before, '/attempts', $name) !== self::pointer($out, '/attempts', $name)) {
            $violations[] = $name . ': top-level attempts unchanged';
        }

        return $violations;
    }

    /**
     * Every pointer of `$expected` must hold the given value (type-strict) in `$doc`.
     *
     * @return list<string>
     */
    private static function pointerViolations(mixed $doc, \stdClass $expected, string $name): array
    {
        $violations = [];
        foreach (get_object_vars($expected) as $pointer => $value) {
            $pointer = (string) $pointer;
            if (! self::has($doc, $pointer)) {
                $violations[] = sprintf('%s: %s missing, expected %s', $name, $pointer, json_encode($value));
            } elseif (! self::deepEqual($value, self::pointer($doc, $pointer, $name))) {
                $violations[] = sprintf('%s: %s must be %s, got %s', $name, $pointer, json_encode($value), json_encode(self::pointer($doc, $pointer, $name)));
            }
        }

        return $violations;
    }

    /**
     * None of the pointers may exist in `$doc`.
     *
     * @param  list<mixed>  $pointers
     * @return list<string>
     */
    private static function absentViolations(mixed $doc, array $pointers, string $name): array
    {
        $violations = [];
        foreach ($pointers as $pointer) {
            if (self::has($doc, (string) $pointer)) {
                $violations[] = sprintf('%s: %s must be absent after re-encode', $name, $pointer);
            }
        }

        return $violations;
    }


    /** @param  array<string, mixed>|\stdClass  $case */
    private function fixture(array|\stdClass $case): string
    {
        $file = $case instanceof \stdClass ? $case->file : $case['file'];
        $path = self::DIR . '/' . (string) $file;
        self::assertFileExists($path);

        return (string) file_get_contents($path);
    }

    /**
     * @param  array<int, array{string, string}>  $sink
     * @return callable(string, string): void
     */
    private static function collector(array &$sink): callable
    {
        return static function (string $message, string $pointer) use (&$sink): void {
            $sink[] = [$message, $pointer];
        };
    }

    private static function parse(string $json): mixed
    {
        return json_decode($json, false, 512, JSON_THROW_ON_ERROR);
    }

    /** @return list<string> */
    private static function tokens(string $pointer): array
    {
        if ($pointer === '') {
            return [];
        }

        return array_map(
            static fn (string $t): string => str_replace(['~1', '~0'], ['/', '~'], $t),
            array_slice(explode('/', $pointer), 1),
        );
    }

    private static function has(mixed $doc, string $pointer): bool
    {
        foreach (self::tokens($pointer) as $token) {
            if ($doc instanceof \stdClass && property_exists($doc, $token)) {
                $doc = $doc->{$token};
            } elseif (is_array($doc) && ctype_digit($token) && array_key_exists((int) $token, $doc)) {
                $doc = $doc[(int) $token];
            } else {
                return false;
            }
        }

        return true;
    }

    private static function pointer(mixed $doc, string $pointer, string $name): mixed
    {
        self::assertTrue(self::has($doc, $pointer), $name . ': ' . $pointer . ' missing');

        foreach (self::tokens($pointer) as $token) {
            $doc = $doc instanceof \stdClass ? $doc->{$token} : $doc[(int) $token];
        }

        return $doc;
    }

    private static function deepEqual(mixed $a, mixed $b): bool
    {
        if ($a instanceof \stdClass || $b instanceof \stdClass) {
            if (! $a instanceof \stdClass || ! $b instanceof \stdClass) {
                return false;
            }
            $av = get_object_vars($a);
            $bv = get_object_vars($b);
            if (count($av) !== count($bv)) {
                return false;
            }
            foreach ($av as $k => $v) {
                if (! array_key_exists($k, $bv) || ! self::deepEqual($v, $bv[$k])) {
                    return false;
                }
            }

            return true;
        }

        if (is_array($a) || is_array($b)) {
            if (! is_array($a) || ! is_array($b) || count($a) !== count($b)) {
                return false;
            }
            foreach (array_values($a) as $i => $v) {
                if (! self::deepEqual($v, array_values($b)[$i])) {
                    return false;
                }
            }

            return true;
        }

        return $a === $b;
    }
}

/**
 * A {@see \BabelQueue\Contracts\Transport} for the `otel` conformance runners: it keeps the envelope it was handed
 * (also when the publish then fails) and fails on demand.
 */
final class ConformanceSpanTransport implements \BabelQueue\Contracts\Transport
{
    public ?string $payload = null;

    public function __construct(private readonly bool $fail = false)
    {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $this->payload = $payload;
        if ($this->fail) {
            throw new \RuntimeException('transport down');
        }

        return null;
    }
}
