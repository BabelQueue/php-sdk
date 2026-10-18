<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Idempotency\Idempotent;
use BabelQueue\Idempotency\InMemoryStore;
use BabelQueue\Otel\Tracing;
use BabelQueue\Tests\Support\ConformanceResults;
use BabelQueue\Tests\Support\EnvelopeMessage;
use BabelQueue\Transport\AmqpTransport;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaRetryRouter;
use BabelQueue\Transport\RedisTransport;
use BabelQueue\Transport\SqsClient;
use BabelQueue\Transport\SqsTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use OpenTelemetry\API\Trace\Span;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Predis\ClientInterface;

/**
 * Runs the broker-free sections of the vendored cross-SDK conformance manifest and records every
 * case for `results.json` (ADR-0032): `cases`, `roundtrip`, `data_shape`, `forbidden_keys`,
 * `extension_write`, `encode_reject`, `payload_schema_unicode`, `trim`, `job_type`, `idempotency`,
 * `traceparent`, `redis`, `redis_layout`, `rabbitmq`, `delay` and `backoff`.
 *
 * Re-encoded output is parsed with objects kept as stdClass so the comparison is
 * type-strict ({} is not [], 1 is not 1.0 or true); object key order is ignored.
 *
 * `delay` and `backoff` are recorded as per-case skips: this package's SQS and Kafka transports are
 * publish-only reference producers (ADR-0010) with no release / retry-republish API, which the
 * accepted ADRs scope to the framework workers (ADR-0012) and `KafkaRetryRouter` (ADR-0015, ADR-0019).
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
     * SQS delay (broker-bindings.md section 3.4): the case is run against the real `SqsTransport` and a
     * recording client. The transport has no way to request a delay today (`publish()` takes no
     * `$delaySeconds`), so every case fails an assertion and is listed in `conformance-known-gaps.json`
     * (follow-up: DEVAM.md "Açık işler"). Once the parameter exists the evaluator checks the real
     * `DelaySeconds` / rejection, so a listed case that starts passing is flagged as a stale gap.
     */
    public function test_sqs_delay(): void
    {
        ConformanceResults::each('delay', function (array $case): void {
            self::assertMethodParameter(SqsTransport::class, 'publish', 'delaySeconds', 'the transport cannot be asked for a DelaySeconds');

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

    /**
     * SQS release (broker-bindings.md section 3.5): needs `ChangeMessageVisibility` on the client seam and a
     * release on the transport; neither exists, so the cases are listed known gaps (see {@see test_sqs_delay()}).
     */
    public function test_sqs_release_visibility(): void
    {
        ConformanceResults::each('delay', function (array $case): void {
            self::assertTrue((new \ReflectionClass(SqsClient::class))->hasMethod('changeMessageVisibility'), 'the SqsClient seam has no changeMessageVisibility operation');
            self::assertTrue(method_exists(SqsTransport::class, 'release'), 'SqsTransport has no release()');

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
     * Kafka backoff tiers (broker-bindings.md section 6.4): the case is run against the real
     * `KafkaRetryRouter`. `route()` picks the tier by attempts and takes no requested delay (no
     * `$delayMs`), so every case fails an assertion and is a listed known gap; once the parameter exists the
     * evaluator checks the real topic and headers, so a case that starts passing is flagged as stale.
     */
    public function test_kafka_backoff_tiers(): void
    {
        ConformanceResults::each('backoff', fn (array $case) => $this->evaluateKafkaTier($case, (int) $case['delay_ms'], 0));
    }

    /** Kafka release/retry republish (section 6.5); same evaluator and gap rules as {@see test_kafka_backoff_tiers()}. */
    public function test_kafka_retry_republish(): void
    {
        ConformanceResults::each('backoff', fn (array $case) => $this->evaluateKafkaTier($case, (int) $case['release_delay_ms'], (int) $case['attempts']), 'retry_republish');
    }

    /**
     * @param  array<string, mixed>  $case
     */
    private function evaluateKafkaTier(array $case, int $delayMs, int $attempts): void
    {
        self::assertMethodParameter(KafkaRetryRouter::class, 'route', 'delayMs', 'the router cannot be asked for a delay');

        $sent = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->zeroOrMoreTimes()->andReturnUsing(static function (string $topic, string $payload, array $headers) use (&$sent): void {
            $sent[] = ['topic' => $topic, 'payload' => $payload, 'headers' => $headers];
        });
        $router = new KafkaRetryRouter($producer, $case['retry_tiers_ms']);
        $message = new KafkaMessage([
            'job' => 'urn:acme:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 1],
            'meta' => ['id' => 'msg-1', 'queue' => $case['work_topic'], 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1],
            'attempts' => $attempts,
        ]);

        if ($case['expect'] === 'reject') {
            try {
                $router->route($message, null, delayMs: $delayMs);
                self::fail('the route must raise BabelQueueException');
            } catch (BabelQueueException) {
                self::assertSame([], $sent, 'a rejected delay must not produce anything');
            }

            return;
        }

        $router->route($message, null, delayMs: $delayMs);
        self::assertCount(1, $sent);

        // The tier is only observable through the topic; this SDK numbers `<topic>.retry.<n>` 1-based by ascending delay.
        $tiers = $case['retry_tiers_ms'];
        sort($tiers);
        $tierNumber = array_search($case['expect']['tier_ms'], $tiers, true);
        self::assertNotFalse($tierNumber, 'fixture consistency: tier_ms is one of retry_tiers_ms');
        self::assertSame($case['work_topic'] . '.retry.' . ($tierNumber + 1), $sent[0]['topic'], 'tier topic');
        foreach ($case['expect']['headers'] as $header => $value) {
            self::assertSame($value, $sent[0]['headers'][$header] ?? null, 'header ' . $header);
        }
        if (isset($case['expect']['body_attempts'])) {
            self::assertSame($case['expect']['body_attempts'], EnvelopeCodec::decode($sent[0]['payload'])['attempts'] ?? null, 'body attempts');
        }
    }

    /** Fails an assertion (an expected known gap, never an error) when `$class::$method()` lacks the named parameter. */
    private static function assertMethodParameter(string $class, string $method, string $parameter, string $why): void
    {
        $names = array_map(static fn (\ReflectionParameter $p): string => $p->getName(), (new \ReflectionMethod($class, $method))->getParameters());
        self::assertContains($parameter, $names, $class . '::' . $method . '() has no $' . $parameter . ' parameter: ' . $why);
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
