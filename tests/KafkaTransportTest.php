<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Transport\KafkaProducer;
use BabelQueue\Transport\KafkaTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * The framework-less Kafka producer: the record value is the canonical envelope, the timestamp
 * mirrors meta.created_at, and the §6 `bq-` headers (UTF-8 strings) let a non-PHP consumer route
 * on `bq-job` without decoding the body. Decoupled from `ext-rdkafka` behind the KafkaProducer seam.
 */
final class KafkaTransportTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        .'"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        .'"schema_version":1,"created_at":1749132727000},"attempts":0}';

    public function test_publish_produces_value_timestamp_and_bq_headers(): void
    {
        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders',
            self::ENVELOPE,
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured['headers'] = $headers;

                return true;
            }),
            1749132727000,
        );

        $id = (new KafkaTransport($producer, 'orders'))->publish(self::ENVELOPE);

        $this->assertSame('msg-1', $id);
        $this->assertSame('urn:babel:orders:created', $captured['headers']['bq-job']);
        $this->assertSame('trace-1', $captured['headers']['bq-trace-id']);
        $this->assertSame('msg-1', $captured['headers']['bq-message-id']);
        $this->assertSame('1', $captured['headers']['bq-schema-version']);
        $this->assertSame('php', $captured['headers']['bq-source-lang']);
        $this->assertSame('0', $captured['headers']['bq-attempts']);
    }

    public function test_publish_applies_default_topic_and_prefix(): void
    {
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with('app.orders', Mockery::any(), Mockery::any(), Mockery::any());

        (new KafkaTransport($producer, 'orders', 'app.'))->publish(self::ENVELOPE);

        $this->addToAssertionCount(1);
    }

    public function test_publish_overrides_topic_and_returns_null_without_meta_id(): void
    {
        $envelope = '{"job":"urn:babel:x","trace_id":"","data":{},"meta":{"queue":"q",'
            .'"lang":"php","schema_version":1},"attempts":3}';

        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'other',
            $envelope,
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured['headers'] = $headers;

                return true;
            }),
            null,   // no created_at → broker time
        );

        $id = (new KafkaTransport($producer, 'orders'))->publish($envelope, 'other');

        $this->assertNull($id);
        $this->assertSame('3', $captured['headers']['bq-attempts']);
        $this->assertArrayNotHasKey('bq-trace-id', $captured['headers']); // empty trace_id → no header
    }

    public function test_it_is_a_header_publisher(): void
    {
        $this->assertInstanceOf(HeaderPublisher::class, new KafkaTransport(Mockery::mock(KafkaProducer::class)));
    }

    /**
     * ADR-0028 / R1-O3: the rider is written as a Kafka record header beside the `bq-` projection and never
     * enters the record value (GR-1).
     */
    public function test_publish_with_headers_carries_the_traceparent_beside_the_bq_projection_and_not_in_the_body(): void
    {
        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders',
            Mockery::on(function (string $payload) use (&$captured): bool {
                $captured['payload'] = $payload;

                return true;
            }),
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured['headers'] = $headers;

                return true;
            }),
            1749132727000,
        );

        $traceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
        $id = (new KafkaTransport($producer, 'orders'))->publishWithHeaders(
            self::ENVELOPE,
            ['traceparent' => $traceparent, 'tracestate' => 'vendor=1'],
        );

        $this->assertSame('msg-1', $id);
        $this->assertSame($traceparent, $captured['headers']['traceparent']);
        $this->assertSame('vendor=1', $captured['headers']['tracestate']);
        // The §6 projection is still there, with the same keys and values as a plain publish.
        $this->assertSame('urn:babel:orders:created', $captured['headers']['bq-job']);
        $this->assertSame('trace-1', $captured['headers']['bq-trace-id']);
        $this->assertSame('msg-1', $captured['headers']['bq-message-id']);
        $this->assertSame('1', $captured['headers']['bq-schema-version']);
        $this->assertSame('php', $captured['headers']['bq-source-lang']);
        $this->assertSame('0', $captured['headers']['bq-attempts']);
        // The record value is the envelope verbatim and has no rider (GR-1).
        $this->assertSame(self::ENVELOPE, $captured['payload']);
        $this->assertStringNotContainsString('traceparent', $captured['payload']);
    }

    public function test_publish_with_headers_lets_the_bq_projection_win_a_key_collision_and_drops_blank_riders(): void
    {
        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders',
            self::ENVELOPE,
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured['headers'] = $headers;

                return true;
            }),
            1749132727000,
        );

        (new KafkaTransport($producer, 'orders'))->publishWithHeaders(
            self::ENVELOPE,
            ['bq-job' => 'urn:evil:override', 'x-empty' => '', '' => 'no-name', 'x-kept' => 'yes'],
        );

        $this->assertSame('urn:babel:orders:created', $captured['headers']['bq-job']);
        $this->assertSame('yes', $captured['headers']['x-kept']);
        $this->assertArrayNotHasKey('x-empty', $captured['headers']);
        $this->assertArrayNotHasKey('', $captured['headers']);
    }

    public function test_publish_with_no_headers_is_exactly_a_plain_publish(): void
    {
        $plain = [];
        $withNone = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->twice()->andReturnUsing(
            function (string $topic, string $payload, array $headers, ?int $timestamp) use (&$plain, &$withNone): void {
                if ($plain === []) {
                    $plain = [$topic, $payload, $headers, $timestamp];
                } else {
                    $withNone = [$topic, $payload, $headers, $timestamp];
                }
            },
        );

        $transport = new KafkaTransport($producer, 'orders');
        $transport->publish(self::ENVELOPE);
        $transport->publishWithHeaders(self::ENVELOPE, []);

        $this->assertSame($plain, $withNone);
    }

    /**
     * Regression: the eight reserved `bq-` contract headers never come from the carrier, also the ones the
     * projection does not write on this record (`bq-delay`, `bq-original-topic`, and the conditional ones). A rider
     * naming `bq-original-topic` would otherwise redirect the record's retry chain to another work topic.
     */
    public function test_publish_with_headers_never_takes_a_reserved_bq_header_from_the_carrier(): void
    {
        // No trace_id, no meta.id, no lang, no schema_version: the projection writes only bq-job and bq-attempts.
        $envelope = '{"job":"urn:babel:x","trace_id":"","data":{},"meta":{"queue":"q"},"attempts":0}';
        $reserved = [
            'bq-job' => 'urn:evil:job', 'bq-trace-id' => 'evil-trace', 'bq-message-id' => 'evil-id',
            'bq-schema-version' => '9', 'bq-source-lang' => 'evil', 'bq-attempts' => '99',
            'bq-delay' => '60000', 'bq-original-topic' => 'payments',
        ];

        $captured = [];
        $producer = Mockery::mock(KafkaProducer::class);
        $producer->shouldReceive('produce')->once()->with(
            'orders',
            $envelope,
            Mockery::on(function (array $headers) use (&$captured): bool {
                $captured = $headers;

                return true;
            }),
            null,
        );

        (new KafkaTransport($producer))->publishWithHeaders($envelope, $reserved + ['traceparent' => '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01', 'x-kept' => 'yes'], 'orders');

        $this->assertSame('urn:babel:x', $captured['bq-job']);
        $this->assertSame('0', $captured['bq-attempts']);
        foreach (['bq-trace-id', 'bq-message-id', 'bq-schema-version', 'bq-source-lang', 'bq-delay', 'bq-original-topic'] as $name) {
            $this->assertArrayNotHasKey($name, $captured, $name . ' must not come from the carrier');
        }
        $this->assertSame('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01', $captured['traceparent']);
        $this->assertSame('yes', $captured['x-kept']);
    }
}
