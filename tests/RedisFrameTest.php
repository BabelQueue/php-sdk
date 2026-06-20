<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\RedisTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use Predis\ClientInterface;

/**
 * The Redis out-of-band header frame (ADR-0028): Redis has no native per-message metadata channel,
 * so headers ride a transport-owned `__bq_frame` JSON frame that wraps the bare wire envelope —
 * with byte-for-byte bare back-compat (the frozen envelope never carries the sentinel). No broker.
 */
final class RedisFrameTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        .'"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        .'"schema_version":1,"created_at":1749132727000},"attempts":0}';

    public function test_frame_round_trips_the_body_and_headers(): void
    {
        $headers = ['traceparent' => '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'];

        $value = RedisTransport::frameValue(self::ENVELOPE, $headers);
        [$body, $recovered] = RedisTransport::unframe($value);

        self::assertSame(self::ENVELOPE, $body);
        self::assertSame($headers, $recovered);
        // The frame is the stored value — and the ack handle — so it carries the sentinel, never
        // the bare envelope.
        self::assertNotSame(self::ENVELOPE, $value);
        self::assertStringContainsString('"__bq_frame"', $value);
    }

    public function test_frame_value_stays_bare_when_no_usable_headers(): void
    {
        // A plain publish and publishWithHeaders-without-headers must store byte-identical values.
        self::assertSame(self::ENVELOPE, RedisTransport::frameValue(self::ENVELOPE, []));
        self::assertSame(self::ENVELOPE, RedisTransport::frameValue(self::ENVELOPE, ['' => 'x', 'blank' => '']));
    }

    public function test_bare_value_unframes_to_itself_with_no_headers(): void
    {
        // A bare wire envelope (older / cross-version / plain publish) consumes exactly as before.
        self::assertSame([self::ENVELOPE, []], RedisTransport::unframe(self::ENVELOPE));
        // Non-JSON and JSON-without-the-sentinel are also treated as bare.
        self::assertSame(['not json', []], RedisTransport::unframe('not json'));
        self::assertSame(['{"job":"x"}', []], RedisTransport::unframe('{"job":"x"}'));
        self::assertSame(['', []], RedisTransport::unframe(''));
    }

    public function test_publish_with_headers_rpushes_a_frame_and_keeps_it_as_the_ack_handle(): void
    {
        $headers = ['traceparent' => '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'];
        $expected = RedisTransport::frameValue(self::ENVELOPE, $headers);

        $client = Mockery::mock(ClientInterface::class);
        // The exact stored value is the frame; LREM would later match it byte-for-byte.
        $client->shouldReceive('rpush')->once()->with('orders', [$expected]);

        $transport = new RedisTransport($client, 'default');
        self::assertNull($transport->publishWithHeaders(self::ENVELOPE, $headers, 'orders'));

        // And a consumer reading the stored frame recovers the verbatim envelope + headers.
        [$body, $recovered] = RedisTransport::unframe($expected);
        self::assertSame(self::ENVELOPE, $body);
        self::assertSame($headers, $recovered);
    }

    public function test_publish_with_headers_degrades_to_a_bare_rpush_without_headers(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        // Byte-identical to a plain publish — the bare envelope, not a frame.
        $client->shouldReceive('rpush')->once()->with('orders', [self::ENVELOPE]);

        (new RedisTransport($client, 'default'))->publishWithHeaders(self::ENVELOPE, [], 'orders');
    }

    public function test_publish_with_headers_uses_the_default_queue(): void
    {
        $headers = ['traceparent' => '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'];
        $expected = RedisTransport::frameValue(self::ENVELOPE, $headers);

        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('rpush')->once()->with('default', [$expected]);

        (new RedisTransport($client, 'default'))->publishWithHeaders(self::ENVELOPE, $headers);
    }
}
