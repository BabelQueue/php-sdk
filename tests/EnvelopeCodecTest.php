<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\HasTraceId;
use BabelQueue\Contracts\PolyglotJob;
use BabelQueue\Exceptions\BabelQueueException;
use PHPUnit\Framework\TestCase;

/**
 * The producer side of the wire contract. Asserts the canonical envelope shape
 * (incl. required top-level trace_id) and that no forbidden/legacy fields leak.
 */
final class EnvelopeCodecTest extends TestCase
{
    private const UUID_PATTERN = '/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i';

    public function test_envelope_has_the_canonical_shape(): void
    {
        $payload = EnvelopeCodec::fromJob(new OrderJobStub(), 'orders');

        $this->assertSame(['job', 'trace_id', 'data', 'meta', 'attempts'], array_keys($payload));
        $this->assertSame('urn:babel:orders:created', $payload['job']);
        $this->assertSame(['order_id' => 1042], $payload['data']);
        $this->assertSame('orders', $payload['meta']['queue']);
        $this->assertSame('php', $payload['meta']['lang']);
        $this->assertSame(EnvelopeCodec::SCHEMA_VERSION, $payload['meta']['schema_version']);
        $this->assertIsInt($payload['meta']['created_at']);
        $this->assertSame(0, $payload['attempts']);
    }

    public function test_forbidden_legacy_fields_are_absent(): void
    {
        $payload = EnvelopeCodec::fromJob(new OrderJobStub(), 'orders');

        $this->assertArrayNotHasKey('timestamp', $payload);
        $this->assertArrayNotHasKey('max_retries', $payload['meta']);
        $this->assertArrayNotHasKey('attempts', $payload['meta']);
        $this->assertArrayNotHasKey('source', $payload['meta']);
        $this->assertArrayNotHasKey('ts', $payload['meta']);
    }

    public function test_trace_id_is_a_generated_uuid_distinct_from_meta_id(): void
    {
        $payload = EnvelopeCodec::fromJob(new OrderJobStub(), 'orders');

        $this->assertMatchesRegularExpression(self::UUID_PATTERN, $payload['trace_id']);
        $this->assertMatchesRegularExpression(self::UUID_PATTERN, $payload['meta']['id']);
        $this->assertNotSame($payload['trace_id'], $payload['meta']['id']);
    }

    public function test_each_message_gets_fresh_ids(): void
    {
        $a = EnvelopeCodec::fromJob(new OrderJobStub(), 'orders');
        $b = EnvelopeCodec::fromJob(new OrderJobStub(), 'orders');

        $this->assertNotSame($a['trace_id'], $b['trace_id']);
        $this->assertNotSame($a['meta']['id'], $b['meta']['id']);
    }

    public function test_inherited_trace_id_is_preserved(): void
    {
        $payload = EnvelopeCodec::fromJob(new TracedJobStub('11111111-2222-3333-4444-555555555555'), 'orders');

        $this->assertSame('11111111-2222-3333-4444-555555555555', $payload['trace_id']);
    }

    public function test_blank_inherited_trace_id_falls_back_to_generation(): void
    {
        $payload = EnvelopeCodec::fromJob(new TracedJobStub('   '), 'orders');

        $this->assertMatchesRegularExpression(self::UUID_PATTERN, $payload['trace_id']);
    }

    public function test_empty_urn_throws(): void
    {
        $this->expectException(BabelQueueException::class);

        EnvelopeCodec::fromJob(new BlankUrnJobStub(), 'orders');
    }

    public function test_encode_decode_round_trips(): void
    {
        $payload = EnvelopeCodec::fromJob(new OrderJobStub(), 'orders');

        $json = EnvelopeCodec::encode($payload);

        $this->assertSame($payload, EnvelopeCodec::decode($json));
        $this->assertStringContainsString('"trace_id"', $json);
    }

    public function test_decode_of_malformed_json_is_empty(): void
    {
        $this->assertSame([], EnvelopeCodec::decode('not-json'));
    }

    public function test_output_matches_the_golden_fixture_shape(): void
    {
        $fixture = json_decode((string) file_get_contents(__DIR__ . '/fixtures/order-created.json'), true);
        $payload = EnvelopeCodec::fromJob(new OrderJobStub(), $fixture['meta']['queue']);

        // Same keys/structure (values like ids/timestamps are intrinsically per-message).
        $this->assertSame(array_keys($fixture), array_keys($payload));
        $this->assertSame(array_keys($fixture['meta']), array_keys($payload['meta']));
        $this->assertSame($fixture['job'], $payload['job']);
        $this->assertSame($fixture['data'], $payload['data']);
        $this->assertSame($fixture['meta']['lang'], $payload['meta']['lang']);
    }

    public function test_accepts_validates_consumer_envelopes(): void
    {
        $valid = [
            'job' => 'urn:babel:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 1042],
            'meta' => ['id' => 'm1', 'schema_version' => EnvelopeCodec::SCHEMA_VERSION],
            'attempts' => 0,
        ];
        $this->assertTrue(EnvelopeCodec::accepts($valid));

        // The "urn" alias is accepted on the consumer side.
        $aliased = ['urn' => 'urn:babel:orders:created'] + $valid;
        unset($aliased['job']);
        $this->assertTrue(EnvelopeCodec::accepts($aliased));

        $this->assertFalse(EnvelopeCodec::accepts(['trace_id' => 't', 'data' => [], 'meta' => ['schema_version' => 1], 'attempts' => 0]), 'missing URN');
        $this->assertFalse(EnvelopeCodec::accepts(['job' => 'u', 'trace_id' => 't', 'data' => [], 'attempts' => 0]), 'missing meta');
        $this->assertFalse(EnvelopeCodec::accepts(['job' => 'u', 'trace_id' => 't', 'data' => [], 'meta' => ['schema_version' => 2], 'attempts' => 0]), 'unsupported schema_version');
        $this->assertFalse(EnvelopeCodec::accepts(['job' => 'u', 'trace_id' => 't', 'data' => 'x', 'meta' => ['schema_version' => 1], 'attempts' => 0]), 'non-object data');
        $this->assertFalse(EnvelopeCodec::accepts(['job' => 'u', 'trace_id' => 't', 'data' => [], 'meta' => ['schema_version' => 1], 'attempts' => '0']), 'non-integer attempts');
        $this->assertFalse(EnvelopeCodec::accepts(['job' => 'u', 'trace_id' => '', 'data' => [], 'meta' => ['schema_version' => 1], 'attempts' => 0]), 'blank trace_id');
    }
}

class OrderJobStub implements PolyglotJob
{
    public function getBabelUrn(): string
    {
        return 'urn:babel:orders:created';
    }

    /** @return array<string, mixed> */
    public function toPayload(): array
    {
        return ['order_id' => 1042];
    }
}

class TracedJobStub implements PolyglotJob, HasTraceId
{
    public function __construct(private ?string $traceId)
    {
    }

    public function getBabelUrn(): string
    {
        return 'urn:babel:orders:created';
    }

    /** @return array<string, mixed> */
    public function toPayload(): array
    {
        return ['order_id' => 1042];
    }

    public function getBabelTraceId(): ?string
    {
        return $this->traceId;
    }
}

class BlankUrnJobStub implements PolyglotJob
{
    public function getBabelUrn(): string
    {
        return '   ';
    }

    /** @return array<string, mixed> */
    public function toPayload(): array
    {
        return [];
    }
}
