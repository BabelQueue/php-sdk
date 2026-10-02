<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\HasTraceId;
use BabelQueue\Contracts\PolyglotJob;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Validation\EnvelopeValidator;
use PHPUnit\Framework\Attributes\DataProvider;
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

    public function test_empty_data_encodes_as_a_json_object(): void
    {
        $json = EnvelopeCodec::encode(EnvelopeCodec::make('urn:babel:orders:created', [], 'orders'));

        self::assertStringContainsString('"data":{}', $json);
        self::assertStringNotContainsString('"data":[]', $json);
    }

    public function test_non_empty_assoc_data_encoding_is_unchanged(): void
    {
        $envelope = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1, 'tags' => []], 'orders');

        self::assertStringContainsString('"data":{"order_id":1,"tags":[]}', EnvelopeCodec::encode($envelope));
    }

    public function test_decode_rejects_list_data_with_a_warning(): void
    {
        $warnings = [];
        $raw = '{"job":"urn:babel:x","trace_id":"t","data":[1,2],"meta":{"schema_version":1},"attempts":0}';

        $envelope = EnvelopeCodec::decode($raw, static function (string $m, string $p) use (&$warnings): void {
            $warnings[] = $p;
        });

        self::assertSame([], $envelope);
        self::assertFalse(EnvelopeCodec::accepts($envelope));
        self::assertSame(['/data'], $warnings);
    }

    public function test_accepts_any_array_data_since_encode_always_writes_an_object(): void
    {
        $envelope = EnvelopeCodec::make('urn:babel:x', [], 'q');
        self::assertTrue(EnvelopeCodec::accepts($envelope));

        // A decoded PHP array cannot tell [1,2] from {"0":1,"1":2}; decode() owns the list verdict.
        $envelope['data'] = [1, 2];
        self::assertTrue(EnvelopeCodec::accepts($envelope));

        $envelope['data'] = 'not-an-object';
        self::assertFalse(EnvelopeCodec::accepts($envelope));
    }

    /**
     * @return array<string, array{string, string}>
     */
    public static function objectDataBodies(): array
    {
        return [
            'index-like keys' => ['{"0":"a","1":"b"}', '{"0":"a","1":"b"}'],
            'single index-like key' => ['{"0":"a"}', '{"0":"a"}'],
            'empty object' => ['{}', '{}'],
            'legacy empty array (php-sdk <= 1.16.0)' => ['[]', '{}'],
        ];
    }

    #[DataProvider('objectDataBodies')]
    public function test_decode_accepts_object_data_by_its_raw_json_shape(string $data, string $reencoded): void
    {
        $warnings = [];
        $raw = '{"job":"urn:babel:x","trace_id":"t","attempts":0,"data":' . $data
            . ',"meta":{"schema_version":1,"id":"m"}}';

        $envelope = EnvelopeCodec::decode($raw, static function (string $m, string $p) use (&$warnings): void {
            $warnings[] = $p;
        });

        self::assertSame([], $warnings);
        self::assertSame('urn:babel:x', EnvelopeCodec::urn($envelope));
        self::assertTrue(EnvelopeCodec::accepts($envelope));
        self::assertNull(EnvelopeValidator::check($envelope));
        self::assertStringContainsString('"data":' . $reencoded . ',', EnvelopeCodec::encode($envelope));
    }

    public function test_decode_rejects_list_data_even_when_nested_objects_have_index_keys(): void
    {
        $warnings = [];
        $raw = '{"job":"urn:babel:x","trace_id":"t","attempts":0,"data":[{"0":"a"}],"meta":{"schema_version":1}}';

        $envelope = EnvelopeCodec::decode($raw, static function (string $m, string $p) use (&$warnings): void {
            $warnings[] = $p;
        });

        self::assertSame([], $envelope);
        self::assertSame(['/data'], $warnings);
    }

    public function test_encode_writes_list_shaped_data_as_an_object(): void
    {
        $envelope = EnvelopeCodec::make('urn:babel:x', [], 'q');
        $envelope['data'] = ['a', 'b'];

        self::assertStringContainsString('"data":{"0":"a","1":"b"}', EnvelopeCodec::encode($envelope));
    }

    public function test_nested_lists_inside_data_stay_json_arrays(): void
    {
        $envelope = EnvelopeCodec::make('urn:babel:x', ['items' => [1, 2]], 'q');

        self::assertStringContainsString('"data":{"items":[1,2]}', EnvelopeCodec::encode($envelope));
    }

    public function test_decode_drops_forbidden_keys_via_the_global_warning_handler(): void
    {
        $warnings = [];
        EnvelopeCodec::setWarningHandler(static function (string $m, string $p) use (&$warnings): void {
            $warnings[] = $m;
        });

        try {
            $envelope = EnvelopeCodec::decode(
                '{"job":"urn:babel:x","trace_id":"t","data":{},"meta":{"schema_version":1,"ts":1},"attempts":0,"timestamp":1}',
            );
        } finally {
            EnvelopeCodec::setWarningHandler(null);
        }

        self::assertArrayNotHasKey('timestamp', $envelope);
        self::assertArrayNotHasKey('ts', $envelope['meta']);
        self::assertCount(2, $warnings);
        self::assertStringContainsString('/timestamp', $warnings[0]);
        self::assertStringContainsString('/meta/ts', $warnings[1]);
        self::assertTrue(EnvelopeCodec::accepts($envelope));
    }

    public function test_encode_and_decode_warnings_say_what_actually_happened(): void
    {
        $warnings = [];
        EnvelopeCodec::setWarningHandler(static function (string $m) use (&$warnings): void {
            $warnings[] = $m;
        });

        try {
            $envelope = EnvelopeCodec::make('urn:babel:x', [], 'q');
            $envelope['timestamp'] = 1;
            EnvelopeCodec::encode($envelope);
            EnvelopeCodec::decode('{"job":"urn:babel:x","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0,"timestamp":1}');
        } finally {
            EnvelopeCodec::setWarningHandler(null);
        }

        self::assertCount(2, $warnings);
        self::assertStringContainsString('it was not emitted', $warnings[0]);
        self::assertStringContainsString('it was dropped from the decoded envelope', $warnings[1]);
    }

    public function test_warnings_fall_back_to_error_log_without_any_handler(): void
    {
        $log = tempnam(sys_get_temp_dir(), 'bq-log');
        self::assertIsString($log);
        $previous = ini_set('error_log', $log);

        try {
            EnvelopeCodec::setWarningHandler(null);
            EnvelopeCodec::decode('{"job":"urn:babel:x","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0,"timestamp":1}');
        } finally {
            ini_set('error_log', $previous === false ? '' : $previous);
        }

        $contents = (string) file_get_contents($log);
        unlink($log);

        self::assertStringContainsString('[babelqueue] BabelQueue envelope carries the forbidden key /timestamp', $contents);
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

    public function test_make_builds_the_canonical_envelope_from_urn_and_data(): void
    {
        $payload = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 7], 'orders');

        $this->assertSame(['job', 'trace_id', 'data', 'meta', 'attempts'], array_keys($payload));
        $this->assertSame('urn:babel:orders:created', $payload['job']);
        $this->assertSame(['order_id' => 7], $payload['data']);
        $this->assertSame('orders', $payload['meta']['queue']);
        $this->assertSame('php', $payload['meta']['lang']);
        $this->assertSame(1, $payload['meta']['schema_version']);
        $this->assertSame(0, $payload['attempts']);
        $this->assertMatchesRegularExpression(self::UUID_PATTERN, $payload['trace_id']);
        $this->assertMatchesRegularExpression(self::UUID_PATTERN, $payload['meta']['id']);
    }

    public function test_make_honours_inherited_trace_id_and_defaults_the_queue(): void
    {
        $payload = EnvelopeCodec::make('urn:babel:orders:created', [], traceId: 'trace-xyz');

        $this->assertSame('trace-xyz', $payload['trace_id']);
        $this->assertSame('default', $payload['meta']['queue']);
    }

    public function test_make_rejects_an_empty_urn(): void
    {
        $this->expectException(BabelQueueException::class);

        EnvelopeCodec::make('   ', ['a' => 1]);
    }

    public function test_from_job_and_make_produce_the_same_shape(): void
    {
        $fromJob = EnvelopeCodec::fromJob(new OrderJobStub(), 'orders');
        $fromMake = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1042], 'orders');

        // Identical structure + invariant fields (ids/trace/timestamp are per-message).
        $this->assertSame(array_keys($fromJob), array_keys($fromMake));
        $this->assertSame($fromJob['job'], $fromMake['job']);
        $this->assertSame($fromJob['data'], $fromMake['data']);
        $this->assertSame($fromJob['meta']['queue'], $fromMake['meta']['queue']);
        $this->assertSame($fromJob['meta']['lang'], $fromMake['meta']['lang']);
        $this->assertSame($fromJob['meta']['schema_version'], $fromMake['meta']['schema_version']);
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
