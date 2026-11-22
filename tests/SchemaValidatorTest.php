<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Validation\SchemaValidator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Offline, producer-strict validation against the *bundled* canonical schema.
 *
 * Where {@see EnvelopeValidatorTest} covers the lenient consumer gate, this
 * pins the strict wire contract: required `job` (no `urn` alias), the full
 * `meta` block, UUID-shaped ids, the `lang` tag pattern, the `schema_version` const,
 * and the optional `dead_letter` block — all without a JSON-Schema library.
 */
final class SchemaValidatorTest extends TestCase
{
    /**
     * A canonical, schema-conformant envelope. UUIDs are real RFC-4122 values
     * because the schema marks `meta.id` and `trace_id` as `format: uuid`.
     *
     * @return array<string, mixed>
     */
    private static function base(): array
    {
        return [
            'job' => 'urn:babel:orders:created',
            'trace_id' => '8f3b2c1d-4e5a-4b6c-8d7e-9f0a1b2c3d4e',
            'data' => ['order_id' => 1042],
            'meta' => [
                'id' => '1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d',
                'queue' => 'orders',
                'lang' => 'php',
                'schema_version' => 1,
                'created_at' => 1749132727000,
            ],
            'attempts' => 0,
        ];
    }

    public function test_a_canonical_envelope_conforms(): void
    {
        $this->assertNull(SchemaValidator::check(self::base()));
        $this->assertTrue(SchemaValidator::isValid(self::base()));
    }

    public function test_the_order_created_fixture_conforms(): void
    {
        /** @var array<string, mixed> $fixture */
        $fixture = json_decode(
            (string) file_get_contents(__DIR__ . '/conformance/fixtures/order-created.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );

        $this->assertNull(SchemaValidator::check($fixture));
    }

    public function test_the_dead_lettered_fixture_conforms(): void
    {
        /** @var array<string, mixed> $fixture */
        $fixture = json_decode(
            (string) file_get_contents(__DIR__ . '/conformance/fixtures/dead-lettered.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );

        $this->assertNull(SchemaValidator::check($fixture));
    }

    public function test_a_valid_dead_letter_block_conforms(): void
    {
        $envelope = self::base();
        $envelope['dead_letter'] = [
            'reason' => 'failed',
            'error' => 'connection reset',
            'exception' => null,
            'failed_at' => 1749132728000,
            'original_queue' => 'orders',
            'attempts' => 3,
            'lang' => 'php',
        ];

        $this->assertNull(SchemaValidator::check($envelope));
    }

    /**
     * Unlike the consumer gate, the schema requires the producer field `job`;
     * the `urn` inbound alias alone does NOT satisfy it.
     */
    public function test_the_urn_alias_alone_does_not_satisfy_the_producer_schema(): void
    {
        $envelope = self::base();
        unset($envelope['job']);
        $envelope = ['urn' => 'urn:babel:orders:created'] + $envelope;

        $this->assertSame('job: ' . SchemaValidator::REASON_MISSING_REQUIRED, SchemaValidator::check($envelope));
    }

    /**
     * @return array<string, array{0: array<string, mixed>, 1: string}>
     */
    public static function violations(): array
    {
        $missingJob = self::base();
        unset($missingJob['job']);

        $emptyJob = self::base();
        $emptyJob['job'] = '';

        $jobNotString = self::base();
        $jobNotString['job'] = 42;

        $traceNotUuid = self::base();
        $traceNotUuid['trace_id'] = 'trace-1';

        $dataNotObject = self::base();
        $dataNotObject['data'] = 'not-an-object';

        $missingMeta = self::base();
        unset($missingMeta['meta']);

        $missingMetaId = self::base();
        unset($missingMetaId['meta']['id']);

        $metaIdNotUuid = self::base();
        $metaIdNotUuid['meta']['id'] = 'msg-1';

        $emptyQueue = self::base();
        $emptyQueue['meta']['queue'] = '';

        $malformedLang = self::base();
        $malformedLang['meta']['lang'] = 'Rust 1';

        $wrongSchemaVersion = self::base();
        $wrongSchemaVersion['meta']['schema_version'] = 2;

        $createdAtNotInt = self::base();
        $createdAtNotInt['meta']['created_at'] = '1749132727000';

        $negativeCreatedAt = self::base();
        $negativeCreatedAt['meta']['created_at'] = -1;

        $attemptsNotInt = self::base();
        $attemptsNotInt['attempts'] = '0';

        $negativeAttempts = self::base();
        $negativeAttempts['attempts'] = -1;

        $badDeadLetterReason = self::base();
        $badDeadLetterReason['dead_letter'] = [
            'reason' => 'whoops',
            'failed_at' => 1749132728000,
            'original_queue' => 'orders',
            'attempts' => 3,
        ];

        $deadLetterMissingField = self::base();
        $deadLetterMissingField['dead_letter'] = [
            'reason' => 'failed',
            'original_queue' => 'orders',
            'attempts' => 3,
        ];

        $malformedDeadLetterLang = self::base();
        $malformedDeadLetterLang['dead_letter'] = [
            'reason' => 'failed',
            'failed_at' => 1749132728000,
            'original_queue' => 'orders',
            'attempts' => 3,
            'lang' => 'Rust 1',
        ];

        return [
            'missing job' => [$missingJob, 'job: ' . SchemaValidator::REASON_MISSING_REQUIRED],
            'empty job' => [$emptyJob, 'job: ' . SchemaValidator::REASON_EMPTY_STRING],
            'job not a string' => [$jobNotString, 'job: ' . SchemaValidator::REASON_WRONG_TYPE],
            'trace_id not a uuid' => [$traceNotUuid, 'trace_id: ' . SchemaValidator::REASON_NOT_UUID],
            'data not an object' => [$dataNotObject, 'data: ' . SchemaValidator::REASON_WRONG_TYPE],
            'missing meta' => [$missingMeta, 'meta: ' . SchemaValidator::REASON_MISSING_REQUIRED],
            'missing meta.id' => [$missingMetaId, 'meta.id: ' . SchemaValidator::REASON_MISSING_REQUIRED],
            'meta.id not a uuid' => [$metaIdNotUuid, 'meta.id: ' . SchemaValidator::REASON_NOT_UUID],
            'empty meta.queue' => [$emptyQueue, 'meta.queue: ' . SchemaValidator::REASON_EMPTY_STRING],
            'malformed meta.lang' => [$malformedLang, 'meta.lang: ' . SchemaValidator::REASON_PATTERN_MISMATCH],
            'malformed dead_letter.lang' => [$malformedDeadLetterLang, 'dead_letter.lang: ' . SchemaValidator::REASON_PATTERN_MISMATCH],
            'wrong schema_version' => [$wrongSchemaVersion, 'meta.schema_version: ' . SchemaValidator::REASON_WRONG_CONST],
            'created_at not an integer' => [$createdAtNotInt, 'meta.created_at: ' . SchemaValidator::REASON_WRONG_TYPE],
            'negative created_at' => [$negativeCreatedAt, 'meta.created_at: ' . SchemaValidator::REASON_BELOW_MINIMUM],
            'attempts not an integer' => [$attemptsNotInt, 'attempts: ' . SchemaValidator::REASON_WRONG_TYPE],
            'negative attempts' => [$negativeAttempts, 'attempts: ' . SchemaValidator::REASON_BELOW_MINIMUM],
            'bad dead_letter reason' => [$badDeadLetterReason, 'dead_letter.reason: ' . SchemaValidator::REASON_NOT_IN_ENUM],
            'dead_letter missing field' => [$deadLetterMissingField, 'dead_letter.failed_at: ' . SchemaValidator::REASON_MISSING_REQUIRED],
        ];
    }

    /**
     * @param  array<string, mixed>  $envelope
     */
    #[DataProvider('violations')]
    public function test_check_reports_the_first_violation_with_its_path(array $envelope, string $expected): void
    {
        $this->assertSame($expected, SchemaValidator::check($envelope));
        $this->assertFalse(SchemaValidator::isValid($envelope));
    }

    /**
     * @return array<string, array{0: string}>
     */
    public static function acceptedLangTags(): array
    {
        return [
            'rust (ADR-0040)' => ['rust'],
            'php' => ['php'],
            'go' => ['go'],
            'python' => ['python'],
            'java' => ['java'],
            'dotnet' => ['dotnet'],
            'node' => ['node'],
            'hyphenated' => ['common-lisp'],
            'digit after the first letter' => ['k8s'],
        ];
    }

    #[DataProvider('acceptedLangTags')]
    public function test_an_open_but_well_formed_meta_lang_conforms(string $lang): void
    {
        $envelope = self::base();
        $envelope['meta']['lang'] = $lang;

        $this->assertNull(SchemaValidator::check($envelope));
    }

    #[DataProvider('acceptedLangTags')]
    public function test_an_open_but_well_formed_dead_letter_lang_conforms(string $lang): void
    {
        $envelope = self::base();
        $envelope['dead_letter'] = [
            'reason' => 'failed',
            'failed_at' => 1749132728000,
            'original_queue' => 'orders',
            'attempts' => 3,
            'lang' => $lang,
        ];

        $this->assertNull(SchemaValidator::check($envelope));
        $this->assertTrue(SchemaValidator::isValid($envelope));
    }

    /**
     * @return array<string, array{0: string}>
     */
    public static function rejectedLangTags(): array
    {
        return [
            'upper case and space' => ['Rust 1'],
            'empty' => [''],
            'leading digit' => ['1go'],
            'leading hyphen' => ['-go'],
            'upper case' => ['PHP'],
            'underscore' => ['dot_net'],
            'trailing newline ($ is not "before a final newline")' => ["rust\n"],
            'embedded newline' => ["ru\nst"],
            'non-ASCII letter' => ['rüst'],
            'invalid UTF-8' => ["rust\xff"],
        ];
    }

    #[DataProvider('rejectedLangTags')]
    public function test_a_malformed_lang_tag_is_a_pattern_violation(string $lang): void
    {
        $envelope = self::base();
        $envelope['meta']['lang'] = $lang;

        $this->assertSame('meta.lang: ' . SchemaValidator::REASON_PATTERN_MISMATCH, SchemaValidator::check($envelope));
        $this->assertFalse(SchemaValidator::isValid($envelope));
    }

    public function test_the_unknown_lang_fixture_conforms_and_malformed_lang_does_not(): void
    {
        $unknown = $this->fixture('unknown-lang');
        $malformed = $this->fixture('malformed-lang');

        $this->assertSame('cobol', $unknown['meta']['lang']);
        $this->assertNull(SchemaValidator::check($unknown));
        $this->assertSame('meta.lang: ' . SchemaValidator::REASON_PATTERN_MISMATCH, SchemaValidator::check($malformed));
    }

    /**
     * The bundled validator must reach the same producer-side verdict as the
     * conformance manifest: a roundtrip case is producer-schema valid unless it
     * says `producer_schema_valid: false`.
     */
    public function test_every_roundtrip_fixture_gets_the_verdict_the_manifest_declares(): void
    {
        /** @var array<string, mixed> $manifest */
        $manifest = json_decode((string) file_get_contents(__DIR__ . '/conformance/manifest.json'), true, 512, JSON_THROW_ON_ERROR);
        /** @var list<array<string, mixed>> $cases */
        $cases = $manifest['roundtrip']['cases'];
        $this->assertNotEmpty($cases);

        foreach ($cases as $case) {
            $fixture = json_decode((string) file_get_contents(__DIR__ . '/conformance/' . $case['file']), true, 512, JSON_THROW_ON_ERROR);
            $this->assertIsArray($fixture);

            $this->assertSame(
                ($case['producer_schema_valid'] ?? true) === true,
                SchemaValidator::isValid($fixture),
                sprintf('roundtrip case "%s": %s', $case['name'], (string) SchemaValidator::check($fixture)),
            );
        }
    }

    public function test_the_bundled_schema_carries_the_lang_pattern_and_no_lang_enum(): void
    {
        $schema = SchemaValidator::schema();

        foreach ([$schema['properties']['meta']['properties']['lang'], $schema['properties']['dead_letter']['properties']['lang']] as $lang) {
            $this->assertSame('^[a-z][a-z0-9-]*$', $lang['pattern']);
            $this->assertArrayNotHasKey('enum', $lang);
        }
    }

    public function test_pattern_is_searched_unanchored_unless_the_pattern_anchors_itself(): void
    {
        $this->assertNull($this->checkWithSchema(['type' => 'string', 'pattern' => 'ab'], 'xxabxx'));
        $this->assertSame('v: ' . SchemaValidator::REASON_PATTERN_MISMATCH, $this->checkWithSchema(['type' => 'string', 'pattern' => '^ab$'], 'xxabxx'));
        $this->assertNull($this->checkWithSchema(['type' => 'string', 'pattern' => '^ab$'], 'ab'));
    }

    public function test_a_pattern_containing_the_first_delimiter_candidate_still_works(): void
    {
        $this->assertNull($this->checkWithSchema(['type' => 'string', 'pattern' => '^a~b$'], 'a~b'));
        $this->assertSame('v: ' . SchemaValidator::REASON_PATTERN_MISMATCH, $this->checkWithSchema(['type' => 'string', 'pattern' => '^a/b$'], 'a-b'));
        $this->assertNull($this->checkWithSchema(['type' => 'string', 'pattern' => '^a/b$'], 'a/b'));
    }

    public function test_pattern_does_not_apply_to_non_strings(): void
    {
        $this->assertSame('v: ' . SchemaValidator::REASON_WRONG_TYPE, $this->checkWithSchema(['type' => 'string', 'pattern' => '^a$'], 5));
    }

    public function test_an_uncompilable_pattern_is_a_schema_defect_and_throws(): void
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('is not a valid regular expression');

        $this->checkWithSchema(['type' => 'string', 'pattern' => '^[a-z'], 'abc');
    }

    public function test_a_pattern_that_cannot_be_delimited_is_a_schema_defect_and_throws(): void
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('cannot be delimited');

        $this->checkWithSchema(['type' => 'string', 'pattern' => '~#%@!`;,'], 'abc');
    }

    /**
     * Validate `$value` as property `v` against a substitute bundled schema whose
     * only property is `$node`, restoring the real schema afterwards.
     *
     * @param  array<string, mixed>  $node
     */
    private function checkWithSchema(array $node, mixed $value): ?string
    {
        $schema = ['type' => 'object', 'properties' => ['v' => $node]];
        $property = new \ReflectionProperty(SchemaValidator::class, 'schema');
        $previous = $property->getValue();
        $property->setValue(null, $schema);

        try {
            return SchemaValidator::check(['v' => $value]);
        } finally {
            $property->setValue(null, $previous);
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function fixture(string $name): array
    {
        /** @var array<string, mixed> */
        return json_decode(
            (string) file_get_contents(__DIR__ . '/conformance/fixtures/' . $name . '.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
    }

    public function test_a_non_object_envelope_is_rejected_at_the_root(): void
    {
        // A list (JSON array) is not a JSON object at the root.
        $this->assertSame('<root>: ' . SchemaValidator::REASON_WRONG_TYPE, SchemaValidator::check(['just', 'a', 'list']));
    }

    public function test_validate_throws_carrying_the_violation_and_envelope(): void
    {
        $envelope = self::base();
        $envelope['meta']['lang'] = 'Rust 1';

        try {
            SchemaValidator::validate($envelope);
            $this->fail('Expected an InvalidEnvelopeException.');
        } catch (InvalidEnvelopeException $e) {
            $this->assertSame('meta.lang: ' . SchemaValidator::REASON_PATTERN_MISMATCH, $e->reason());
            $this->assertSame($envelope, $e->envelope());
        }
    }

    public function test_validate_passes_a_conformant_envelope_silently(): void
    {
        SchemaValidator::validate(self::base());

        $this->expectNotToPerformAssertions();
    }

    public function test_the_bundled_schema_is_the_canonical_envelope_schema(): void
    {
        $schema = SchemaValidator::schema();

        $this->assertSame('BabelQueueMessageEnvelope', $schema['title']);
        $this->assertSame(['job', 'trace_id', 'data', 'meta', 'attempts'], $schema['required']);
    }

    /**
     * Drift guard: the copy bundled under src/ (which ships in the published
     * package) must stay byte-identical to the vendored conformance copy CI
     * diffs against the canonical suite, so the two can never diverge.
     */
    public function test_bundled_schema_matches_the_vendored_conformance_copy(): void
    {
        $bundled = (string) file_get_contents(__DIR__ . '/../src/Validation/message-envelope.schema.json');
        $vendored = (string) file_get_contents(__DIR__ . '/conformance/schema/message-envelope.schema.json');

        $this->assertSame($vendored, $bundled);
    }
}
