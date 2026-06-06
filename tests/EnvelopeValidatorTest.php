<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Validation\EnvelopeValidator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The consumer-side gate: every accept path of EnvelopeCodec, plus the *reason*
 * each rejection reports so a framework-less consumer can quarantine vs. drop.
 */
final class EnvelopeValidatorTest extends TestCase
{
    /**
     * @return array<string, mixed>
     */
    private static function base(): array
    {
        return [
            'job' => 'urn:babel:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 1042],
            'meta' => [
                'id' => 'msg-1',
                'queue' => 'orders',
                'lang' => 'php',
                'schema_version' => 1,
                'created_at' => 1749132727000,
            ],
            'attempts' => 0,
        ];
    }

    public function test_a_canonical_envelope_is_valid(): void
    {
        $this->assertNull(EnvelopeValidator::check(self::base()));
        $this->assertTrue(EnvelopeValidator::isValid(self::base()));
    }

    public function test_it_accepts_the_urn_inbound_alias(): void
    {
        $envelope = self::base();
        unset($envelope['job']);
        $envelope = ['urn' => 'urn:babel:orders:created'] + $envelope;

        $this->assertTrue(EnvelopeValidator::isValid($envelope));
    }

    /**
     * @return array<string, array{0: array<string, mixed>, 1: string}>
     */
    public static function rejections(): array
    {
        $missingUrn = self::base();
        $missingUrn['job'] = null;

        $missingMeta = self::base();
        $missingMeta['meta'] = null;

        $badVersion = self::base();
        $badVersion['meta']['schema_version'] = 2;

        $badData = self::base();
        $badData['data'] = 'not-an-object';

        $blankTrace = self::base();
        $blankTrace['trace_id'] = '';

        $badAttempts = self::base();
        $badAttempts['attempts'] = '0';

        return [
            'missing urn' => [$missingUrn, EnvelopeValidator::REASON_MISSING_URN],
            'missing meta' => [$missingMeta, EnvelopeValidator::REASON_MISSING_META],
            'unknown schema version' => [$badVersion, EnvelopeValidator::REASON_UNSUPPORTED_SCHEMA_VERSION],
            'non-object data' => [$badData, EnvelopeValidator::REASON_INVALID_DATA],
            'blank trace id' => [$blankTrace, EnvelopeValidator::REASON_MISSING_TRACE_ID],
            'non-integer attempts' => [$badAttempts, EnvelopeValidator::REASON_INVALID_ATTEMPTS],
        ];
    }

    /**
     * @param  array<string, mixed>  $envelope
     */
    #[DataProvider('rejections')]
    public function test_check_reports_the_first_violation(array $envelope, string $reason): void
    {
        $this->assertSame($reason, EnvelopeValidator::check($envelope));
        $this->assertFalse(EnvelopeValidator::isValid($envelope));
    }

    public function test_validate_throws_carrying_the_reason_and_envelope(): void
    {
        $envelope = self::base();
        $envelope['meta']['schema_version'] = 2;

        try {
            EnvelopeValidator::validate($envelope);
            $this->fail('Expected an InvalidEnvelopeException.');
        } catch (InvalidEnvelopeException $e) {
            $this->assertSame(EnvelopeValidator::REASON_UNSUPPORTED_SCHEMA_VERSION, $e->reason());
            $this->assertSame($envelope, $e->envelope());
        }
    }

    public function test_validate_passes_a_valid_envelope_silently(): void
    {
        EnvelopeValidator::validate(self::base());

        $this->expectNotToPerformAssertions();
    }

    public function test_unsupported_schema_version_is_flagged_for_quarantine(): void
    {
        $newer = self::base();
        $newer['meta']['schema_version'] = 99;

        $this->assertTrue(EnvelopeValidator::isUnsupportedSchemaVersion($newer));
        $this->assertFalse(EnvelopeValidator::isUnsupportedSchemaVersion(self::base()));
    }
}
