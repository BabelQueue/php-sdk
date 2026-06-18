<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Schema;

use BabelQueue\Schema\PayloadValidator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The per-URN `data` validator (ADR-0024): a subset of Draft-07 tuned for payloads, whose
 * verdicts match the Go `schema` validator and babelqueue-registry's `compat` linter.
 */
final class PayloadValidatorTest extends TestCase
{
    public function test_object_required_types_and_additional_properties(): void
    {
        $schema = (array) json_decode(
            '{"type":"object","required":["order_id"],"properties":{"order_id":{"type":"integer"},"note":{"type":"string","minLength":1}},"additionalProperties":false}',
            true
        );

        self::assertNull(PayloadValidator::check($schema, ['order_id' => 7]));
        self::assertNotNull(PayloadValidator::check($schema, []));
        self::assertNotNull(PayloadValidator::check($schema, ['order_id' => 'x']));
        self::assertNotNull(PayloadValidator::check($schema, ['order_id' => 7, 'extra' => 1]));
        self::assertNotNull(PayloadValidator::check($schema, ['order_id' => 7, 'note' => '']));
    }

    public function test_enum_minimum_and_array_items(): void
    {
        $schema = (array) json_decode(
            '{"type":"object","properties":{"status":{"enum":["new","paid"]},"qty":{"type":"integer","minimum":1},"tags":{"type":"array","items":{"type":"string"}}}}',
            true
        );

        self::assertNull(PayloadValidator::check($schema, ['status' => 'paid', 'qty' => 2, 'tags' => ['a', 'b']]));
        self::assertNotNull(PayloadValidator::check($schema, ['status' => 'cancelled']));
        self::assertNotNull(PayloadValidator::check($schema, ['qty' => 0]));
        self::assertNotNull(PayloadValidator::check($schema, ['tags' => ['a', 1]]));
    }

    #[DataProvider('scalarCases')]
    public function test_scalar_types(string $schemaJson, mixed $value, bool $valid): void
    {
        $schema = (array) json_decode($schemaJson, true);
        $violation = PayloadValidator::check($schema, $value);
        self::assertSame($valid, $violation === null, (string) $violation);
    }

    /**
     * @return array<int, array{0: string, 1: mixed, 2: bool}>
     */
    public static function scalarCases(): array
    {
        return [
            ['{"type":"boolean"}', true, true],
            ['{"type":"boolean"}', 'x', false],
            ['{"type":"null"}', null, true],
            ['{"type":"null"}', 1, false],
            ['{"type":"number","minimum":0.5}', 0.6, true],
            ['{"type":"number","minimum":0.5}', 0.4, false],
            ['{"type":"number"}', 'x', false],
            ['{"type":"string"}', 5, false],
            ['{"type":"integer"}', 1.0, true],
            ['{"type":"integer"}', 1.5, false],
            ['{"const":"v1"}', 'v1', true],
            ['{"const":"v1"}', 'v2', false],
        ];
    }
}
