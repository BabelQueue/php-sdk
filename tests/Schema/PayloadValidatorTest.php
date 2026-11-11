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

    public function test_union_type_accepts_any_listed_type(): void
    {
        $schema = ['type' => ['string', 'null']];

        self::assertNull(PayloadValidator::check($schema, 'abc'));
        self::assertNull(PayloadValidator::check($schema, null));
        self::assertNotNull(PayloadValidator::check($schema, 5));
        self::assertNotNull(PayloadValidator::check($schema, false));
    }

    public function test_integer_accepts_integral_floats_only(): void
    {
        foreach ([1, 1.0, 100.0, 0.0, -3.0] as $value) {
            self::assertNull(PayloadValidator::check(['type' => 'integer'], $value), var_export($value, true));
        }
        foreach ([1.5, true, '1', INF, NAN] as $value) {
            self::assertNotNull(PayloadValidator::check(['type' => 'integer'], $value), var_export($value, true));
        }
        self::assertNull(PayloadValidator::check(['type' => ['integer', 'null']], 1.0));
        self::assertNotNull(PayloadValidator::check(['type' => ['integer', 'boolean']], '1'));
    }

    public function test_keywords_of_the_matched_union_type_still_apply(): void
    {
        $age = ['type' => ['integer', 'null'], 'minimum' => 0];
        self::assertNull(PayloadValidator::check($age, null));
        self::assertNotNull(PayloadValidator::check($age, -1));

        $obj = ['type' => ['object', 'null'], 'required' => ['a'], 'properties' => ['a' => ['type' => 'string']]];
        self::assertNull(PayloadValidator::check($obj, null));
        self::assertNull(PayloadValidator::check($obj, ['a' => 'x']));
        self::assertStringContainsString('a', (string) PayloadValidator::check($obj, []));
        self::assertNotNull(PayloadValidator::check($obj, ['a' => 1]));

        $text = ['type' => ['string', 'null'], 'minLength' => 2, 'maxLength' => 3];
        self::assertNull(PayloadValidator::check($text, null));
        self::assertNotNull(PayloadValidator::check($text, 'a'));
        self::assertNotNull(PayloadValidator::check($text, 'abcd'));
    }

    public function test_union_of_unknown_names_constrains_nothing(): void
    {
        self::assertNull(PayloadValidator::check(['type' => ['nope']], 1));
        self::assertNull(PayloadValidator::check(['type' => []], 1));
        self::assertNotNull(PayloadValidator::check(['type' => ['nope', 'string']], 1));
    }

    public function test_non_finite_length_bounds_are_ignored(): void
    {
        // A NaN / INF bound never changes a verdict (a raw (int) cast would make it 0 and reject "a").
        foreach (['minLength', 'maxLength'] as $keyword) {
            foreach ([NAN, INF, -INF] as $bound) {
                $schema = ['type' => 'string', $keyword => $bound];
                foreach (['', 'a', 'abc'] as $text) {
                    self::assertNull(PayloadValidator::check($schema, $text), $keyword . ' ' . var_export($bound, true));
                }
            }
        }
    }

    public function test_fractional_length_bounds_are_truncated_toward_zero(): void
    {
        self::assertNull(PayloadValidator::check(['type' => 'string', 'minLength' => 2.5], 'ab'));
        self::assertNotNull(PayloadValidator::check(['type' => 'string', 'minLength' => 2.5], 'a'));
        self::assertNull(PayloadValidator::check(['type' => 'string', 'maxLength' => 2.9], 'ab'));
        self::assertNotNull(PayloadValidator::check(['type' => 'string', 'maxLength' => 2.9], 'abc'));
        // A negative fractional bound truncates to 0.
        self::assertNull(PayloadValidator::check(['type' => 'string', 'maxLength' => -0.5], ''));
        self::assertNotNull(PayloadValidator::check(['type' => 'string', 'maxLength' => -0.5], 'a'));
    }

    public function test_max_length_counts_code_points(): void
    {
        $schema = ['type' => 'string', 'maxLength' => 3];
        self::assertNull(PayloadValidator::check($schema, str_repeat("\u{1F600}", 3)));
        self::assertNotNull(PayloadValidator::check($schema, str_repeat("\u{1F600}", 4)));
        self::assertNotNull(PayloadValidator::check(['type' => 'string', 'maxLength' => 1], "e\u{0301}"));
    }

    public function test_const_and_enum_compare_numbers_by_value_across_int_and_float(): void
    {
        self::assertNull(PayloadValidator::check(['const' => 1], 1.0));
        self::assertNull(PayloadValidator::check(['const' => 1.0], 1));
        self::assertNull(PayloadValidator::check(['const' => 2.5], 2.50));
        self::assertNull(PayloadValidator::check(['enum' => [1, 2.5]], 1.0));
        self::assertNotNull(PayloadValidator::check(['const' => 1], 1.5));
        self::assertNotNull(PayloadValidator::check(['enum' => [1, 2.5]], 3));
        // above 2^53 an int and its nearest float are not the same number
        self::assertNotNull(PayloadValidator::check(['const' => 9007199254740993], 9007199254740992.0));
        self::assertNull(PayloadValidator::check(['const' => 9007199254740992], 9007199254740992.0));
    }

    public function test_booleans_strings_and_null_stay_apart_from_numbers(): void
    {
        self::assertNotNull(PayloadValidator::check(['const' => 1], true));
        self::assertNotNull(PayloadValidator::check(['const' => true], 1));
        self::assertNotNull(PayloadValidator::check(['const' => false], 0));
        self::assertNotNull(PayloadValidator::check(['const' => 1], '1'));
        self::assertNotNull(PayloadValidator::check(['const' => 1], null));
        self::assertNotNull(PayloadValidator::check(['const' => null], 0));
        self::assertNotNull(PayloadValidator::check(['const' => null], ''));
        self::assertNull(PayloadValidator::check(['const' => null], null));
        self::assertNotNull(PayloadValidator::check(['enum' => [true, null, 0]], 1));
        self::assertNull(PayloadValidator::check(['enum' => [true, null, 0]], 0.0));
    }

    public function test_equality_is_recursive_and_key_order_free(): void
    {
        self::assertNull(PayloadValidator::check(['const' => ['a' => [1]]], ['a' => [1.0]]));
        self::assertNotNull(PayloadValidator::check(['const' => ['a' => [1]]], ['a' => [true]]));
        self::assertNotNull(PayloadValidator::check(['const' => ['a' => [1]]], ['a' => [1, 2]]));
        self::assertNotNull(PayloadValidator::check(['const' => ['a' => [1]]], ['a' => [1], 'b' => 1]));
        self::assertNotNull(PayloadValidator::check(['const' => ['a' => [1, 2]]], ['a' => [2, 1]]));
        self::assertNull(PayloadValidator::check(
            ['const' => ['a' => 1, 'b' => [2, ['c' => 3]]]],
            ['b' => [2.0, ['c' => 3.0]], 'a' => 1.0],
        ));
        self::assertNull(PayloadValidator::check(['enum' => [['n' => 1], ['n' => 2]]], ['n' => 1.0]));
        self::assertNotNull(PayloadValidator::check(['enum' => [['n' => 1], ['n' => 2]]], ['n' => '1']));
    }

    public function test_unsupported_keywords_are_listed_per_occurrence_with_pointers(): void
    {
        $schema = [
            'type' => 'object',
            'pattern' => 'x',
            'properties' => [
                'a/b' => ['type' => 'string', 'pattern' => 'x'],
                'm~n' => ['type' => 'string', 'format' => 'x'],
                'list' => ['type' => 'array', 'items' => ['type' => 'integer', 'multipleOf' => 2]],
            ],
            'additionalProperties' => ['type' => 'string', 'maxItems' => 1],
        ];

        $got = array_map(static fn (array $w): string => $w['pointer'] . ' ' . $w['keyword'], PayloadValidator::unsupportedKeywords($schema));
        sort($got);
        self::assertSame(
            [' pattern', '/additionalProperties maxItems', '/properties/a~1b pattern', '/properties/list/items multipleOf', '/properties/m~0n format'],
            $got,
        );
    }

    public function test_annotations_and_property_names_never_warn(): void
    {
        $schema = [
            '$schema' => 'http://json-schema.org/draft-07/schema#', '$id' => 'x', 'title' => 't',
            'description' => 'd', 'examples' => [], 'default' => [], '$comment' => 'c', 'x-owner' => 'billing',
            'type' => 'object',
            'properties' => [
                'title' => ['type' => 'string', 'x-gdpr-sensitive' => true],
                'pattern' => ['type' => 'string'],
                'format' => ['type' => 'string'],
            ],
        ];

        self::assertSame([], PayloadValidator::unsupportedKeywords($schema));
    }

    public function test_an_unsupported_container_is_one_warning_and_is_not_descended(): void
    {
        $schema = ['type' => 'object', 'allOf' => [['pattern' => 'x']], 'definitions' => ['s' => ['format' => 'x']]];

        $got = array_map(static fn (array $w): string => $w['keyword'], PayloadValidator::unsupportedKeywords($schema));
        sort($got);
        self::assertSame(['allOf', 'definitions'], $got);
    }

    public function test_unsupported_keywords_do_not_change_the_verdict(): void
    {
        $schema = ['type' => 'string', 'pattern' => '^a', 'format' => 'email', 'maximum' => 1];
        self::assertNull(PayloadValidator::check($schema, 'nope'));
        self::assertNotNull(PayloadValidator::check($schema, 12));
    }

    public function test_the_keyword_walk_does_not_descend_into_json_lists(): void
    {
        $schema = [
            'type' => 'object',
            'properties' => [
                // tuple-style items: a list of schemas, never walked
                'tuple' => ['type' => 'array', 'items' => [['type' => 'string', 'pattern' => 'x'], ['format' => 'y']]],
                // a list where a schema object belongs
                'listy' => ['type' => 'array', 'additionalProperties' => [['pattern' => 'x']]],
                'real' => ['type' => 'string', 'format' => 'email'],
            ],
        ];
        $got = array_map(static fn (array $w): string => $w['pointer'] . ' ' . $w['keyword'], PayloadValidator::unsupportedKeywords($schema));
        self::assertSame(['/properties/real format'], $got);

        // a list-valued `properties` is skipped as a whole
        self::assertSame([], PayloadValidator::unsupportedKeywords(['type' => 'object', 'properties' => [['pattern' => 'x']]]));
    }
}
