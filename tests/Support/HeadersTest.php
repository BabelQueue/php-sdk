<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Support;

use BabelQueue\Support\Headers;
use PHPUnit\Framework\TestCase;

/**
 * The pure out-of-band header helpers (ADR-0028): sanitise (drop blank keys/values, stringify
 * scalars) and merge-not-clobber (later sources win), with no broker.
 */
final class HeadersTest extends TestCase
{
    public function test_sanitize_drops_blank_keys_and_values_and_stringifies_scalars(): void
    {
        $clean = Headers::sanitize([
            'traceparent' => '00-abc-def-01',
            '' => 'no-key',     // blank key dropped
            'blank' => '',      // blank value dropped
            'count' => 7,       // int stringified
            'flag' => true,     // true → '1'
            'off' => false,     // false → '0' (a non-blank value, so it is kept)
            'obj' => new \stdClass(), // non-scalar dropped
        ]);

        self::assertSame(
            ['traceparent' => '00-abc-def-01', 'count' => '7', 'flag' => '1', 'off' => '0'],
            $clean,
        );
    }

    public function test_sanitize_returns_empty_when_nothing_survives(): void
    {
        self::assertSame([], Headers::sanitize([]));
        self::assertSame([], Headers::sanitize(['' => 'x', 'k' => '']));
    }

    public function test_merge_lets_later_sources_win_so_the_contract_can_be_passed_last(): void
    {
        $merged = Headers::merge(
            ['traceparent' => '00-rider', 'x-source-lang' => 'rider-should-lose'],
            ['x-source-lang' => 'php', 'x-schema-version' => 1], // contract, passed last → wins
        );

        self::assertSame(
            ['traceparent' => '00-rider', 'x-source-lang' => 'php', 'x-schema-version' => '1'],
            $merged,
        );
    }
}
