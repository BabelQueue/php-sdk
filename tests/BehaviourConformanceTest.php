<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Schema\PayloadValidator;
use PHPUnit\Framework\TestCase;

/**
 * Runs the behaviour sections of the vendored cross-SDK conformance manifest:
 * `roundtrip`, `data_shape`, `forbidden_keys` and `payload_schema_unicode`.
 *
 * Re-encoded output is parsed with objects kept as stdClass so the comparison is
 * type-strict ({} is not [], 1 is not 1.0 or true); object key order is ignored.
 */
final class BehaviourConformanceTest extends TestCase
{
    private const DIR = __DIR__ . '/conformance';

    protected function tearDown(): void
    {
        EnvelopeCodec::setWarningHandler(null);
    }

    public function test_roundtrip_cases(): void
    {
        foreach ($this->cases('roundtrip') as $case) {
            $name = (string) $case['name'];
            $warnings = [];

            $envelope = EnvelopeCodec::decode($this->fixture($case), self::collector($warnings));
            self::assertTrue(EnvelopeCodec::accepts($envelope), $name . ': decode must accept');

            self::assertIsInt($envelope['attempts']);
            $envelope['attempts']++;

            $out = self::parse(EnvelopeCodec::encode($envelope));
            self::assertSame($case['expect_attempts'], self::pointer($out, '/attempts', $name), $name . ': attempts');

            foreach ($this->expectedPreserved($name) as $pointer => $expected) {
                self::assertTrue(
                    self::deepEqual($expected, self::pointer($out, $pointer, $name)),
                    sprintf('%s: %s must be preserved as %s, got %s', $name, $pointer, json_encode($expected), json_encode(self::pointer($out, $pointer, $name))),
                );
            }
        }
    }

    public function test_data_shape_cases(): void
    {
        foreach ($this->cases('data_shape') as $case) {
            $name = (string) $case['name'];

            if ($case['mode'] === 'encode') {
                /** @var array<string, mixed> $data */
                $data = (array) $case['data'];
                $json = EnvelopeCodec::encode(EnvelopeCodec::make((string) $case['urn'], $data, (string) $case['queue']));
                $out = self::parse($json);
                $raw = json_encode(self::pointer($out, '/data', $name), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
                self::assertSame($case['expect_encoded_data_json'], $raw, $name . ': encoded data');

                continue;
            }

            self::assertSame('decode', $case['mode'], $name . ': unknown mode');
            $warnings = [];
            $envelope = EnvelopeCodec::decode($this->fixture($case), self::collector($warnings));
            self::assertSame((bool) $case['valid'], EnvelopeCodec::accepts($envelope), $name . ': verdict');
        }
    }

    public function test_forbidden_key_cases(): void
    {
        foreach ($this->cases('forbidden_keys') as $case) {
            $name = (string) $case['name'];
            $forbidden = (string) $case['forbidden_key'];
            self::assertSame('warn', $case['expect'], $name . ': only warn is defined for R0');

            /** @var list<array{string, string}> $warnings */
            $warnings = [];
            $envelope = EnvelopeCodec::decode($this->fixture($case), self::collector($warnings));
            self::assertTrue(EnvelopeCodec::accepts($envelope), $name . ': decode must succeed');

            $named = array_filter($warnings, static fn (array $w): bool => str_contains($w[0], $forbidden));
            self::assertNotEmpty($named, $name . ': a warning must name ' . $forbidden);

            $encodeWarnings = [];
            EnvelopeCodec::setWarningHandler(self::collector($encodeWarnings));
            $out = self::parse(EnvelopeCodec::encode($envelope));
            self::assertSame([], $encodeWarnings, $name . ': nothing left to strip on re-encode');

            foreach ((array) $case['expect_absent_after_reencode'] as $pointer) {
                self::assertFalse(self::has($out, (string) $pointer), $name . ': ' . $pointer . ' must be absent');
            }
            self::assertSame(self::pointer(self::parse($this->fixture($case)), '/attempts', $name), self::pointer($out, '/attempts', $name), $name . ': top-level attempts unchanged');
        }
    }

    public function test_payload_schema_unicode_cases(): void
    {
        $section = $this->manifest()['payload_schema_unicode'] ?? null;
        if (! is_array($section)) {
            self::markTestSkipped('manifest has no payload_schema_unicode section');
        }

        /** @var array<string, mixed> $schema */
        $schema = $section['schema'];
        self::assertNotEmpty($section['cases']);

        foreach ($section['cases'] as $case) {
            /** @var array<string, mixed> $data */
            $data = (array) $case['data'];
            $valid = PayloadValidator::check($schema, $data) === null;
            self::assertSame((bool) $case['valid'], $valid, 'case ' . (string) $case['name']);
        }
    }

    public function test_encoder_never_emits_forbidden_keys(): void
    {
        $envelope = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1], 'orders');
        $envelope['timestamp'] = 1;
        $envelope['meta']['ts'] = 1;
        $envelope['meta']['source'] = 'php';
        $envelope['meta']['max_retries'] = 3;
        $envelope['meta']['attempts'] = 2;

        $warnings = [];
        EnvelopeCodec::setWarningHandler(self::collector($warnings));
        $out = self::parse(EnvelopeCodec::encode($envelope));

        foreach (['/timestamp', '/meta/ts', '/meta/source', '/meta/max_retries', '/meta/attempts'] as $pointer) {
            self::assertFalse(self::has($out, $pointer), $pointer);
        }
        self::assertCount(5, $warnings);
        self::assertSame(0, self::pointer($out, '/attempts', 'encode'));
    }

    // ------------------------------------------------------------------ helpers

    /** @return array<string, mixed> */
    private function manifest(): array
    {
        $path = self::DIR . '/manifest.json';
        if (! is_file($path)) {
            self::markTestSkipped('vendored conformance suite not present');
        }

        /** @var array<string, mixed> */
        return (array) json_decode((string) file_get_contents($path), true, 512, JSON_THROW_ON_ERROR);
    }

    /** @return list<array<string, mixed>> */
    private function cases(string $section): array
    {
        $block = $this->manifest()[$section] ?? null;
        if (! is_array($block) || ! is_array($block['cases'] ?? null)) {
            self::markTestSkipped('manifest has no ' . $section . ' section');
        }
        self::assertNotEmpty($block['cases']);

        /** @var list<array<string, mixed>> */
        return $block['cases'];
    }

    /**
     * `expect_preserved` re-read with objects kept as stdClass, so {} stays {}.
     *
     * @return array<string, mixed>
     */
    private function expectedPreserved(string $name): array
    {
        $raw = json_decode((string) file_get_contents(self::DIR . '/manifest.json'), false, 512, JSON_THROW_ON_ERROR);
        foreach ($raw->roundtrip->cases as $case) {
            if ($case->name === $name) {
                return get_object_vars($case->expect_preserved);
            }
        }

        self::fail('roundtrip case not found: ' . $name);
    }

    /** @param  array<string, mixed>  $case */
    private function fixture(array $case): string
    {
        $path = self::DIR . '/' . (string) $case['file'];
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
