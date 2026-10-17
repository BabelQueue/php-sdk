<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Schema\PayloadValidator;
use PHPUnit\Framework\TestCase;

/**
 * Runs the behaviour sections of the vendored cross-SDK conformance manifest:
 * `roundtrip`, `data_shape`, `forbidden_keys`, `extension_write`, `encode_reject` and
 * `payload_schema_unicode`.
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
        $this->cases('roundtrip');

        foreach ($this->objectCases('roundtrip') as $case) {
            self::assertSame([], $this->roundtripViolations($case, $this->fixture($case)), (string) $case->name);
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
        $this->cases('forbidden_keys');

        foreach ($this->objectCases('forbidden_keys') as $case) {
            self::assertSame('warn', $case->expect, $case->name . ': only warn is defined for R0');
            self::assertSame([], $this->forbiddenKeyViolations($case, $this->fixture($case)), (string) $case->name);
        }
    }

    public function test_extension_write_cases(): void
    {
        $this->cases('extension_write');

        foreach ($this->objectCases('extension_write') as $case) {
            $name = (string) $case->name;
            /** @var array<string, mixed> $meta */
            $meta = get_object_vars($case->meta);
            /** @var array<string, mixed> $data */
            $data = (array) $case->data;

            $envelope = EnvelopeCodec::make((string) $case->urn, $data, (string) $case->queue, meta: $meta);
            $out = self::parse(EnvelopeCodec::encode($envelope));
            self::assertSame([], self::pointerViolations($out, $case->expect_envelope, $name . ' (encode)'), $name);

            // Re-emit chain: decode -> attempts+1 -> encode.
            $warnings = [];
            $decoded = EnvelopeCodec::decode(EnvelopeCodec::encode($envelope), self::collector($warnings));
            self::assertSame([], $warnings, $name . ': decode must not warn');
            self::assertIsInt($decoded['attempts']);
            $decoded['attempts']++;
            $again = self::parse(EnvelopeCodec::encode($decoded));
            self::assertSame([], self::pointerViolations($again, $case->expect_envelope, $name . ' (re-emit)'), $name);
            self::assertSame(1, self::pointer($again, '/attempts', $name), $name . ': attempts');
        }
    }

    public function test_encode_reject_cases(): void
    {
        $this->cases('encode_reject');

        $ran = 0;
        foreach ($this->objectCases('encode_reject') as $case) {
            $name = (string) $case->name;
            self::assertSame('reject', $case->expect, $name);

            // A repeated key cannot be expressed through make(meta: [...]) (one map per call).
            if (isset($case->meta_entries)) {
                continue;
            }

            $produced = null;

            try {
                /** @var array<string, mixed> $meta */
                $meta = isset($case->meta) ? get_object_vars($case->meta) : [];
                /** @var array<string, mixed> $data */
                $data = (array) $case->data;
                $envelope = EnvelopeCodec::make((string) $case->urn, $data, (string) $case->queue, meta: $meta);

                foreach (isset($case->extras_top) ? get_object_vars($case->extras_top) : [] as $key => $value) {
                    $envelope[(string) $key] = $value;
                }
                foreach (isset($case->extras_meta) ? get_object_vars($case->extras_meta) : [] as $key => $value) {
                    $envelope['meta'][(string) $key] = $value;
                }

                $produced = EnvelopeCodec::encode($envelope);
            } catch (BabelQueueException $e) {
                self::assertStringContainsString((string) $case->offending_key, $e->getMessage(), $name . ': message names the key');
                $ran++;

                continue;
            }

            self::fail($name . ': must be rejected, but produced ' . (string) $produced);
        }

        self::assertGreaterThan(0, $ran);
    }

    /**
     * The runner must really evaluate the optional assertion fields: on synthetic cases that
     * violate them it has to report a violation, otherwise a runner that ignores the fields
     * would be exactly as green as one that reads them.
     */
    public function test_the_runner_reports_violations_of_the_optional_fields(): void
    {
        $body = '{"job":"urn:acme:a","trace_id":"t","data":{"order_id":1},"meta":{"id":"i","queue":"q","lang":"php","schema_version":1,"created_at":1,"vendor_flag":true},"extra_top":1,"attempts":0}';

        $good = (object) [
            'name' => 'synthetic',
            'expect_attempts' => 1,
            'expect_preserved' => (object) ['/meta/vendor_flag' => true, '/extra_top' => 1],
            'expect_absent_after_reencode' => ['/meta/missing'],
        ];
        self::assertSame([], $this->roundtripViolations($good, $body), 'a satisfied case reports nothing');

        $absent = clone $good;
        $absent->expect_absent_after_reencode = ['/extra_top'];
        self::assertNotSame([], $this->roundtripViolations($absent, $body), 'expect_absent_after_reencode: a present pointer');

        $preserved = clone $good;
        $preserved->expect_preserved = (object) ['/meta/vendor_flag' => false];
        self::assertNotSame([], $this->roundtripViolations($preserved, $body), 'expect_preserved: a different value');

        $retyped = clone $good;
        $retyped->expect_preserved = (object) ['/data' => []];
        self::assertNotSame([], $this->roundtripViolations($retyped, $body), 'expect_preserved: {} is not []');

        $deadLetter = clone $good;
        $deadLetter->apply_dead_letter = (object) ['reason' => 'failed', 'original_queue' => 'orders'];
        $deadLetter->expect_dead_letter = (object) ['reason' => 'failed', 'original_queue' => 'orders'];
        self::assertSame([], $this->roundtripViolations($deadLetter, $body), 'a satisfied dead-letter case reports nothing');

        $deadLetter->expect_dead_letter = (object) ['original_queue' => 'elsewhere'];
        self::assertNotSame([], $this->roundtripViolations($deadLetter, $body), 'expect_dead_letter: a different value');

        $forbiddenBody = '{"job":"urn:acme:a","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0,"timestamp":1}';
        $forbidden = (object) [
            'name' => 'synthetic-forbidden',
            'forbidden_key' => '/timestamp',
            'expect' => 'warn',
            'expect_absent_after_reencode' => ['/timestamp'],
            'expect_preserved' => (object) ['/job' => 'urn:acme:a'],
        ];
        self::assertSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'a satisfied forbidden case reports nothing');

        $forbidden->expect_preserved = (object) ['/job' => 'urn:acme:other'];
        self::assertNotSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'forbidden_keys expect_preserved: a different value');

        $forbidden->expect_preserved = (object) [];
        $forbidden->expect_absent_after_reencode = ['/job'];
        self::assertNotSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'forbidden_keys: a present pointer');

        $forbidden->expect_absent_after_reencode = ['/timestamp'];
        $forbidden->forbidden_key = '/meta/ts';
        self::assertNotSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'forbidden_keys: no warning names the key');
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

        $this->expectException(InvalidEnvelopeException::class);
        $this->expectExceptionMessageMatches('#/timestamp, /meta/max_retries, /meta/attempts, /meta/source, /meta/ts#');

        EnvelopeCodec::encode($envelope);
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
     * Decode -> attempts+1 [-> dead-letter] -> encode, then every assertion of the case.
     *
     * @return list<string> the violations; empty when the case holds
     */
    private function roundtripViolations(\stdClass $case, string $body): array
    {
        $name = (string) $case->name;
        $violations = [];

        $warnings = [];
        $envelope = EnvelopeCodec::decode($body, self::collector($warnings));
        if ($warnings !== []) {
            $violations[] = $name . ': decode must emit no warning, got ' . $warnings[0][0];
        }
        if (! EnvelopeCodec::accepts($envelope)) {
            return [...$violations, $name . ': decode must accept'];
        }

        self::assertIsInt($envelope['attempts']);
        $envelope['attempts']++;

        $deadLettered = isset($case->apply_dead_letter);
        if ($deadLettered) {
            $envelope = DeadLetter::annotate(
                $envelope,
                (string) $case->apply_dead_letter->reason,
                new \RuntimeException('conformance runner'),
                (string) $case->apply_dead_letter->original_queue,
                $envelope['attempts'],
            );
        }

        $out = self::parse(EnvelopeCodec::encode($envelope));

        if (isset($case->expect_attempts) && (! self::has($out, '/attempts') || self::pointer($out, '/attempts', $name) !== $case->expect_attempts)) {
            $violations[] = $name . ': /attempts must be ' . $case->expect_attempts;
        }

        $violations = [...$violations, ...self::pointerViolations($out, $case->expect_preserved ?? (object) [], $name)];
        $violations = [...$violations, ...self::absentViolations($out, (array) ($case->expect_absent_after_reencode ?? []), $name)];

        if ($deadLettered && isset($case->expect_dead_letter)) {
            if (! self::has($out, '/dead_letter')) {
                $violations[] = $name . ': /dead_letter must exist';
            } else {
                foreach (get_object_vars($case->expect_dead_letter) as $field => $expected) {
                    $pointer = '/dead_letter/' . $field;
                    if (! self::has($out, $pointer) || ! self::deepEqual($expected, self::pointer($out, $pointer, $name))) {
                        $violations[] = sprintf('%s: %s must be %s', $name, $pointer, json_encode($expected));
                    }
                }
            }
        }

        return $violations;
    }

    /**
     * Decode a forbidden-key fixture (must warn, naming the key), re-encode it, then the optional
     * `expect_preserved` / `expect_absent_after_reencode` assertions.
     *
     * @return list<string>
     */
    private function forbiddenKeyViolations(\stdClass $case, string $body): array
    {
        $name = (string) $case->name;
        $forbidden = (string) $case->forbidden_key;
        $violations = [];

        /** @var list<array{string, string}> $warnings */
        $warnings = [];
        $envelope = EnvelopeCodec::decode($body, self::collector($warnings));
        if (! EnvelopeCodec::accepts($envelope)) {
            return [$name . ': decode must succeed'];
        }

        $named = array_filter($warnings, static fn (array $w): bool => str_contains($w[0], $forbidden));
        if ($named === []) {
            $violations[] = $name . ': a warning must name ' . $forbidden;
        }

        $out = self::parse(EnvelopeCodec::encode($envelope));

        $violations = [...$violations, ...self::absentViolations($out, (array) ($case->expect_absent_after_reencode ?? []), $name)];
        $violations = [...$violations, ...self::pointerViolations($out, $case->expect_preserved ?? (object) [], $name)];

        $before = self::parse($body);
        if (self::pointer($before, '/attempts', $name) !== self::pointer($out, '/attempts', $name)) {
            $violations[] = $name . ': top-level attempts unchanged';
        }

        return $violations;
    }

    /**
     * Every pointer of `$expected` must hold the given value (type-strict) in `$doc`.
     *
     * @return list<string>
     */
    private static function pointerViolations(mixed $doc, \stdClass $expected, string $name): array
    {
        $violations = [];
        foreach (get_object_vars($expected) as $pointer => $value) {
            $pointer = (string) $pointer;
            if (! self::has($doc, $pointer)) {
                $violations[] = sprintf('%s: %s missing, expected %s', $name, $pointer, json_encode($value));
            } elseif (! self::deepEqual($value, self::pointer($doc, $pointer, $name))) {
                $violations[] = sprintf('%s: %s must be %s, got %s', $name, $pointer, json_encode($value), json_encode(self::pointer($doc, $pointer, $name)));
            }
        }

        return $violations;
    }

    /**
     * None of the pointers may exist in `$doc`.
     *
     * @param  list<mixed>  $pointers
     * @return list<string>
     */
    private static function absentViolations(mixed $doc, array $pointers, string $name): array
    {
        $violations = [];
        foreach ($pointers as $pointer) {
            if (self::has($doc, (string) $pointer)) {
                $violations[] = sprintf('%s: %s must be absent after re-encode', $name, $pointer);
            }
        }

        return $violations;
    }

    /**
     * A manifest section re-read with objects kept as stdClass, so {} stays {}.
     *
     * @return list<\stdClass>
     */
    private function objectCases(string $section): array
    {
        $raw = json_decode((string) file_get_contents(self::DIR . '/manifest.json'), false, 512, JSON_THROW_ON_ERROR);

        /** @var list<\stdClass> */
        return $raw->{$section}->cases;
    }

    /** @param  array<string, mixed>|\stdClass  $case */
    private function fixture(array|\stdClass $case): string
    {
        $file = $case instanceof \stdClass ? $case->file : $case['file'];
        $path = self::DIR . '/' . (string) $file;
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
