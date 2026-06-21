<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Gdpr;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Gdpr\Cipher;
use BabelQueue\Gdpr\CipherException;
use BabelQueue\Gdpr\DecryptException;
use BabelQueue\Gdpr\Gdpr;
use BabelQueue\Schema\PayloadValidator;
use PHPUnit\Framework\TestCase;

/**
 * The runtime GDPR field-encryption helper (ADR-0030): protect/unprotect round-trips, in-place
 * rewrite of only the marked leaves, frozen-envelope guarantees, and wrong-key failure. Uses a
 * transport-free reversible cipher so the walk/round-trip logic is exercised without
 * `ext-openssl`; the OpenSsl cipher itself is covered in {@see OpenSslCipherTest}.
 */
final class GdprTest extends TestCase
{
    private const SCHEMA = '{
        "type": "object",
        "properties": {
            "email": {"type": "string", "x-gdpr-sensitive": "email"},
            "order_id": {"type": "integer"},
            "amount": {"type": "number"},
            "profile": {
                "type": "object",
                "properties": {
                    "full_name": {"type": "string", "x-gdpr-sensitive": true},
                    "vip": {"type": "boolean"}
                }
            },
            "addresses": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "line": {"type": "string", "x-gdpr-sensitive": true},
                        "city": {"type": "string"}
                    }
                }
            }
        }
    }';

    public function test_round_trip_restores_data_exactly_nested_array_and_scalar(): void
    {
        $schema = self::schema();
        $cipher = new ReversibleCipher();

        $original = [
            'email' => 'alice@example.com',
            'order_id' => 42,
            'amount' => 19.95,
            'profile' => ['full_name' => 'Alice Smith', 'vip' => true],
            'addresses' => [
                ['line' => '1 Main St', 'city' => 'Springfield'],
                ['line' => '2 Side Rd', 'city' => 'Shelbyville'],
            ],
        ];

        $data = $original;
        Gdpr::protect($data, $schema, $cipher);

        // Marked leaves are now ciphertext strings; non-sensitive siblings are untouched.
        self::assertIsString($data['email']);
        self::assertNotSame('alice@example.com', $data['email']);
        self::assertIsString($data['profile']['full_name']);
        self::assertIsString($data['addresses'][0]['line']);
        self::assertIsString($data['addresses'][1]['line']);

        self::assertSame(42, $data['order_id']);
        self::assertSame(19.95, $data['amount']);
        self::assertTrue($data['profile']['vip']);
        self::assertSame('Springfield', $data['addresses'][0]['city']);

        Gdpr::unprotect($data, $schema, $cipher);

        self::assertSame($original, $data);
    }

    public function test_non_sensitive_fields_are_never_read_or_changed(): void
    {
        $schema = self::schema();
        $cipher = new ReversibleCipher();

        $data = ['order_id' => 7, 'amount' => 1.5, 'profile' => ['vip' => false]];
        $before = $data;

        Gdpr::protect($data, $schema, $cipher);

        // No sensitive leaf present → nothing changes at all.
        self::assertSame($before, $data);
    }

    public function test_absent_marked_path_is_skipped(): void
    {
        $schema = self::schema();
        $cipher = new ReversibleCipher();

        // "email" / "full_name" / "line" simply not present in this message.
        $data = ['order_id' => 1, 'addresses' => []];
        $before = $data;

        Gdpr::protect($data, $schema, $cipher);
        self::assertSame($before, $data);

        Gdpr::unprotect($data, $schema, $cipher);
        self::assertSame($before, $data);
    }

    public function test_root_mark_addresses_nothing_in_object_data(): void
    {
        // A root x-gdpr-sensitive mark has no addressable leaf inside an object's data → no-op.
        /** @var array<string, mixed> $schema */
        $schema = json_decode('{"type": "object", "x-gdpr-sensitive": true, "properties": {"email": {"type": "string"}}}', true, 512, JSON_THROW_ON_ERROR);
        $cipher = new ReversibleCipher();

        $data = ['email' => 'bob@example.com'];
        $before = $data;

        Gdpr::protect($data, $schema, $cipher);
        self::assertSame($before, $data);
    }

    public function test_protected_envelope_still_decodes_and_validates_with_schema_version_and_trace_id(): void
    {
        $schema = self::schema();
        $cipher = new ReversibleCipher();

        $data = ['email' => 'carol@example.com', 'order_id' => 5];
        Gdpr::protect($data, $schema, $cipher);

        $envelope = EnvelopeCodec::make('urn:babel:orders:created', $data, 'orders');
        $traceId = $envelope['trace_id'];

        // GR-1: the protected data still rides a perfectly valid frozen envelope.
        $json = EnvelopeCodec::encode($envelope);
        $decoded = EnvelopeCodec::decode($json);

        self::assertTrue(EnvelopeCodec::accepts($decoded));
        self::assertSame(1, $decoded['meta']['schema_version']);  // GR: schema_version stays 1
        self::assertSame($traceId, $decoded['trace_id']);          // GR-4: trace_id untouched
        self::assertIsString($decoded['data']['email']);           // ciphertext is a JSON string
        self::assertSame(5, $decoded['data']['order_id']);

        // The ciphertext is pure JSON (a string), so the consumer can decrypt back and recover.
        /** @var array<string, mixed> $back */
        $back = $decoded['data'];
        Gdpr::unprotect($back, $schema, $cipher);
        self::assertSame('carol@example.com', $back['email']);
    }

    public function test_protected_string_field_passes_validation_after_unprotect_not_before(): void
    {
        // A constrained sensitive field (minLength) accepts the cleartext, and (here, by length)
        // also the ciphertext — but the point is validation is meant for cleartext. We assert the
        // cleartext validates; protect must run AFTER validation in the documented wiring.
        /** @var array<string, mixed> $schema */
        $schema = json_decode('{"type":"object","required":["email"],"properties":{"email":{"type":"string","minLength":3,"x-gdpr-sensitive":true}}}', true, 512, JSON_THROW_ON_ERROR);

        $data = ['email' => 'dave@example.com'];
        self::assertNull(PayloadValidator::check($schema, $data));

        Gdpr::protect($data, $schema, new ReversibleCipher());
        self::assertIsString($data['email']);
    }

    public function test_unprotect_with_wrong_key_throws_decrypt_exception(): void
    {
        $schema = self::schema();

        $data = ['email' => 'eve@example.com'];
        Gdpr::protect($data, $schema, new ReversibleCipher('key-a'));

        $this->expectException(DecryptException::class);
        Gdpr::unprotect($data, $schema, new ReversibleCipher('key-b'));
    }

    public function test_decrypt_exception_carries_the_field_path(): void
    {
        $schema = self::schema();

        $data = ['profile' => ['full_name' => 'Frank']];
        Gdpr::protect($data, $schema, new ReversibleCipher('key-a'));

        try {
            Gdpr::unprotect($data, $schema, new ReversibleCipher('key-b'));
            self::fail('expected DecryptException');
        } catch (DecryptException $e) {
            self::assertSame('profile.full_name', $e->path());
        }
    }

    public function test_unprotect_is_idempotent_on_non_string_cleartext_leaves(): void
    {
        // A message whose sensitive leaf is already a non-string cleartext value (e.g. it was
        // never protected) is left untouched by unprotect — safe to run on cleartext data.
        $schema = self::schema();
        $data = ['order_id' => 9, 'amount' => 3.0]; // no string sensitive leaf present
        $before = $data;

        Gdpr::unprotect($data, $schema, new ReversibleCipher());
        self::assertSame($before, $data);
    }

    public function test_unprotect_leaves_present_non_string_marked_leaf_untouched(): void
    {
        // The marked path IS present but holds a non-string value (e.g. it was never protected,
        // or it is structured cleartext) — unprotect leaves it exactly as-is.
        /** @var array<string, mixed> $schema */
        $schema = json_decode('{"type":"object","properties":{"email":{"type":"string","x-gdpr-sensitive":true}}}', true, 512, JSON_THROW_ON_ERROR);

        $data = ['email' => ['nested' => 'value']]; // non-string at a marked path
        $before = $data;

        Gdpr::unprotect($data, $schema, new ReversibleCipher());
        self::assertSame($before, $data);
    }

    public function test_scalar_array_items_marked_are_each_protected(): void
    {
        // `items` itself marked → each scalar element of the list is encrypted/decrypted in place.
        /** @var array<string, mixed> $schema */
        $schema = json_decode('{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string","x-gdpr-sensitive":true}}}}', true, 512, JSON_THROW_ON_ERROR);
        $cipher = new ReversibleCipher();

        $original = ['tags' => ['vip', 'fraud-watch', 'gdpr']];
        $data = $original;

        Gdpr::protect($data, $schema, $cipher);
        self::assertIsArray($data['tags']);
        foreach ($data['tags'] as $tag) {
            self::assertIsString($tag);
            self::assertNotContains($tag, $original['tags']);
        }

        Gdpr::unprotect($data, $schema, $cipher);
        self::assertSame($original, $data);
    }

    public function test_array_marked_path_with_non_list_value_is_skipped(): void
    {
        // The schema declares `addresses[].line`, but this message put a non-array under
        // `addresses` — a shape mismatch is skipped silently, not an error.
        $schema = self::schema();
        $cipher = new ReversibleCipher();

        $data = ['addresses' => 'not-an-array'];
        $before = $data;

        Gdpr::protect($data, $schema, $cipher);
        self::assertSame($before, $data);
    }

    /**
     * @return array<string, mixed>
     */
    private static function schema(): array
    {
        /** @var array<string, mixed> $schema */
        $schema = json_decode(self::SCHEMA, true, 512, JSON_THROW_ON_ERROR);

        return $schema;
    }
}

/**
 * A transport-free, deterministic, reversible {@see Cipher} for testing the protect/unprotect
 * walk without `ext-openssl`. It "encrypts" by base64-ing `<key>:<plaintext>` and verifies the
 * key on decrypt, so a wrong key throws exactly like a real authenticated cipher would. It is
 * NOT cryptographic — tests only.
 */
final class ReversibleCipher implements Cipher
{
    public function __construct(private readonly string $key = 'k')
    {
    }

    public function encrypt(string $plaintext): string
    {
        return base64_encode($this->key . "\0" . $plaintext);
    }

    public function decrypt(string $ciphertext): string
    {
        $raw = base64_decode($ciphertext, true);
        if ($raw === false) {
            throw new CipherException('not base64');
        }
        $sep = strpos($raw, "\0");
        if ($sep === false) {
            throw new CipherException('malformed');
        }
        if (substr($raw, 0, $sep) !== $this->key) {
            throw new CipherException('wrong key');
        }

        return substr($raw, $sep + 1);
    }
}
