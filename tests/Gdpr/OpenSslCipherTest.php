<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Gdpr;

use BabelQueue\Gdpr\CipherException;
use BabelQueue\Gdpr\Gdpr;
use BabelQueue\Gdpr\OpenSslCipher;
use PHPUnit\Framework\TestCase;

/**
 * The reference AES-256-GCM cipher (ADR-0030). `ext-openssl` is OPTIONAL (a Composer `suggest`),
 * so every test here skips cleanly when the extension is absent — the rest of the GDPR helper is
 * covered transport-free in {@see GdprTest}.
 */
final class OpenSslCipherTest extends TestCase
{
    protected function setUp(): void
    {
        if (! \extension_loaded('openssl')) {
            self::markTestSkipped('ext-openssl is not installed; OpenSslCipher is an optional path.');
        }
    }

    public function test_round_trips_a_field_value(): void
    {
        $cipher = new OpenSslCipher(self::key(32));

        $plaintext = '{"full_name":"Alice"}';
        $ciphertext = $cipher->encrypt($plaintext);

        self::assertNotSame($plaintext, $ciphertext);
        self::assertSame($plaintext, $cipher->decrypt($ciphertext));
    }

    public function test_each_encryption_uses_a_fresh_iv(): void
    {
        $cipher = new OpenSslCipher(self::key(32));

        self::assertNotSame($cipher->encrypt('x'), $cipher->encrypt('x'));
    }

    public function test_accepts_128_192_256_bit_keys(): void
    {
        foreach ([16, 24, 32] as $size) {
            $cipher = new OpenSslCipher(self::key($size));
            self::assertSame('payload', $cipher->decrypt($cipher->encrypt('payload')));
        }
    }

    public function test_invalid_key_size_throws(): void
    {
        $this->expectException(CipherException::class);
        new OpenSslCipher(self::key(10));
    }

    public function test_wrong_key_throws_on_decrypt(): void
    {
        $ciphertext = (new OpenSslCipher(self::key(32, 'a')))->encrypt('secret');

        $this->expectException(CipherException::class);
        (new OpenSslCipher(self::key(32, 'b')))->decrypt($ciphertext);
    }

    public function test_tampered_ciphertext_throws_on_decrypt(): void
    {
        $cipher = new OpenSslCipher(self::key(32));
        $ciphertext = $cipher->encrypt('secret');

        // Flip a byte in the base64 payload — GCM authentication must reject it.
        $tampered = $ciphertext;
        $tampered[20] = $tampered[20] === 'A' ? 'B' : 'A';

        $this->expectException(CipherException::class);
        $cipher->decrypt($tampered);
    }

    public function test_non_base64_input_throws(): void
    {
        $this->expectException(CipherException::class);
        (new OpenSslCipher(self::key(32)))->decrypt('!!! not base64 !!!');
    }

    public function test_too_short_input_throws(): void
    {
        $this->expectException(CipherException::class);
        (new OpenSslCipher(self::key(32)))->decrypt(base64_encode('short'));
    }

    public function test_end_to_end_protect_unprotect_with_openssl(): void
    {
        /** @var array<string, mixed> $schema */
        $schema = json_decode(
            '{"type":"object","properties":{"email":{"type":"string","x-gdpr-sensitive":"email"},"order_id":{"type":"integer"}}}',
            true,
            512,
            JSON_THROW_ON_ERROR
        );
        $cipher = new OpenSslCipher(self::key(32));

        $original = ['email' => 'alice@example.com', 'order_id' => 7];
        $data = $original;

        Gdpr::protect($data, $schema, $cipher);
        self::assertNotSame('alice@example.com', $data['email']);

        Gdpr::unprotect($data, $schema, $cipher);
        self::assertSame($original, $data);
    }

    private static function key(int $size, string $fill = 'k'): string
    {
        return str_repeat($fill, $size);
    }
}
