<?php

declare(strict_types=1);

namespace BabelQueue\Gdpr;

/**
 * A reference {@see Cipher} built ONLY on the `ext-openssl` extension: AES-GCM authenticated
 * encryption with a fresh random IV per call, a 16-byte authentication tag, and the whole frame
 * (`IV || tag || ciphertext`) base64-encoded so it drops straight into a JSON string. It is the
 * PHP analogue of the Go `AESGCMCipher` reference.
 *
 * The key is the CALLER's — this type performs no key management, rotation or derivation; bind a
 * KMS-backed {@see Cipher} for that. A 32-byte key selects AES-256-GCM (the recommended size);
 * 24- and 16-byte keys select AES-192/128-GCM. GCM authenticates the ciphertext, so
 * {@see self::decrypt()} rejects any tampered or wrong-key input with a {@see CipherException}
 * (it never returns corrupt plaintext).
 *
 * `ext-openssl` is an OPTIONAL path — it is a Composer `suggest`, not a `require`, so the core
 * stays `ext-json` (GR-7). A caller who needs field encryption either installs `ext-openssl` and
 * uses this, or binds their own {@see Cipher} onto a KMS/Vault. It is safe for concurrent use:
 * it is stateless beyond the immutable key and cipher selection.
 */
final class OpenSslCipher implements Cipher
{
    /** GCM uses a 12-byte (96-bit) IV — the recommended size. */
    private const IV_LENGTH = 12;

    /** GCM authentication tag length in bytes (128-bit). */
    private const TAG_LENGTH = 16;

    private readonly string $algorithm;

    /**
     * @param  string  $key  a raw 16-, 24-, or 32-byte symmetric key (32 ⇒ AES-256-GCM)
     *
     * @throws CipherException when `ext-openssl` is unavailable or the key size is invalid
     */
    public function __construct(private readonly string $key)
    {
        if (! \extension_loaded('openssl')) {
            throw new CipherException('BabelQueue\\Gdpr\\OpenSslCipher requires ext-openssl.');
        }

        $this->algorithm = match (\strlen($key)) {
            16 => 'aes-128-gcm',
            24 => 'aes-192-gcm',
            32 => 'aes-256-gcm',
            default => throw new CipherException(
                'AES key must be 16, 24, or 32 bytes (got ' . \strlen($key) . ').'
            ),
        };
    }

    /**
     * Seal $plaintext with a fresh random IV and base64-encode `IV || tag || ciphertext`.
     */
    public function encrypt(string $plaintext): string
    {
        $iv = \random_bytes(self::IV_LENGTH);
        $tag = '';
        $ciphertext = \openssl_encrypt(
            $plaintext,
            $this->algorithm,
            $this->key,
            OPENSSL_RAW_DATA,
            $iv,
            $tag,
            '',
            self::TAG_LENGTH
        );
        if ($ciphertext === false) {
            throw new CipherException('GDPR field encryption failed: ' . self::lastError());
        }

        return \base64_encode($iv . $tag . $ciphertext);
    }

    /**
     * Reverse {@see self::encrypt()}: base64-decode, split off the IV and tag, and open the GCM
     * ciphertext. A wrong key or tampered input fails GCM authentication and throws.
     */
    public function decrypt(string $ciphertext): string
    {
        $raw = \base64_decode($ciphertext, true);
        if ($raw === false) {
            throw new CipherException('GDPR ciphertext is not valid base64.');
        }
        if (\strlen($raw) < self::IV_LENGTH + self::TAG_LENGTH) {
            throw new CipherException('GDPR ciphertext is shorter than its IV and tag.');
        }

        $iv = \substr($raw, 0, self::IV_LENGTH);
        $tag = \substr($raw, self::IV_LENGTH, self::TAG_LENGTH);
        $sealed = \substr($raw, self::IV_LENGTH + self::TAG_LENGTH);

        $plaintext = \openssl_decrypt(
            $sealed,
            $this->algorithm,
            $this->key,
            OPENSSL_RAW_DATA,
            $iv,
            $tag
        );
        if ($plaintext === false) {
            // GCM authentication failed: wrong key, tampered ciphertext, or not our output.
            throw new CipherException('GDPR field decryption failed (wrong key or tampered ciphertext).');
        }

        return $plaintext;
    }

    private static function lastError(): string
    {
        $error = \openssl_error_string();

        return $error === false ? 'unknown openssl error' : $error;
    }
}
