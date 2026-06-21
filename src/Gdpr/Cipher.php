<?php

declare(strict_types=1);

namespace BabelQueue\Gdpr;

/**
 * The field-level protection primitive the CALLER provides — a seam onto a KMS, Vault transit,
 * an HSM, a tokenisation service, or the reference {@see OpenSslCipher} below.
 * {@see Gdpr::protect()} runs {@see self::encrypt()} over every `x-gdpr-sensitive` leaf's value
 * (after it is canonically JSON-encoded); {@see Gdpr::unprotect()} runs {@see self::decrypt()} to
 * restore it. Keeping this an interface is what holds GR-7: the core package never pulls a
 * crypto/KMS dependency — only a caller who binds a concrete backend does.
 *
 * Contract for an implementation:
 *
 *  - `encrypt()` takes the canonical JSON of one field value (see {@see Gdpr::protect()}) and
 *    returns the ciphertext as a STRING valid for placement inside a JSON document (the
 *    {@see OpenSslCipher} reference returns base64, which is). The same plaintext MAY encrypt to a
 *    different string each call (a random IV/nonce is expected and good).
 *  - `decrypt()` is the exact inverse: given a string `encrypt()` produced, it returns the
 *    original JSON string byte-for-byte. A string it did not produce, or one produced under a
 *    different key, MUST throw rather than return silent garbage, so a wrong-key consume fails
 *    loudly (and the message takes retry / DLQ).
 */
interface Cipher
{
    /**
     * Protect one field value (its canonical JSON) and return a JSON-safe ciphertext string.
     *
     * @throws CipherException on an unrecoverable encryption failure
     */
    public function encrypt(string $plaintext): string;

    /**
     * Reverse {@see self::encrypt()}, returning the original field-value JSON.
     *
     * @throws CipherException when the input is not a ciphertext this cipher produced, or was
     *                         produced under a different key / has been tampered with
     */
    public function decrypt(string $ciphertext): string;
}
