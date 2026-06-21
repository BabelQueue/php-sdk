<?php

declare(strict_types=1);

namespace BabelQueue\Gdpr;

use BabelQueue\Schema\SensitivePath;
use BabelQueue\Schema\SensitivePaths;
use JsonException;

/**
 * The RUNTIME half of ADR-0030: SDK-side field-level encryption of the `data` fields a registry
 * declared `x-gdpr-sensitive`. babelqueue-registry only DECLARES and AUDITS sensitivity (and
 * offers one-way masking for safe logging); this enforces it on the wire — a producer encrypts
 * each marked leaf before publish, a consumer decrypts it after decode. It is the PHP mirror of
 * the Go `gdpr` reference, so the contract is deliberately tight:
 *
 *  - **The envelope stays FROZEN (GR-1).** {@see self::protect()} mutates only VALUES inside
 *    `data`: a sensitive leaf's value becomes a ciphertext STRING. It never adds, renames,
 *    removes or retypes an envelope field; `meta.schema_version` stays `1`; `trace_id` is
 *    untouched (GR-4). `data` stays pure JSON (GR-3) — a JSON string is still pure JSON, so any
 *    SDK can carry the envelope even without the key (it just can't read the protected fields).
 *    The which-fields-are-sensitive fact lives in the schema, not the message.
 *  - **Zero heavy dependencies (GR-7).** The crypto is a caller-provided {@see Cipher}
 *    (KMS/Vault/HSM/tokenisation); the bundled {@see OpenSslCipher} is `ext-openssl`-only and a
 *    Composer `suggest`. The core stays `ext-json`.
 *
 * The sensitive paths come from the SAME per-URN `data` schema the produce/consume validation
 * path already loads (a {@see \BabelQueue\Schema\SchemaProvider}) — the `x-gdpr-sensitive` marks
 * ride on it ({@see SensitivePaths}). {@see self::protect()} / {@see self::unprotect()} are
 * standalone helpers, so it is strictly opt-in: a producer/consumer that never calls them behaves
 * exactly as before.
 *
 * Typical wiring (producer):
 *
 *     $schema = $provider->schemaFor($urn);
 *     if ($schema !== null) {
 *         SchemaValidated::assert($provider, $urn, $data); // optional: validate cleartext first
 *         Gdpr::protect($data, $schema, $cipher);          // encrypt marked leaves IN PLACE
 *     }
 *     $payload = EnvelopeCodec::encode(EnvelopeCodec::make($urn, $data, 'orders'));
 *
 * and the inverse on the consumer, after decode and before the handler reads `data`:
 *
 *     $schema = $provider->schemaFor($message->getUrn());
 *     if ($schema !== null) {
 *         Gdpr::unprotect($data, $schema, $cipher);        // decrypt marked leaves IN PLACE
 *     }
 *
 * Validate cleartext BEFORE protect / AFTER unprotect — a schema that constrains a sensitive
 * field (`minLength`, `enum`, …) would reject the ciphertext string otherwise.
 */
final class Gdpr
{
    /**
     * Encrypt, in place, every value in $data located at a path the schema marked
     * `x-gdpr-sensitive` — the producer-side step, run after building `data` and before encode /
     * publish. Each marked leaf's value is canonically JSON-encoded and replaced by the cipher's
     * ciphertext STRING; the envelope frame, non-sensitive fields, and key order are untouched
     * (GR-1).
     *
     * A marked path absent from $data is skipped (not an error) — schemas evolve and a message
     * need not carry every optional field. A schema with no marks is a no-op. A whole object or
     * array marked sensitive is encoded and encrypted as one ciphertext string.
     *
     * On any cipher error this throws and leaves $data partially protected; treat a thrown
     * `protect()` as fatal for that message (do not publish it).
     *
     * @param  array<string, mixed>  $data    rewritten in place
     * @param  array<string, mixed>  $schema  the decoded `data` JSON Schema for the URN
     *
     * @throws CipherException on an encryption failure
     */
    public static function protect(array &$data, array $schema, Cipher $cipher): void
    {
        foreach (SensitivePaths::of($schema) as $sp) {
            self::applyAtPath($data, self::parsePath($sp->path), static function (mixed $value) use ($cipher): array {
                return [self::encryptLeaf($value, $cipher), true];
            });
        }
    }

    /**
     * The consumer-side inverse of {@see self::protect()}: decrypt, in place, every value in
     * $data at an `x-gdpr-sensitive` path, restoring the original JSON value byte-for-byte. Run
     * it after decode and before the handler reads `data`.
     *
     * An absent path is skipped. A leaf that is NOT a string — it was never protected, or this is
     * a re-run after a successful `unprotect()` — is left as-is, so re-invoking `unprotect()` on
     * already-cleartext data is safe (idempotent for non-string leaves). A string the cipher
     * cannot open (wrong key, tampered, or not a ciphertext) throws {@see DecryptException} — the
     * consumer should fail the message (retry / dead-letter) rather than process unreadable PII.
     *
     * @param  array<string, mixed>  $data    rewritten in place
     * @param  array<string, mixed>  $schema  the decoded `data` JSON Schema for the URN
     *
     * @throws DecryptException when a protected field cannot be restored
     */
    public static function unprotect(array &$data, array $schema, Cipher $cipher): void
    {
        foreach (SensitivePaths::of($schema) as $sp) {
            self::applyAtPath($data, self::parsePath($sp->path), static function (mixed $value) use ($cipher, $sp): array {
                return self::decryptLeaf($value, $cipher, $sp->path);
            });
        }
    }

    /**
     * Canonically JSON-encode one field value and encrypt it. The JSON encoding is what makes the
     * round-trip exact: {@see self::decryptLeaf()}'s `json_decode` restores the same decoded-JSON
     * value (arrays for objects, floats/ints for numbers, …) the envelope codec would produce, so
     * protect → unprotect is byte-for-byte.
     */
    private static function encryptLeaf(mixed $value, Cipher $cipher): string
    {
        try {
            $plaintext = json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        } catch (JsonException $e) {
            throw new CipherException('Cannot JSON-encode a field for encryption: ' . $e->getMessage(), 0, $e);
        }

        return $cipher->encrypt($plaintext);
    }

    /**
     * Reverse {@see self::encryptLeaf()}. A non-string leaf is left untouched (the second tuple
     * element is `false`) so `unprotect()` is safe to re-run on already-cleartext data; a string
     * that fails to open or to JSON-decode raises {@see DecryptException} so the consumer fails
     * the message rather than handling unreadable PII.
     *
     * @return array{0: mixed, 1: bool}  [newValue, replace?]
     */
    private static function decryptLeaf(mixed $value, Cipher $cipher, string $path): array
    {
        if (! is_string($value)) {
            // Not a ciphertext string (already cleartext, or never protected) — leave as-is.
            return [$value, false];
        }

        try {
            $plaintext = $cipher->decrypt($value);
        } catch (CipherException $e) {
            throw DecryptException::at($path, $e);
        }

        try {
            $restored = json_decode($plaintext, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw DecryptException::at($path, $e);
        }

        return [$restored, true];
    }

    /**
     * One step of a sensitive path: a named object key, optionally with array-descent.
     * `"addresses[].line"` parses to `[['addresses', true], ['line', false]]`. An empty path (a
     * root mark) yields no segments and addresses nothing in `data`.
     *
     * @return list<array{key: string, array: bool}>
     */
    private static function parsePath(string $path): array
    {
        if ($path === '') {
            return [];
        }

        $segments = [];
        foreach (explode('.', $path) as $part) {
            $isArray = false;
            if (str_ends_with($part, '[]')) {
                $isArray = true;
                $part = substr($part, 0, -2);
            }
            $segments[] = ['key' => $part, 'array' => $isArray];
        }

        return $segments;
    }

    /**
     * Resolve $segments against the array $node and run $op on the leaf(s). It descends objects by
     * key and, when a segment is an array, fans out over every element. An absent key or a type
     * mismatch (a path that does not exist in this particular message) is skipped silently —
     * schemas describe the union of possible shapes; a given message need not contain every field.
     *
     * $node is the addressed object (an associative `data` sub-object), rewritten in place. A
     * non-leaf segment only ever descends into a further object, so every by-ref node stays a
     * keyed object; nested children are mutated on a working copy and written back into their slot
     * (PHP arrays are value types).
     *
     * @param  array<string, mixed>  $node
     * @param  list<array{key: string, array: bool}>  $segments
     * @param  callable(mixed): array{0: mixed, 1: bool}  $op
     */
    private static function applyAtPath(array &$node, array $segments, callable $op): void
    {
        if ($segments === []) {
            return; // root mark or exhausted path with no leaf key — nothing addressable in data
        }

        $segment = $segments[0];
        $key = $segment['key'];
        if (! array_key_exists($key, $node)) {
            return; // absent field — skip (not an error)
        }

        $rest = array_slice($segments, 1);
        $last = $rest === [];
        $child = $node[$key];

        if ($segment['array']) {
            if (! is_array($child) || ! array_is_list($child)) {
                return; // declared array but message has a non-list — skip
            }
            foreach ($child as $i => $elem) {
                if ($last) {
                    [$newValue, $replace] = $op($elem);
                    if ($replace) {
                        $child[$i] = $newValue;
                    }
                } elseif (self::isObject($elem)) {
                    self::applyAtPath($elem, $rest, $op);
                    $child[$i] = $elem;
                }
            }
            $node[$key] = $child;

            return;
        }

        if ($last) {
            [$newValue, $replace] = $op($child);
            if ($replace) {
                $node[$key] = $newValue;
            }

            return;
        }

        if (self::isObject($child)) {
            self::applyAtPath($child, $rest, $op);
            $node[$key] = $child;
        }
    }

    /**
     * Whether $value is a decoded-JSON object (a non-list array) we can descend into. A list or a
     * scalar is not an object node and is skipped (the path does not exist in this message shape).
     *
     * @phpstan-assert-if-true array<string, mixed> $value
     */
    private static function isObject(mixed $value): bool
    {
        return is_array($value) && ($value === [] || ! array_is_list($value));
    }
}
