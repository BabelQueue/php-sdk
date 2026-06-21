<?php

declare(strict_types=1);

namespace BabelQueue\Gdpr;

use BabelQueue\Exceptions\BabelQueueException;

/**
 * {@see Gdpr::unprotect()} could not restore a protected field on the consume side (ADR-0030): a
 * wrong key, a tampered / garbled ciphertext, or a value that is not the string
 * {@see Gdpr::protect()} produced. It is the PHP mirror of the Go `gdpr.ErrDecrypt`.
 *
 * `unprotect()` stops at the first such failure and throws this, so it is distinguishable from a
 * missing field (which is skipped, not an error). A consumer should treat it as a poison message
 * — fail it (retry / dead-letter) rather than process unreadable PII. {@see self::path()} is the
 * dotted path of the field that could not be decrypted.
 */
final class DecryptException extends BabelQueueException
{
    private function __construct(
        private readonly string $path,
        string $message,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }

    public static function at(string $path, ?\Throwable $previous = null): self
    {
        $where = $path === '' ? 'the root field' : "field [{$path}]";
        $detail = $previous !== null ? ': ' . $previous->getMessage() : '';

        return new self($path, "Cannot decrypt protected {$where}{$detail}.", $previous);
    }

    /** The dotted path of the field that could not be decrypted (`""` for a root mark). */
    public function path(): string
    {
        return $this->path;
    }
}
