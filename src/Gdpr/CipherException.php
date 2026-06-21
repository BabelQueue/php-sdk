<?php

declare(strict_types=1);

namespace BabelQueue\Gdpr;

use BabelQueue\Exceptions\BabelQueueException;

/**
 * A {@see Cipher} could not complete an `encrypt()` or `decrypt()` operation (ADR-0030): an
 * invalid key size, a malformed ciphertext, or a failed authentication tag (wrong key / tampered
 * input). The reference {@see OpenSslCipher} throws this; a KMS-backed cipher should too.
 *
 * On the consume side {@see Gdpr::unprotect()} catches this and re-throws a {@see DecryptException}
 * so a wrong-key / tampered field is distinguishable from an absent one (which is skipped).
 */
final class CipherException extends BabelQueueException
{
}
