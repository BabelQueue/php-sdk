<?php

declare(strict_types=1);

namespace BabelQueue\Support;

/**
 * Minimal, dependency-free RFC 4122 version 4 (random) UUID generator.
 *
 * The core deliberately avoids pulling in ramsey/uuid, symfony/uid or
 * illuminate/support just to mint an id — honouring the "zero heavy
 * dependencies" rule. trace_id and meta.id are generated with this.
 */
final class Uuid
{
    /** Generate a random (v4) UUID, e.g. "7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b". */
    public static function v4(): string
    {
        $bytes = random_bytes(16);

        // Set version (4) and variant (10xx) bits.
        $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
        $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
    }
}
