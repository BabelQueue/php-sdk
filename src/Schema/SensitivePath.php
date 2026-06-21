<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

/**
 * One property a schema marked `x-gdpr-sensitive` (ADR-0030), located by its dotted path from
 * the schema root. Array elements use the `"field[]"` segment the payload validator and
 * babelqueue-registry's `compat` linter already use (e.g. `addresses[].line`). A mark on the
 * root schema itself is reported as the empty path `""`.
 *
 * {@see Category} is the optional free-form category from the string form
 * (`"x-gdpr-sensitive": "email"`), or `""` when the keyword was the boolean `true`. It is the
 * PHP mirror of the Go `schema.SensitivePath` value; {@see \BabelQueue\Gdpr\Gdpr} consumes these
 * paths to encrypt on produce and decrypt on consume.
 */
final class SensitivePath
{
    public function __construct(
        public readonly string $path,
        public readonly string $category = '',
    ) {
    }
}
