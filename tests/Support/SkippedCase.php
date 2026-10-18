<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Support;

/**
 * Thrown from a {@see ConformanceResults::each()} callback to skip one case. The message is the
 * recorded reason and must cite the authorising ADR (ADR-0032: no silent skip).
 */
final class SkippedCase extends \RuntimeException
{
}
