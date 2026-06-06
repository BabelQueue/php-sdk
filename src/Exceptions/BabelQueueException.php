<?php

declare(strict_types=1);

namespace BabelQueue\Exceptions;

use RuntimeException;

/**
 * Base exception for every recoverable BabelQueue failure (bad config,
 * unsupported broker feature, empty URN, ...). Catching this type lets callers
 * handle all BabelQueue-originated errors with a single catch block.
 */
class BabelQueueException extends RuntimeException
{
}
