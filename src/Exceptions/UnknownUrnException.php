<?php

declare(strict_types=1);

namespace BabelQueue\Exceptions;

/**
 * Thrown when a consumed message carries a URN that has no handler mapped
 * (and the "unknown URN" strategy is "fail").
 */
class UnknownUrnException extends BabelQueueException
{
}
