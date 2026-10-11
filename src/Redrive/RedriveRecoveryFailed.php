<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

use RuntimeException;
use Throwable;

/**
 * Thrown by {@see Redrive} when more than one step of a failed run failed: the redrive write and its
 * restore, or either of them and the {@see RewindingRedriveIO::rewind()}. Every underlying error is kept
 * in {@see getErrors()}, in the order they happened (flat); the first is also the exception's `previous`.
 */
final class RedriveRecoveryFailed extends RuntimeException
{
    /**
     * @param  non-empty-list<Throwable>  $errors
     */
    public function __construct(string $message, private readonly array $errors)
    {
        parent::__construct($message, 0, $errors[0]);
    }

    /**
     * @return non-empty-list<Throwable>
     */
    public function getErrors(): array
    {
        return $this->errors;
    }
}
