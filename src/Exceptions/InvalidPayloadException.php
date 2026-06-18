<?php

declare(strict_types=1);

namespace BabelQueue\Exceptions;

/**
 * A message's `data` block failed validation against the JSON Schema registered for its URN
 * (ADR-0024). {@see self::violation()} is the first `"<json-pointer>: <reason>"` mismatch,
 * {@see self::urn()} the message URN, and {@see self::data()} the offending payload — for
 * logging, branching, or dead-lettering.
 *
 * Thrown by the consumer-side {@see \BabelQueue\Schema\SchemaValidated::wrap()} so the
 * consume runtime redelivers (and eventually dead-letters) a poison message; the recommended
 * primary use is producer-side ({@see \BabelQueue\Schema\SchemaValidated::assert()}) so
 * invalid data never enters the queue.
 */
final class InvalidPayloadException extends BabelQueueException
{
    /**
     * @param  array<string, mixed>  $data
     */
    private function __construct(
        private readonly string $violation,
        private readonly string $urn,
        private readonly array $data,
    ) {
        parent::__construct("Message data for [{$urn}] does not match its URN schema: {$violation}.");
    }

    /**
     * @param  array<string, mixed>  $data
     */
    public static function because(string $violation, string $urn, array $data): self
    {
        return new self($violation, $urn, $data);
    }

    /** The first `"<json-pointer>: <reason>"` violation. */
    public function violation(): string
    {
        return $this->violation;
    }

    /** The message URN whose schema was violated. */
    public function urn(): string
    {
        return $this->urn;
    }

    /**
     * The offending `data` payload.
     *
     * @return array<string, mixed>
     */
    public function data(): array
    {
        return $this->data;
    }
}
