<?php

declare(strict_types=1);

namespace BabelQueue\Schema;

use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Exceptions\InvalidPayloadException;

/**
 * Optional per-URN `data` schema validation for a babelqueue producer or consumer
 * (ADR-0024). A {@see SchemaProvider} supplies a JSON Schema for a message URN — typically
 * read from a babelqueue-registry `registry.json` — and the message's `data` is validated
 * against it. It is opt-in: a URN with no registered schema is never validated.
 *
 * - **Producer-side (recommended):** call {@see self::assert()} before publishing so invalid
 *   data never enters the queue, or {@see self::check()} to branch without throwing.
 * - **Consumer-side (safety net):** wrap a handler with {@see self::wrap()}. It composes with
 *   the consume runtime's ack-on-return / redeliver-on-throw contract
 *   ({@see \BabelQueue\Consume\Dispatcher}):
 *
 *       $dispatch->on('urn:babel:orders:created', SchemaValidated::wrap($provider, $handler));
 *
 *   Invalid data throws {@see InvalidPayloadException}, so the message redelivers and is
 *   eventually dead-lettered (a poison message does not become valid on retry — hence
 *   producer-side validation is preferred). A URN with no schema runs the handler unchanged.
 *
 * The PHP mirror of the Go `schema.Check` / `schema.Wrap` helpers.
 */
final class SchemaValidated
{
    /**
     * The first `data` violation for ($urn, $data), or null when it is valid or when no
     * schema is registered for the URN (opt-in). Non-throwing; for producer-side branching.
     *
     * @param  array<string, mixed>  $data
     */
    public static function check(SchemaProvider $provider, string $urn, array $data): ?string
    {
        $schema = $provider->schemaFor($urn);
        if ($schema === null) {
            return null;
        }

        return PayloadValidator::check($schema, $data);
    }

    /**
     * Assert that ($urn, $data) matches its registered schema, throwing otherwise. The
     * producer-side guard: call it before publishing.
     *
     * @param  array<string, mixed>  $data
     *
     * @throws InvalidPayloadException
     */
    public static function assert(SchemaProvider $provider, string $urn, array $data): void
    {
        $violation = self::check($provider, $urn, $data);
        if ($violation !== null) {
            throw InvalidPayloadException::because($violation, $urn, $data);
        }
    }

    /**
     * Wrap a consume handler so each message's `data` is validated against its URN schema
     * before the handler runs.
     *
     * @param  callable(ConsumedMessage): void  $handler
     * @return callable(ConsumedMessage): void
     */
    public static function wrap(SchemaProvider $provider, callable $handler): callable
    {
        return static function (ConsumedMessage $message) use ($provider, $handler): void {
            self::assert($provider, $message->getUrn(), $message->getData());
            $handler($message);
        };
    }
}
