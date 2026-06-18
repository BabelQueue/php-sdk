<?php

declare(strict_types=1);

namespace BabelQueue\Idempotency;

use BabelQueue\Contracts\ConsumedMessage;

/**
 * Wraps a consume handler so a message whose `meta.id` was already processed
 * successfully is **skipped** instead of run again — the dependency-free helper for the
 * "handlers SHOULD be idempotent" guidance in error-handling.md §1 (ADR-0022).
 *
 * It composes with the consume runtime's **ack-on-return / redeliver-on-throw** contract
 * ({@see \BabelQueue\Consume\Dispatcher}):
 *
 *     $dispatch->on('urn:babel:orders:created', Idempotent::wrap($store, $handler));
 *
 * - A previously-succeeded id → the wrapper **returns**, so the loop acks it and the
 *   broker stops redelivering.
 * - The handler **throws** → the id is left unmarked and the exception propagates, so
 *   retry / DLQ (§§5–6) still apply and a later delivery runs the handler again.
 * - A message with no usable `meta.id` runs unchanged (fail-open).
 *
 * This is the PHP mirror of the Go `idempotency.Wrap(store, handler)` helper.
 */
final class Idempotent
{
    /**
     * @param  callable(ConsumedMessage): void  $handler
     * @return callable(ConsumedMessage): void
     */
    public static function wrap(IdempotencyStore $store, callable $handler): callable
    {
        return static function (ConsumedMessage $message) use ($store, $handler): void {
            $meta = $message->getMeta();
            $id = isset($meta['id']) && is_string($meta['id']) ? $meta['id'] : '';

            // No usable id → cannot dedupe; run the handler unchanged.
            if ($id === '') {
                $handler($message);

                return;
            }

            // Already processed on an earlier delivery: skip + return so the loop acks it.
            if ($store->seen($id)) {
                return;
            }

            // First success wins. A throw here leaves the id unmarked → retry/DLQ apply.
            $handler($message);
            $store->remember($id);
        };
    }
}
