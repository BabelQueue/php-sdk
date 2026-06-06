<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * A framework-agnostic producible message: something that carries a URN
 * ({@see HasBabelUrn}) and a pure, JSON-serialisable payload.
 *
 * This is the core producer contract. Framework adapters extend it with their
 * own dispatch ergonomics — e.g. the Laravel adapter's `ShouldQueuePolyglot`
 * additionally extends Illuminate's `ShouldQueue` so the familiar `dispatch()`
 * pipeline keeps working. The encoder ({@see \BabelQueue\Codec\EnvelopeCodec})
 * only ever depends on this interface, never on any framework type.
 *
 * Implementations MUST expose nothing but pure, JSON-encodable data from
 * {@see toPayload()} — no objects, closures or resources.
 */
interface PolyglotJob extends HasBabelUrn
{
    /**
     * The pure, JSON-serialisable payload carried under the envelope's "data".
     *
     * @return array<string, mixed>
     */
    public function toPayload(): array;
}
