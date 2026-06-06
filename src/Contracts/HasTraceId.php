<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * Optional contract for a polyglot job that wants to *continue an existing
 * distributed trace* rather than start a new one.
 *
 * Every BabelQueue envelope carries a required top-level "trace_id" — a
 * cross-service correlation id that is preserved and forwarded unchanged across
 * every hop and language boundary. Normally the producing SDK mints a fresh
 * trace id automatically. But when a consumer handles a message (trace X) and
 * dispatches a *downstream* job as a result, that new job should stay part of
 * trace X. Implementing this interface lets the job declare the inherited trace
 * id so {@see \BabelQueue\Codec\EnvelopeCodec} reuses it instead of minting a
 * new one.
 *
 * Return null (or an empty string) to fall back to automatic generation.
 *
 * Distinct from {@see HasBabelUrn} (identity) — this is about
 * correlation/observability, not routing.
 */
interface HasTraceId
{
    /**
     * The trace id to carry on the wire, or null to mint a new one.
     */
    public function getBabelTraceId(): ?string;
}
