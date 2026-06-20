<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * The consume-side counterpart of {@see HeaderPublisher}: an {@see InboundMessage} that can
 * surface the **out-of-band transport headers** a producer carried beside the frozen wire
 * envelope (GR-1) — never inside it.
 *
 * It is the PHP mirror of Go's `babelqueue.ReceivedMessage.Headers` /
 * `babelqueue.HeadersFromContext` and Python's `babelqueue.headers.headers_from_context`
 * (ADR-0028). A handler — or an optional wrapper such as {@see \BabelQueue\Otel\Tracing::wrap()} —
 * reads per-message metadata that travelled beside the envelope (e.g. a W3C `traceparent` for
 * cross-hop span parent-child linkage, or the `bq-replay-bypass` marker, ADR-0027).
 *
 * A message whose transport surfaces no headers returns an empty map; reads are always nil-safe.
 * The returned map is read-only — treat it as immutable.
 */
interface HasHeaders
{
    /**
     * The out-of-band transport headers that arrived with this message, or an empty map when none
     * were carried (or the transport surfaces none). Read-only.
     *
     * @return array<string, string>
     */
    public function headers(): array;
}
