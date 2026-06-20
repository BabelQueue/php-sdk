<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * An optional {@see Transport} capability: publish an already-encoded envelope **together with
 * out-of-band transport headers**, for brokers that carry per-message metadata beside the body.
 *
 * It is the PHP counterpart of Go's `babelqueue.HeaderPublisher` (`PublishWithHeaders`) and
 * Python's `BabelQueue.publish_with_headers` (ADR-0028). The headers ride **beside** the frozen
 * wire envelope (GR-1) — never inside it; `schema_version` stays `1`. This is the same
 * out-of-band seam the replay-bypass marker uses (ADR-0027); a W3C `traceparent` for cross-hop
 * span parent-child linkage is the second rider on it.
 *
 * A transport that does **not** implement this simply does not propagate headers — callers
 * (e.g. {@see \BabelQueue\Otel\Tracing::publish()}) fall back to a plain {@see Transport::publish()},
 * dropping the headers with no error, exactly as ADR-0027 specifies. Implementations MUST keep a
 * header-less publish byte-identical to {@see Transport::publish()} so a plain producer and a
 * cross-version consumer interoperate unchanged.
 */
interface HeaderPublisher extends Transport
{
    /**
     * Publish a raw, already-encoded (UTF-8 JSON) envelope onto a queue together with the given
     * out-of-band transport headers.
     *
     * @param  string  $payload  The already-encoded wire envelope (unchanged; GR-1).
     * @param  array<string, string>  $headers  Out-of-band headers to carry beside the envelope
     *                                           (e.g. `['traceparent' => '00-…']`). An empty map,
     *                                           or one whose keys/values are all blank, MUST behave
     *                                           exactly like {@see Transport::publish()}.
     * @param  string|null  $queue  Logical queue name, or null for the default.
     * @return string|null  The published message id, if the transport exposes one.
     */
    public function publishWithHeaders(string $payload, array $headers, ?string $queue = null): ?string;
}
