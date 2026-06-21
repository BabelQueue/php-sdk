<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

/**
 * An optional {@see RedriveIO} capability: re-publish a redriven body **with out-of-band transport
 * headers** beside it — the redrive-side counterpart of {@see \BabelQueue\Contracts\HeaderPublisher}.
 *
 * It exists so {@see Redrive} can stamp the `bq-replay-bypass` marker (ADR-0027) on a replayed
 * message when its options request it, the same way Go's `Redrive` checks whether its transport is a
 * `HeaderPublisher` before stamping the header. A {@see RedriveIO} that does **not** implement this
 * cannot carry the marker, so `bypass` is a documented no-op on it (sandbox routing remains the
 * fallback) — exactly the best-effort contract ADR-0027 specifies. Headers ride beside the frozen
 * wire envelope, never inside it (GR-1).
 */
interface HeaderRedriveIO extends RedriveIO
{
    /**
     * Publish a raw, already-encoded envelope body onto `$queue` together with the given out-of-band
     * transport headers. An empty header map (or one whose keys/values are all blank) MUST behave
     * exactly like {@see RedriveIO::publish()}.
     *
     * @param  array<string, string>  $headers  e.g. `['bq-replay-bypass' => '1']`.
     */
    public function publishWithHeaders(string $queue, string $body, array $headers): void;
}
