<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Support\Headers;
use Predis\ClientInterface;

/**
 * A framework-less Redis transport: publishes the canonical envelope with a plain
 * `RPUSH <queue> <payload>` — the list convention every BabelQueue SDK shares —
 * so a PHP producer interoperates with Go/Python/Node consumers on the identical
 * queue (they reserve with `BLMOVE <queue> <queue>:processing` and ack with
 * `LREM`). This is the seam used by a plain PHP / Slim / Mezzio app; the Laravel
 * and Symfony adapters publish through their own native queues instead.
 *
 * Optional dependency: `predis/predis` (a pure-PHP client; no extension needed).
 * phpredis (`ext-redis`) users can implement the one-method {@see \BabelQueue\Contracts\Transport}
 * directly — it is just an `rpush`.
 *
 * **Out-of-band headers (ADR-0028).** Redis stores only the raw list value (the LREM ack handle
 * *is* that value), so — unlike AMQP headers or SQS `MessageAttributes` — there is no native
 * per-message metadata channel. To carry headers (e.g. a W3C `traceparent` for cross-hop span
 * linkage) the transport owns a tiny JSON *frame* distinct from the wire envelope:
 *
 *     {"__bq_frame":1,"headers":{"traceparent":"00-…"},"body":"<raw wire envelope>"}
 *
 * `RPUSH` stores the frame, so the LREM ack handle stays byte-for-byte what was pushed and the
 * reliable-queue semantics are untouched. Framing is **opt-in and backward compatible**: only
 * {@see self::publishWithHeaders()} with a non-empty header map writes a frame; plain
 * {@see self::publish()} (and `publishWithHeaders` with no usable headers) stores the **bare**
 * envelope byte-for-byte, exactly as before. A consumer recovers the body + headers with
 * {@see self::unframe()}, which detects frame-vs-bare by the reserved `__bq_frame` sentinel — a
 * frozen wire envelope can never carry it — so a bare value yields `['<value>', []]` and
 * cross-version queues interoperate.
 */
final class RedisTransport implements HeaderPublisher
{
    /**
     * The reserved header-frame discriminator key + its current schema version. A frozen wire
     * envelope never carries `__bq_frame`, so its presence is how {@see self::unframe()} tells a
     * transport frame from a bare envelope without structural guessing.
     */
    private const FRAME_KEY = '__bq_frame';

    private const FRAME_VERSION = 1;

    public function __construct(
        private readonly ClientInterface $client,
        private readonly string $defaultQueue = 'default',
    ) {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $this->client->rpush($queue ?? $this->defaultQueue, [$payload]);

        // Redis lists carry no broker-assigned id; the envelope's own meta.id is
        // the message identity. Returning null keeps the contract honest.
        return null;
    }

    /**
     * Append `$payload` to the queue together with out-of-band `$headers`
     * ({@see HeaderPublisher}, ADR-0028).
     *
     * A non-empty (post-sanitisation) header map is RPUSHed as a transport-owned frame
     * (`{"__bq_frame":1,"headers":…,"body":<raw envelope>}`) that carries the headers beside the
     * frozen envelope (GR-1); blank keys/values are dropped, and if nothing survives it degrades to
     * a byte-identical bare {@see self::publish()}. The stored frame *is* the LREM ack handle, so
     * the reliable-queue semantics are untouched.
     *
     * @param  array<string, string>  $headers
     */
    public function publishWithHeaders(string $payload, array $headers, ?string $queue = null): ?string
    {
        $this->client->rpush($queue ?? $this->defaultQueue, [self::frameValue($payload, $headers)]);

        return null;
    }

    /**
     * The pure produce-side decision: the exact string to RPUSH for `$body` + `$headers`.
     *
     * With no usable headers it returns `$body` verbatim (the bare form, so plain
     * {@see self::publish()} and {@see self::publishWithHeaders()}-without-headers store
     * byte-identical values); otherwise it returns the transport-owned frame JSON. Kept pure so the
     * framing decision is unit-testable without a broker.
     *
     * @param  array<string, string>  $headers
     */
    public static function frameValue(string $body, array $headers): string
    {
        $clean = Headers::sanitize($headers);
        if ($clean === []) {
            return $body;
        }

        $frame = json_encode(
            [self::FRAME_KEY => self::FRAME_VERSION, 'headers' => $clean, 'body' => $body],
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
        );

        // json_encode of a string-keyed array of strings cannot fail; the bare fallback is purely
        // defensive so framing never throws.
        return $frame === false ? $body : $frame;
    }

    /**
     * Interpret a stored Redis list value: return `[wire-envelope-body, headers]`.
     *
     * A value is a header frame iff it is a JSON object carrying the reserved `__bq_frame`
     * sentinel (a frozen wire envelope never has it); then it yields the unframed body plus the
     * carried headers. Any other value — a bare envelope, non-JSON, or JSON without the sentinel —
     * is returned verbatim as the body with `[]` headers, so older/cross-version queue values
     * consume exactly as before. A framework worker calls this on the reserved list value, then
     * acks with the **original** value (the frame, when present) so LREM still matches.
     *
     * @return array{0: string, 1: array<string, string>}
     */
    public static function unframe(string $value): array
    {
        // Cheap reject: a frame is always a JSON object, and the sentinel substring must appear.
        // This avoids a full decode for the overwhelmingly common bare-envelope case (the check
        // only short-circuits negatives).
        if ($value === '' || $value[0] !== '{' || ! str_contains($value, '"' . self::FRAME_KEY . '"')) {
            return [$value, []];
        }

        $decoded = json_decode($value, true);
        if (
            ! is_array($decoded)
            || empty($decoded[self::FRAME_KEY])
            || ! array_key_exists('body', $decoded)
            || ! is_string($decoded['body'])
        ) {
            return [$value, []];
        }

        $headers = is_array($decoded['headers'] ?? null) ? Headers::sanitize($decoded['headers']) : [];

        return [$decoded['body'], $headers];
    }
}
