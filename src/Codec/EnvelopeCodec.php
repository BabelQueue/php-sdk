<?php

declare(strict_types=1);

namespace BabelQueue\Codec;

use BabelQueue\Contracts\HasTraceId;
use BabelQueue\Contracts\PolyglotJob;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Support\Uuid;

/**
 * Builds, encodes and decodes the BabelQueue wire envelope — the single PHP
 * implementation of the canonical format that every PHP framework adapter
 * (Laravel, Symfony, ...) reuses, so they can never drift from one another.
 *
 * The shape is frozen as { job, trace_id, data, meta, attempts } so a strongly
 * typed consumer in any language (Go struct, Java POJO, ...) can bind it
 * directly. "job" carries the message URN (never a class name); "trace_id" is a
 * cross-service correlation id preserved across every hop; PHP's native
 * serialize() is never involved.
 *
 * Full spec: https://babelqueue.com
 */
final class EnvelopeCodec
{
    /**
     * Bumped only on a breaking envelope change, so consumers can refuse
     * messages they do not understand.
     */
    public const SCHEMA_VERSION = 1;

    /** Producer language tag for every PHP framework. */
    public const SOURCE_LANG = 'php';

    /**
     * Non-canonical top-level keys (message-envelope.md §10). Decode drops them with
     * a warning; encode never emits them (K-15).
     */
    public const FORBIDDEN_TOP_LEVEL_KEYS = ['timestamp'];

    /**
     * Non-canonical "meta" keys (message-envelope.md §10). Decode drops them with a
     * warning; encode never emits them (K-15).
     */
    public const FORBIDDEN_META_KEYS = ['max_retries', 'attempts', 'source', 'ts'];

    /**
     * Process-wide warning sink: fn(string $message, string $pointer): void.
     * When null, warnings go to error_log().
     *
     * @var (callable(string, string): void)|null
     */
    private static $warningHandler = null;

    /**
     * Build the canonical envelope directly from a URN + pure-JSON data — the
     * data-first entry point shared by every BabelQueue SDK (Go `Make`, Python
     * `make`, Node/Java/.NET `make`/`Make`). Use {@see fromJob()} when you already
     * have a {@see PolyglotJob} object.
     *
     * A non-empty `$traceId` continues an existing distributed trace; otherwise a
     * fresh UUID is minted. "attempts" is a top-level transport counter kept OUT of
     * the immutable "meta" block.
     *
     * @param  array<string, mixed>  $data  Pure, JSON-serialisable payload.
     * @param  string  $queue  The logical queue name (not the broker key).
     * @return array{job: string, trace_id: string, data: array<string, mixed>, meta: array<string, mixed>, attempts: int}
     *
     * @throws BabelQueueException When the URN is empty.
     */
    public static function make(string $urn, array $data = [], string $queue = 'default', ?string $traceId = null): array
    {
        $resolvedUrn = trim($urn);

        if ($resolvedUrn === '') {
            throw new BabelQueueException(
                'EnvelopeCodec::make() requires a non-empty URN so consumers can identify the '
                . 'message without any language-specific class name.',
            );
        }

        $inheritedTrace = $traceId === null ? '' : trim($traceId);

        return [
            'job' => $resolvedUrn,
            'trace_id' => $inheritedTrace !== '' ? $inheritedTrace : Uuid::v4(),
            'data' => $data,
            'meta' => [
                'id' => Uuid::v4(),
                'queue' => $queue,
                'lang' => self::SOURCE_LANG,
                'schema_version' => self::SCHEMA_VERSION,
                'created_at' => self::nowInMilliseconds(),
            ],
            'attempts' => 0,
        ];
    }

    /**
     * Build the canonical envelope for a {@see PolyglotJob} object. Delegates to
     * {@see make()}; "trace_id" is inherited when the job implements
     * {@see HasTraceId}, otherwise a fresh UUID is minted.
     *
     * @param  string  $queue  The logical queue name (not the broker key).
     * @return array{job: string, trace_id: string, data: array<string, mixed>, meta: array<string, mixed>, attempts: int}
     *
     * @throws BabelQueueException When the job exposes an empty URN.
     */
    public static function fromJob(PolyglotJob $job, string $queue): array
    {
        return self::make(
            self::resolveUrn($job),
            $job->toPayload(),
            $queue,
            $job instanceof HasTraceId ? $job->getBabelTraceId() : null,
        );
    }

    /**
     * Encode the envelope as a UTF-8 JSON string, failing fast on bad data.
     *
     * A "data" array that PHP holds as a list — empty, or keyed 0..n-1 such as a
     * decoded {"0":"a","1":"b"} — is written as a JSON object ({} / {"0":"a",...}),
     * never as a JSON array, since "data" is always an object on the wire
     * (message-envelope.md §3) and a decode → re-encode must keep its shape. Forbidden
     * keys (§10) are never emitted (K-15): they are stripped and reported through
     * the warning handler.
     *
     * @param  array<string, mixed>  $payload
     *
     * @throws \JsonException When the payload is not cleanly encodable.
     */
    public static function encode(array $payload): string
    {
        $payload = self::stripForbiddenKeys($payload, null, 'it was not emitted');

        if (array_key_exists('data', $payload) && is_array($payload['data']) && array_is_list($payload['data'])) {
            $payload['data'] = (object) $payload['data'];
        }

        return json_encode(
            $payload,
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES,
        );
    }

    /**
     * Decode a raw JSON body into an envelope array; returns [] on malformed
     * input so callers can treat it as an empty (poison) envelope.
     *
     * Forbidden keys (message-envelope.md §10) are tolerated but dropped, each with
     * a warning naming its JSON pointer (K-15), so a re-encode never carries them.
     * A "data" value that is a non-empty JSON array (a list, not an object) is
     * rejected: the result is [] (poison), like malformed JSON. The verdict comes
     * from the raw JSON shape, so an object with index-like keys ({"0":"a"}) is
     * accepted. An empty "data":[] is tolerated as {} because php-sdk <= 1.16.0
     * encoded an empty payload that way.
     *
     * @param  (callable(string, string): void)|null  $onWarning  Per-call warning
     *     sink; falls back to {@see setWarningHandler()} then error_log().
     * @return array<string, mixed>
     */
    public static function decode(string $rawBody, ?callable $onWarning = null): array
    {
        $decoded = json_decode($rawBody, true);

        if (! is_array($decoded)) {
            return [];
        }

        if (self::hasListData($decoded['data'] ?? null, $rawBody)) {
            self::warn(
                'BabelQueue envelope rejected: "data" (/data) is a JSON array, not an object '
                . '(message-envelope.md §3).',
                '/data',
                $onWarning,
            );

            return [];
        }

        return self::stripForbiddenKeys($decoded, $onWarning, 'it was dropped from the decoded envelope');
    }

    /**
     * Install (or clear, with null) the process-wide warning sink used by
     * {@see decode()} and {@see encode()} — e.g. a PSR-3 logger's warning() method.
     *
     * @param  (callable(string, string): void)|null  $handler  fn(string $message, string $pointer)
     */
    public static function setWarningHandler(?callable $handler): void
    {
        self::$warningHandler = $handler;
    }

    /**
     * The message URN: canonical "job", with "urn" accepted as an inbound alias.
     *
     * @param  array<string, mixed>  $envelope
     */
    public static function urn(array $envelope): string
    {
        $urn = $envelope['job'] ?? $envelope['urn'] ?? '';

        return is_string($urn) ? $urn : '';
    }

    /**
     * Whether a consumer should accept this envelope (consumer-side validation):
     * a non-empty URN, a supported meta.schema_version, a non-blank trace_id, an
     * array "data" and an integer "attempts". A list-shaped JSON "data" is rejected
     * by {@see decode()}, which still sees the raw JSON; a decoded PHP array cannot
     * tell [1,2] from {"0":1,"1":2}, and {@see encode()} always writes it as an
     * object. Accepts the "urn" alias (unlike the producer JSON Schema, which
     * requires "job").
     *
     * @param  array<string, mixed>  $envelope
     */
    public static function accepts(array $envelope): bool
    {
        if (self::urn($envelope) === '') {
            return false;
        }

        $meta = $envelope['meta'] ?? null;
        if (! is_array($meta) || ($meta['schema_version'] ?? null) !== self::SCHEMA_VERSION) {
            return false;
        }

        if (! is_array($envelope['data'] ?? null)) {
            return false;
        }

        if (! is_int($envelope['attempts'] ?? null)) {
            return false;
        }

        $traceId = $envelope['trace_id'] ?? null;

        return is_string($traceId) && $traceId !== '';
    }

    /**
     * Whether the raw body's "data" is a non-empty JSON array (a list, not an object).
     *
     * json_decode(..., true) turns both [1,2] and {"0":1,"1":2} into the same PHP
     * list, so a list-shaped "data" is re-checked against the raw JSON in object
     * mode, where an object stays a stdClass. That second decode runs only on the
     * list-shaped branch, never on the common path. An empty array stays tolerated
     * (php-sdk <= 1.16.0 encoded an empty payload as "data":[]).
     *
     * @param  mixed  $data  The "data" value as decoded by json_decode(..., true).
     */
    private static function hasListData(mixed $data, string $rawBody): bool
    {
        if (! is_array($data) || $data === [] || ! array_is_list($data)) {
            return false;
        }

        $raw = json_decode($rawBody);

        return $raw instanceof \stdClass && is_array($raw->data ?? null);
    }

    /**
     * Remove the §10 forbidden keys, reporting each one by its JSON pointer.
     *
     * @param  array<string, mixed>  $envelope
     * @param  (callable(string, string): void)|null  $onWarning
     * @param  string  $outcome  What happened to the key, for the warning text.
     * @return array<string, mixed>
     */
    private static function stripForbiddenKeys(array $envelope, ?callable $onWarning, string $outcome): array
    {
        foreach (self::FORBIDDEN_TOP_LEVEL_KEYS as $key) {
            if (array_key_exists($key, $envelope)) {
                unset($envelope[$key]);
                self::warnForbidden('/' . $key, $onWarning, $outcome);
            }
        }

        if (isset($envelope['meta']) && is_array($envelope['meta'])) {
            foreach (self::FORBIDDEN_META_KEYS as $key) {
                if (array_key_exists($key, $envelope['meta'])) {
                    unset($envelope['meta'][$key]);
                    self::warnForbidden('/meta/' . $key, $onWarning, $outcome);
                }
            }
        }

        return $envelope;
    }

    /**
     * @param  (callable(string, string): void)|null  $onWarning
     */
    private static function warnForbidden(string $pointer, ?callable $onWarning, string $outcome): void
    {
        self::warn(
            sprintf(
                'BabelQueue envelope carries the forbidden key %s (message-envelope.md §10); %s.',
                $pointer,
                $outcome,
            ),
            $pointer,
            $onWarning,
        );
    }

    /**
     * @param  (callable(string, string): void)|null  $onWarning
     */
    private static function warn(string $message, string $pointer, ?callable $onWarning): void
    {
        $handler = $onWarning ?? self::$warningHandler;

        if ($handler !== null) {
            $handler($message, $pointer);

            return;
        }

        error_log('[babelqueue] ' . $message);
    }

    /**
     * Resolve and validate the job's URN. A blank URN is a programming error.
     *
     * @throws BabelQueueException
     */
    private static function resolveUrn(PolyglotJob $job): string
    {
        $urn = trim($job->getBabelUrn());

        if ($urn === '') {
            throw new BabelQueueException(sprintf(
                '%s::getBabelUrn() returned an empty value. A polyglot message must expose a '
                . 'stable, non-empty URN so consumers can identify it without any PHP class name.',
                $job::class,
            ));
        }

        return $urn;
    }

    /** Current Unix time in milliseconds (UTC). */
    private static function nowInMilliseconds(): int
    {
        return (int) round(microtime(true) * 1000);
    }
}
