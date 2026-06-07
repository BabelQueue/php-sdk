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
     * @param  array<string, mixed>  $payload
     *
     * @throws \JsonException When the payload is not cleanly encodable.
     */
    public static function encode(array $payload): string
    {
        return json_encode(
            $payload,
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES,
        );
    }

    /**
     * Decode a raw JSON body into an envelope array; returns [] on malformed
     * input so callers can treat it as an empty (poison) envelope.
     *
     * @return array<string, mixed>
     */
    public static function decode(string $rawBody): array
    {
        $decoded = json_decode($rawBody, true);

        return is_array($decoded) ? $decoded : [];
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
     * object "data" and an integer "attempts". Accepts the "urn" alias (unlike the
     * producer JSON Schema, which requires "job").
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
