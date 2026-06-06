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
     * Build the canonical envelope for a polyglot job.
     *
     * "trace_id" is inherited when the job implements {@see HasTraceId} (so a
     * handler can keep a downstream message inside the same trace), otherwise a
     * fresh UUID is minted. "attempts" is a top-level transport counter kept OUT
     * of the immutable "meta" block — it also satisfies Laravel's Redis
     * reservation script, which increments payload.attempts on every pop.
     *
     * @param  string  $queue  The logical queue name (not the broker key).
     * @return array{job: string, trace_id: string, data: array<string, mixed>, meta: array<string, mixed>, attempts: int}
     *
     * @throws BabelQueueException When the job exposes an empty URN.
     */
    public static function fromJob(PolyglotJob $job, string $queue): array
    {
        return [
            'job' => self::resolveUrn($job),
            'trace_id' => self::resolveTraceId($job),
            'data' => $job->toPayload(),
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

    /**
     * Reuse an inherited trace id ({@see HasTraceId}) so the message stays part
     * of the same distributed trace; otherwise mint a fresh v4 UUID. Distinct
     * from meta.id, which is unique per message.
     */
    private static function resolveTraceId(PolyglotJob $job): string
    {
        if ($job instanceof HasTraceId) {
            $traceId = trim((string) $job->getBabelTraceId());

            if ($traceId !== '') {
                return $traceId;
            }
        }

        return Uuid::v4();
    }

    /** Current Unix time in milliseconds (UTC). */
    private static function nowInMilliseconds(): int
    {
        return (int) round(microtime(true) * 1000);
    }
}
