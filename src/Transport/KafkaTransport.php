<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Contracts\KeyedPublisher;
use BabelQueue\Support\Headers;

/**
 * A framework-less **Apache Kafka** producer — PHP's path to the §6 binding via the
 * `ext-rdkafka` extension (the `php-rdkafka` PECL extension over `librdkafka`).
 *
 * **GR-7 note:** unlike every other PHP transport, this one's only viable client is a **C
 * extension**, not a pure-userland "standard driver", so it is **opt-in** and deliberately relaxes
 * the zero-heavy-dependencies rule for Kafka (see ADR-0019). It stays decoupled from `ext-rdkafka`
 * behind the one-method {@see KafkaProducer} seam, so the transport itself is dependency-free and
 * unit-tests against a fake.
 *
 * Per [§6 of the broker-bindings contract](https://babelqueue.com): the record **value** is the
 * canonical envelope JSON; the record **timestamp** mirrors `meta.created_at` (Unix ms); and the
 * contract fields are mirrored onto `bq-` **record headers** (UTF-8 byte strings, so a Java / Go /
 * Node / Python / .NET consumer routes on `bq-job` without decoding the body) — note **hyphens**
 * here (Kafka), unlike the §7 Artemis `bq_` underscores. The envelope is unchanged
 * (`schema_version` stays 1); Apache Kafka is purely additive.
 *
 * It is also a {@see HeaderPublisher} (ADR-0028): out-of-band headers such as a W3C `traceparent` /
 * `tracestate` ride as extra record headers beside the `bq-` projection, never inside the envelope
 * (GR-1); a `bq-` header of the projection wins a key collision (broker-bindings.md §6.3).
 *
 * This is a **producer** (the core {@see \BabelQueue\Contracts\Transport} is publish-only, like the Redis/AMQP/SQS/STOMP
 * transports); PHP consumes Kafka via a framework worker.
 */
final class KafkaTransport implements KeyedPublisher, HeaderPublisher
{
    /**
     * The record headers this transport owns: the §6.3 projection plus the §6.4 retry-topic pair. They are never
     * accepted from the out-of-band carrier of {@see self::publishWithHeaders()}, whether or not the projection
     * writes them on this record (same set as the Go `contractHeaders`).
     */
    private const RESERVED_HEADERS = [
        'bq-job', 'bq-trace-id', 'bq-message-id', 'bq-schema-version', 'bq-source-lang', 'bq-attempts',
        'bq-delay', 'bq-original-topic',
    ];

    public function __construct(
        private readonly KafkaProducer $producer,
        private readonly string $defaultTopic = 'default',
        private readonly string $topicPrefix = '',
    ) {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        return $this->send($payload, $queue, null, []);
    }

    /**
     * Publish with the original record key carried through ({@see KeyedPublisher}); a null key — or a
     * producer that is not a {@see KeyedKafkaProducer} — is exactly {@see self::publish()}.
     */
    public function publishKeyed(string $payload, ?string $queue, ?string $key): ?string
    {
        return $this->send($payload, $queue, $key, []);
    }

    /**
     * Publish with extra out-of-band record headers ({@see HeaderPublisher}), e.g. a W3C `traceparent`
     * (ADR-0028). They are written beside the §6 `bq-` projection; on a key collision the projection wins
     * (broker-bindings.md §6.3). Blank names and blank values are dropped, and the eight reserved `bq-` contract
     * headers (`bq-job` ... `bq-delay`, `bq-original-topic`) are never taken from the carrier, so a rider cannot
     * redirect a record's retry chain. An empty map is exactly {@see self::publish()}.
     */
    public function publishWithHeaders(string $payload, array $headers, ?string $queue = null): ?string
    {
        return $this->send($payload, $queue, null, $headers);
    }

    /**
     * @param  array<string, string>  $extraHeaders
     */
    private function send(string $payload, ?string $queue, ?string $key, array $extraHeaders): ?string
    {
        $target = $queue ?? $this->defaultTopic;
        // Decoded only to project the transport headers; the payload is published verbatim, so a
        // silent sink keeps decode from logging a misleading "dropped" warning (K-15).
        $envelope = EnvelopeCodec::decode($payload, static function (): void {});
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $timestamp = isset($meta['created_at']) && is_int($meta['created_at']) ? $meta['created_at'] : null;
        // Reserved contract headers never come from the carrier; the projection is merged last as well.
        $riders = array_diff_key(Headers::sanitize($extraHeaders), array_flip(self::RESERVED_HEADERS));
        $headers = Headers::merge($riders, $this->headers($envelope));
        KafkaProduce::send($this->producer, $this->topicPrefix . $target, $payload, $headers, $timestamp, $key);

        $id = $meta['id'] ?? null;

        return is_scalar($id) ? (string) $id : null;
    }

    /**
     * The §6 `bq-` header projection (UTF-8 strings; integers stringified).
     *
     * @param  array<string, mixed>  $envelope
     * @return array<string, string>
     */
    private function headers(array $envelope): array
    {
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $headers = [];

        $urn = EnvelopeCodec::urn($envelope);
        if ($urn !== '') {
            $headers['bq-job'] = $urn;
        }

        $traceId = $envelope['trace_id'] ?? null;
        if (is_string($traceId) && $traceId !== '') {
            $headers['bq-trace-id'] = $traceId;
        }

        if (isset($meta['id']) && is_scalar($meta['id'])) {
            $headers['bq-message-id'] = (string) $meta['id'];
        }

        if (isset($meta['schema_version']) && is_scalar($meta['schema_version'])) {
            $headers['bq-schema-version'] = (string) $meta['schema_version'];
        }

        if (isset($meta['lang']) && is_string($meta['lang']) && $meta['lang'] !== '') {
            $headers['bq-source-lang'] = $meta['lang'];
        }

        $attempts = $envelope['attempts'] ?? 0;
        $headers['bq-attempts'] = (string) (is_scalar($attempts) ? $attempts : 0);

        return $headers;
    }
}
