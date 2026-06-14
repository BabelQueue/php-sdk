<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\Transport;

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
 * This is a **producer** (the core {@see Transport} is publish-only, like the Redis/AMQP/SQS/STOMP
 * transports); PHP consumes Kafka via a framework worker.
 */
final class KafkaTransport implements Transport
{
    public function __construct(
        private readonly KafkaProducer $producer,
        private readonly string $defaultTopic = 'default',
        private readonly string $topicPrefix = '',
    ) {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $target = $queue ?? $this->defaultTopic;
        $envelope = EnvelopeCodec::decode($payload);
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $timestamp = isset($meta['created_at']) && is_int($meta['created_at']) ? $meta['created_at'] : null;
        $this->producer->produce($this->topicPrefix . $target, $payload, $this->headers($envelope), $timestamp);

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
