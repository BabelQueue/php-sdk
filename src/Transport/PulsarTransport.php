<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\Transport;

/**
 * A framework-less **Apache Pulsar** producer — PHP's path to the §5 binding via Pulsar's native
 * **WebSocket API**. Pulsar has no mature native PHP client (its binary protocol is C++/Java/Go
 * first), but it ships a first-class WebSocket producer/consumer service on the HTTP port, and a
 * pure-PHP WebSocket client (e.g. `textalk/websocket`) drives it.
 *
 * **GR-7 stays intact** (unlike the §6 Kafka path, ADR-0019): the WebSocket client is
 * pure-userland — no C extension — so this transport is opt-in behind a `suggest`, like the §7
 * Artemis STOMP path (ADR-0020). It is decoupled from the WebSocket library behind the one-method
 * {@see PulsarWebSocketClient} seam, so the transport itself is dependency-free and unit-tests
 * against a fake.
 *
 * Per [§5 of the broker-bindings contract](https://babelqueue.com): the message **value** is the
 * canonical envelope JSON; the contract fields are mirrored onto `bq-` native **message
 * properties** (Pulsar properties are an arbitrary string→string map, so integers are stringified
 * and a Java / Go / Node / Python / .NET consumer routes on `bq-job` without decoding the body) —
 * note **hyphens** here (as on Kafka/SQS), unlike the §7 Artemis `bq_` underscores. The native
 * `publishTimestamp` is broker-set and informational; the body's `meta.created_at` stays
 * authoritative (the adapter MUST NOT set the WebSocket frame's `eventTime`). The envelope is
 * unchanged (`schema_version` stays 1); Apache Pulsar is purely additive.
 *
 * The BabelQueue "queue" maps to the Pulsar topic `persistent://<tenant>/<namespace>/<queue>`
 * (§5.1; tenant/namespace default to `public`/`default`). This is a **producer** (the core
 * {@see Transport} is publish-only, like the Redis/AMQP/SQS/STOMP/Kafka transports); PHP consumes
 * Pulsar via a framework worker.
 */
final class PulsarTransport implements Transport
{
    public function __construct(
        private readonly PulsarWebSocketClient $client,
        private readonly string $defaultQueue = 'default',
        private readonly string $tenant = 'public',
        private readonly string $namespace = 'default',
    ) {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $target = $queue ?? $this->defaultQueue;
        $envelope = EnvelopeCodec::decode($payload);

        $topic = sprintf('persistent://%s/%s/%s', $this->tenant, $this->namespace, $target);
        $this->client->publish($topic, $payload, $this->properties($envelope));

        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];
        $id = $meta['id'] ?? null;

        return is_scalar($id) ? (string) $id : null;
    }

    /**
     * The §5 `bq-` native-property projection (string→string; integers stringified).
     *
     * @param  array<string, mixed>  $envelope
     * @return array<string, string>
     */
    private function properties(array $envelope): array
    {
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $properties = [];

        $urn = EnvelopeCodec::urn($envelope);
        if ($urn !== '') {
            $properties['bq-job'] = $urn;
        }

        $traceId = $envelope['trace_id'] ?? null;
        if (is_string($traceId) && $traceId !== '') {
            $properties['bq-trace-id'] = $traceId;
        }

        if (isset($meta['id']) && is_scalar($meta['id'])) {
            $properties['bq-message-id'] = (string) $meta['id'];
        }

        if (isset($meta['schema_version']) && is_scalar($meta['schema_version'])) {
            $properties['bq-schema-version'] = (string) $meta['schema_version'];
        }

        if (isset($meta['lang']) && is_string($meta['lang']) && $meta['lang'] !== '') {
            $properties['bq-source-lang'] = $meta['lang'];
        }

        $attempts = $envelope['attempts'] ?? 0;
        $properties['bq-attempts'] = (string) (is_scalar($attempts) ? $attempts : 0);

        return $properties;
    }
}
