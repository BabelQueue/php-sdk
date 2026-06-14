<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\Transport;

/**
 * A framework-less Apache ActiveMQ **Artemis** producer over **STOMP** — PHP's path to the
 * §7 binding, since Artemis speaks AMQP 1.0 (not RabbitMQ's 0-9-1) and has no modern native
 * PHP AMQP-1.0 client, but ships a STOMP acceptor and `stomp-php` is a mature pure-PHP client.
 *
 * Artemis bridges **STOMP ↔ core ↔ AMQP 1.0 ↔ JMS** on the same address, so a message this
 * transport produces is consumed natively by the Java (JMS) and .NET / Node / Python / Go
 * (AMQP 1.0) Artemis SDKs. The envelope JSON is the frame **body** (authoritative), and the
 * §7 fields map onto STOMP headers that survive the bridge:
 *
 *  - `correlation-id`  ← `trace_id`   (→ AMQP `correlation-id`, native)
 *  - `content-type`    = `application/json`
 *  - `bq_schema_version` / `bq_source_lang` / `bq_attempts` / `bq_app_id`  (→ AMQP
 *    application-properties; underscores, per the §7 JMS-legal naming, ADR-0017)
 *
 * **Routing is body-authoritative for STOMP-produced messages:** a STOMP header cannot set the
 * `x-opt-jms-type` *message annotation* the §7 consumers prefer (STOMP custom headers become
 * AMQP application-properties, not annotations), so every Artemis consumer falls back to the
 * body's `job` URN — which is always present. The body stays the single source of truth.
 *
 * The destination MUST be the **anycast** address (JMS/AMQP queues are anycast), so the
 * transport sends to the bare queue name; configure the Artemis address as anycast (the default
 * for an auto-created JMS queue) for a shared PHP+Java/Python fleet.
 *
 * Optional dependency: `stomp-php/stomp-php`. Wrap its client in a one-line {@see StompClient}
 * adapter; the transport itself stays dependency-free and unit-tests against a fake.
 *
 * This is a **producer** (the core {@see Transport} is publish-only, like the Redis/AMQP/SQS
 * transports); PHP consumes Artemis via a framework worker (a follow-up Laravel STOMP driver).
 */
final class StompTransport implements Transport
{
    public function __construct(
        private readonly StompClient $client,
        private readonly string $defaultQueue = 'default',
        private readonly string $destinationPrefix = '',
    ) {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $target = $queue ?? $this->defaultQueue;
        $envelope = EnvelopeCodec::decode($payload);

        $this->client->send($this->destinationPrefix.$target, $payload, $this->headers($envelope));

        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];
        $id = $meta['id'] ?? null;

        return is_scalar($id) ? (string) $id : null;
    }

    /**
     * The §7 STOMP header projection: `correlation-id`, `content-type`, and the string-valued
     * `bq_` application properties. The URN rides the body's `job` (see the class docs).
     *
     * @param  array<string, mixed>  $envelope
     * @return array<string, string>
     */
    private function headers(array $envelope): array
    {
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $headers = [
            'content-type' => 'application/json',
            'bq_app_id' => 'babelqueue',
        ];

        $traceId = $envelope['trace_id'] ?? null;
        if (is_string($traceId) && $traceId !== '') {
            $headers['correlation-id'] = $traceId;
        }

        if (isset($meta['schema_version']) && is_scalar($meta['schema_version'])) {
            $headers['bq_schema_version'] = (string) $meta['schema_version'];
        }

        if (isset($meta['lang']) && is_string($meta['lang']) && $meta['lang'] !== '') {
            $headers['bq_source_lang'] = $meta['lang'];
        }

        $attempts = $envelope['attempts'] ?? 0;
        $headers['bq_attempts'] = (string) (is_scalar($attempts) ? $attempts : 0);

        return $headers;
    }
}
