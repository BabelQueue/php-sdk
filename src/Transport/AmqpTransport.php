<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Support\Headers;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * A framework-less RabbitMQ transport: declares a durable queue and publishes the
 * canonical envelope as a persistent message, mapping the envelope onto the AMQP
 * properties every BabelQueue SDK agrees on so a non-PHP consumer can route and
 * trace *without decoding the body first*:
 *
 *  - `type`            ← the job URN
 *  - `correlation_id`  ← trace_id
 *  - `message_id`      ← meta.id
 *  - headers `x-schema-version` / `x-source-lang` / `x-attempts`
 *
 * Publishing goes to the default exchange with the queue name as the routing key
 * (the AMQP equivalent of "push onto this queue").
 *
 * **Out-of-band headers (ADR-0028).** It also implements the optional {@see HeaderPublisher}
 * capability: {@see self::publishWithHeaders()} carries out-of-band transport headers (e.g. a W3C
 * `traceparent` for cross-hop span linkage) in the AMQP **message headers** (`application_headers`),
 * merged **beside** the contract `x-*` headers — the contract wins a key collision (GR-1: the wire
 * envelope itself is untouched). A plain {@see self::publish()} stays byte-identical to before.
 *
 * Optional dependency: `php-amqplib/php-amqplib`.
 */
final class AmqpTransport implements HeaderPublisher
{
    public function __construct(
        private readonly AMQPChannel $channel,
        private readonly string $defaultQueue = 'default',
    ) {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        return $this->send($payload, $queue, []);
    }

    /**
     * Publish the envelope together with out-of-band `$headers` ({@see HeaderPublisher}, ADR-0028).
     * The headers ride in the AMQP message headers beside the contract `x-*` headers, which win a
     * key collision. An empty/blank map degrades to a plain {@see self::publish()}.
     *
     * @param  array<string, string>  $headers
     */
    public function publishWithHeaders(string $payload, array $headers, ?string $queue = null): ?string
    {
        return $this->send($payload, $queue, $headers);
    }

    /**
     * @param  array<string, string>  $extraHeaders  out-of-band headers to carry beside the contract `x-*`
     */
    private function send(string $payload, ?string $queue, array $extraHeaders): ?string
    {
        $target = $queue ?? $this->defaultQueue;
        $envelope = EnvelopeCodec::decode($payload);

        // passive=false, durable=true, exclusive=false, auto_delete=false.
        $this->channel->queue_declare($target, false, true, false, false);
        $this->channel->basic_publish($this->toMessage($payload, $envelope, $extraHeaders), '', $target);

        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];
        $id = $meta['id'] ?? null;

        return is_scalar($id) ? (string) $id : null;
    }

    /**
     * @param  array<string, mixed>  $envelope
     * @param  array<string, string>  $extraHeaders
     */
    private function toMessage(string $payload, array $envelope, array $extraHeaders): AMQPMessage
    {
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $properties = [
            'content_type' => 'application/json',
            'content_encoding' => 'utf-8',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'app_id' => 'babelqueue',
        ];

        $urn = EnvelopeCodec::urn($envelope);
        if ($urn !== '') {
            $properties['type'] = $urn;
        }

        $traceId = $envelope['trace_id'] ?? null;
        if (is_string($traceId) && $traceId !== '') {
            $properties['correlation_id'] = $traceId;
        }

        if (isset($meta['id']) && is_scalar($meta['id'])) {
            $properties['message_id'] = (string) $meta['id'];
        }

        $contract = array_filter([
            'x-schema-version' => $meta['schema_version'] ?? null,
            'x-source-lang' => $meta['lang'] ?? null,
            'x-attempts' => $envelope['attempts'] ?? null,
        ], static fn ($value): bool => $value !== null);

        // Out-of-band riders go in first; the contract x-* headers overwrite them last so they win
        // a key collision (merge-not-clobber, shared across every SDK) — and keep their native
        // value types, so a plain publish (no extra headers) is byte-identical to before. GR-1: the
        // wire envelope body is never touched.
        $headers = Headers::sanitize($extraHeaders);
        foreach ($contract as $key => $value) {
            $headers[$key] = $value;
        }

        if ($headers !== []) {
            $properties['application_headers'] = new AMQPTable($headers);
        }

        return new AMQPMessage($payload, $properties);
    }
}
