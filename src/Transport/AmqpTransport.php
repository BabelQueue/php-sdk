<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\Transport;
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
 * Optional dependency: `php-amqplib/php-amqplib`.
 */
final class AmqpTransport implements Transport
{
    public function __construct(
        private readonly AMQPChannel $channel,
        private readonly string $defaultQueue = 'default',
    ) {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $target = $queue ?? $this->defaultQueue;
        $envelope = EnvelopeCodec::decode($payload);

        // passive=false, durable=true, exclusive=false, auto_delete=false.
        $this->channel->queue_declare($target, false, true, false, false);
        $this->channel->basic_publish($this->toMessage($payload, $envelope), '', $target);

        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];
        $id = $meta['id'] ?? null;

        return is_scalar($id) ? (string) $id : null;
    }

    /**
     * @param  array<string, mixed>  $envelope
     */
    private function toMessage(string $payload, array $envelope): AMQPMessage
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

        $headers = array_filter([
            'x-schema-version' => $meta['schema_version'] ?? null,
            'x-source-lang' => $meta['lang'] ?? null,
            'x-attempts' => $envelope['attempts'] ?? null,
        ], static fn ($value): bool => $value !== null);

        if ($headers !== []) {
            $properties['application_headers'] = new AMQPTable($headers);
        }

        return new AMQPMessage($payload, $properties);
    }
}
