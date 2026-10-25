<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Transport\Internal\WebSocketConnection;

/**
 * A minimal, dependency-free **Pulsar WebSocket producer**: one {@see PulsarWebSocketClient::publish()} opens
 * `<service>/ws/v2/producer/persistent/<tenant>/<namespace>/<topic>`, sends one JSON frame (`payload` base64,
 * `properties`), waits for the broker's `{"result":"ok"}` and closes. A non-`ok` result, a refused connection, a
 * failed handshake or a timeout throws {@see BabelQueueException}, so a failed publish is never silently lost (GR-1).
 *
 * It exists so {@see PulsarConsumer} can re-publish a retry on its own (derived from a client's
 * {@see PulsarServiceEndpoint}). It speaks only what the producer endpoint needs (RFC 6455 over
 * `stream_socket_client`, `ssl://` for `wss`/`https` with the peer verified), so GR-7 holds: no extension and no
 * library. The tenant, namespace and topic are parsed from the FULL topic name passed to `publish()`; a local name
 * has no tenant and is refused. It never sets `eventTime` (the body's `meta.created_at` is authoritative).
 */
final class PulsarWebSocketProducer implements PulsarWebSocketClient
{
    /**
     * @param  string  $serviceUrl  `ws://host:8080`, `wss://...`, `http://...` or `https://...` (optional path prefix)
     * @param  string|null  $authToken  sent as `Authorization: Bearer <token>` when set
     * @param  float  $timeoutSeconds  connect and read timeout
     */
    public function __construct(
        private readonly string $serviceUrl,
        private readonly ?string $authToken = null,
        private readonly float $timeoutSeconds = 10.0,
    ) {
        if (WebSocketConnection::parseService($serviceUrl) === null) {
            throw new \InvalidArgumentException(sprintf('Not a Pulsar WebSocket service URL: [%s] (expected ws://host:port, wss://, http:// or https://).', $serviceUrl));
        }
    }

    public function publish(string $topic, string $payload, array $properties): void
    {
        $path = $this->producerPath($topic);
        $frame = json_encode([
            'payload' => base64_encode($payload),
            'properties' => $properties === [] ? new \stdClass() : $properties,
            'context' => bin2hex(random_bytes(8)),
        ], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);

        $connection = WebSocketConnection::open($this->serviceUrl, $path, $this->authToken, $this->timeoutSeconds);

        try {
            $connection->sendText($frame);
            $reply = json_decode((string) $connection->readMessage(), true);

            if (! is_array($reply) || ($reply['result'] ?? null) !== 'ok') {
                $detail = 'unreadable reply';
                if (is_array($reply)) {
                    $reason = $reply['errorMsg'] ?? $reply['result'] ?? null;
                    $detail = is_string($reason) ? $reason : 'unknown';
                }
                throw new BabelQueueException(sprintf('Pulsar producer rejected the message for [%s]: %s', $topic, $detail));
            }

            $connection->sendClose(); // the broker already acknowledged the message
        } finally {
            $connection->close();
        }
    }

    /** The producer endpoint path for a full topic: `/ws/v2/producer/persistent/<tenant>/<namespace>/<topic>`. */
    public function producerPath(string $topic): string
    {
        $parts = PulsarWebSocketSubscription::splitTopic($topic);
        if ($parts === null) {
            throw new BabelQueueException(sprintf('Cannot derive the Pulsar producer endpoint: [%s] is not a full topic name (persistent://<tenant>/<namespace>/<topic>).', $topic));
        }

        $service = WebSocketConnection::parseService($this->serviceUrl);

        return sprintf('%s/ws/v2/producer/%s', $service['path'] ?? '', self::topicPath($parts));
    }

    /** @param array{domain: string, tenant: string, namespace: string, topic: string} $parts */
    public static function topicPath(array $parts): string
    {
        return sprintf('%s/%s/%s/%s', $parts['domain'], rawurlencode($parts['tenant']), rawurlencode($parts['namespace']), rawurlencode($parts['topic']));
    }
}
