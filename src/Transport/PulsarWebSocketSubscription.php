<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Transport\Internal\WebSocketConnection;

/**
 * The SDK's own **Pulsar WebSocket consumer client**: it reads one subscription over
 * `<service>/ws/v2/consumer/persistent/<tenant>/<namespace>/<topic>/<subscription>` and implements the
 * {@see PulsarWebSocketConsumerClient} seam {@see PulsarConsumer} consumes through. It also implements
 * {@see PulsarConsumerDescriptor} (full topic, subscription type) and {@see PulsarServiceEndpoint} (service URL and
 * token), so `new PulsarConsumer(new PulsarWebSocketSubscription(...))` needs nothing more: the queue, the
 * re-publish topic and the subscription type are known, and the retry producer is derived on the same service, so
 * the `maxAttempts` cap holds on an `Exclusive` / `Failover` subscription too (ADR-0036 decision 1).
 *
 * Pure userland (RFC 6455 over `stream_socket_client`, `ssl://` for `wss` / `https`), so GR-7 holds. The connection
 * is opened lazily by the first {@see self::receive()}; when it breaks, `receive()` throws
 * {@see BabelQueueException} and the next call opens a new one (messages that were delivered but not acknowledged
 * on the old connection are redelivered by the broker). `receive()` returns null when nothing arrived within the
 * read window, so a loop can poll and stop.
 */
final class PulsarWebSocketSubscription implements PulsarWebSocketConsumerClient, PulsarConsumerDescriptor, PulsarServiceEndpoint
{
    private const SUBSCRIPTION_TYPES = ['Exclusive', 'Failover', 'Shared', 'Key_Shared'];

    private ?WebSocketConnection $connection = null;

    private readonly string $subscriptionType;

    /**
     * @param  string  $serviceUrl  `ws://host:8080`, `wss://...`, `http://...` or `https://...` (optional path prefix)
     * @param  string  $topic  the FULL topic name, `persistent://<tenant>/<namespace>/<topic>`
     * @param  string  $subscription  the subscription name
     * @param  string  $subscriptionType  `Exclusive` (Pulsar's default), `Failover`, `Shared` or `Key_Shared`: what the
     *         subscription is (it is sent to the broker, which creates it so)
     * @param  string|null  $authToken  sent as `Authorization: Bearer <token>` (consumer and derived producer)
     * @param  float  $readWindowSeconds  how long `receive()` waits for a message before returning null
     * @param  float  $timeoutSeconds  connect and in-frame read timeout
     * @param  int|null  $receiverQueueSize  Pulsar's consumer prefetch (null = the broker default)
     *
     * @throws \InvalidArgumentException When the URL, the topic or the subscription type is not valid
     */
    public function __construct(
        private readonly string $serviceUrl,
        private readonly string $topic,
        private readonly string $subscription,
        string $subscriptionType = 'Exclusive',
        private readonly ?string $authToken = null,
        private readonly float $readWindowSeconds = 1.0,
        private readonly float $timeoutSeconds = 10.0,
        private readonly ?int $receiverQueueSize = null,
    ) {
        if (WebSocketConnection::parseService($serviceUrl) === null) {
            throw new \InvalidArgumentException(sprintf('Not a Pulsar WebSocket service URL: [%s] (expected ws://host:port, wss://, http:// or https://).', $serviceUrl));
        }
        if (self::splitTopic($topic) === null) {
            throw new \InvalidArgumentException(sprintf('[%s] is not a full topic name (persistent://<tenant>/<namespace>/<topic>).', $topic));
        }
        if ($subscription === '') {
            throw new \InvalidArgumentException('A Pulsar subscription needs a name.');
        }

        $normalised = null;
        foreach (self::SUBSCRIPTION_TYPES as $known) {
            if (strcasecmp($subscriptionType, $known) === 0) {
                $normalised = $known;
            }
        }
        if ($normalised === null) {
            throw new \InvalidArgumentException(sprintf('Unknown Pulsar subscription type [%s]; expected Exclusive, Failover, Shared or Key_Shared.', $subscriptionType));
        }
        $this->subscriptionType = $normalised;
    }

    /**
     * @return array{domain: string, tenant: string, namespace: string, topic: string}|null null when it is not a full name
     */
    public static function splitTopic(string $topic): ?array
    {
        if (preg_match('#^(persistent|non-persistent)://([^/]+)/([^/]+)/(.+)$#', $topic, $m) !== 1) {
            return null;
        }

        return ['domain' => $m[1], 'tenant' => $m[2], 'namespace' => $m[3], 'topic' => $m[4]];
    }

    public function receive(): ?array
    {
        $connection = $this->connection();

        try {
            $text = $connection->readMessage($this->readWindowSeconds);
        } catch (BabelQueueException $e) {
            $this->close();

            throw $e;
        }

        if ($text === null) {
            return null;
        }

        $frame = json_decode($text, true);
        if (! is_array($frame) || ! is_string($frame['messageId'] ?? null) || ! is_string($frame['payload'] ?? null)) {
            return null; // a control / error frame, not a delivery
        }

        $payload = base64_decode($frame['payload'], true);
        $properties = [];
        if (is_array($frame['properties'] ?? null)) {
            foreach ($frame['properties'] as $key => $value) {
                if (is_string($value)) {
                    $properties[(string) $key] = $value;
                }
            }
        }
        $redelivery = $frame['redeliveryCount'] ?? 0;

        return [
            'messageId' => $frame['messageId'],
            'payload' => $payload === false ? '' : $payload,
            'properties' => $properties,
            'redeliveryCount' => is_int($redelivery) ? $redelivery : 0,
            'topic' => $this->topic,
        ];
    }

    public function acknowledge(string $messageId): void
    {
        $this->sendControl(['messageId' => $messageId]);
    }

    public function negativeAcknowledge(string $messageId): void
    {
        $this->sendControl(['type' => 'negativeAcknowledge', 'messageId' => $messageId]);
    }

    public function topic(): string
    {
        return $this->topic;
    }

    public function subscriptionType(): string
    {
        return $this->subscriptionType;
    }

    public function serviceUrl(): string
    {
        return $this->serviceUrl;
    }

    public function authToken(): ?string
    {
        return $this->authToken;
    }

    /** Close the connection (a later `receive()` opens a new one). */
    public function close(): void
    {
        if ($this->connection !== null) {
            $this->connection->sendClose();
            $this->connection->close();
            $this->connection = null;
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    /** The consumer endpoint path with its query string. */
    public function consumerPath(): string
    {
        $parts = self::splitTopic($this->topic);
        $service = WebSocketConnection::parseService($this->serviceUrl);
        assert($parts !== null && $service !== null);

        $query = ['subscriptionType' => $this->subscriptionType];
        if ($this->receiverQueueSize !== null) {
            $query['receiverQueueSize'] = (string) $this->receiverQueueSize;
        }

        return sprintf(
            '%s/ws/v2/consumer/%s/%s?%s',
            $service['path'],
            PulsarWebSocketProducer::topicPath($parts),
            rawurlencode($this->subscription),
            http_build_query($query),
        );
    }

    private function connection(): WebSocketConnection
    {
        return $this->connection ??= WebSocketConnection::open($this->serviceUrl, $this->consumerPath(), $this->authToken, $this->timeoutSeconds);
    }

    /** @param array<string, string> $frame */
    private function sendControl(array $frame): void
    {
        if ($this->connection === null) {
            throw new BabelQueueException('The Pulsar consumer connection is closed; the message will be redelivered by the broker.');
        }

        try {
            $this->connection->sendText(json_encode($frame, JSON_THROW_ON_ERROR));
        } catch (BabelQueueException $e) {
            $this->close();

            throw $e;
        }
    }
}
