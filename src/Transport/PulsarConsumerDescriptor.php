<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * An optional capability of a {@see PulsarWebSocketConsumerClient} adapter: it tells the {@see PulsarConsumer}
 * which topic it reads and with which subscription type, so the consumer can derive its queue name (for
 * `<queue>.dlq`), the topic of the retry re-publish and whether a negative acknowledgement can bound the retry
 * at all (ADR-0036 decision 1) without extra configuration. An adapter that also implements
 * {@see PulsarWebSocketClient} (it owns the connection, so it can publish too) is used as the retry producer.
 *
 * Both values are known to the adapter: it derives the WebSocket endpoint
 * (`ws://<host>/ws/v2/consumer/persistent/<tenant>/<namespace>/<topic>/<subscription>`) from them.
 */
interface PulsarConsumerDescriptor
{
    /**
     * The topic this subscription reads: the full name (`persistent://public/default/orders`) or its local name
     * (`orders`; the consumer then builds the full name from its tenant and namespace). An empty string means
     * the adapter does not know it.
     */
    public function topic(): string;

    /**
     * The subscription type: `Exclusive`, `Failover`, `Shared` or `Key_Shared` (case-insensitive), or null when
     * the adapter does not know it.
     */
    public function subscriptionType(): ?string;
}
