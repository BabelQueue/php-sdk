<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * Optional extension of {@see KafkaRetryConsumerClient} for clients that can pause fetching.
 * {@see KafkaRetryConsumer} calls {@see self::pause()} before a cooperative delay wait and
 * {@see self::resume()} after it, so the keep-alive {@see KafkaRetryConsumerClient::poll()}
 * heartbeats during the wait keep the consumer in its group without fetching further records
 * (ADR-0033 A6: wait via partition pause/resume, never by blocking the poll loop).
 *
 * Back it with `RdKafka\KafkaConsumer::pause()` / `::resume()` over the current assignment.
 * A client that does not implement it still works: the wait then relies on the heartbeat alone.
 */
interface PausableKafkaRetryConsumerClient extends KafkaRetryConsumerClient
{
    /** Pause fetching on the current assignment (the group membership stays alive via {@see poll()}). */
    public function pause(): void;

    /** Resume fetching on the current assignment. */
    public function resume(): void;
}
