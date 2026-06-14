<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * The single Kafka operation the framework-less {@see KafkaTransport} needs: produce a record to
 * a topic with headers and a timestamp. The `ext-rdkafka` extension exposes a matching
 * {@code RdKafka\ProducerTopic::producev()} — wrap a `RdKafka\Producer` in a one-line adapter
 * (see the {@see KafkaTransport} class docs). Keeping the transport behind this tiny seam leaves
 * the core dependency-free and unit-testable with a fake (no `ext-rdkafka`, no broker).
 */
interface KafkaProducer
{
    /**
     * Produce one record.
     *
     * @param  string  $topic  The Kafka topic (the §6 work topic).
     * @param  string  $payload  The record value — the canonical envelope JSON.
     * @param  array<string, string>  $headers  Record headers (the §6 `bq-` projection; UTF-8 strings).
     * @param  int|null  $timestampMs  CreateTime in Unix ms (`meta.created_at`), or null for broker time.
     */
    public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void;
}
