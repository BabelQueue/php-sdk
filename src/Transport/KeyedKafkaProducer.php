<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * An optional {@see KafkaProducer} capability: produce a record **with a record key**.
 *
 * The Kafka record key is native broker metadata with no BabelQueue contract meaning (it is not part
 * of the envelope; `schema_version` stays 1). It matters only because it drives partition affinity:
 * the retry / retry→work re-inject / dead-letter writes ({@see KafkaRetryRouter},
 * {@see KafkaRetryConsumer}, {@see \BabelQueue\Consume\DeadLetterPublisher}) carry the **original
 * record's key** through unchanged when the producer implements this interface. A producer that does
 * not keeps today's behaviour (the key is dropped, with no error).
 *
 * An `ext-rdkafka` adapter implements it with `RdKafka\ProducerTopic::producev($partition, $msgflags,
 * $payload, $key, $headers, $timestampMs)`.
 *
 * Contract for implementations: `$key === null` means a **null** key (no key — do not synthesise
 * one); an empty string is a real, empty key and is passed as such. Partition equality with the
 * original record is not guaranteed.
 */
interface KeyedKafkaProducer extends KafkaProducer
{
    /**
     * Produce one record with a record key.
     *
     * @param  string  $topic  The Kafka topic.
     * @param  string  $payload  The record value — the canonical envelope JSON.
     * @param  array<string, string>  $headers  Record headers (the §6 `bq-` projection; UTF-8 strings).
     * @param  int|null  $timestampMs  CreateTime in Unix ms, or null for broker time.
     * @param  string|null  $key  The record key bytes (byte string), or null for a null key.
     */
    public function produceKeyed(string $topic, string $payload, array $headers, ?int $timestampMs, ?string $key): void;
}
