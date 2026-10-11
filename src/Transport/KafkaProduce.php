<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * Internal helper: produce through a {@see KafkaProducer}, carrying a record key when there is one
 * and the producer can take it ({@see KeyedKafkaProducer}). A null key — or a producer without key
 * support — falls back to the plain {@see KafkaProducer::produce()}, so a keyless record stays
 * keyless and an existing producer keeps working unchanged. A key is never synthesised.
 *
 * @internal
 */
final class KafkaProduce
{
    /**
     * @param  array<string, string>  $headers
     */
    public static function send(
        KafkaProducer $producer,
        string $topic,
        string $payload,
        array $headers,
        ?int $timestampMs,
        ?string $key,
    ): void {
        if ($key !== null && $producer instanceof KeyedKafkaProducer) {
            $producer->produceKeyed($topic, $payload, $headers, $timestampMs, $key);

            return;
        }

        $producer->produce($topic, $payload, $headers, $timestampMs);
    }
}
