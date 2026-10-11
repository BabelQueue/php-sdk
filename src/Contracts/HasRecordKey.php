<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * A consumed message that also carries the broker record key it arrived with (Kafka). The key is
 * native broker metadata — not part of the envelope — and is surfaced only so the dead-letter path
 * can carry it onto the `<queue>.dlq` copy ({@see \BabelQueue\Consume\DeadLetterPublisher}).
 * Implemented by {@see \BabelQueue\Transport\KafkaMessage}.
 */
interface HasRecordKey
{
    /**
     * The record key bytes, or null when the record had a null key. An empty string is a real,
     * empty key.
     */
    public function recordKey(): ?string;
}
