<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * An optional hook told about every dead-letter record the SDK itself wrote (ADR-0039 section 5.1).
 *
 * It is the seam behind the `babelqueue.dlq.messages` metric ({@see \BabelQueue\Otel\Metrics} implements
 * it), but it is generic: a logging or alerting hook can use it too. The core never imports
 * OpenTelemetry for it (GR-7).
 *
 * **When it is called.** Once per dead-letter record the SDK wrote and the broker confirmed, by one of the
 * three PHP write primitives (ADR-0039 section 5.2):
 *
 *  - {@see \BabelQueue\Consume\DeadLetterPublisher::publish()} and `::publishRaw()` (so also the
 *    {@see \BabelQueue\Consume\Dispatcher} and `PulsarConsumer`);
 *  - {@see \BabelQueue\Transport\KafkaRetryRouter} when attempts are exhausted;
 *  - {@see \BabelQueue\Transport\KafkaConsumer} when it quarantines a poison / identity-less record.
 *
 * **When it is not called:** when the write fails (the message stays unsettled), when the broker
 * dead-letters on its own, or when DLQ writing is off and the message is dropped instead.
 *
 * **Isolation.** The caller catches anything the observer throws and logs it (`error_log`): an observer
 * MUST NOT change the settle outcome, and it cannot, because no ack, nack or retry depends on it.
 */
interface DeadLetterObserver
{
    /**
     * A dead-letter record was written.
     *
     * @param  string  $queue  the source queue/topic the message was consumed from; empty when unknown
     * @param  string  $deadLetterQueue  the DLQ the record went to (e.g. `orders.dlq`)
     * @param  string  $reason  the `dead_letter.reason` written (`failed`, `unknown_urn`, `poison`); empty for a raw body
     */
    public function deadLettered(string $queue, string $deadLetterQueue, string $reason): void;
}
