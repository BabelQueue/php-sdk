<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use Throwable;

/**
 * A framework-less **Apache Kafka** consumer — PHP's path to the §6 binding's consume side over
 * `ext-rdkafka`, the counterpart to {@see KafkaTransport} (produce).
 *
 * **GR-7 note:** like the producer, the only viable PHP Kafka client is the **C extension**
 * `ext-rdkafka` (ADR-0019), so this is opt-in behind the one-method {@see KafkaConsumerClient}
 * seam — the consumer itself stays dependency-free and unit-tests against a fake.
 *
 * Consume is **process-then-commit** (at-least-once, §6): {@see self::consume()} polls a record,
 * runs the handler, and commits the offset only on success; on a thrown handler it leaves the
 * record **uncommitted**, so it is re-delivered on the next restart/rebalance. Per §6 the
 * **`attempts` counter is the `bq-attempts` header (authoritative — Kafka has no native delivery
 * count), falling back to the body's `attempts`** when the header is absent — note this is *not* a
 * `max` (the header overrides even when lower), unlike §5 Pulsar. The reconciled value is exposed
 * via {@see KafkaMessage::attempts()} so a handler can implement its own retry-topic / DLQ policy.
 *
 * URN dispatch, `on_unknown_urn` and the §6 retry-topic / DLQ machinery stay the caller's concern
 * (a framework adapter or a runner), keeping the core minimal — it routes on `bq-job` via the
 * body's `job` URN ({@see EnvelopeCodec::urn()}).
 */
final class KafkaConsumer
{
    public function __construct(
        private readonly KafkaConsumerClient $client,
    ) {
    }

    /**
     * Poll and decode the next record, with §6 `attempts` reconciled onto the envelope (the
     * `bq-attempts` header wins, else the body's own), or null if none arrived within the poll
     * timeout.
     */
    public function receive(): ?KafkaMessage
    {
        $raw = $this->client->receive();

        if ($raw === null) {
            return null;
        }

        $envelope = EnvelopeCodec::decode($raw['payload']);

        // §6: the bq-attempts header is authoritative (Kafka has no native delivery count); fall
        // back to the body only when the header is absent. NOT a max — the header overrides.
        $header = $raw['headers']['bq-attempts'] ?? null;
        if ($header !== null && $header !== '') {
            $envelope['attempts'] = (int) $header;
        }

        return new KafkaMessage($envelope);
    }

    /**
     * Commit the offset of the last received record (process-then-commit).
     */
    public function commit(): void
    {
        $this->client->commit();
    }

    /**
     * Run the consume loop: poll each record and pass it to $handler; commit the offset on success
     * (process-then-commit), or leave it uncommitted if the handler throws (re-delivered on the
     * next restart/rebalance — at-least-once). Idle (null) polls are skipped. The loop runs until
     * $shouldStop() returns true (omit it to run forever, the standard worker model).
     *
     * @param  callable(KafkaMessage): void  $handler
     * @param  (callable(): bool)|null  $shouldStop
     */
    public function consume(callable $handler, ?callable $shouldStop = null): void
    {
        while ($shouldStop === null || ! $shouldStop()) {
            $message = $this->receive();

            if ($message === null) {
                continue;
            }

            try {
                $handler($message);
                $this->commit();
            } catch (Throwable) {
                // At-least-once: leave the record uncommitted so it is re-delivered later. A handler
                // owns poison handling via $message->attempts() (e.g. republish to a retry topic or
                // <queue>.dlq past a cap), which the §6 retry-topic machinery formalises.
            }
        }
    }
}
