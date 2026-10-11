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
 * URN dispatch, `on_unknown_urn` and DLQ routing can be handled by the framework-less
 * {@see \BabelQueue\Consume\Dispatcher} runtime (or a framework adapter): it is callable, so
 * `consume($dispatcher, $shouldStop)` routes on `bq-job` via the body's `job` URN
 * ({@see EnvelopeCodec::urn()}). The §6.4/§6.5 retry-topic delay/backoff + DLQ tier is the
 * {@see KafkaRetryRouter} (route a failure to `<topic>.retry.<n>` / `<topic>.dlq`) plus the
 * {@see KafkaRetryConsumer} (wait the tier delay, re-inject into the work topic).
 *
 * **Version gate (§6.7).** Before the body is decoded the `bq-schema-version` record header is read.
 * A missing or blank value (empty or only ASCII whitespace) changes nothing; any value other than
 * {@see EnvelopeCodec::SCHEMA_VERSION} (`"2"`, `"x"`, `"01"`, ...; exact string match) leaves the body
 * undecoded and yields a message whose routing URN is the reserved
 * {@see self::UNSUPPORTED_SCHEMA_URN_PREFIX}`<value>`. No handler owns it, so the Dispatcher takes its
 * existing unknown-URN path (`on_unknown_urn`: dead-letter / ...), and the error names the rejected
 * version. The envelope of such a message is only `['raw' => <body>, 'attempts' => n]`, so a
 * dead-letter copy keeps the producer's bytes in the same `{"raw": ...}` shape as a poison body.
 */
final class KafkaConsumer
{
    /**
     * Reserved routing URN prefix for a record rejected by the §6.7 version gate; the rejected
     * `bq-schema-version` value is appended. No handler is ever registered for it.
     */
    public const UNSUPPORTED_SCHEMA_URN_PREFIX = 'babelqueue:unsupported-schema-version:';

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

        // §6.7: gate on bq-schema-version before the body is decoded.
        $rejectedVersion = self::unsupportedSchemaVersion($raw['headers']['bq-schema-version'] ?? null);
        if ($rejectedVersion !== null) {
            $rejectedAttempts = $raw['headers']['bq-attempts'] ?? null;

            return new KafkaMessage(
                [
                    'raw' => $raw['payload'],
                    'attempts' => is_string($rejectedAttempts) && $rejectedAttempts !== '' ? (int) $rejectedAttempts : 0,
                ],
                $raw['headers'],
                $raw['payload'],
                self::UNSUPPORTED_SCHEMA_URN_PREFIX . $rejectedVersion,
                $raw['key'] ?? null,
            );
        }

        $envelope = EnvelopeCodec::decode($raw['payload']);

        // §6: the bq-attempts header is authoritative (Kafka has no native delivery count); fall
        // back to the body only when the header is absent. NOT a max — the header overrides.
        $header = $raw['headers']['bq-attempts'] ?? null;
        if ($header !== null && $header !== '') {
            $envelope['attempts'] = (int) $header;
        }

        // Carry the raw bq- headers so the §6.4/§6.5 retry-topic machinery can recover a retry
        // record's original work topic (bq-original-topic) across hops.
        return new KafkaMessage($envelope, $raw['headers'], $raw['payload'], null, $raw['key'] ?? null);
    }

    /**
     * The `bq-schema-version` value when it is present, non-blank and not the supported version
     * (exact string match against {@see EnvelopeCodec::SCHEMA_VERSION}); null when the body may be decoded.
     */
    private static function unsupportedSchemaVersion(mixed $value): ?string
    {
        if (! is_string($value) || self::isBlankASCII($value)) {
            return null;
        }

        return $value === (string) EnvelopeCodec::SCHEMA_VERSION ? null : $value;
    }

    /**
     * Blank means the empty string or only ASCII whitespace (space, `\t`, `\n`, `\v`, `\f`, `\r`) — the
     * same set in every SDK (GR-5); not `trim()`, whose default set contains `\0` and lacks `\f`.
     */
    private static function isBlankASCII(string $value): bool
    {
        return preg_match('/\A[ \t\n\x0B\f\r]*\z/', $value) === 1;
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
