<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\PoisonBody;
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
 *
 * **Poison and identity-less bodies (ADR-0034 §10, §6.5).** After the version gate and before the
 * handler sees anything, the raw value is classified with {@see KafkaBodyClassifier}. A body that is
 * not {@see KafkaBodyClassifier::OK} (undecodable, wrong-typed member, or no `job`/`urn` identity)
 * never reaches the handler: it is written to `<topic>.dlq` with `dead_letter.reason = poison` (the
 * raw JSON object verbatim, else `{"raw": "<body>"}`; original headers and record key carried
 * byte-for-byte) and only then is its offset committed. {@see self::receive()} returns null for such
 * a record, exactly as for an idle poll, so {@see self::consume()} simply moves on. DLQ routing is
 * opt-in: with no `$deadLetterProducer` the record degrades to commit-and-drop (§6.5), so it is never
 * redelivered in a loop. If the DLQ write itself throws, the offset is **not** committed and the
 * exception propagates (a later commit would otherwise silently skip the record).
 */
final class KafkaConsumer
{
    /**
     * Reserved routing URN prefix for a record rejected by the §6.7 version gate; the rejected
     * `bq-schema-version` value is appended. No handler is ever registered for it.
     */
    public const UNSUPPORTED_SCHEMA_URN_PREFIX = 'babelqueue:unsupported-schema-version:';

    /**
     * @param  KafkaProducer|null  $deadLetterProducer  where poison / identity-less records are dead-lettered
     *                                                  (a {@see KeyedKafkaProducer} also carries the record key);
     *                                                  null = commit-and-drop (§6.5, DLQ is opt-in)
     * @param  string  $dlqSuffix  the dead-letter-topic suffix (`<topic>.dlq`)
     */
    public function __construct(
        private readonly KafkaConsumerClient $client,
        private readonly ?KafkaProducer $deadLetterProducer = null,
        private readonly string $dlqSuffix = '.dlq',
    ) {
    }

    /**
     * Poll and decode the next record, with §6 `attempts` reconciled onto the envelope (the
     * `bq-attempts` header wins, else the body's own), or null if none arrived within the poll
     * timeout. A poison / identity-less record is dead-lettered and committed here (see the class
     * docblock) and also yields null: it is never handed to the caller.
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

        // ADR-0034 §10: poison and no_identity share one fate - DLQ with reason `poison`, no handler.
        if (KafkaBodyClassifier::classify($raw['payload']) !== KafkaBodyClassifier::OK) {
            $this->quarantine($raw);

            return null;
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
     * Dead-letter a poison / identity-less record (when a producer is configured) and commit its
     * offset. The DLQ write comes first; if it throws, nothing is committed.
     *
     * @param  array{payload: string, headers: array<string, string>, key?: string|null, topic?: string|null}  $raw
     */
    private function quarantine(array $raw): void
    {
        if ($this->deadLetterProducer !== null) {
            $topic = $raw['topic'] ?? null;
            if (! is_string($topic) || $topic === '') {
                $topic = PoisonBody::queueHint($raw['payload']) ?? 'default';
            }

            $headerAttempts = $raw['headers']['bq-attempts'] ?? null;
            $attempts = is_string($headerAttempts) && $headerAttempts !== '' ? (int) $headerAttempts : 0;

            // The copy is built by PoisonBody, which cannot fail on the body (invalid UTF-8, 1e400, ...):
            // a deterministic encode error must not stop the commit below, or the partition would wedge.
            KafkaProduce::send(
                $this->deadLetterProducer,
                $topic . $this->dlqSuffix,
                PoisonBody::annotate($raw['payload'], 'poison', null, $topic, $attempts),
                $raw['headers'],
                (int) round(microtime(true) * 1000),
                $raw['key'] ?? null,
            );
        }

        $this->commit();
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
