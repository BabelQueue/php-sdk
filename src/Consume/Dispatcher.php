<?php

declare(strict_types=1);

namespace BabelQueue\Consume;

use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasQueue;
use BabelQueue\Contracts\HasRawBody;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Routing\UnknownUrnStrategy;
use BabelQueue\Codec\StrictBodyClassifier;
use BabelQueue\Transport\KafkaMessage;
use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarMessage;
use Throwable;

/**
 * The framework-less **consume runtime** for the core consumers: a URN → handler registry that
 * routes each decoded message to its handler, applies the four `on_unknown_urn` strategies, and
 * (optionally) dead-letters poison messages — composing the pieces the core already ships
 * ({@see UnknownUrnStrategy}, {@see DeadLetterPublisher}, {@see \BabelQueue\DeadLetter\DeadLetter}).
 *
 * It is **callable**, so it plugs straight into a consumer's `consume()` loop:
 *
 *     $dispatch = (new Dispatcher(onUnknownUrn: UnknownUrnStrategy::DEAD_LETTER, maxAttempts: 5,
 *                                 deadLetters: new DeadLetterPublisher($pulsarProducer)))
 *         ->on('urn:babel:orders:created', fn (ConsumedMessage $m) => handle($m));
 *     $consumer->consume($dispatch, $shouldStop);
 *
 * It uses the consumer loop's **ack-on-return / redeliver-on-throw** contract: a handler that
 * returns normally acks the message; a thrown handler redelivers it (at-least-once).
 *
 * **Every message terminates (ADR-0036 decision 3).** The delivery is *counted* as
 * `attempts() + 1` (the reconciled attempt count plus the current delivery) and compared with
 * `$maxAttempts` (default **3**; `0` = no cap). A retryable outcome — a handler that throws, an unknown
 * URN under `fail`/`release`, a poison body with no publisher — is redelivered (the exception is
 * rethrown) while `counted < maxAttempts`. At the cap the message is **exhausted**:
 *
 *  - with a {@see DeadLetterPublisher}: written to `<queue>.dlq` (`reason` `failed` / `unknown_urn`,
 *    `dead_letter.attempts` = the counted value);
 *  - without one, on **Pulsar** ({@see PulsarMessage}): acknowledged, dropped and logged;
 *  - without one, on **Kafka** ({@see KafkaMessage}) or any other message type: the exception is
 *    rethrown. The `Dispatcher` drops only a {@see PulsarMessage}; over Kafka the
 *    {@see \BabelQueue\Transport\KafkaRetryRouter} or the consumer decides (a drop there would commit
 *    the offset and lose the record), and for an unknown message type the previous behaviour is kept. **Note for Kafka users:** the default cap
 *    (3) dead-letters a record with a publisher at its third delivery, before a `KafkaRetryRouter` with
 *    its default `maxAttempts` of 5; pass `maxAttempts: 0` (or no publisher) to leave the cap to the router.
 *
 * **Retry is the consumer's re-publish.** The `Dispatcher` signals "retry" by rethrowing; on Pulsar the
 * {@see \BabelQueue\Transport\PulsarConsumer} (given a `$retryProducer`) turns that into a re-publish with
 * `bq-attempts` + 1 and an ack, so the cap is reached on every subscription type, `Exclusive` / `Failover`
 * included, where a negative acknowledgement does not advance Pulsar's redelivery count (ADR-0036 decision 1).
 *
 * **An unnameable queue.** Nothing is ever written to `default.dlq` (ADR-0036 decision 3): the DLQ is
 * `<queue>.dlq` with the queue taken from the consumer's topic ({@see HasQueue}: `PulsarConsumer`'s `queue:`
 * argument or the frame topic, `KafkaConsumer`'s frame / `$topic` / single subscribed topic), never from the
 * message's `meta.queue`. When a {@see DeadLetterPublisher} IS set but the consumer's queue is unknown (for example a
 * multi-topic Kafka subscription), the section 6.5 degrade applies on Pulsar AND Kafka: the message is committed or
 * acknowledged, dropped and logged, and the `Dispatcher` does not rethrow (a rethrow would redeliver a record that can
 * never be addressed and wedge the partition). With NO publisher the rule above is unchanged: dropped and logged on
 * Pulsar, rethrown on Kafka.
 *
 * **Unknown URN** (a valid envelope with no handler): `delete` acks and drops; `dead_letter` writes
 * `<queue>.dlq` (`reason: unknown_urn`) then acks — with no publisher it acks, drops and **logs** (rethrows
 * on Kafka, unless a publisher is set whose queue is unknown); `fail`/`release` are bounded as above.
 *
 * **Poison** (a body that does not decode into an envelope, or has no valid `job`/`urn` identity,
 * ADR-0036 decision 4) never reaches a handler and has its own route, not the unknown-URN one: with a
 * publisher (and any strategy but `delete`) the ADR-0035 record goes to `<queue>.dlq` with
 * `reason: "poison"` and **no retry**, the queue being the consumer's topic ({@see HasQueue}); `delete`
 * acks and drops; with no publisher `fail`/`release` take the bounded path above (a poison body is
 * never released forever) and `dead_letter` acks, drops and logs (rethrows on Kafka).
 *
 * **Version gate (ADR-0037 decision 1, Pulsar §5.7).** A {@see PulsarMessage} whose `bq-schema-version` property is
 * present, non-blank and not the supported version ({@see PulsarConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX}) is neither an
 * unknown URN nor poison: it is settled on its **first** delivery, without a handler, an `onUnknownUrn` hook or a retry,
 * whatever `bq-attempts`, the redelivery count or `$maxAttempts` say. `delete` acks and drops; with no publisher (or no
 * nameable queue) it acks, drops and logs (the `dead_letter` strategy degrades to `delete` the same way); otherwise the
 * **raw record** (the body byte for byte, no `dead_letter` block, {@see DeadLetterPublisher::publishRaw()}) goes to
 * `<queue>.dlq` and the message is acked. A failed write throws {@see DeadLetterWriteFailed}, which leaves the delivery
 * unsettled (the consumer negatively acknowledges it); the gate rejects the redelivery again, so the write is retried.
 * `fail` and `release` behave alike (a version-gated delivery is never released). Kafka is out of scope of this rule: a
 * version-gated Kafka record keeps the unknown-URN path.
 *
 * A drop is logged through `$onDrop($message, $why)` (default: `error_log`); it never carries the body.
 *
 * **Retry backoff (ADR-0034 §9, opt-in).** Hand it a {@see RetryBackoff} and a consumer that releases
 * a failed message with a delay asks {@see self::retryDelaySeconds()} for the seconds to pass to its
 * transport. To have the dispatcher apply it on the consume path, pass an `$onRetry` closure: whenever a
 * handler throws and the message is *not* dead-lettered or dropped, `$onRetry($message, $delaySeconds, $e)`
 * runs (before the exception is rethrown for redelivery), e.g.
 * `fn ($m, $delay) => $sqs->release($handleOf($m), $delay)` — `SqsTransport::release()` clamps an
 * unbounded delay to SQS's 12 h limit. Without a backoff (the default) the delay is 0 — the previous
 * behaviour. Not applied on Kafka.
 *
 * Note on Kafka: `attempts` only grows across redeliveries when retry topics increment `bq-attempts`
 * (§6.4/§6.5), so the `maxAttempts` cap is effective on Pulsar (native redelivery count)
 * and on Kafka once retry topics are configured.
 */
final class Dispatcher
{
    /** @var array<string, callable(ConsumedMessage): void> */
    private array $handlers = [];

    /**
     * @param  string  $onUnknownUrn  one of {@see UnknownUrnStrategy}
     * @param  int  $maxAttempts  total deliveries a message gets, the first included (default 3); 0 = no cap
     * @param  DeadLetterPublisher|null  $deadLetters  where exhausted / poison messages go; null = dead-lettering off
     * @param  \Closure|null  $onDrop  `fn(ConsumedMessage $message, string $why): void`, called whenever the
     *                                 Dispatcher acknowledges and drops a message; null = log via `error_log`
     */
    public function __construct(
        private readonly string $onUnknownUrn = UnknownUrnStrategy::FAIL,
        private readonly int $maxAttempts = 3,
        private readonly ?DeadLetterPublisher $deadLetters = null,
        private readonly ?RetryBackoff $retryBackoff = null,
        private readonly ?\Closure $onRetry = null,
        private readonly ?\Closure $onDrop = null,
    ) {
    }

    /**
     * Whole seconds a failed `$message` should wait before its next attempt (rounded up so a
     * sub-second backoff never shortens): `d(n)` for `n = attempts + 1`, 0 when no
     * {@see RetryBackoff} is configured.
     */
    public function retryDelaySeconds(ConsumedMessage $message): int
    {
        if ($this->retryBackoff === null) {
            return 0;
        }

        $delay = $this->retryBackoff->delaySeconds($message->attempts() + 1);

        return $delay >= PHP_INT_MAX ? PHP_INT_MAX : (int) ceil($delay);
    }

    /**
     * Register the handler for a URN. Returns $this for fluent chaining.
     *
     * @param  callable(ConsumedMessage): void  $handler
     */
    public function on(string $urn, callable $handler): self
    {
        $this->handlers[$urn] = $handler;

        return $this;
    }

    /**
     * Dispatch one message. Returning acks it; throwing redelivers it.
     */
    public function __invoke(ConsumedMessage $message): void
    {
        if ($this->isVersionGated($message)) {
            $this->handleVersionGated($message);

            return;
        }

        if ($this->isPoison($message)) {
            $this->handlePoison($message);

            return;
        }

        $handler = $this->handlers[$message->getUrn()] ?? null;

        if ($handler === null) {
            $this->handleUnknownUrn($message);

            return;
        }

        try {
            $handler($message);
        } catch (Throwable $e) {
            if ($this->exhausted($message)) {
                if ($this->canDeadLetter($message, 'failed')) {
                    $this->deadLetter($message, 'failed', $e, $this->counted($message));

                    return;
                }

                if ($message instanceof PulsarMessage || $this->isUnaddressable($message)) {
                    $this->drop($message, 'the handler kept failing and no dead-letter publisher can name its queue');

                    return;
                }
                // Kafka without ANY publisher (and any other type) never drops: rethrow below so the retry router /
                // consumer decides. With a publisher whose queue is unknown, ADR-0036 decision 3 says commit, drop, log:
                // a rethrow would redeliver a record that can never be addressed and wedge the partition.
            }

            if ($this->onRetry !== null) {
                ($this->onRetry)($message, $this->retryDelaySeconds($message), $e);
            }

            throw $e;
        }
    }

    /**
     * The delivery counted against the cap: the reconciled attempts plus the current delivery.
     */
    private function counted(ConsumedMessage $message): int
    {
        return $message->attempts() + 1;
    }

    /**
     * True when this delivery reaches the cap (`counted >= maxAttempts`); never with `maxAttempts` 0.
     */
    private function exhausted(ConsumedMessage $message): bool
    {
        return $this->maxAttempts > 0 && $this->counted($message) >= $this->maxAttempts;
    }

    /**
     * A body that cannot be handed to a handler (ADR-0036 decision 4): no routing identity, or a raw
     * body the strict classifier does not accept (undecodable, a wrong-typed member, no `job`/`urn`).
     * The version-gate sentinel is not poison: it is an unknown URN by design. A Kafka record was
     * already classified by the consumer, so only its identity is checked here.
     */
    private function isPoison(ConsumedMessage $message): bool
    {
        $urn = $message->getUrn();
        if (str_starts_with($urn, PulsarConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX)) {
            return false;
        }

        if ($urn === '') {
            return true;
        }

        if ($message instanceof KafkaMessage || ! $message instanceof HasRawBody) {
            return false;
        }

        $raw = $message->rawBody();

        return $raw !== null && StrictBodyClassifier::isPoison($raw);
    }

    /**
     * A Pulsar delivery rejected by the §5.7 `bq-schema-version` property gate (the body was never decoded).
     */
    private function isVersionGated(ConsumedMessage $message): bool
    {
        return $message instanceof PulsarMessage
            && str_starts_with($message->getUrn(), PulsarConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX);
    }

    /**
     * ADR-0037 decision 1: settle a version-gated delivery on its first delivery. Only the *configured* strategy is
     * read (`fail` when none is); `fail` and `release` are alike. Never decodes, never calls a handler, never retries.
     *
     * @throws DeadLetterWriteFailed When the raw record cannot be written (the delivery stays unsettled)
     */
    private function handleVersionGated(ConsumedMessage $message): void
    {
        if ($this->onUnknownUrn === UnknownUrnStrategy::DELETE) {
            return; // ack + drop (the explicit strategy: nothing to log)
        }

        $queue = $this->queueOf($message);
        $body = $message instanceof HasRawBody ? $message->rawBody() : null;

        if ($this->deadLetters === null || $queue === null || $queue === '' || $body === null) {
            // Dead-lettering is off (or the record cannot be addressed): ack, drop and log, visibly.
            $this->drop($message, 'unsupported bq-schema-version, no dead-letter publisher can write the raw record');

            return;
        }

        $this->deadLetters->publishRaw($message, $body, $queue);
    }

    private function handlePoison(ConsumedMessage $message): void
    {
        switch ($this->onUnknownUrn) {
            case UnknownUrnStrategy::DELETE:
                $this->drop($message, 'poison body, on_unknown_urn is delete');

                return;

            case UnknownUrnStrategy::DEAD_LETTER:
                if ($this->canDeadLetter($message, 'poison')) {
                    $this->deadLetter($message, 'poison', null);

                    return;
                }

                $this->dropOrRethrow($message, 'poison body, on_unknown_urn is dead_letter and no dead-letter publisher can name its queue', null, true);

                return;

            case UnknownUrnStrategy::RELEASE:
            case UnknownUrnStrategy::FAIL:
            default:
                if ($this->canDeadLetter($message, 'poison')) {
                    $this->deadLetter($message, 'poison', null); // no retry: the same bytes would fail again

                    return;
                }

                // No publisher: the bounded `fail` path (a poison body is never released forever).
                if ($this->exhausted($message)) {
                    $this->dropOrRethrow($message, 'poison body, attempts exhausted and no dead-letter publisher can name its queue');

                    return;
                }

                throw new UnknownUrnException('The message body is poison: it does not decode into an envelope with a job or urn.');
        }
    }

    private function handleUnknownUrn(ConsumedMessage $message): void
    {
        switch ($this->onUnknownUrn) {
            case UnknownUrnStrategy::DELETE:
                return; // ack + drop (the explicit strategy: nothing to log)

            case UnknownUrnStrategy::DEAD_LETTER:
                if ($this->canDeadLetter($message, 'unknown_urn')) {
                    $this->deadLetter($message, 'unknown_urn', null);

                    return;
                }

                // Dead-lettering is off: the strategy degrades to delete, visibly.
                $this->dropOrRethrow($message, sprintf('no handler for URN [%s], on_unknown_urn is dead_letter and no dead-letter publisher can name its queue', $message->getUrn()), null, true);

                return;

            case UnknownUrnStrategy::RELEASE:
            case UnknownUrnStrategy::FAIL:
            default:
                $e = new UnknownUrnException(
                    sprintf('No handler is registered for URN [%s].', $message->getUrn()),
                );

                if (! $this->exhausted($message)) {
                    throw $e;
                }

                if ($this->canDeadLetter($message, 'unknown_urn')) {
                    $this->deadLetter($message, 'unknown_urn', $e, $this->counted($message));

                    return;
                }

                $this->dropOrRethrow($message, sprintf('no handler for URN [%s], attempts exhausted and no dead-letter publisher can name its queue', $message->getUrn()), $e);
        }
    }

    /**
     * Write the DLQ record. `$attempts` is the counted value for a retry-exhaustion record, null for a
     * direct dead-letter (the delivered attempts, informational).
     */
    private function deadLetter(ConsumedMessage $message, string $reason, ?Throwable $e, ?int $attempts = null): void
    {
        /** @var DeadLetterPublisher $publisher */
        $publisher = $this->deadLetters;
        $publisher->publish($message, $reason, $e, $attempts, $this->queueOf($message));
    }

    /**
     * Whether a record for `$message` can be written: a publisher is set AND the queue is known (the consumer's
     * for a {@see HasQueue} message). Nothing is ever written to `default.dlq`; an unnameable queue is handled
     * like "no publisher".
     */
    private function canDeadLetter(ConsumedMessage $message, string $reason): bool
    {
        // A message that knows its consumer (Pulsar, Kafka) is addressed by the consumer's queue only: a producer's
        // `meta.queue` (e.g. the `default` that `EnvelopeCodec::make()` writes) must not pick the DLQ. A message of a
        // type that does not carry one keeps using its own `meta.queue`.
        if ($message instanceof HasQueue && $this->queueOf($message) === null) {
            return false;
        }

        return $this->deadLetters !== null
            && $this->deadLetters->resolveQueue($message, $reason, $this->queueOf($message)) !== null;
    }

    /**
     * A dead-letter publisher IS configured but the consumer's queue (topic) is unknown, so no `<queue>.dlq` can be
     * named (a multi-topic Kafka subscription, a Pulsar client that reports no topic). ADR-0036 decision 3: the
     * section 6.5 degrade applies, commit / acknowledge, drop and log, on Kafka as on Pulsar; the Dispatcher does
     * NOT rethrow, because that would redeliver a record that can never be addressed. Without any publisher the
     * earlier rule is unchanged (Kafka rethrows).
     */
    private function isUnaddressable(ConsumedMessage $message): bool
    {
        return $this->deadLetters !== null && $message instanceof HasQueue && $this->queueOf($message) === null;
    }

    /**
     * The consumer's queue (its topic), when the message knows it.
     */
    private function queueOf(ConsumedMessage $message): ?string
    {
        return $message instanceof HasQueue ? $message->queue() : null;
    }

    /**
     * Drop a message the Dispatcher cannot dead-letter: acknowledge it and log on Pulsar, and on Kafka when a
     * publisher is set but the topic is unknown ({@see self::isUnaddressable()}); rethrow on Kafka without any
     * publisher, where a normal return would commit the offset and lose the record, and for a message type the
     * Dispatcher does not know (the previous behaviour). `$strategy` marks the `dead_letter` strategy
     * degrading to `delete` (§3): that is the configured behaviour, so every non-Kafka type acks and logs.
     */
    private function dropOrRethrow(ConsumedMessage $message, string $why, ?Throwable $e = null, bool $strategy = false): void
    {
        if ($message instanceof PulsarMessage || $this->isUnaddressable($message) || ($strategy && ! $message instanceof KafkaMessage)) {
            $this->drop($message, $why);

            return;
        }

        throw $e ?? new UnknownUrnException($why);
    }

    /**
     * Acknowledge-and-drop bookkeeping: tell `$onDrop`, or log. Never logs the body.
     */
    private function drop(ConsumedMessage $message, string $why): void
    {
        if ($this->onDrop !== null) {
            ($this->onDrop)($message, $why);

            return;
        }

        $id = $message->getMeta()['id'] ?? null;
        error_log(sprintf(
            '[babelqueue] dropped a message (id %s, delivery %d): %s',
            is_string($id) && $id !== '' ? $id : 'unknown',
            $this->counted($message),
            $why,
        ));
    }
}
