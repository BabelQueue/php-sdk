<?php

declare(strict_types=1);

namespace BabelQueue\Consume;

use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Routing\UnknownUrnStrategy;
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
 * returns normally acks the message; a thrown handler redelivers it (at-least-once). On an unknown
 * URN it applies the strategy — `delete` acks and drops, `dead_letter` routes to `<queue>.dlq` then
 * acks (degrading to `delete` when no publisher is set), and `fail`/`release` throw to redeliver
 * (delayed `release` is a binding-native / retry-topic concern, see the §x.5 bindings). When
 * `maxAttempts > 0` and a publisher is set, a handler that keeps throwing is dead-lettered once the
 * reconciled attempt count would reach the cap, instead of redelivering forever.
 *
 * **Retry backoff (ADR-0034 §9, opt-in).** Hand it a {@see RetryBackoff} and a consumer that releases
 * a failed message with a delay asks {@see self::retryDelaySeconds()} for the seconds to pass to its
 * transport. To have the dispatcher apply it on the consume path, pass an `$onRetry` closure: whenever a
 * handler throws and the message is *not* dead-lettered, `$onRetry($message, $delaySeconds, $e)` runs
 * (before the exception is rethrown for redelivery), e.g.
 * `fn ($m, $delay) => $sqs->release($handleOf($m), $delay)` — `SqsTransport::release()` clamps an
 * unbounded delay to SQS's 12 h limit. Without a backoff (the default) the delay is 0 — the previous
 * behaviour. Not applied on Kafka.
 *
 * Note on Kafka: `attempts` only grows across redeliveries when retry topics increment `bq-attempts`
 * (§6.4/§6.5), so the `maxAttempts` dead-letter cap is effective on Pulsar (native redelivery count)
 * and on Kafka once retry topics are configured.
 */
final class Dispatcher
{
    /** @var array<string, callable(ConsumedMessage): void> */
    private array $handlers = [];

    public function __construct(
        private readonly string $onUnknownUrn = UnknownUrnStrategy::FAIL,
        private readonly int $maxAttempts = 0,
        private readonly ?DeadLetterPublisher $deadLetters = null,
        private readonly ?RetryBackoff $retryBackoff = null,
        private readonly ?\Closure $onRetry = null,
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
        $handler = $this->handlers[$message->getUrn()] ?? null;

        if ($handler === null) {
            $this->handleUnknownUrn($message);

            return;
        }

        try {
            $handler($message);
        } catch (Throwable $e) {
            if ($this->shouldDeadLetter($message)) {
                /** @var DeadLetterPublisher $publisher */
                $publisher = $this->deadLetters;
                $publisher->publish($message, 'failed', $e);

                return;
            }

            if ($this->onRetry !== null) {
                ($this->onRetry)($message, $this->retryDelaySeconds($message), $e);
            }

            throw $e;
        }
    }

    private function shouldDeadLetter(ConsumedMessage $message): bool
    {
        return $this->maxAttempts > 0
            && $this->deadLetters !== null
            && $message->attempts() + 1 >= $this->maxAttempts;
    }

    private function handleUnknownUrn(ConsumedMessage $message): void
    {
        switch ($this->onUnknownUrn) {
            case UnknownUrnStrategy::DELETE:
                return; // ack + drop

            case UnknownUrnStrategy::DEAD_LETTER:
                if ($this->deadLetters !== null) {
                    $this->deadLetters->publish($message, 'unknown_urn', null);
                }

                return; // ack (degrades to delete when no publisher is configured)

            case UnknownUrnStrategy::RELEASE:
            case UnknownUrnStrategy::FAIL:
            default:
                throw new UnknownUrnException(
                    sprintf('No handler is registered for URN [%s].', $message->getUrn()),
                );
        }
    }
}
