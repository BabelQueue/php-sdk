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
    ) {
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
