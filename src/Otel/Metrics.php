<?php

declare(strict_types=1);

namespace BabelQueue\Otel;

use BabelQueue\Contracts\DeadLetterObserver;
use OpenTelemetry\API\Metrics\CounterInterface;
use OpenTelemetry\API\Metrics\HistogramInterface;
use OpenTelemetry\API\Metrics\MeterInterface;

/**
 * The standard BabelQueue metric set (ADR-0039), recorded through an OpenTelemetry {@see MeterInterface}.
 *
 * | Instrument | Kind | Unit | Recorded by |
 * | :--- | :--- | :--- | :--- |
 * | `messaging.process.duration` | histogram | `s` | {@see Tracing::wrap()}: once per handler call |
 * | `messaging.client.sent.messages` | counter | `{message}` | {@see Tracing::publish()}: once per publish attempt |
 * | `messaging.client.consumed.messages` | counter | `{message}` | {@see Tracing::wrap()}: once per delivery |
 * | `babelqueue.retries` | counter | `{message}` | {@see Tracing::wrap()}: per delivery with `attempts > 0` |
 * | `babelqueue.dlq.messages` | counter | `{message}` | this class as a {@see DeadLetterObserver} |
 *
 * Every data point carries `messaging.system` (the `system` option, `babelqueue` when absent or empty) and
 * `messaging.operation.name`, plus `messaging.destination.name` when the queue is known (present and not
 * blank) and `error.type` on a failed operation. Nothing else is ever a metric attribute: no URN, message id,
 * `trace_id`, attempts, schema version or dead-letter reason (ADR-0039 section 2, cardinality limit).
 *
 * **Opt-in (GR-7).** The core never loads this class: it needs the optional `open-telemetry/api` package,
 * and `Tracing::wrap()` / `Tracing::publish()` create one only when you pass a `meter`. For the
 * dead-letter counter hand the instance to the component that writes the records:
 *
 *     $metrics = new Metrics($meter, system: 'kafka');
 *     $publisher = new DeadLetterPublisher($transport, observer: $metrics);
 *
 * The instruments are created once per instance. Create the meter with {@see Tracing::SCHEMA_URL} to pin
 * the semantic-conventions release (ADR-0038 section 1).
 */
final class Metrics implements DeadLetterObserver
{
    public const PROCESS_DURATION = 'messaging.process.duration';
    public const SENT_MESSAGES = 'messaging.client.sent.messages';
    public const CONSUMED_MESSAGES = 'messaging.client.consumed.messages';
    public const RETRIES = 'babelqueue.retries';
    public const DLQ_MESSAGES = 'babelqueue.dlq.messages';

    /** The semconv advisory bucket boundaries of `messaging.process.duration`, in seconds. */
    public const PROCESS_DURATION_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10];

    private readonly string $system;
    private readonly HistogramInterface $processDuration;
    private readonly CounterInterface $sent;
    private readonly CounterInterface $consumed;
    private readonly CounterInterface $retries;
    private readonly CounterInterface $dlq;

    /** @var \WeakMap<MeterInterface, array<string, self>>|null one instance per meter and system, for the static producer path */
    private static ?\WeakMap $shared = null;

    /**
     * The shared instance for `$meter` and `$system`, so a static producer call does not create the instruments on
     * every message (ADR-0039 section 3: once, not per message).
     */
    public static function shared(MeterInterface $meter, ?string $system = null): self
    {
        $system = Tracing::systemOf($system);
        self::$shared ??= new \WeakMap();
        $perSystem = self::$shared[$meter] ?? [];

        if (! isset($perSystem[$system])) {
            $perSystem[$system] = new self($meter, $system);
            self::$shared[$meter] = $perSystem;
        }

        return $perSystem[$system];
    }

    /**
     * @param  string|null  $system  the `messaging.system` value; null or empty = `babelqueue` (deprecated default, ADR-0038 section 5)
     */
    public function __construct(MeterInterface $meter, ?string $system = null)
    {
        $this->system = Tracing::systemOf($system);
        $this->processDuration = $meter->createHistogram(
            self::PROCESS_DURATION,
            's',
            'Duration of processing a delivered message.',
            ['ExplicitBucketBoundaries' => self::PROCESS_DURATION_BUCKETS],
        );
        $this->sent = $meter->createCounter(self::SENT_MESSAGES, '{message}', 'Messages a producer attempted to send.');
        $this->consumed = $meter->createCounter(self::CONSUMED_MESSAGES, '{message}', 'Messages delivered to a consumer.');
        $this->retries = $meter->createCounter(self::RETRIES, '{message}', 'Deliveries that were a retry (attempts above zero).');
        $this->dlq = $meter->createCounter(self::DLQ_MESSAGES, '{message}', 'Dead-letter records the SDK wrote.');
    }

    /**
     * A delivery entered the consumer wrapper: `consumed.messages` +1, and `retries` +1 when `$attempts > 0`.
     */
    public function consumed(string $queue, int $attempts): void
    {
        $attributes = $this->attributes('process', $queue);
        $this->consumed->add(1, $attributes);
        if ($attempts > 0) {
            $this->retries->add(1, $attributes);
        }
    }

    /**
     * The handler returned (`$errorType` null) or failed: one `process.duration` observation of `$seconds`.
     */
    public function processed(string $queue, float $seconds, ?string $errorType = null): void
    {
        $this->processDuration->record($seconds, $this->attributes('process', $queue, $errorType));
    }

    /**
     * A publish was attempted: `sent.messages` +1, with `error.type` when it failed.
     */
    public function sent(string $queue, ?string $errorType = null): void
    {
        $this->sent->add(1, $this->attributes('send', $queue, $errorType));
    }

    /**
     * {@see DeadLetterObserver}: a dead-letter record was written, `dlq.messages` +1 for the source queue.
     * The DLQ name and the reason are not attributes (cardinality limit).
     */
    public function deadLettered(string $queue, string $deadLetterQueue, string $reason): void
    {
        $this->dlq->add(1, $this->attributes('process', $queue));
    }

    /**
     * @return array<non-empty-string, string>
     */
    private function attributes(string $operation, string $queue, ?string $errorType = null): array
    {
        $attributes = [
            'messaging.system' => $this->system,
            'messaging.operation.name' => $operation,
        ];
        $destination = Tracing::destinationOf($queue);
        if ($destination !== null) {
            $attributes['messaging.destination.name'] = $destination;
        }
        if ($errorType !== null) {
            $attributes['error.type'] = $errorType;
        }

        return $attributes;
    }
}
