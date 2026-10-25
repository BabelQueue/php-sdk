<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Codec\StrictBodyClassifier;
use BabelQueue\Consume\DeadLetterWriteFailed;
use BabelQueue\Exceptions\BabelQueueException;
use Closure;
use Throwable;

/**
 * A framework-less **Apache Pulsar** consumer — PHP's path to the §5 binding's consume side over
 * Pulsar's native **WebSocket consumer API**, the counterpart to {@see PulsarTransport} (produce).
 *
 * **GR-7 stays intact** (like the producer, ADR-0020): the WebSocket client is pure-userland — no C
 * extension — so the consumer is opt-in behind the one-method {@see PulsarWebSocketConsumerClient}
 * seam, dependency-free and unit-testable with a fake.
 *
 * It exposes the §5.5 consume primitives — {@see self::receive()} (decode + route + reconcile
 * attempts), {@see self::acknowledge()} (§5 "delete") and {@see self::release()}
 * (`negativeAcknowledge` → redelivery, at-least-once) — plus a {@see self::consume()} convenience
 * loop.
 *
 * Per §5.2/§5.7 **routing follows the `bq-job` message property** — {@see PulsarMessage::getUrn()}
 * returns the trimmed, non-blank property and falls back to the body's URN
 * ({@see EnvelopeCodec::urn()}) when it is absent or blank. The body is always decoded and
 * {@see PulsarMessage::envelope()} is always the body's envelope. A body without a `job` is never
 * routed, even when `bq-job` is set (it yields an empty URN, which the
 * {@see \BabelQueue\Consume\Dispatcher} treats as a poison body, ADR-0036 decision 4).
 *
 * Per §5.2/§5.7 the **`attempts` counter** is the authoritative `bq-attempts` property (valid only
 * when it is plain decimal digits, `^[0-9]+$`, no greater than 2147483647), falling back to the
 * body's `attempts` when it is absent or invalid; the result is then floored with
 * `max(.., redeliveryCount)` (Pulsar's redelivery count is 0-based, so no `-1`) and exposed via
 * {@see PulsarMessage::attempts()} so a handler can apply its own retry / dead-letter policy.
 *
 * When the `bq-job` property and the body URN differ, the optional `$onRoutingMismatch` hook is
 * called as `fn(string $propertyUrn, string $bodyUrn, array $envelope)`. It is a separate,
 * best-effort diagnostic: any exception it throws is swallowed and the message flow is unchanged.
 * It runs after the `attempts` reconcile, so it sees the envelope the handler gets. Unlike the
 * Java/.NET/Node consumers, this consumer has no `accepts()` gate and the
 * {@see \BabelQueue\Consume\Dispatcher} does not validate the envelope either (it only looks the
 * routing URN up), so the hook is not suppressed for a non-conformant envelope.
 *
 * **Version gate (§5.7 step 3).** Before the body is decoded the `bq-schema-version` property is
 * read. A missing or blank value (empty or only ASCII whitespace) changes nothing; any value other than
 * {@see EnvelopeCodec::SCHEMA_VERSION} (`"2"`, `"x"`, `"01"`, ...; exact string match) leaves the body
 * undecoded and yields a message whose routing URN is the reserved
 * {@see self::UNSUPPORTED_SCHEMA_URN_PREFIX}`<value>`. No handler owns it, so the Dispatcher takes
 * its existing unknown-URN path (`on_unknown_urn`: release / dead-letter / ...), and the error names
 * the rejected version. The envelope of such a message is only `['raw' => <body>, 'attempts' => n]`,
 * so a dead-letter copy keeps the producer's bytes in the same `{"raw": ...}` shape as a poison body.
 *
 * **Queue name (ADR-0036 decision 3).** A Pulsar message frame carries no topic, so the consumer is told
 * the queue it reads: pass `$queue` (the topic's local name, e.g. `orders` for
 * `persistent://public/default/orders`), or let the client adapter add an optional `topic` key to the
 * frame it returns from `receive()` (the full topic or the local name; the part after the last `/` is
 * used). The name is exposed as {@see PulsarMessage::queue()} and is where the
 * {@see \BabelQueue\Consume\DeadLetterPublisher} writes `<queue>.dlq` for a message with no `meta.queue`
 * (a poison body, a version-gated delivery); without it the {@see \BabelQueue\Consume\Dispatcher} cannot name the
 * queue and treats such a message like "no publisher" (nothing is written to `default.dlq`). The queue is
 * **derived** when not given: from a client that implements {@see PulsarConsumerDescriptor} (its `topic()`), else
 * from the frame's `topic` key.
 *
 * **Out of the box.** The SDK's own {@see PulsarWebSocketSubscription} is the consumer client for a Pulsar WebSocket
 * service; it reports its topic, subscription type and service URL, so `new PulsarConsumer($subscription)` needs no
 * further argument and the retry cap holds on every subscription type.
 *
 * **Retry is a re-publish (ADR-0036 decision 1, the Go model).** A negative acknowledgement advances Pulsar's
 * redelivery count only on a `Shared` / `Key_Shared` subscription; on `Exclusive` / `Failover` it does not, so a
 * nack-only retry would never reach the {@see \BabelQueue\Consume\Dispatcher}'s `maxAttempts`. So, through
 * the {@see PulsarWebSocketClient} producer seam, {@see self::retry()} — which {@see self::consume()} calls
 * when a handler throws — re-publishes the message to the **same topic** with `bq-attempts` + 1 (and the body's
 * top-level `attempts` kept in sync for an envelope; a poison or version-gated body keeps its exact bytes and
 * properties), then acknowledges the original, so the bound holds whatever the subscription type. A failed
 * re-publish leaves the original unacknowledged (negative ack).
 *
 * **The consumer derives what it needs** (an explicit argument always wins; the API is unchanged):
 *  - the retry producer: `$retryProducer`, else the client itself when it also implements
 *    {@see PulsarWebSocketClient} (one connection object that reads and publishes), else a
 *    {@see PulsarWebSocketProducer} on the Pulsar WebSocket service (`$serviceUrl`, else the client's
 *    {@see PulsarServiceEndpoint::serviceUrl()}): the producer endpoint is the consumer's own host, and the tenant,
 *    namespace and topic come from the consumer's real topic;
 *  - the topic: `$queue` (built into `persistent://<tenant>/<namespace>/<queue>`), else a client that implements
 *    {@see PulsarConsumerDescriptor} (its `topic()`, used as-is when it is a full name), else the frame's `topic`;
 *  - the subscription type: `$subscriptionType`, else {@see PulsarConsumerDescriptor::subscriptionType()}.
 * **Fail-closed, only when nothing can be derived.** A nack-only retry is correct on `Shared` / `Key_Shared` only,
 * and Pulsar cannot be asked which type a subscription has over this seam, so an unknown type is treated as
 * `Exclusive` (Pulsar's own default). When no producer can be derived at all (no `$retryProducer`, no `$serviceUrl`,
 * and a client that is neither a {@see PulsarWebSocketClient} nor a {@see PulsarServiceEndpoint}) the constructor throws unless the subscription is known to be `Shared` /
 * `Key_Shared` (a clear setup error instead of a retry that can never reach the cap), and a retry whose topic is
 * still unknown throws too. To keep a nack-only consumer, say so: pass `subscriptionType: 'Shared'` (or
 * `'Key_Shared'`), or implement {@see PulsarConsumerDescriptor}.
 *
 * URN dispatch, `on_unknown_urn` and DLQ routing can be handled by the framework-less
 * {@see \BabelQueue\Consume\Dispatcher} runtime (or a framework adapter): it is callable, so
 * `consume($dispatcher, $shouldStop)` dispatches on {@see PulsarMessage::getUrn()} and
 * dead-letters poison messages — the consumer stays minimal.
 */
final class PulsarConsumer
{
    /** Largest valid `bq-attempts` value (int32, shared across the SDKs for parity). */
    private const MAX_ATTEMPTS = 2147483647;

    /**
     * Reserved routing URN prefix for a delivery rejected by the §5.7 version gate; the rejected
     * `bq-schema-version` value is appended. No handler is ever registered for it.
     */
    public const UNSUPPORTED_SCHEMA_URN_PREFIX = 'babelqueue:unsupported-schema-version:';

    private readonly ?Closure $onRoutingMismatch;

    /** The retry producer: given, else the client itself when it can publish. */
    private readonly ?PulsarWebSocketClient $producer;

    /** The full topic the client reads, when it said so; the retry re-publishes to it as-is. */
    private readonly ?string $retryTopic;

    /** The queue (local topic name) the client reported, used when `$queue` is not given. */
    private readonly ?string $clientQueue;

    /** `Exclusive` | `Failover` | `Shared` | `Key_Shared`, or null when unknown. */
    private readonly ?string $subscriptionType;

    /**
     * @param  (callable(string, string, array<string, mixed>): mixed)|null  $onRoutingMismatch
     *         optional diagnostic hook `fn($propertyUrn, $bodyUrn, $envelope)` called when the
     *         `bq-job` property and the body URN differ; default no-op, its errors are swallowed
     * @param  string|null  $queue  the queue this consumer reads (its topic's local name); names `<queue>.dlq`
     *         for a message without `meta.queue`. Null = take it from the frame's optional `topic` key, else unknown
     * @param  PulsarWebSocketClient|null  $retryProducer  the producer {@see self::retry()} re-publishes through;
     *         null = the client itself when it is a {@see PulsarWebSocketClient}, else a nack (`Shared` / `Key_Shared` only)
     * @param  string|null  $tenant  the tenant, used with `$namespace` to build the re-publish topic from a LOCAL topic
     *         name only; never guessed (`public` is not assumed): a local name without both is a retry error
     * @param  string|null  $namespace  the namespace, see `$tenant`
     * @param  string|null  $subscriptionType  `Exclusive` | `Failover` | `Shared` | `Key_Shared` (case-insensitive);
     *         null = ask the client ({@see PulsarConsumerDescriptor}), else unknown, which counts as `Exclusive`
     *
     * @param  string|null  $serviceUrl  the Pulsar WebSocket service (`ws://host:8080`, `http://host:8080`, ...) the retry
     *         producer is built on ({@see PulsarWebSocketProducer}); null = ask the client ({@see PulsarServiceEndpoint})
     *
     * @param  string|null  $serviceAuthToken  the bearer token of the derived producer; null = the client's
     *         {@see PulsarServiceEndpoint::authToken()}. Needed on a cluster that requires authentication when `$serviceUrl` is given
     * @param  bool  $topicFromFrame  declare that the adapter puts the FULL topic name in every frame's `topic` key and that
     *         the re-publish topic may be taken from there. Without it (and without a descriptor topic, or `$queue` with
     *         `$tenant` and `$namespace`) the constructor refuses a producer that would not know where to publish
     *
     * @throws \InvalidArgumentException When `$subscriptionType` is not a Pulsar subscription type, or the service URL is not one
     * @throws BabelQueueException When no retry producer can be derived and the subscription is not known to be
     *                             `Shared` / `Key_Shared` (`Exclusive`, `Failover` or unknown)
     */
    public function __construct(
        private readonly PulsarWebSocketConsumerClient $client,
        ?callable $onRoutingMismatch = null,
        private readonly ?string $queue = null,
        ?PulsarWebSocketClient $retryProducer = null,
        private readonly ?string $tenant = null,
        private readonly ?string $namespace = null,
        ?string $subscriptionType = null,
        ?string $serviceUrl = null,
        ?string $serviceAuthToken = null,
        bool $topicFromFrame = false,
    ) {
        $this->onRoutingMismatch = $onRoutingMismatch === null ? null : Closure::fromCallable($onRoutingMismatch);

        $descriptor = $client instanceof PulsarConsumerDescriptor ? $client : null;

        $this->producer = $retryProducer
            ?? ($client instanceof PulsarWebSocketClient ? $client : null)
            ?? self::producerFromServiceUrl($serviceUrl, $serviceAuthToken, $client);

        $topic = $descriptor?->topic();
        $topic = $topic !== null && $topic !== '' ? $topic : null;
        $this->retryTopic = $topic !== null && str_contains($topic, '://') ? $topic : null;
        $this->clientQueue = $topic !== null ? self::localName($topic) : null;

        $type = $subscriptionType ?? $descriptor?->subscriptionType();
        $this->subscriptionType = $type === null ? null : self::normaliseSubscriptionType($type);

        if ($this->producer === null && $this->nackCannotBoundRetry()) {
            throw new BabelQueueException(sprintf(
                'A %s Pulsar subscription needs a retry producer: a negative acknowledgement does not advance the redelivery count there, so the retry could never reach maxAttempts, and none could be derived (no $retryProducer, no $serviceUrl, and the client implements neither %s nor %s). Pass one of them, or declare a Shared / Key_Shared subscription (subscriptionType: or %s) if that is what it is.',
                $this->subscriptionType ?? 'unknown (treated as Exclusive)',
                PulsarWebSocketClient::class,
                PulsarServiceEndpoint::class,
                PulsarConsumerDescriptor::class,
            ));
        }

        if (
            $this->producer !== null
            && $this->nackCannotBoundRetry()
            && ! $topicFromFrame
            && $this->retryTopic === null
            && ! ($queue !== null && $tenant !== null && $namespace !== null)
        ) {
            throw new BabelQueueException(sprintf(
                'The retry of an %s Pulsar subscription re-publishes to the topic the message was read from, and this consumer cannot know it: the client reports no full topic (%s::topic()) and no $tenant and $namespace were given with $queue. Report a full topic, pass them, or pass topicFromFrame: true if every frame carries the full "topic".',
                $this->subscriptionType ?? 'unknown (treated as Exclusive)',
                PulsarConsumerDescriptor::class,
            ));
        }
    }

    /**
     * The retry producer built on the Pulsar WebSocket service: the explicit `$serviceUrl`, else the client's own
     * ({@see PulsarServiceEndpoint}). Null when neither names one. Its tenant, namespace and topic come from the
     * topic of each message at retry time, never from here.
     */
    private static function producerFromServiceUrl(?string $serviceUrl, ?string $serviceAuthToken, PulsarWebSocketConsumerClient $client): ?PulsarWebSocketClient
    {
        $token = $serviceAuthToken;
        if ($client instanceof PulsarServiceEndpoint) {
            if ($serviceUrl === null || $serviceUrl === '') {
                $serviceUrl = $client->serviceUrl();
            }
            $token ??= $client->authToken();
        }

        return $serviceUrl !== null && $serviceUrl !== '' ? new PulsarWebSocketProducer($serviceUrl, $token) : null;
    }

    /**
     * True unless the subscription is known to be `Shared` / `Key_Shared`: on `Exclusive` / `Failover` a nack leaves
     * the native redelivery count unchanged, and an unknown type is treated as `Exclusive` (fail-closed).
     */
    private function nackCannotBoundRetry(): bool
    {
        return $this->subscriptionType !== 'Shared' && $this->subscriptionType !== 'Key_Shared';
    }

    private static function normaliseSubscriptionType(string $type): string
    {
        foreach (['Exclusive', 'Failover', 'Shared', 'Key_Shared'] as $known) {
            if (strcasecmp($type, $known) === 0) {
                return $known;
            }
        }

        throw new \InvalidArgumentException(sprintf('Unknown Pulsar subscription type [%s]; expected Exclusive, Failover, Shared or Key_Shared.', $type));
    }

    /** The part after the last `/` of a topic name (the whole name when it has none), null when empty. */
    private static function localName(string $topic): ?string
    {
        $slash = strrpos($topic, '/');
        $name = $slash === false ? $topic : substr($topic, $slash + 1);

        return $name !== '' ? $name : null;
    }

    /**
     * Receive and decode the next message, routed per `bq-job` with §5 `attempts` reconciled onto
     * the envelope, or null if none arrived within the client's read window.
     */
    public function receive(): ?PulsarMessage
    {
        $raw = $this->client->receive();

        if ($raw === null) {
            return null;
        }

        $properties = $raw['properties'];
        $queue = $this->queueOf($raw);
        $topic = $this->topicOf($raw);

        // §5.7 step 3: gate on bq-schema-version before the body is decoded.
        $rejectedVersion = self::unsupportedSchemaVersion($properties['bq-schema-version'] ?? null);
        if ($rejectedVersion !== null) {
            $attempts = self::parseAttempts($properties['bq-attempts'] ?? null) ?? 0;

            return new PulsarMessage(
                ['raw' => $raw['payload'], 'attempts' => max($attempts, $raw['redeliveryCount'])],
                $raw['messageId'],
                $raw['payload'],
                self::UNSUPPORTED_SCHEMA_URN_PREFIX . $rejectedVersion,
                $queue,
                $properties,
                $topic,
            );
        }

        $envelope = EnvelopeCodec::decode($raw['payload']);

        // §5.2/§5.7: attempts = the authoritative bq-attempts property, else the body's value; then
        // floored with the 0-based redelivery count (no -1).
        $attempts = self::parseAttempts($properties['bq-attempts'] ?? null);
        if ($attempts === null) {
            $body = $envelope['attempts'] ?? 0;
            $attempts = is_int($body) ? $body : 0;

            // A poison body has no decodable `attempts` (pulsar_exhaustion: its input is `bq-attempts` and the
            // redelivery count only), even when the lenient codec could read one (e.g. a wrong-typed `trace_id`).
            if ($attempts > 0 && StrictBodyClassifier::isPoison($raw['payload'])) {
                $attempts = 0;
            }
        }
        $envelope['attempts'] = max($attempts, $raw['redeliveryCount']);

        // §5.2/§5.7: route on the bq-job property, falling back to the body URN when it is absent or
        // blank. A body without a job is never routed (the property cannot stand in for it).
        $bodyUrn = EnvelopeCodec::urn($envelope);
        $rawJob = $properties['bq-job'] ?? null;
        $propertyUrn = is_string($rawJob) ? trim($rawJob, EnvelopeCodec::BLANK_CHARS) : '';
        $routedUrn = null;

        if ($bodyUrn !== '' && $propertyUrn !== '') {
            $routedUrn = $propertyUrn;

            if ($propertyUrn !== $bodyUrn) {
                $this->notifyRoutingMismatch($propertyUrn, $bodyUrn, $envelope);
            }
        }

        return new PulsarMessage($envelope, $raw['messageId'], $raw['payload'], $routedUrn, $queue, $properties, $topic);
    }

    /**
     * The queue of a frame: the configured one, else the local name of the frame's optional `topic`
     * (the part after the last `/`), else null.
     *
     * @param  array<string, mixed>  $raw
     */
    private function topicOf(array $raw): ?string
    {
        if ($this->retryTopic !== null) {
            return $this->retryTopic;
        }

        $topic = $raw['topic'] ?? null;

        return is_string($topic) && str_contains($topic, '://') ? $topic : null;
    }

    /**
     * @param  array<string, mixed>  $raw
     */
    private function queueOf(array $raw): ?string
    {
        if ($this->queue !== null && $this->queue !== '') {
            return $this->queue;
        }

        if ($this->clientQueue !== null) {
            return $this->clientQueue;
        }

        $topic = $raw['topic'] ?? null;

        return is_string($topic) ? self::localName($topic) : null;
    }

    /**
     * The `bq-schema-version` value when it is present, non-blank and not the supported version
     * (exact string match against {@see EnvelopeCodec::SCHEMA_VERSION}); null when the body may be decoded.
     * Blank means the empty string or only ASCII whitespace (space, `\t`, `\n`, `\v`, `\f`, `\r`) — the
     * same set in every SDK (GR-5); not `trim()`, whose default set contains `\0` and lacks `\f`.
     */
    private static function unsupportedSchemaVersion(mixed $value): ?string
    {
        if (! is_string($value) || preg_match('/\A[ \t\n\x0B\f\r]*\z/', $value) === 1) {
            return null;
        }

        return $value === (string) EnvelopeCodec::SCHEMA_VERSION ? null : $value;
    }

    /**
     * Parse `bq-attempts`: valid only as plain decimal digits (`^[0-9]+$` — no sign, whitespace or
     * empty string) within 0..2147483647; anything else is invalid (null → fall back to the body).
     */
    private static function parseAttempts(mixed $value): ?int
    {
        // The D modifier stops `$` from matching before a trailing newline.
        if (! is_string($value) || preg_match('/^[0-9]+$/D', $value) !== 1) {
            return null;
        }

        $digits = ltrim($value, '0');
        if ($digits === '') {
            return 0;
        }

        if (strlen($digits) > 10 || (int) $digits > self::MAX_ATTEMPTS) {
            return null;
        }

        return (int) $digits;
    }

    /**
     * @param  array<string, mixed>  $envelope
     */
    private function notifyRoutingMismatch(string $propertyUrn, string $bodyUrn, array $envelope): void
    {
        if ($this->onRoutingMismatch === null) {
            return;
        }

        try {
            ($this->onRoutingMismatch)($propertyUrn, $bodyUrn, $envelope);
        } catch (Throwable) {
            // Diagnostic only: a failing hook must never change the ack / redelivery flow.
        }
    }

    /**
     * Acknowledge a successfully-handled message (§5 "delete").
     */
    public function acknowledge(PulsarMessage $message): void
    {
        $this->client->acknowledge($message->messageId());
    }

    /**
     * Release a message back for redelivery (§5 "release" — `negativeAcknowledge`, at-least-once).
     */
    public function release(PulsarMessage $message): void
    {
        $this->client->negativeAcknowledge($message->messageId());
    }

    /**
     * Retry a message: re-publish it to the same topic with `bq-attempts` + 1 and acknowledge the original
     * (see the class docblock). The original is acknowledged only after the re-publish succeeded; if the
     * re-publish fails it is negatively acknowledged and the failure is logged, so nothing is lost. With no
     * producer (possible only on a known `Shared` / `Key_Shared` subscription, see the class docblock) it is
     * {@see self::release()}.
     */
    public function retry(PulsarMessage $message): void
    {
        if ($this->producer === null) {
            $this->release($message); // Shared / Key_Shared (or an unknown type): the nack advances the count

            return;
        }

        // The topic the message was read from, whole (descriptor / frame): never rebuilt from a guessed tenant and
        // namespace. A local name is completed only with an explicit $tenant and $namespace.
        $queue = $message->queue();
        $topic = $message->topic();
        if ($topic === null && $queue !== null && $this->tenant !== null && $this->namespace !== null) {
            $topic = sprintf('persistent://%s/%s/%s', $this->tenant, $this->namespace, $queue);
        }

        if ($topic === null) {
            if ($this->nackCannotBoundRetry()) {
                throw new BabelQueueException('Cannot retry: the full topic of this Pulsar consumer is unknown, and the copy must not go to a guessed tenant/namespace. Let the client implement ' . PulsarConsumerDescriptor::class . ' with a full topic name, put a full "topic" in the frame, or pass $tenant and $namespace with $queue.');
            }

            $this->release($message);

            return;
        }

        $next = $message->attempts() + 1;
        $properties = $message->properties();
        $properties['bq-attempts'] = (string) $next;

        try {
            $this->producer->publish($topic, $this->retryBody($message, $next), $properties);
        } catch (Throwable $e) {
            error_log('[babelqueue] Pulsar retry re-publish failed, the message is negatively acknowledged: ' . $e->getMessage());
            $this->release($message);

            return;
        }

        $this->acknowledge($message);
    }

    /**
     * The body of the re-published copy: a decodable envelope is re-encoded with its top-level `attempts` set to
     * `$next` (in sync with `bq-attempts`); a poison or version-gated body keeps its exact bytes (the count lives
     * in the property, which is authoritative).
     */
    private function retryBody(PulsarMessage $message, int $next): string
    {
        $raw = $message->rawBody() ?? '';

        if (
            str_starts_with($message->getUrn(), self::UNSUPPORTED_SCHEMA_URN_PREFIX)
            || $message->getUrn() === ''
            || StrictBodyClassifier::isPoison($raw)
        ) {
            return $raw;
        }

        try {
            $envelope = $message->envelope();
            $envelope['attempts'] = $next;

            return EnvelopeCodec::encode($envelope);
        } catch (Throwable) {
            return $raw;
        }
    }

    /**
     * Run the consume loop: receive each message and pass it to $handler; acknowledge on success,
     * retry (see {@see self::retry()}) if the handler throws. Idle (null) receives are skipped. The loop runs
     * until $shouldStop() returns true (omit it to run forever, the standard worker model).
     *
     * @param  callable(PulsarMessage): void  $handler
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
                $this->acknowledge($message);
            } catch (DeadLetterWriteFailed) {
                // The dead-letter write failed (section 5.5): leave the original unacknowledged and nack it, so it is
                // redelivered and the write is tried again; a retry copy would move it on without a DLQ record.
                $this->release($message);
            } catch (Throwable) {
                // At-least-once: retry on failure. A handler owns poison handling via
                // $message->attempts() (e.g. ack-and-drop or republish to <queue>.dlq past a cap).
                $this->retry($message);
            }
        }
    }
}
