# Changelog

All notable changes to `babelqueue/php-sdk` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**).

## [1.9.0] - 2026-06-14

### Added
- **Apache Kafka retry-topic machinery (§6.4/§6.5)** — `BabelQueue\Transport\KafkaRetryRouter` and
  `BabelQueue\Transport\KafkaRetryConsumer`, the SDK-owned **retry / DLQ** pattern Kafka can't give
  natively (no delayed delivery, no retry queue, no dead-letter queue), completing reliable Kafka
  consume for PHP. On a work-topic handler **failure**, `KafkaRetryRouter::route($message, $e)`
  republishes the envelope to the next **tiered delay topic** `<workTopic>.retry.<delayMs>` (default
  tiers **5s / 30s / 5m / 30m**) with `bq-attempts` incremented (and the body's top-level `attempts`
  kept in sync), stamping the `bq-delay` (tier ms) and `bq-original-topic` (work topic) re-injection
  headers; once attempts are exhausted (`next >= maxAttempts`) it dead-letters to `<workTopic>.dlq`
  with the additive `dead_letter` block (`DeadLetter::annotate`, ADR-0009) instead. The caller then
  returns so the original record commits (process-then-commit, §6). A separate process runs
  `KafkaRetryConsumer::consume()` over the retry topics: it reads each `<workTopic>.retry.<delayMs>`
  record, **waits the tier delay cooperatively** (there is no broker timer — it heartbeats the
  consumer via a keep-alive `poll(0)` so a wait longer than `max.poll.interval.ms` does not evict it
  from its group, rather than one blocking sleep), then **re-injects** the record into its
  `bq-original-topic` and commits. Attempts are **not** re-incremented on re-injection (the
  work-topic handler counts the attempt). `bq-trace-id` is preserved byte-for-byte across every
  retry hop (GR-4). The re-injection consumer takes a new one-method
  `BabelQueue\Transport\KafkaRetryConsumerClient` seam (`receive()` / `poll()` keep-alive /
  `commit()`) — a separate seam from `KafkaConsumerClient` so its existing adapter is untouched — and
  the wait is driven by injectable `$now`/`$sleep` seams, so both classes stay dependency-free behind
  the seams and unit-test against fakes with no real time. `KafkaMessage` now additively surfaces the
  raw `bq-` record headers (`header()` / `headers()`) so the router can route a retry record back to
  its work topic across hops. This makes the `Consume\Dispatcher` `maxAttempts` dead-letter cap
  **effective on Kafka** (since `bq-attempts` now grows across retry hops, not just on Pulsar's native
  redelivery count). Proven live with a **retry → re-inject → success** cycle and a
  **poison → exhaust → DLQ** path over a real Redpanda via `ext-rdkafka`. The envelope is unchanged
  (`schema_version: 1`); purely additive. Ships as a MINOR.

## [1.8.0] - 2026-06-14

### Added
- **Framework-less consume runtime** — `BabelQueue\Consume\Dispatcher` and
  `BabelQueue\Consume\DeadLetterPublisher`, the missing glue that makes the core consumers
  (`PulsarConsumer`, `KafkaConsumer`) usefully runnable without a framework. The `Dispatcher` is a
  **URN → handler registry** that is itself **callable**, so it plugs straight into a consumer's
  loop: `$consumer->consume($dispatcher, $shouldStop)`. It routes each decoded message to its
  handler, applies the four `on_unknown_urn` strategies (`fail`/`delete`/`release`/`dead_letter`,
  via the existing `Routing\UnknownUrnStrategy`), and — with `maxAttempts` + a publisher — caps a
  poison message by **dead-lettering** it instead of redelivering forever, all through the consume
  loop's ack-on-return / redeliver-on-throw contract. `DeadLetterPublisher` enriches the envelope
  with the additive `dead_letter` block (`DeadLetter::annotate`, ADR-0009) and publishes it to
  `<queue>.dlq` via any `Transport`. A new `Contracts\ConsumedMessage` (the read-only view +
  `attempts()` + `envelope()`), now implemented by `PulsarMessage`/`KafkaMessage`, is the runtime's
  input. This brings PHP framework-less consume to parity with the Python/Node runtimes' dispatch +
  unknown-URN + DLQ. (The Kafka **retry-topic** delay/backoff tier — §6.4/§6.5 — is a follow-up; the
  `maxAttempts` cap is effective on Pulsar today and on Kafka once retry topics are configured.) The
  envelope is unchanged (`schema_version: 1`); purely additive. Ships as a MINOR.

## [1.7.0] - 2026-06-14

### Added
- **Offline JSON Schema validator** — `BabelQueue\Validation\SchemaValidator`, a dependency-free,
  producer-side structural validator that checks a decoded envelope against the **bundled** canonical
  JSON Schema (`src/Validation/message-envelope.schema.json`), so an app can validate an envelope at
  runtime with **no network access and no JSON-Schema library**. It is the strict counterpart to the
  lenient consumer-side `EnvelopeValidator`: it requires `job` (no `urn` alias), the full `meta`
  block, UUID-shaped ids, the `lang` enum and the `schema_version` const, plus the optional
  `dead_letter` block, returning `"<path>: <reason>"` violations. The bundled schema is byte-identical
  to the canonical contract/conformance schema (a drift-guard test enforces it). The envelope is
  unchanged (`schema_version: 1`); purely additive. Ships as a MINOR.

### Internal
- **CI:** `release.yml` now runs the **≥90% coverage gate** (mirroring `ci.yml`), so a sub-threshold
  tag can no longer publish.

## [1.6.0] - 2026-06-14

### Added
- **Apache Kafka consumer** — `BabelQueue\Transport\KafkaConsumer`, the framework-less **consume**
  half for §6 over `ext-rdkafka` (the counterpart to `KafkaTransport`;
  [§6](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-kafka), ADR-0019). Consume is
  **process-then-commit** (at-least-once): the `consume(callable $handler, ?callable $shouldStop)`
  loop polls a record, runs the handler, and **commits the offset only on success** — a thrown
  handler leaves the record uncommitted (re-delivered on the next restart/rebalance). Per §6 the
  **`attempts` counter is the `bq-attempts` record header (authoritative — Kafka has no native
  delivery count), falling back to the body's `attempts`** when the header is absent; note this is
  *not* a `max` (the header overrides even when lower), unlike §5 Pulsar. The reconciled value is
  exposed via `KafkaMessage::attempts()` so a handler can implement its own retry-topic / DLQ
  policy. Also exposes the `receive()` / `commit()` primitives. `KafkaMessage` is the read-only
  `InboundMessage` view. Decoupled from `ext-rdkafka` behind a one-method
  `BabelQueue\Transport\KafkaConsumerClient` seam — dependency-free and unit-testable with a fake
  (the **GR-7 relaxation is the C extension, opt-in, ADR-0019**, same as the producer). URN dispatch
  / `on_unknown_urn` / the §6 retry-topic machinery stay the caller's concern, keeping the core
  minimal. Proven live with a **Java → PHP(ext-rdkafka)** round-trip over a real Redpanda. The
  envelope is unchanged (`schema_version: 1`). Ships as a MINOR.

## [1.5.0] - 2026-06-14

### Added
- **Apache Pulsar consumer** — `BabelQueue\Transport\PulsarConsumer`, the framework-less **consume**
  half for §5 over Pulsar's native **WebSocket consumer API** (the counterpart to `PulsarTransport`;
  [§5.5](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-pulsar), ADR-0020). It is the
  first consume surface on the otherwise publish-only core. It exposes the §5.5 primitives —
  `receive()` (decode + reconcile attempts), `acknowledge()` (delete) and `release()`
  (`negativeAcknowledge` → redelivery, at-least-once) — plus a `consume(callable $handler, ?callable
  $shouldStop)` loop that acks on success and releases when the handler throws. Per §5.5 it
  **reconciles `attempts = max(body.attempts, redeliveryCount)`** (Pulsar's redelivery count is
  0-based, so no `-1`), exposed via `PulsarMessage::attempts()` so a handler owns its retry /
  dead-letter policy on a poison message. `PulsarMessage` is the read-only `InboundMessage` view
  (URN/trace/data/meta) plus the ack handle. Decoupled from the WebSocket library behind a
  one-method `BabelQueue\Transport\PulsarWebSocketConsumerClient` seam — dependency-free and
  unit-testable with a fake; **GR-7 stays intact** (pure-PHP WebSocket, no C extension, like the
  producer). URN dispatch / `on_unknown_urn` / DLQ stay the caller's concern (a framework adapter or
  runner), keeping the core minimal. Proven live with a **Java(native) → PHP(WebSocket)** round-trip
  over a real Pulsar standalone. `textalk/websocket` is a Composer **suggest**. The envelope is
  unchanged (`schema_version: 1`). Ships as a MINOR.

## [1.4.0] - 2026-06-14

### Added
- **Apache Pulsar transport** — `BabelQueue\Transport\PulsarTransport`, a framework-less Pulsar
  producer over Pulsar's native **WebSocket API** ([§5 of the broker-bindings
  contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-pulsar), ADR-0020). The
  message **value** is the canonical envelope; the contract fields are mirrored onto `bq-` native
  **message properties** (string→string, integers stringified —
  `bq-job`/`bq-trace-id`/`bq-message-id`/`bq-schema-version`/`bq-source-lang`/`bq-attempts`; note
  **hyphens**, as on Kafka/SQS, unlike the §7 Artemis `bq_` underscores). The BabelQueue queue maps
  to `persistent://<tenant>/<namespace>/<queue>`; the native `publishTimestamp` is broker-set and
  informational, so the body's `meta.created_at` stays authoritative (the adapter must not set the
  frame's `eventTime`). **Unlike the §6 Kafka path (ADR-0019), GR-7 stays intact**: Pulsar's
  WebSocket service is driven by a **pure-PHP** client (e.g. `textalk/websocket`) — no C extension —
  so this is opt-in like the §7 Artemis STOMP path. Decoupled from the WebSocket library behind a
  one-method `BabelQueue\Transport\PulsarWebSocketClient` seam — dependency-free and unit-testable
  with a fake (the adapter derives the `ws://…/ws/v2/producer/…` endpoint, base64-encodes the
  payload, attaches the properties, and checks the producer ack). `textalk/websocket` is a Composer
  **suggest**. Proven live with a PHP(WebSocket)→native-client round-trip over a real Pulsar
  standalone (the §5 `bq-` properties and the envelope body read back byte-identical). A producer
  transport (PHP consumes Pulsar via a framework worker — a consumer is a follow-up). The envelope
  is unchanged (`schema_version: 1`). Ships as a MINOR.

## [1.3.0] - 2026-06-14

### Added
- **Apache Kafka transport** — `BabelQueue\Transport\KafkaTransport`, a framework-less Kafka
  producer ([§6 of the broker-bindings
  contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-kafka), ADR-0019). The
  record **value** is the canonical envelope, the record **timestamp** mirrors `meta.created_at`
  (Unix ms), and the contract fields are mirrored onto `bq-` **record headers** (UTF-8 strings —
  `bq-job`/`bq-trace-id`/`bq-message-id`/`bq-schema-version`/`bq-source-lang`/`bq-attempts`; note
  **hyphens**, unlike the §7 Artemis `bq_` underscores). Decoupled from `ext-rdkafka` behind a
  one-method `BabelQueue\Transport\KafkaProducer` seam — dependency-free and unit-testable with a
  fake (wrap a real `RdKafka\Producer` in a one-line adapter calling `producev()`). **`ext-rdkafka`
  is a Composer `suggest`, never a `require`: it is a C extension over `librdkafka`, so this is an
  opt-in transport that deliberately relaxes the zero-heavy-dependencies rule for Kafka only
  (ADR-0019) — every Kafka-free app keeps the pure-userland install.** A producer transport (PHP
  consumes Kafka via a framework worker — a consumer is a follow-up). The envelope is unchanged
  (`schema_version: 1`). Ships as a MINOR.

## [1.2.0] - 2026-06-14

### Added
- **Apache ActiveMQ Artemis transport (STOMP)** — `BabelQueue\Transport\StompTransport`, a
  framework-less Artemis producer over **STOMP** ([§7 of the broker-bindings
  contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-activemq-artemis),
  ADR-0018). PHP has no mature native AMQP-1.0 client, so it reaches Artemis over STOMP (the
  pure-PHP `stomp-php`); Artemis bridges STOMP↔AMQP-1.0↔JMS on the same address, so a message it
  produces is consumed natively by the Java/.NET/Node/Python/Go Artemis SDKs. The envelope is the
  frame body; the §7 fields map onto STOMP headers (`correlation-id` = `trace_id`, `content-type`,
  and the string `bq_schema_version`/`bq_source_lang`/`bq_attempts`/`bq_app_id`). Routing is
  **body-authoritative** (a STOMP header cannot set the `x-opt-jms-type` annotation, so consumers
  route on the body's `job` URN). Decoupled from `stomp-php` behind a one-method
  `BabelQueue\Transport\StompClient` seam — dependency-free and unit-testable with a fake (wrap a
  real `Stomp\StatefulStomp` in a one-line adapter). `stomp-php/stomp-php` is a Composer
  **suggest**. Proven live with a PHP(STOMP)→Python(AMQP 1.0) round-trip over a real Artemis. The
  envelope is unchanged (`schema_version: 1`); a producer transport (PHP consumes Artemis via a
  framework worker — a Laravel STOMP driver is a follow-up). Ships as a MINOR.

## [1.1.0] - 2026-06-12

### Added
- **Amazon SQS transport** — `BabelQueue\Transport\SqsTransport`, a framework-less SQS
  producer implementing [§3 of the broker-bindings contract](https://babelqueue.com): it
  sends the canonical envelope as the `MessageBody` and projects the envelope onto native
  SQS `MessageAttributes` (`bq-job`/`bq-trace-id`/`bq-message-id`/`bq-schema-version`/
  `bq-source-lang`/`bq-created-at`; FIFO `MessageGroupId`/`MessageDeduplicationId`). It is
  decoupled from `aws/aws-sdk-php` behind a one-method `BabelQueue\Transport\SqsClient`
  seam, so it is dependency-free and unit-testable with a fake (wrap a real
  `Aws\Sqs\SqsClient` in a one-line adapter — see the class docs). The envelope is
  unchanged (`schema_version: 1`); SQS is purely additive.

## [1.0.0] - 2026-06-07

**1.0.0 — the public API is now SemVer-stable**: breaking changes require a MAJOR,
following the deprecation policy (deprecate → remove across a MINOR window). The
wire envelope is unchanged and remains `schema_version: 1`. Full reference + the
contract live at [babelqueue.com](https://babelqueue.com).

### Added
- `EnvelopeCodec::make(string $urn, array $data, string $queue = 'default', ?string $traceId = null)`
  — the data-first envelope builder shared by every other SDK (`make`/`Make`).
  `fromJob()` now delegates to it (byte-identical output). Closes the cross-SDK
  producer-API parity gap (see [API review](https://babelqueue.com)).

### Changed
- `EnvelopeCodec::urn()` returns `''` for a non-string `job`/`urn` instead of
  coercing it; a non-string URN is then rejected by `accepts()` /
  `EnvelopeValidator` (URNs must be strings — GR-2).

### Internal
- CI runs **PHPStan (level 9)** over `src` and enforces a **>=90% line-coverage
  gate** (`bin/check-coverage.php`). Locally: `composer analyse`,
  `composer test:coverage`, `composer coverage:check`.
- **GR-8 latency benchmark** (`OverheadBenchmarkTest`) — asserts the envelope
  encode/decode path adds **≤2%** over plain-JSON serialization vs a conservative
  750µs broker round-trip.

## [0.3.0] - 2026-06-06

### Added
- `Validation\EnvelopeValidator` — consumer-side validation *with a reason*
  (`check()`/`isValid()`/`validate()`). `isUnsupportedSchemaVersion()` and the
  `REASON_UNSUPPORTED_SCHEMA_VERSION` reason let a consumer **quarantine** a
  message from a newer producer instead of silently dropping it.
  `Exceptions\InvalidEnvelopeException` carries the reason and the offending
  envelope.
- Framework-less reference transports implementing `Contracts\Transport` for use
  without Laravel/Symfony: `Transport\RedisTransport` (`RPUSH` onto the shared
  list, interoperable with every SDK's reliable-queue consumer) and
  `Transport\AmqpTransport` (durable queue, persistent message, contract AMQP
  properties: `type`=URN, `correlation_id`=`trace_id`, `message_id`=`meta.id`,
  `x-schema-version`/`x-source-lang`/`x-attempts`). Their broker clients
  (`predis/predis`, `php-amqplib/php-amqplib`) are **optional** — declared under
  `suggest`, not `require`, so the core stays dependency-free.

## [0.2.0] - 2026-06-06

### Added
- `EnvelopeCodec::urn()` — resolve the message URN (`job`, accepting `urn` as an
  inbound alias).
- `EnvelopeCodec::accepts()` — consumer-side envelope validation (rejects an empty
  URN, an unsupported `meta.schema_version`, a blank `trace_id`, non-object `data`
  or non-integer `attempts`).
- Shared **cross-SDK conformance suite** under `tests/conformance/` (vendored from
  the canonical `conformance/` set) plus a `ConformanceTest` runner.

## [0.1.0] - 2026-06-06

### Added
- `Codec\EnvelopeCodec` — builds, encodes and decodes the canonical
  `{job, trace_id, data, meta, attempts}` envelope (`SCHEMA_VERSION`, `SOURCE_LANG`).
  The single PHP implementation of the wire format.
- Contracts: `PolyglotJob`, `HasBabelUrn`, `HasTraceId`, `InboundMessage`, `Transport`.
- `DeadLetter\DeadLetter::annotate()` — additive `dead_letter` block builder.
- `Routing\UnknownUrnStrategy` — `fail` / `delete` / `release` / `dead_letter` constants.
- `Support\Uuid` — dependency-free UUIDv4.
- `Exceptions\BabelQueueException` and `UnknownUrnException`.
- Golden conformance fixtures under `tests/fixtures/`.

### Notes
- Pre-1.0: the public API may change before the `1.0.0` tag.
- Framework-agnostic core. Requires PHP `^8.2` and `ext-json` only — no heavy deps.
- Framework adapters (`babelqueue/laravel`, `babelqueue/symfony`) build on this.

[Unreleased]: https://github.com/BabelQueue/php-sdk/compare/v1.9.0...HEAD
[1.9.0]: https://github.com/BabelQueue/php-sdk/compare/v1.8.0...v1.9.0
[1.8.0]: https://github.com/BabelQueue/php-sdk/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/BabelQueue/php-sdk/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/BabelQueue/php-sdk/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/BabelQueue/php-sdk/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/BabelQueue/php-sdk/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/BabelQueue/php-sdk/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/BabelQueue/php-sdk/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/BabelQueue/php-sdk/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/BabelQueue/php-sdk/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/BabelQueue/php-sdk/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/BabelQueue/php-sdk/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/BabelQueue/php-sdk/releases/tag/v0.1.0
