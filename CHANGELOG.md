# Changelog

All notable changes to `babelqueue/php-sdk` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**).

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

[Unreleased]: https://github.com/BabelQueue/php-sdk/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/BabelQueue/php-sdk/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/BabelQueue/php-sdk/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/BabelQueue/php-sdk/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/BabelQueue/php-sdk/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/BabelQueue/php-sdk/releases/tag/v0.1.0
