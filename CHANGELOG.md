# Changelog

All notable changes to `babelqueue/php-sdk` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**).

## [Unreleased]

### Changed
- `EnvelopeCodec::urn()` returns `''` for a non-string `job`/`urn` instead of
  coercing it; a non-string URN is then rejected by `accepts()` /
  `EnvelopeValidator` (URNs must be strings — GR-2).

### Internal
- CI runs **PHPStan (level 9)** over `src` and enforces a **>=90% line-coverage
  gate** (`bin/check-coverage.php`). Locally: `composer analyse`,
  `composer test:coverage`, `composer coverage:check`.

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

[Unreleased]: https://github.com/BabelQueue/php-sdk/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/BabelQueue/php-sdk/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/BabelQueue/php-sdk/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/BabelQueue/php-sdk/releases/tag/v0.1.0
