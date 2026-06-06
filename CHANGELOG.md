# Changelog

All notable changes to `babelqueue/php-sdk` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**).

## [Unreleased]

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

[Unreleased]: https://github.com/BabelQueue/php-sdk/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/BabelQueue/php-sdk/releases/tag/v0.1.0
