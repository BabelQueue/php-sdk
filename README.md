# BabelQueue PHP SDK (core)

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> The **framework-agnostic core** of BabelQueue for PHP — the canonical polyglot
> queue envelope codec, contracts and dead-letter helpers. Framework adapters
> (`babelqueue/laravel`, `babelqueue/symfony`, …) are built on top of this.

You usually don't install this directly — you install an adapter:

```bash
composer require babelqueue/laravel    # Laravel
composer require babelqueue/symfony    # Symfony (Messenger)
```

…and the adapter pulls this core in. Install it directly only for a
framework-less PHP app, or to build a new adapter.

```bash
composer require babelqueue/php-sdk
```

## What's in here

| Area | Class | Role |
| :--- | :--- | :--- |
| Codec | `BabelQueue\Codec\EnvelopeCodec` | Build / encode / decode the canonical `{job, trace_id, data, meta, attempts}` envelope (`schema_version` 1). **The single PHP implementation of the wire format** — every adapter reuses it, so Laravel and Symfony can't drift. |
| Contracts | `BabelQueue\Contracts\PolyglotJob` | Producible message: `getBabelUrn()` + `toPayload()`. |
| | `BabelQueue\Contracts\HasBabelUrn` / `HasTraceId` | URN identity / optional trace-id propagation. |
| | `BabelQueue\Contracts\InboundMessage` | Read-only decoded view of a consumed envelope. |
| | `BabelQueue\Contracts\Transport` | Minimal publish seam (framework-less / adapter use). |
| Dead-letter | `BabelQueue\DeadLetter\DeadLetter` | Annotate an envelope with the additive `dead_letter` block (ADR-0009). |
| Routing | `BabelQueue\Routing\UnknownUrnStrategy` | `fail` / `delete` / `release` / `dead_letter` constants. |
| Support | `BabelQueue\Support\Uuid` | Dependency-free UUIDv4 (no ramsey/symfony-uid needed). |
| Errors | `BabelQueue\Exceptions\BabelQueueException` / `UnknownUrnException` | Two-level exception hierarchy. |

The contract this core implements is defined in the project SSOT:
[`.ssot/contracts/`](../.ssot/contracts/). The golden conformance fixtures live in
[`tests/fixtures/`](tests/fixtures/) — every PHP package must round-trip them.

## Design

This core is the **contract runtime**, not a worker. It does not own a broker
loop or retry — adapters bind to each framework's native queue (Laravel's
drop-in driver, Symfony Messenger) and reuse that framework's worker/retry. See
[ADR-0010](../.ssot/architecture/adr/0010-framework-agnostic-php-core.md).

## Testing

```bash
composer install
vendor/bin/phpunit
```

## License

MIT © Muhammet Şafak.
