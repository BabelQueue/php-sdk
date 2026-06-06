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
| Validation | `BabelQueue\Validation\EnvelopeValidator` | Consumer-side validation **with a reason** — quarantine an unsupported `meta.schema_version` instead of dropping it. |
| Transports | `BabelQueue\Transport\RedisTransport` / `AmqpTransport` | Optional framework-less reference `Transport` impls (Redis `RPUSH`; RabbitMQ durable + contract AMQP properties). |
| Dead-letter | `BabelQueue\DeadLetter\DeadLetter` | Annotate an envelope with the additive `dead_letter` block (ADR-0009). |
| Routing | `BabelQueue\Routing\UnknownUrnStrategy` | `fail` / `delete` / `release` / `dead_letter` constants. |
| Support | `BabelQueue\Support\Uuid` | Dependency-free UUIDv4 (no ramsey/symfony-uid needed). |
| Errors | `BabelQueue\Exceptions\BabelQueueException` / `UnknownUrnException` / `InvalidEnvelopeException` | Exception hierarchy; `InvalidEnvelopeException` carries the rejection reason + envelope. |

The contract this core implements — the canonical envelope, URN scheme, broker
bindings and versioning policy — is documented at
[babelqueue.com](https://babelqueue.com). The golden conformance fixtures live in
[`tests/fixtures/`](tests/fixtures/) — every PHP package must round-trip them.

## Framework-less use

Produce the canonical envelope from a plain PHP app and let any other SDK consume
it. The reference transports keep the core dependency-free — install only the
broker client you use:

```bash
composer require predis/predis              # for RedisTransport
composer require php-amqplib/php-amqplib    # for AmqpTransport
```

```php
use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Transport\RedisTransport;
use BabelQueue\Validation\EnvelopeValidator;

// Produce — a Go/Python/Node consumer reads the identical envelope off "orders".
$transport = new RedisTransport(new Predis\Client('redis://localhost:6379'));
$transport->publish(EnvelopeCodec::encode(EnvelopeCodec::fromJob($job, 'orders')), 'orders');

// Consume (your own loop) — validate before dispatch, quarantine the unknown.
$envelope = EnvelopeCodec::decode($rawBody);
if ($reason = EnvelopeValidator::check($envelope)) {
    // $reason === 'unsupported_schema_version' → dead-letter, don't drop.
    return;
}
```

phpredis (`ext-redis`) users can implement the one-method `Transport` directly —
it is just an `rpush`.

## Design

This core is the **contract runtime**, not a worker. It does not own a broker
loop or retry — adapters bind to each framework's native queue (Laravel's
drop-in driver, Symfony Messenger) and reuse that framework's worker/retry.

## Testing

```bash
composer install
vendor/bin/phpunit
```

## License

MIT © Muhammet Şafak.
