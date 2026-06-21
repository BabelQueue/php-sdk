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
| Idempotency | `BabelQueue\Idempotency\Idempotent` / `IdempotencyStore` / `InMemoryStore` | Dedupe on `meta.id` (ADR-0022): `Idempotent::wrap($store, $handler)` skips an already-processed message. |
| | `BabelQueue\Idempotency\ClaimingStore` / `PdoStore` / `RedisStore` / `ClaimingDispatch` | **Persistent** stores for a fleet: an **atomic claim** (PDO unique-INSERT / Redis `SET NX PX`) serializes concurrent deliveries of one id. `PdoStore` is `ext-pdo`-only; `RedisStore` reuses the optional `predis` client. |
| Redrive | `BabelQueue\Redrive\Redrive` / `RedriveIO` / `RedriveOptions` | Safe DLQ replay (ADR-0026): reset + dry-run + sandbox + select, driven through a `RedriveIO` you bind to your broker. |
| | `BabelQueue\Redrive\ReplayBypass` / `HeaderRedriveIO` | Replay-bypass (ADR-0027): a redrive can stamp `bq-replay-bypass` so a handler skips already-fired external effects (`bypassExternalEffects`). |
| Outbox | `BabelQueue\Outbox\Outbox` / `OutboxRelay` / `OutboxStore` | Transactional outbox (ADR-0029): persist the message **atomically with the business write**, relay it later. Dependency-free — `OutboxStore` is an interface you bind to your DB. |
| GDPR | `BabelQueue\Gdpr\Gdpr` / `Cipher` / `OpenSslCipher` | Opt-in field-level encryption (ADR-0030) of the `data` fields a schema marked `x-gdpr-sensitive`: `Gdpr::protect()` before publish, `Gdpr::unprotect()` after decode. The envelope stays frozen — a sensitive value becomes a ciphertext **string**. The `Cipher` is a caller-provided seam (bind a KMS/Vault); the reference `OpenSslCipher` (AES-256-GCM) is `ext-openssl`-only and a `suggest`. |
| Tracing | `BabelQueue\Otel\Tracing` | Optional OpenTelemetry produce/consume spans (ADR-0025/0028): correlate across hops via `trace_id`, and — when the transport carries headers — link spans across hops via a W3C `traceparent`. Opt-in; `open-telemetry/api` is a `suggest`. |
| Headers | `BabelQueue\Contracts\HeaderPublisher` / `HasHeaders` | The out-of-band transport-header seam (ADR-0027/0028): publish headers **beside** the frozen envelope, and surface them on a consumed message. |
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

## Idempotency — dedupe on `meta.id` (ADR-0022)

BabelQueue is **at-least-once**, so handlers should be idempotent — dedupe on the envelope's
`meta.id`. `Idempotent::wrap($store, $handler)` makes that a one-liner: a previously-succeeded id is
skipped + acked; a throw leaves it unmarked so retry/DLQ still apply. The reference `InMemoryStore`
is single-process; for a **fleet** of consumers two **persistent** stores share one dedupe record
and add an **atomic claim** so two workers handed the same id never both run (GR-7 — nothing added to
`require`):

```php
use BabelQueue\Idempotency\ClaimingDispatch;
use BabelQueue\Idempotency\PdoStore;
use BabelQueue\Idempotency\RedisStore;

// PDO — Postgres / MySQL / SQLite. Atomic claim = a unique-key INSERT caught as a duplicate
// (every engine enforces the PRIMARY KEY atomically — no dialect-specific upsert). Ship the table:
$pdo->exec(PdoStore::ddl());                 // CREATE TABLE IF NOT EXISTS bq_idempotency (...)
$store = new PdoStore($pdo);

// Redis — atomic claim = SET key value NX PX <ttl>. Reuses your predis client.
$store = new RedisStore($predis);

// ClaimingDispatch drives claim → run → commit; a duplicate skips, a concurrent in-flight
// delivery parks (throws ClaimParkedException → redelivered, not acked), a throw releases the claim.
$dispatch->on('urn:babel:orders:created', ClaimingDispatch::wrap($store, fn ($m) => handle($m)));
```

A claim's TTL bounds a crash between claim and commit, after which a redelivery may re-run — still
at-least-once, **not** exactly-once (the dual-write window; the outbox below narrows the produce
side). The frozen `IdempotencyStore` interface is untouched; the claim contract is the opt-in
`ClaimingStore` extension.

## Safe DLQ replay + replay-bypass (ADR-0026 / ADR-0027)

`Redrive` replays dead-lettered messages back to their source (or a sandbox) — reset for
reprocessing, with dry-run and `select`, driven through a `RedriveIO` you bind to your broker.
Replaying into the *real* queue re-runs the handler, so its external effects (charge, email) would
re-fire. **Replay-bypass** closes that: a redrive can stamp the out-of-band `bq-replay-bypass`
transport header (beside the frozen envelope, never in it — GR-1), and a handler wraps its external,
non-idempotent side in `ReplayBypass::bypassExternalEffects()` to skip it on a replay while the
idempotent core still runs.

```php
use BabelQueue\Redrive\Redrive;
use BabelQueue\Redrive\RedriveOptions;
use BabelQueue\Redrive\ReplayBypass;

// Producer side — stamp the marker on a replay (needs a RedriveIO that also implements
// HeaderRedriveIO; over a plain RedriveIO bypass is a best-effort no-op and item->bypassed = false).
Redrive::run($io, 'orders.dlq', new RedriveOptions(bypass: true));

// Consumer side — the idempotent core always runs; the external effect is skipped on a replay.
$dispatch->on('urn:babel:orders:created', static function ($m): void {
    saveOrder($m);                                   // idempotent core
    ReplayBypass::bypassExternalEffects($m, static fn () => sendConfirmationEmail($m));
});
```

The marker `bq-replay-bypass` is identical across SDKs (Go's `HeaderReplayBypass`), so a Go-produced
replay is recognised by a PHP consumer. It rides the same `HeaderPublisher` / `HasHeaders` seam as
the OTel `traceparent`; `schema_version` stays **1** and `trace_id` is preserved.

## Transactional outbox (ADR-0029)

A plain producer makes a **dual write** — commit the business row *and* publish to the
broker — two systems that can disagree on a crash. The outbox removes it: the message is
written into the **same database, in the same transaction** as the business data (so they
commit or roll back atomically), and a separate **relay** publishes it afterwards. No
distributed transaction; exactly-once *handoff* into the broker (then at-least-once on the
wire, deduped on `meta.id` by the consumer-side `Idempotent::wrap`, ADR-0022).

The helper is dependency-free (GR-7): the core defines `OutboxStore` and you bind it to
your DB. **The transaction boundary is yours** — `OutboxStore::save()` runs inside the
transaction *you* opened around your business write. The envelope is stored **verbatim**
(GR-1) and relayed byte-for-byte, so `trace_id` is preserved end-to-end (GR-4).

```php
use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Outbox\Outbox;
use BabelQueue\Outbox\OutboxRelay;

// WRITE side — one transaction for the business row AND the message (your tx boundary).
$outbox = new Outbox($store);                 // $store implements OutboxStore (your DB)
$db->transaction(function () use ($db, $outbox, $order): void {
    $db->insertOrder($order);                                          // business write
    $outbox->write(EnvelopeCodec::make('urn:babel:orders:created', $order, 'orders'));
});                                            // both commit, or neither

// READ side — a worker/cron drains the durable rows onto the broker.
$relay = new OutboxRelay($transport, $store); // $transport is any BabelQueue Transport
$relay->drain();                              // publishes verbatim, marks published/failed
```

`OutboxStore` is four methods — `save()`, `fetchUnpublished()`, `markPublished()`,
`markFailed()`. A reference `InMemoryOutboxStore` ships for tests; a runnable
**InitORM-backed** adapter + the outbox-table DDL live in
[`babelqueue-examples/outbox-initorm/`](https://github.com/BabelQueue/babelqueue-examples/tree/main/outbox-initorm),
keeping this core DB-free.

## GDPR field encryption (ADR-0030)

When a schema marks a `data` field `x-gdpr-sensitive`, BabelQueue can **encrypt that field's
value end-to-end**: the producer encrypts it before publish, the consumer decrypts it after
decode. It is opt-in and standalone — a producer/consumer that never calls it behaves exactly
as before.

The **envelope stays frozen** (GR-1): only the sensitive *value* changes — it becomes a
ciphertext **string**, so `data` stays pure JSON (GR-3), `meta.schema_version` stays `1`, and
`trace_id` is untouched (GR-4). An SDK or hop without the key still carries the envelope
byte-compatibly; it just can't read the protected fields.

The crypto is a **caller-provided `Cipher`** (GR-7 — the core pulls no crypto dependency, it
stays `ext-json`). Bind it to your KMS / Vault / HSM / tokenisation service, or use the bundled
reference `OpenSslCipher` (AES-256-GCM) — which needs `ext-openssl`, an **optional** `suggest`:

```bash
# only if you use the reference cipher; otherwise bind your own Cipher onto a KMS/Vault
docker-php-ext-enable openssl   # ext-openssl is usually already present
```

```php
use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Gdpr\Gdpr;
use BabelQueue\Gdpr\OpenSslCipher;
use BabelQueue\Schema\SchemaValidated;

$cipher = new OpenSslCipher($key);            // 32 bytes ⇒ AES-256-GCM; key is yours to manage
$schema = $provider->schemaFor('urn:babel:orders:created');

// PRODUCE — validate CLEARTEXT first, then encrypt marked leaves in place, then encode.
$data = ['email' => 'alice@example.com', 'order_id' => 1042];
if ($schema !== null) {
    SchemaValidated::assert($provider, 'urn:babel:orders:created', $data); // cleartext
    Gdpr::protect($data, $schema, $cipher);   // 'email' becomes a ciphertext string, in place
}
$body = EnvelopeCodec::encode(EnvelopeCodec::make('urn:babel:orders:created', $data, 'orders'));

// CONSUME — decode, decrypt marked leaves in place, THEN read/validate cleartext.
$envelope = EnvelopeCodec::decode($body);
$data = $envelope['data'];
if ($schema !== null) {
    Gdpr::unprotect($data, $schema, $cipher); // restores 'email' byte-for-byte
}
```

Mark a field in the `data` schema with the `x-gdpr-sensitive` keyword — the boolean `true`, or a
free-form category string for documentation. It is **validation-neutral** (it never makes a value
valid or invalid), so annotating a schema is never a breaking change:

```json
{
  "type": "object",
  "properties": {
    "email":   {"type": "string", "x-gdpr-sensitive": "email"},
    "profile": {"type": "object", "properties": {
        "full_name": {"type": "string", "x-gdpr-sensitive": true}
    }},
    "addresses": {"type": "array", "items": {"type": "object", "properties": {
        "line": {"type": "string", "x-gdpr-sensitive": true}
    }}}
  }
}
```

`Gdpr::protect()` / `unprotect()` walk every marked leaf — nested objects (`profile.full_name`),
array items (`addresses[].line`), scalar array items, and a root mark — and rewrite each **in
place**. Each value is canonically JSON-encoded then replaced by the ciphertext string (and the
exact inverse on the way back), so the round-trip restores the original value **byte-for-byte**.
Behaviour at the edges:

- **Validate cleartext** — run schema validation *before* `protect()` and *after* `unprotect()`; a
  constrained sensitive field (`minLength`, `enum`, …) would reject the ciphertext string otherwise.
- **Absent field** — a marked path missing from a message is skipped (not an error); schemas evolve.
- **Idempotent decrypt** — a non-string leaf is left untouched, so `unprotect()` is safe to re-run
  on already-cleartext data.
- **Wrong key / tampered** — `unprotect()` throws the typed `BabelQueue\Gdpr\DecryptException`, so
  the consumer fails the message (retry / dead-letter) rather than process unreadable PII.

The sensitive marks are read from the same per-URN `data` schema the validation path already loads
(`BabelQueue\Schema\SensitivePaths` exposes them, mirroring babelqueue-registry's inventory). The
registry **declares and audits** sensitivity; this is the SDK that **enforces** it on the wire.

## OpenTelemetry tracing (ADR-0025 / ADR-0028)

`BabelQueue\Otel\Tracing` adds **optional** OpenTelemetry spans to a producer or consumer. It is
opt-in and dependency-light — `open-telemetry/api` is only a `suggest`, so the core stays
`ext-json` (GR-7). Cross-hop trace propagation layers two levels:

- **`trace_id` ↔ TraceId** (v0.1): the envelope's `trace_id` maps 1:1 to an OTel trace id, so every
  hop that shares a `trace_id` shares one trace — correlation + per-hop timing with **zero**
  wire/transport change. Preserved end-to-end (GR-4).
- **W3C `traceparent`** (v0.2): the producer also injects the active span context as a `traceparent`
  **transport header** — beside the frozen envelope, never in it (GR-1) — so the consumer starts its
  span as a true **child** of the producer span (real cross-hop parent-child linkage). With no
  `traceparent` present it falls back to the v0.1 behaviour — a strict, backward-compatible upgrade.

```bash
composer require open-telemetry/api          # + open-telemetry/sdk to export
```

```php
use BabelQueue\Otel\Tracing;

// Produce — a PRODUCER span "publish <urn>"; the active span's traceparent rides the transport
// header when the transport carries headers (AMQP/SQS/Redis below), else it degrades to a plain
// publish with the trace_id still stamped (no error).
Tracing::publish($tracer, $transport, 'urn:babel:orders:created', ['order_id' => 1042], 'orders');

// Consume — a CONSUMER span "process <urn>", started as a child of the producer span when the
// delivered message surfaces a traceparent (HasHeaders); else from the trace_id (v0.1).
$dispatch->on('urn:babel:orders:created', Tracing::wrap($tracer, fn ($m) => handle($m)));
```

The `traceparent` rides the out-of-band `HeaderPublisher` / `HasHeaders` seam (the same seam as the
replay-bypass marker, ADR-0027). Which reference transports carry it:

| Transport | Carries `traceparent` | Carrier |
| :--- | :--- | :--- |
| `RedisTransport` | **Yes** | A transport-owned `__bq_frame` JSON frame wrapping the bare envelope (Redis has no native per-message metadata channel). A header-less publish stays byte-for-byte bare; `unframe()` is back-compatible with bare/cross-version values. |
| `AmqpTransport` | **Yes** | AMQP message headers, beside the contract `x-*` headers (contract wins a collision). |
| `SqsTransport` | **Yes** | SQS `MessageAttributes` (String), beside the contract `bq-*` attributes (contract wins; bounded by the 10-attribute SQS cap). |
| `KafkaTransport` / `PulsarTransport` / `StompTransport` | Deferred | These producers already project rich `bq-*`/`bq_*` headers; wiring `traceparent` through their retry-topic / annotation machinery is a documented follow-up. Until then they degrade to v0.1 `trace_id` correlation (no error). `KafkaMessage` already surfaces its record headers via `HasHeaders`, the consume-side hook a future Kafka producer would reach. |

A plain `publish()` (no tracer) is unchanged on every transport.

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
