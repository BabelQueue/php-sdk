# Golden conformance fixtures

These canonical envelopes are the **cross-SDK conformance set**. Every BabelQueue
SDK — in any language — must be able to **decode** them and (for producer fields)
**reproduce** the same shape. They are the executable form of
[`contracts/message-envelope.md`](https://babelqueue.com) (`schema_version` 1).

| Fixture | What it represents |
| :--- | :--- |
| `order-created.json` | A normal produced envelope: `{ job, trace_id, data, meta, attempts }`. |
| `dead-lettered.json` | The same envelope after dead-lettering: original preserved verbatim + an additive top-level `dead_letter` block (ADR-0009). |

Notes for SDK authors:
- Per-message fields (`meta.id`, `trace_id`, `meta.created_at`) are intrinsically
  unique; assert their **presence/shape**, not their literal values.
- Consumers must also accept `urn` as an inbound alias for `job`, and must reject
  an unknown `meta.schema_version`.
- `dead_letter` appears **only** on a dead-letter queue; normal consumers ignore it.
