# Bundled envelope JSON Schema

`message-envelope.schema.json` in this directory is a **verbatim copy** of the
canonical BabelQueue wire-envelope schema (`schema_version` 1). It is bundled so
an app can validate a decoded envelope **offline**, at runtime, via
[`SchemaValidator`](SchemaValidator.php) — no network access and no JSON-Schema
library required (GR-7: zero heavy dependencies).

## Provenance & drift policy

The schema is copied along this chain, each link byte-identical to the last:

```
.ssot/contracts/message-envelope.schema.json   ← canonical source of truth
  → conformance/schema/message-envelope.schema.json   (cross-SDK suite)
    → tests/conformance/schema/message-envelope.schema.json   (vendored; CI diffs this)
      → src/Validation/message-envelope.schema.json   (this file; ships in the package)
```

CI's `conformance` job diffs the vendored `tests/conformance/schema/` copy
against the canonical suite, so that copy can never silently drift. Keep **this**
`src/` copy byte-identical to the vendored one
(`tests/conformance/schema/message-envelope.schema.json`) — a test asserts they
match. If the contract changes (which requires an ADR + a `schema_version`
decision), update both copies in the same change. **The contract is
authoritative**: if they ever disagree, this copy is wrong, not the contract.
