# Model configuration

**`config/provider/` is the source of truth for the served model set.** One JSON
file per row. `build.rs` scans this directory **recursively** — every `*.json` at
any depth, regardless of its file name — validates the whole set, sorts the rows
by `id`, and compiles them into the `CLIENTS` slice (`$OUT_DIR/model_rows.rs`,
included by `src/clients.rs`). A bad file is a compile error. The table is baked
into the binary, so a deploy still ships one executable plus the vault artifact —
there is no config directory to scan at process start.

No credentials and no endpoint values live here. `vault.*` names REFERENCE slots
in `config/vault/generated/agentic-provider.json`; `src/vault.rs` resolves the
actual values at runtime.

## Layout

One folder per provider, named after the `id` prefix:

```
config/provider/
  local/          local-strata.json, local-basalt.json
  makora/         makora-glm5.json, makora-ds4-flash.json, makora-glm5-flash.json
  merge/          merge-glm5.json, merge-glm5-flash.json, …
  nvidia/         nvidia-glm5.json, nvidia-kimi3.json, …
  open-router/    open-router-glm5-flash.json
  sail/           sail-glm5.json, sail-kimi3.json, …
  token-router/   token-router-glm5.json, …
  vultr/          vultr-glm5.json, …
```

The folder name is a convention, not data: the scanner recurses through the
whole tree, and `id` is the identity. Files may be nested deeper than one level.

## File

`config/provider/<provider>/<provider>-<model>.json` — the file name is for
humans, and it may live in a subdirectory of `config/provider/`. Neither the name
nor the nesting is read or sorted; identity comes from `id`, and the table order
is the alphabetical order of `id`.

```json
{
    "id": "vultr/glm-5.3",
    "provider": "Vultr",
    "model": "glm-5.3",
    "vault": {
        "vendor": "vultr",
        "credential_slot": "vultr_api_keys",
        "endpoint_slot": "vultr_api_endpoints"
    },
    "behavior": "chat-plain",
    "pricing": {
        "input": 0.75,
        "cache_read": 0.75,
        "output": 3.0
    },
    "enabled": true,
    "reasoning_effort": "max",
    "context_length": 262144
}
```

## Fields

| Field | Required | Meaning |
| --- | --- | --- |
| `id` | yes | Routing/catalog id, `<provider>/<model>`. Unique. Row order is the alphabetical order of this field. |
| `provider` | yes | Display name printed by `/stats` and the request log. |
| `model` | yes | Upstream wire model, clamped into every request. May differ from `id`'s model. |
| `vault.vendor` | yes | Vault vendor id (`vultr`, `token_router`, …). |
| `vault.credential_slot` | yes, except keyless | Vault credential slot, ending in `_api_keys`. `null` for a keyless relay. |
| `vault.endpoint_slot` | yes | Vault endpoint slot, ending in `_api_endpoints`. |
| `behavior` | yes | `chat-plain`, `chat-routed`, `chat-credential-drain`, `chat-keyless-relay`. |
| `routing_vendor` | routed only | `telnyx`, `pareto`, `modal`, `deepinfra`, `openai` — injected as the body's `vendor`. |
| `credential_advance` | drain only | `same-then-next` (Makora) or `next-on-every-rotation` (Nvidia). |
| `pricing` | no | USD per ONE MILLION tokens. Omit for a row with no published rates (renders `$0.0000`). |
| `enabled` | no (default `true`) | `false` keeps the row routable by id but removes it from `/models`. |
| `reasoning_effort` | no | Default injected when the caller sends none. |
| `coerce_none_effort` | no (default `false`) | Treat an explicit `'none'` as "not provided" and inject `reasoning_effort` (the three GPT-family rows). |
| `output_cap` | no | Rewrite the request's `max_tokens` to this value (local relays: 32768). |
| `usage_option` | no (default `true`) | `false` stops injecting `stream_options.include_usage`. |
| `context_length` | no (default `262144`) | The number `/models` advertises. |

## Adding a row

1. Add `config/provider/<provider>/<provider>-<model>.json` (one folder per
   provider; any deeper nesting is also scanned) with a new, unused `id`.
2. Run `cargo check` (or any cargo verb): `build.rs` discovers the file, validates
   it, sorts by `id`, and regenerates the table.

No renumbering and no manifest edit. The registry still validates the row against
the live vault artifact at startup and refuses unknown vendors/slots with a
problem list.
