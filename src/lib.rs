//! `typecrate_provider` - the Rust port of the `@agentic/provider` package core.
//!
//! Crate role (mirror of `packages/agentic/provider/src`): the private model-provider
//! substrate - the credentials and endpoints a model client needs, the shared
//! OpenAI-compatible client base, the per-vendor clients, the registry the routes
//! dispatch through, and the four service routes in front of them. Every module in here
//! has one TypeScript origin, and that origin is the specification:
//!
//! | Rust module | TypeScript origin                                                  |
//! |-------------|--------------------------------------------------------------------|
//! | `vault`     | the artifact contract in `config/vault/src/vault/**` (`types.ts` for the shapes, `sentinels.ts:vaultEntryValidity` for the classifier this crate re-implements, `slots.ts:vaultSlotSecret`/`vaultSlotOptional` for the kind-derived flags, `snapshot.ts` for the emitted key order), plus `config/vault/scripts/export-vault.ts` (the writer of `config/vault/generated/<scope>.json`) and the reading half today: `packages/agentic/provider/src/model-class.ts:39-44` (`INVALID_API_KEY_VALUES`) + `:52-60` (`isValidApiKey`, the validity gate) and the `@config/vault` imports the vault grant list pins in `config/vault/src/vault/grants.test.ts` |
//! | `transport` | the outbound half of `packages/agentic/provider/src/model-class.ts`: the request transport and retry/rotation gate at `:557-654` — the `fetch` wrapper and its status loop at `:588-619` (429 pause at `:572-576`, the 404 rotation default at `:634-635`, "no handler ⇒ never retried" at `:599-600`), the per-request neutralisation of the SDK's own retries at `:885-892`, and the client budget `timeout: 30 * 60 * 1000` / `maxRetries: 3` at `:624-628` —, the `stream_options.include_usage` opt-in (it is PER CLIENT in 32 files, not in `model-class.ts`; samples `nvidia/nvidia-glm5-flash.ts:76`, `daytona/daytona-qwen3-8.ts:498`, `lightning/lightning-glm-5-3.ts:112`), the `APIError.status === 404` failover gate (`runtime/endpoint/provider/private/responses/private-responses.ts:25-27` `is404`, its rotation loop `:104-125`, its exhausted-upstream throw `:118`), the credential gate (sentinel set `:39-44`, `isValidApiKey` `:52-60`), the accounting seam `hasGeneratedPayload` (`:206-241`) and `estimateRequestTokens` (`:82-85`), the result-type branch `isAsyncIterable` (`:170-171`, branched at `:723-733`), plus the two request paths the routes drive (`runtime/endpoint/provider/private/chat-completion/private-chat-completion.ts` and `.../responses/private-responses.ts`) |
//! | `sse`       | the SSE decode half of `node_modules/openai@6.42.0/core/streaming.js` — the SDK this provider talks to and the only decoder whose verdict the wire format has to satisfy: `iterSSEChunks` at `:222-244` (split on a double newline, trailing bytes flushed at `:241-243`), `SSEDecoder` at `:245-285` (trailing-`\r` strip `:252-254`, empty-line dispatch `:255-268`, **multiple `data:` lines joined with `\n` at `:261`**, `:` comment skip `:270-272`, one leading space stripped `:274-276`), the `startsWith('[DONE]')` sentinel at `:35-38`, and the in-band `data.error` throw at `:49-51` — plus the emitted wire format: `runtime/endpoint/provider/private/chat-completion/private-chat-completion.ts:163` (`data: <json>` per chunk) and `:186` (trailing `data: [DONE]`), which the gateway writes as `stream.write(chunk + '\n\n')` at `packages/underload/service/src/server/start.ts:853` (object chunks go through `writeSSE` at `:855-857`); `.../responses/private-responses.ts:60`/`:64` is the same rule on the Responses surface |
//! | `error`     | the upstream-failure vocabulary in `packages/underload/service/src/server/start.ts`: the closed code set at `:318-325` (six classes, doc comment `:317`, alias `:327`) and the frame shape at `:329-338`, redaction/flatten/sanitisation at `:421-466`, the name / errno / text tables at `:352-419`, the error-chain readers at `:472-534`, the classifier port at `:581-621` (`classifyStreamError`), and the frame sentences plus wire key order at `:635-705` (`buildStreamErrorPayload` — status gate `:643`, root message `:656`, sentences `:672-686`, frame literal `:695-702`). There is NO separate emit function: the frame is written inline in the stream `catch` at `:860-873` (`console.error` `:861`, `writeSSE` `:866-871`) and nothing follows it, so no `[DONE]` after an error. `errorSentence` / `errorPayload` / `emitStreamError` are NOT functions of this gateway. The SDK-side messages the transport must reproduce come from `node_modules/openai/core/error.js:21-38` (`makeMessage`) and `:72-93` (the abort / connection / timeout sentences) |
//! | `pricing`   | the three cost fields of `packages/agentic/provider/src/model-class.ts:398-409` (`inputCost` / `outputCost` / `cacheCost`, dollars per million tokens) and the derived getters at `:534-550` (`costInputTotal` / `costOutputTotal` / `costTotal`), as projected by `runtime/endpoint/provider/private/stats/private-stats.ts:229-260` and printed by `.../chat-completion/private-chat-completion.ts:174-184` |
//! | `accounting` | the counter block of `packages/agentic/provider/src/model-class.ts:417-495`, `resetTotals` at `:507-519`, the `lastRequestTokens` baseline at `:525`, and the three settlement points: `accountUpstreamCall` at `:700-734`, `recordRequestTokens` at `:770-786`, `trackStream` at `:831-879`. The consumers are `runtime/endpoint/provider/private/stats/private-stats.ts:197-341` (per-model projection and fleet roll-up) and `.../chat-completion/private-chat-completion.ts:174-184` (the request log line) |
//! | `client`    | `packages/agentic/provider/src/model-class.ts` as the subclass base - the identity and gate fields at `:305-389` (`model` `:310` / `provider` `:321` / `id` `:333` / `enabled` `:368` / `hasValidApiKey` `:378-386` / `isDiscoverable` `:388-389`), the status-handler and `fetch`-wrapper transport at `:555-654` (the 429 pause `:572-576`, the 404 `switchBaseUrl` failover default `:634-635`, the 30-minute ceiling and 3-retry budget `:624-628`), `withModel` at `:899-901`, `create` at `:922-924`, `switchBaseUrl` at `:933-935` - plus the per-family `create()`/`switchBaseUrl()` differences encoded as row data: `makora/makora-glm-5-3.ts:19-28,59-89`, `nvidia/nvidia-glm5-flash.ts:25-40,56-85`, `local/local-strata.ts:161,189-235,243-261`, `local/local-basalt.ts:147,177-223,231-252`, the eight `vendor:` injections (five distinct values) that only `merge/*` performs, and the `reasoning_effort` defaulting `local/*`, `makora/*`, `merge/*`, `vultr/*`, `sail/*`, `token-router/*`, `open-router/*` and `lightning/*` all perform per client |
//! | `clients`   | `packages/agentic/provider/src/registry.ts` (`privateClients` - contents AND order) cross-checked against every registered client's field declarations in `packages/agentic/provider/src/{makora,local,nvidia,merge,vultr,token-router,open-router,sail}/*.ts`; the discoverability expectations reconcile with `packages/agentic/provider/src/model-class.test.ts:243-318` |
//! | `registry`  | `packages/agentic/provider/src/registry.ts` (the shared `privateClients` array and its ordering rule), consumed by `runtime/endpoint/provider/private/chat-completion/private-chat-completion.ts` (id lookup, no discoverability filter), `runtime/endpoint/provider/private/models/service-route.ts` (order-preserving catalog projection), and `runtime/endpoint/provider/private/stats/private-stats.ts:196-280` (project-then-sort) |
//! | `serve`     | the four service routes in front of that registry: `runtime/endpoint/provider/private/chat-completion/{service-route.ts,private-chat-completion.ts}` (incl. the `404 'Model not found'` block at `:78-83`, the SSE frame loop + `data: [DONE]` at `:161-186`, and the request log line at `:174-184`), `.../responses/{service-route.ts,private-responses.ts}` (its `is404` gate at `:25-27`, the `toResponsesStream` frame loop + sentinel at `:56-65`, the stream branch at `:104-125` and the non-streaming answer at `:131-150`), `.../models/service-route.ts:15-48` (the `{object:'list', data:[...]}` envelope and row order — `object` at `:19`, `data` at `:37`), and `.../stats/{service-route.ts,private-stats.ts}` (the `ModelStats` shape at `:43-102`, the projection at `:197-341`, the HTML document at `:1013-1044`, and `privateStatsReset` at `:1063-1090` with its three status codes) - plus the gateway halves they depend on from `packages/underload/service/src/server/`: `handler.ts:42` (`400 {"error":"handler not found"}` for a verb the route object does not declare), `start.ts:811-819` (the body parse that folds a malformed JSON body into `{}`) and `start.ts:908-911` (the `500 {"error":"Internal Server Error"}` fallback for a handler that throws), the per-route `port:` field whose value is `LOCAL_AREA_NETWORK_PROVIDER_PORT` = `6660` (`config/environment/src/port.ts:4`; this crate publishes `8800`, recorded on `serve::PROVIDER_PORT`), and the `streamSSE` header set hono writes for `text/event-stream` |
//!
//! The vault artifact is the ONLY input this crate has for credentials and endpoints:
//! the TypeScript package reaches the same data through named `import { NVIDIA_API_KEYS }
//! from '@config/vault'` specifiers, which have no Rust equivalent and are not meant to
//! have one. A Rust process reads `config/vault/generated/<scope>.json` (or captures the
//! `--stdout` payload line) and parses it; it never re-derives the vault. The normative
//! contract is `config/vault/knowledge.md` "Rust consumption contract".
//!
//! STILL TO PORT (later tasks in this port, in dependency order - each one adds its own
//! module row above and its own dependencies to `Cargo.toml`):
//!   1. the `/stats` **HTML** dashboard and its persisted buffer - the TypeScript answers
//!      `GET /stats` with a server-rendered `text/html` document and folds earlier processes'
//!      figures in through `stats/stats-buffer.ts`. `serve` publishes the same projection as
//!      JSON and resets only the live counters; both halves are recorded as deviations in
//!      `serve`'s module doc, not silently dropped.
//!   2. the sandbox substrate behind `daytona/daytona-qwen3-8.ts` (its base URL exists only
//!      after a GPU-sandbox boot), and the `lightning/*` Chat-to-Responses adapter - see the
//!      two surfaces listed below, neither of which the client layer ports on purpose.
//!
//! PORTED HERE, NOT AS A ROUTE FILE: the four routes themselves (`crate::serve`) plus the
//! runtime resolver half of `runtime/endpoint/provider/localhost/registry.ts`
//! (`servedClients` / `resolveServedClient`), which is `crate::registry` + `crate::clients`
//! restated - the served set is the row table in array order, and the lookup is an exact
//! `id` match with no discoverability filter (the catalog route adds that filter itself).
//! `packages/typecrate/provider/endpoint/provider/provider-route.rs` is the bridge that makes
//! the same handlers visible to `packages/typecrate/services`: it declares the paths, verbs and
//! port, and contains no logic, so there is exactly one implementation of the contract.
//!
//! Two TypeScript surfaces are deliberately NOT ported by the client layer, and each carries
//! its reason where it is recorded (`clients.rs` module doc, and
//! `crate::client`'s lineage section):
//!   * `daytona/daytona-qwen3-8.ts` - its base URL exists only after a GPU-sandbox boot, so a
//!     row built from the vault artifact would advertise an upstream that cannot answer a
//!     request; the sandbox handle is a separate substrate.
//!   * `lightning/lightning-*.ts` and their Chat-to-Responses adapter - the TypeScript does not
//!     register those four clients at all, so omitting them IS the parity behaviour.
//!
//! Nothing above is a placeholder today: this crate exposes the vault artifact loader, the
//! outbound transport substrate, the accounting core, the declarative client, the row table,
//! and the registry that binds rows to vault - and the public signatures of every one of them
//! are contractual, not incidental, because the route task reads them.

/// Crate name, for banners and diagnostics (matches `[package] name` with `-` -> `_`).
pub const CRATE_NAME: &str = "typecrate_provider";

/// The crate version (the same string `package.json` carries).
///
/// `CARGO_PKG_VERSION` is compile-time, so a version bump that forgets one of the two
/// places fails `version_matches_the_npm_line` instead of lying on the wire - the same
/// handshake `packages/typecrate/harness/src/lib.rs:71-79` and
/// `packages/typecrate/services/src/lib.rs:19-24` declare.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

// The failure vocabulary shared by every layer: the closed `FailureKind` set, the
// `classifyStreamError` port, the SDK-compatible `TransportError` messages, and the
// `StreamFailure` frame whose wire key order the dashboard and the route tests pin.
// Declared first because `sse` and `transport` both name these types.
pub mod error;

// The incremental SSE frame codec (`SseDecoder`) plus the frame encoders the tests and the
// route task use to script and emit `data:` frames.
pub mod sse;

// The outbound OpenAI-compatible substrate: endpoint/credential pools with forward-only
// rotation, the retry/429/rotation loop, the JSON and SSE request paths, and the settlement
// facts the client layer folds into its accounting. Reads `vault` for the artifact shapes and
// `error` for the taxonomy.
pub mod transport;

// The vault artifact loader: the credential/endpoint substrate every later module reads.
pub mod vault;

// Per-million prices and the dollar arithmetic the stats projection and the request log line
// render. Declared before `accounting` because the cost getters are a read of the counters.
pub mod pricing;

// The ten lifetime counters of `model-class.ts`, the input/cache diff, the stream settlement,
// and the fleet roll-up the stats route sums.
pub mod accounting;

// The declarative model client: one row per served model, one shared process-wide singleton per
// row, the payload rewrite, the 404 failover loop, and the discoverability gate.
pub mod client;

// The served model set itself - twenty-nine rows in `registry.ts` order. Data only: every
// behaviour lives in `client`, and the `ClientSpec` builder methods are declared here because
// they are the row vocabulary.
pub mod clients;

// Rows + vault -> the shared client list the routes dispatch through, in array order, refusing
// with a problem list instead of coercing.
pub mod registry;

// The four service routes in front of that registry: the OpenAI-compatible relays, the catalog
// projection, the stats document and its reset verb, plus the vault gate and the SSE assembly.
// Declared last because it is the top of the crate: every module above is an input to it, and it is
// the only module an `endpoint/**` route file has to name.
pub mod serve;

#[cfg(test)]
mod tests {
    use serde_json::Value;

    /// The npm/cargo version handshake, read out of the real `package.json` next to
    /// `Cargo.toml` (the sibling crates pin the literal `1.0.2`; this crate reads the
    /// file, so the two cannot drift in either direction).
    #[test]
    fn version_matches_the_npm_line() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/package.json");
        let raw = std::fs::read_to_string(path).expect("package.json must sit beside Cargo.toml");
        let document: Value = serde_json::from_str(&raw).expect("package.json must be valid JSON");
        assert_eq!(document["version"], Value::from(super::version()));
    }

    #[test]
    fn crate_name_matches_the_library_target() {
        assert_eq!(super::CRATE_NAME, "typecrate_provider");
    }
}
