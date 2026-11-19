# typecrate-provider — port knowledge

Decisions taken while porting `packages/agentic/provider` (+ its `runtime/endpoint/provider/private/**`
routes) to `packages/typecrate/provider`. Every entry names the TypeScript or Rust source it came
from, with a `file:line` that was opened and read on the pass that wrote this file, so a later stage
can re-check it instead of re-deriving it. `README.md` carries the operator-facing summary; this file
carries the reasoning and the consequences.

> **House model.** `packages/typecrate/harness/knowledge.md` is the template, because it is the only
> completed TypeScript→Rust port in this repo: H1 + one-paragraph identity (`harness/knowledge.md:1-5`),
> a stage/port map (`:7-66`), then `## Decisions` records grouped by concern (`:68` onward), each
> naming its source. The deviation-recording pattern (state the deviation, its evidence, and what a
> later stage must not re-break) follows that file's stage sections; the README ledger is the
> index and this file is the record.

## Port map (what exists, what is next)

The crate was built in waves, and the waves are named by the code itself rather than by this file:
`src/transport.rs:563` records a "DEVIATION FROM **C2**", and `tests/registry.rs:1` labels the
registry suite "(**C3**)". The map below is read off those labels, the module docs and the
acceptance letters they cite.

1. **C1 (done) — the vault substrate.** `src/vault.rs`: the artifact loader, the five validity
   classes, the classifier, `Secret`, order-preserving pools, all-problems-at-once refusal. Its scope
   note is `package.json` `#scope` ("PORTED TODAY: `src/vault.rs`"), and its contract is
   `config/vault/knowledge.md` "Rust consumption contract".
2. **C2 (done, then corrected) — the outbound transport.** `src/transport.rs` + `src/sse.rs` +
   `src/error.rs`: pools behind forward-only atomic cursors, the retry/429/404 rules, the SSE codec,
   the failure taxonomy. The correction is recorded in-code at `transport.rs:563` (keyless headers
   were first hard-coded to "no header" for every keyless credential).
3. **C3 (done) — rows, clients, registry.** `src/clients.rs` (29 rows), `src/client.rs` (the
   declarative client + `Behavior`), `src/registry.rs` (ordered, validated, process-wide). Pinned by
   the acceptance letters in `tests/registry.rs:3-16` (A4 pool order, A5 drain + discoverability,
   A6 labels, A7 catalog reconciliation, A8 the whole table, A9 shapes + refusals).
4. **C4 (done) — the HTTP surface.** `src/serve.rs`: the lazy vault gate, the verb gate, the two
   relays, the catalog envelope, the stats document and its reset verb, the request log line.
5. **C5 (done) — discoverability from `typecrate-services`.**
   `endpoint/provider/provider-route.rs`: four logic-free wrappers plus one `service_routes!` block
   with `, port: 8800;` (`:101-117`), and the path dependency
   `typecrate-provider = { path = "../provider" }` in the services manifest
   (`packages/typecrate/services/Cargo.toml:92` as of this pass — that file is edited concurrently,
   so re-check the line before citing it).
6. **Still to port** (each is listed in the crate's own "still to port" block, `src/lib.rs:29-38`):
   the Daytona sandbox substrate behind `daytona/daytona-qwen3-8.ts` (S1 in the README ledger), the
   Lightning Chat↔Responses adapter (S2), and the `/stats` HTML dashboard with its cross-session
   buffer (S3, `runtime/endpoint/provider/private/stats/stats-buffer.ts`). Nothing else in the crate
   is a placeholder.
7. **Open parity decisions** a later stage has to settle, not implement blind: the port number
   (S4), whether a drained row should drop its credential header (the drain markers in
   `tests/clients.rs:1242` and `tests/registry.rs:1052`), and whether JSON-only `/stats` is the
   target shape or a stopgap.

## Decisions

### Workspace, naming, and the lockfile discipline

- **Standalone `[workspace]`, not a root-workspace member.** `Cargo.toml:27`. The package lives where
  the npm package lives, and the root workspace globs `members = ["rust/crates/*"]`; without the
  boundary cargo walks up and fails with `current package believes it's in a workspace when it's
  not`, which is the documented failure in `rust/README.md:52-61`. The two sanctioned alternatives
  (add to `members`, or add to `exclude`) are both worse here: the crate is not a `rust/crates/*`
  placeholder, and the sibling typecrate crates already chose the standalone shape
  (`packages/typecrate/harness/Cargo.toml`, `packages/typecrate/services/Cargo.toml`). Consequence:
  **nothing** can use `.workspace = true`, so `edition` (`Cargo.toml:38`), `rust-version` (`:39`),
  `license` (`:40`), `publish` (`:41`), `[lints.rust]` (`:170-171`) and `[profile.release]`
  (`:178-183`) are restated literally. A maintainer who "simplifies" one of those into inheritance
  gets a resolution error, not a cleanup.
- **Naming is the repo rule.** `rust/README.md:35-45`: `@typecrate/provider` → crate
  `typecrate-provider` (`Cargo.toml:33`) → `typecrate_provider` in code (`Cargo.toml:45`,
  `src/lib.rs:64`). `version()` reads `CARGO_PKG_VERSION` (`src/lib.rs:72-75`) and the test
  `version_matches_the_npm_line` (`:131-136`) parses the sibling `package.json:3`, so the two version
  lines cannot drift in either direction — the same handshake `packages/typecrate/harness` and
  `packages/typecrate/services` declare.
- **The lockfile is a strict subset, on purpose.** This workspace resolves its own `Cargo.lock` and
  the repo has no vendored registry, so a dependency or feature that no sibling resolves needs a
  network fetch that this host does not have. Hence `tracing` with `default-features = false,
  features = ["std"]` (`Cargo.toml:83`) — dropping the `attributes` feature avoids
  `tracing-attributes`, which appears in neither sibling lock — and `axum = "0.8"` with **default
  features** and no `sse` feature (`Cargo.toml:146`), because the SSE response is
  `Body::from_stream` + two headers (`serve.rs:567-578`). `tests/transport.rs`'s
  `the_lock_package_set_is_a_strict_subset_of_the_siblings` fails the moment the set leaks past
  either sibling graph, and
  `tests/transport.rs::axum_sse_response_needs_no_sse_feature` proves the response shape needs nothing
  more — which is also why `packages/typecrate/services` (axum `json,query,ws`) does not have to grow
  a feature to compile this crate's handler into its binary.

### The vault artifact is the only credential input

- **Data, not code.** The TypeScript reaches credentials through named
  `import { NVIDIA_API_KEYS } from '@config/vault'` specifiers (build-time); Rust has no equivalent
  and is not meant to get one (`vault.rs:13-21`). The single interface is
  `config/vault/generated/<scope>.json`, written by
  `yarn workspace @config/vault export -- --consumer agentic-provider`
  (`config/vault/package.json:10`; the string the 503 body prints is `serve.rs:199-200`). The
  artifact is git-ignored (`config/vault/.gitignore`) and `--consumer` is required, with no full-vault
  default (`config/vault/scripts/export-vault.ts:285`).
- **Ordering is load-bearing, and the tie-break is enforced, not assumed.** `vault.rs:49-55`: pools
  are `Vec`, `entries[0]` is the live default and the rotation head, and the generator emits
  `default = entries[0].value`. The loader accepts that tie-break in exactly one place
  (`VaultSlot::head`, `vault.rs:33-34`) and still **reports** a disagreement as a problem, so a load
  that succeeds never hides one (unit test
  `default_disagreeing_with_the_pool_head_is_refused`). Deserialising into a map would silently change
  which credential every consumer uses; nothing in the crate sorts (`registry.rs:14-20` inherits the
  rule for the row order too). This is the whole parity basis for "which key serves the first
  request", and it is what `tests/registry.rs::d_the_nvidia_credential_pool_is_the_document_order_and_
  rotation_walks_it_once` pins.
- **Least privilege is verifiable.** The scope table is hand-maintained at
  `config/vault/src/vault/consumers.ts:45-57` and lists exactly the 11 vendors the provider reads —
  the comment above it (`:44`) already names this crate's role: "This is the scope the Rust port of
  `@agentic/provider` loads."
  The live artifact carries those 11 vendors, 20 slots and 43 entries, with
  `consumers: ["agentic-provider"]` (re-counted on this pass). The upstream guard that keeps the
  scope honest is `config/vault/src/vault/grants.test.ts` (regression guard #9), which scans
  `packages/agentic/provider/src` — see "Cutover" below for what has to move when this crate becomes
  the consumer.
- **The `sentinels` block is deliberately global.** The exporter filters vendors by scope and
  re-lists `sentinels` verbatim (`config/vault/scripts/export-vault.ts:308`, with the reason at
  `:173`: global vocabulary, not a secret). Consequence: the vocabulary is **not** scope-filtered, so
  this crate must not assume it narrowed with the vendor list — and it does not: the classifier unions
  the artifact's three lists with the built-in floors (`vault.rs:73-76` placeholder suffix,
  `:78-85` `none`, `:87-91` the four invalid literals) and never narrows them (`vault.rs:82-84`). The
  check order ports `sentinels.ts`'s classifier exactly (`vault.rs:327-340`), including the rule that
  `Error` is not `error` (`vault.rs:389`).
- **Redaction is a type, not a convention.** `Secret` (`vault.rs:128-164`) has hand-written `Debug`
  (`:153-157`) and `Display` (`:160-164`) that print fixed text, so no `{:?}` of any enclosing struct
  can reach a value; equality still compares the real string, because `default == entries[0]` needs
  it and comparing two held values is not a leak (`vault.rs:124-127`). There is no `Default` on
  purpose: an empty credential that looks set is already a validity class (`VaultValidity::Empty`,
  `vault.rs:180-193`). `serde`'s value echoes go through a separate gate (`vault.rs:93-102`, the
  `REDACT_MIN_LEN` threshold at `:102`).

### Ambient input lives in exactly one layer

`vault.rs:23-29` and `registry.rs:32-37` state the rule (injected input, never ambient — the same
discipline `packages/typecrate/harness` uses, where every ambient read sits in one module,
`packages/typecrate/harness/src/environment.rs:199-218`), and `serve.rs:488-494`
(`vault_source_from_env`) is the only place in the crate that calls `std::env::var` — the
`VaultSource` doc at `serve.rs:311-338` states the rule, the two reads are at `:493-494`. Consequence
(`vault_source_from_env`) is the only place in the crate that calls `std::env::var` — the
`VaultSource` doc at `serve.rs:311-338` states the rule, the two reads are at `:483-484`. Consequence
for tests: a suite builds a `VaultConfig` from a string and a
`Registry` from rows, while the precedence helper stays a pure function
(`serve::vault_source_from_values`, `serve.rs:501-511`) instead of a mutation of the process
environment — which edition 2024 would have made `unsafe` and this crate forbids `unsafe` outright
(`Cargo.toml:171`). The lazy `OnceLock` state (`serve.rs:414-435`) also installs the registry
process-wide through `Registry::initialize_process` (`registry.rs:256`), first call wins, so any other
consumer in the process sees the same rows, counters and rotation cursors.

### Rows instead of subclasses

`client.rs:11-33` is the argument; the consequence for a maintainer is the recipe in
`clients.rs:45-66` (identity → method → pricing → visibility → request shaping) and the invariant that
`Behavior` is a **closed enum** so the request path is an exhaustive `match` with no vtable. The
per-family differences are data: `vendor` injection for the gateway-routed family
(`client.rs:186-198`, five variants because exactly five vendor names appear in
`merge/merge-*.ts`), the two credential-drain shapes (`CredentialAdvance`, see below), the effort and
`max_tokens` rewrites, and the local-relay bundle (`clients.rs:97-103`, which always travels with
"no credential slot" and "suppress the header").

### Pool ownership, and two drain shapes rather than one

- **Per client, not per family** (`client.rs:96-112`). TypeScript keeps its rotation queue at module
  scope (`nvidia/nvidia-glm5-flash.ts:9`, `makora/makora-glm-5-3.ts:5`), so four Nvidia clients share
  one queue and a key burned by one sibling is gone for the others. The Rust transport owns the cursor
  and takes its target by value, so each row rotates its own copy. Order is unchanged (document order,
  never re-sorted); drain *budget* is not: each row drains after its own `len` rotations. A later
  stage must not "fix" this into a shared `Arc` without re-deriving the drain tests.
- **`NextOnEveryRotation` vs `SameThenNext`** (`client.rs:80-92`). NVIDIA shifts the head and returns
  the new head (`nvidia/nvidia-glm5-flash.ts:13-18`); Makora shifts the head and returns the entry it
  removed (`makora/makora-glm-5-3.ts:6-10`), so with a single live key Makora wastes exactly one retry
  before draining. Collapsing them would change which credential serves a request — the property A4
  pins (`tests/clients.rs::a_makora_pool_re_sends_its_head_before_it_advances`).
- **Exhaustion keeps the last entry** rather than falling back to "nothing", because
  `local/local-strata.ts:245-249` returns false without clearing `baseURL`: the endpoint last set
  stays in force. The same rule applied to credentials produces the port's most visible parity break:
  a drained row keeps sending its last credential, where the TypeScript sets `apiKey = ''`
  (`makora-glm-5-3.ts:86`, `nvidia-glm5-flash.ts:83`) and the SDK then sends no header at all. Both
  are hidden from `/models` either way (`is_discoverable` is computed, never cached,
  `client.rs:114-134`) and both stay routable; only the header differs. Pinned at
  `tests/clients.rs:1242` and `tests/registry.rs:1052`.

### The 404 failover boundary

`transport.rs:55-63` returns a 404 as a **typed** verdict and never retries it; `client.rs:56-78` owns
the rotation loop, because in TypeScript `switchBaseUrl()` is called by a 404 status handler inside a
`while (true)` around `fetch` (`model-class.ts:933` is the method; the wrapper is the
`:557-654` region). Two consequences that must not be re-broken: the rotation loop is **not** bounded
by the retry budget (the SDK's own retries are neutralised upstream with `maxRetries: 0`), and the
accounting wrapper sits **outside** the loop, so a request that rotates five times still counts one
`requestTotal` and settles its prompt split once. The exhausted case is the route's sentence —
`transport::EXHAUSTED_BASE_URLS_MESSAGE` (`transport.rs:174`) — which is the text the Responses route
throws by hand today (`runtime/endpoint/provider/private/responses/private-responses.ts:118`).

Related: `rebaseRequestInput` (`model-class.ts:141`) has **no** Rust counterpart, because the retry
rebuilds its URL from the rotated endpoint instead of string-patching the previous attempt's URL
(`transport.rs:61-63`).

### Retry policy is bounded, and spends the budget differently

`transport.rs:65-85`. The deployed rule, re-verified line by line on this pass rather than assumed: the
429 status handler pauses 3 s and returns `true` with NO attempt counter (`model-class.ts:572-576`
inside the `while (true)` fetch wrapper at `:588-619`), so a relay that always answers 429 holds
a request open indefinitely — the 30-minute per-attempt ceiling (`:624`) does not stop it, because
each attempt restarts the clock. The client is BUILT with `maxRetries: 3` (`:628`) but every
request forces `maxRetries: 0` (`:885-892`), so the SDK's own retries never run, and a status with
no handler is returned un-retried (`:599-600`) — a dropped socket fails on the spot today.

This port bounds EVERY retryable outcome to `1 + max_retries` (`MAX_RETRIES` = 3,
`transport.rs:157`) and, in exchange, spends the budget on the connection and timeout failures
the deployed provider surfaces immediately — justified by the TypeScript's own comment at
`model-class.ts:625-627` (the budget exists for transient drops when an idle proxy connection
is reaped mid cold-start).
The 3 s pause is injectable (`transport.rs:584-590`) so a test proves the pause happened without
waiting for it. **Retry behaviour is explicitly not claimed to match the TypeScript**:
`tests/transport.rs` pins the bounded count, the 404 rotation stop and the never-retried arms
instead of a parity match, and README's deviation rows `transport.rs:77` / `:80` state the two
asymmetries an operator feels.

### Keyless is a policy, not a constant

`transport.rs:88-101` and `:558-575`. Selecting the `none` sentinel must mean both "this endpoint needs
no credential" (a **usable** state: the model stays discoverable,
`model-class.ts:32-35` and `:378`) and "here is what reaches the wire". The TypeScript is not one
behaviour: `openai@6.42.0` builds the header from `apiKey` for anything that is not `null`
(`node_modules/openai/client.js:233-241`), so every client with `apiKey: 'none'` sends
`Authorization: Bearer none`, while the two local relays additionally pin
`defaultHeaders: { Authorization: null }` (`local/local-strata.ts:161`, `local/local-basalt.ts:147`)
and send nothing. `KeylessHeaderPolicy::SendNone` is `#[default]` (`transport.rs:570`) because that is
what today's relays actually receive; `Suppress` (`:574`) is per-row and used by exactly
`ClientSpec::keyless_relay()` (`clients.rs:97-103`). This is a **correction**, not a preference: the
first cut of the module sent nothing for every keyless credential, which was wrong on every keyless
row except the two relays (`transport.rs:97-100`, `:563`).

### Usage opt-in parity

Every streaming chat row sends `stream_options: { include_usage: true }`, because without it no stream
chunk carries usage and `outputTokenTotal` would freeze at 0. In TypeScript the rule is **per family,
not in the base class**: 32 client files carry the identical
`stream_options: stream ? { include_usage: true } : undefined` (`find`/`grep` count on this pass;
verified samples: `nvidia/nvidia-glm5-flash.ts:76`, `daytona/daytona-qwen3-8.ts:498`,
`lightning/lightning-glm-5-3.ts:112`), and `model-class.ts` contains no `stream_options` at all. The
Rust spelling is the row flag `requires_usage_option` (`client.rs:339-345`), defaulting to true and
cleared only by `.without_usage_option()` (`clients.rs:151-160`) — legitimate only for the Responses
passthrough, whose terminal event carries usage. The transport removes both the `null` and the absent
spelling (`transport.rs:1663`, and `tests/clients.rs::stream_options_reaches_only_a_streaming_request_
that_needs_it`).

### Settlement without `finally`

TypeScript settles a stream in `trackStream`'s `finally` (`model-class.ts:831`, folding through the
block at `:859-878`), which runs on exhaustion, on a mid-stream throw **and** on abandonment. Rust has
no `finally`, so `transport.rs:102-112` settles in two places — the terminal transition in `poll_next`
and `Drop` for abandonment — funnelled through one hook that is *taken*, so it fires exactly once, and
`Drop` additionally cancels the request's child `CancellationToken` so the upstream body stops being
read. Four paths are pinned by name in `tests/clients.rs` (`a_streamed_success_settles_exactly_once`,
`an_undecodable_frame_settles_the_stream_as_a_failure_once`,
`an_abandoned_stream_settles_once_and_is_not_a_failure`, `a_cancelled_stream_settles_once_as_a_failure`)
and in `tests/transport.rs::the_settlement_fires_once_for_clean_failure_and_cancellation`.
`transport.rs:1917` records the one judgement call: an undecodable frame is a fatal stream whose
**origin** is `Transport` while the failure **code** stays `decode`, so a consumer that branches on the
code still sees the TypeScript's class.

### Accounting: one lock, one critical section, truncated milliseconds

`accounting.rs:68-78`: the counters and the continuation baseline live in one `Mutex<Ledger>` and the
input/cache diff (`lastRequestTokens`-style, TypeScript `model-class.ts:525`, `:770-787`) happens
inside one acquisition, because a Rust client is shared across worker threads while the TypeScript gets
that atomicity from a single-threaded runtime. `RefCell` is not an option: the ledger sits behind the
`Arc` of a `Send + Sync` client. The two port counters (`accounting.rs:142`, `:146`) and the truncation
rule (`accounting.rs:58-66`) are recorded in the README ledger; the load-bearing asymmetry a later
stage must keep is that the **estimate** is computed over the caller's body while the bytes sent are
the rewritten payload (`client.rs:50-54`, mirroring `model-class.ts:707` vs `:733`).

### Pricing is `f64` with an honest gap

`pricing.rs:30-35`: JavaScript has one numeric type and the deployed figures are JS numbers end to end
(`(tokens × price) / 1_000_000`, `model-class.ts:534`, `:542`, `:548`), so the port keeps `f64`, the
same literal divisor, and the multiply-before-divide order. Renaming `cacheCost` → `cache_read` says
what the field buys (that field prices a cache **read**); `cache_write` exists as a slot and is priced
into nothing (`pricing.rs:22-28`), because no TypeScript client declares a cache-write rate.

### Errors: signals as data, needles not regexes, redaction at capture

`error.rs:38-75`, expanded in the README marker table. The one thing to understand before touching this
module: the closed kind set here (`error.rs:91-103`: `Timeout`, `Connection`, `Aborted`,
`UpstreamStatus`, `Decode`, `Unknown`) mirrors the wire vocabulary
`packages/underload/service/src/server/start.ts:317-327` (`STREAM_ERROR_CODES` = `timeout`,
`connection`, `aborted`, `upstream_status`, `decode`, `unknown`; the `StreamErrorCode` alias is
`:327`), and the classifier's rule **order** is part of the contract (`start.ts:581-621`). A new
failure shape means new `FailureSignals`, never a new kind. Redaction and the 200-character cap are
the gateway's own (`start.ts:435` (`redactSecrets`), `start.ts:346` (`MAX_ERROR_DETAIL_LENGTH`)),
applied at capture rather than at frame-build time.

**The signal the SDK does not carry, so neither does this crate.** An error that arrives INSIDE the
stream (`data: {"error":…}`) is read by the SDK, which throws `new APIError(undefined, data.error,
undefined, response.headers)` (`node_modules/openai/core/streaming.js:50`); the constructor
assigns `this.status = status` (`node_modules/openai/core/error.js:12`), so the object the
gateway classifies has **no numeric status at all**, whatever the upstream wrote in
`error.status`. `readUpstreamStatus` (`start.ts:506-517`) therefore finds nothing, rule 3 of
`start.ts:581-621` cannot fire, and an ordinary upstream message falls through to `unknown`:
the frozen sentence (`:685`) and a payload with NO `status` key (`:699` writes `undefined`,
which `JSON.stringify` drops). `FailureSignals::from_upstream_frame` mirrors the SDK object rather than
the JSON body for exactly that reason, and `FailureSignals::frame_status` (`error.rs:624`) /
`StreamFailure::frame_status` (`:1031`) keep the frame's own status for the log only. A `status`
on the wire is stamped only by a PRE-stream HTTP status (`TransportError::Status`, the gate at
`start.ts:644`). `tests/routes.rs::a_mid_stream_error_frame_with_a_status_is_still_an_unknown_frame`
and `tests/transport.rs::a_mid_stream_error_frame_with_a_status_stays_unknown_and_carries_no_status`
pin both halves; an earlier cut of this crate lifted the in-band status and answered `upstream_status`
where the deployed TypeScript answers `unknown`.

### SSE and JSON: key order is contract

`serde_json`'s `preserve_order` feature (`Cargo.toml:64`) is what lets `serve.rs` state envelope key
order at all: the catalog envelope (`object` then `data`, `serve.rs:900-906`), the stats rows and
totals (`serve.rs:1050-1106`, `:1111-1161`), and the error frames are compared as bytes downstream, and
a chunk that is re-serialised on the way to the client must keep its own order (`sse.rs:43-52`). The
event-boundary rule — one event, one payload, the `data:` lines of one event joined with `\n` at the
blank separator line, exactly what `streaming.js:255-268` does — is `sse.rs:36-53`, and the three
one-directional leniencies this decoder accepts where the SDK drops or rejects bytes are
`sse.rs:54-80` — and the direction is not uniform: leniencies 1 and 2 can hand this caller a chunk
the SDK's own decoder never produced (an event left open by the end of the body is relayed here and
dropped there, `streaming.js:212-216` with `:255-268`), and only leniency 3 delivers strictly less
here. Two
formatting rules that are parity, not taste: `toFixed`'s tie rule for money
(`serve.rs:1206-1212` — `$0.13` where `{:.2}` prints `$0.12`, and a dyadic price
over a round token count lands exactly on that boundary) and `en-US` thousands grouping
(`serve.rs:1181-1204`); the stats sort is a code-unit compare over ids, which `Ord for str` already
is (`serve.rs:1008-1011`, against TypeScript's `<`/`>` compare at `private-stats.ts:285`).

### The route bridge

- **`ANY` because the refusal is in-handler.** `provider-route.rs:34-49`; the gateway's verb miss is
  `400 {"error":"handler not found"}` (`packages/underload/service/src/server/handler.ts:38-46`, the
  line at `:42`), so a router-level verb filter would pre-empt the contract. Same reasoning as the
  harness bridge, different reason worth naming: the harness needs `ANY` for its own 405 sentences,
  the provider needs it because its miss is a 400.
- **`, port: 8800;` is a literal, so the number is written twice.** The `service_routes!` grammar binds
  a literal (`packages/typecrate/services/src/server/metadata.rs`; precedent
  `packages/typecrate/services/examples/endpoints/health/health-route.rs`), so the Rust constant
  (`serve.rs:168`) and the route-file literal (`provider-route.rs:104`, `:108`, `:112`, `:116`) meet
  only in `tests/routes.rs::the_mount_is_the_one_the_route_file_declares`. Every group carries the
  trailer; a group that omitted it would merge into the services default listener instead.
- **No `State`, because the state is the provider's.** `provider-route.rs:18-32`: the generated router
  mounts handlers into the *services* `AppState`, while the provider's registry+counters live in
  `serve::ServeState`; taking nothing out of the services state also keeps the handlers plain
  `(state, request) -> Response`, which is what lets `tests/routes.rs` mount them against a synthetic
  registry with no services binary and no artifact on disk.
- **A real CORS preflight never reaches these handlers** on the services gateway: the gateway layer
  answers it with `204` before routing
  (`packages/typecrate/services/src/server/mod.rs:328-337` explains why the predicate needs
  `Access-Control-Request-Method`, `:386-389` is `is_cors_preflight`, and `:398-405` is the `gateway`
  layer that returns `StatusCode::NO_CONTENT` at `:402`). A bare `OPTIONS` does, and
  gets the `400`.

## Deviation records — the reasoning README's table cannot hold

**S1 Daytona.** The row is absent, not disabled: `enabled = false` would still advertise a routable id
whose upstream cannot exist until a sandbox boots, and its endpoint is a sandbox handle rather than a
vault entry (`daytona_api_endpoints/STANDARD` is a placeholder URL). The TypeScript keeps it registered
at `packages/agentic/provider/src/registry.ts:68` and pins it discoverable at
`model-class.test.ts:311-315`. Consequence: `/models` answers 11 rows where the TypeScript answers 12
(`model-class.test.ts:278`), and a caller routing `daytona/qwen3.8-27b` gets this port's 404. Porting
it means porting the sandbox substrate — a separate module row in `lib.rs`, not a new `ClientSpec`.

**S2 Lightning.** Four clients, unregistered in TypeScript (`registry.ts`: the offering is disabled,
so every `lightning/*` routing key resolves to nothing). Omitting them **is** parity, and the adapter
has no registered consumer to port for. `clients.rs:620-624` records it as an asserted expected diff,
so re-enabling Lightning in TypeScript turns this into a test failure rather than a silent gap.

**S3 `/stats` JSON only, no buffer.** The TypeScript's `privateStats` (`private-stats.ts:1013`) returns
a raw `text/html` `Response` (`:1033-1041`) so the gateway passes the page through untouched, and
`collectStats` folds a persisted cross-session snapshot into the live rows before rendering. The port
publishes the same projection as JSON and nothing else. Consequences, each deliberate: no hide-unused
filter (`:276-284`), so every row appears with `enabled`/`discoverable` beside it and a zero row is
distinguishable from a broken registry; the grouped tile projection and the HTML bar-scale maxima do
not exist; counters are process-local, so a restart starts from zero and the `POST` reset clears only
live counters (the TypeScript also clears the buffer entry, `private-stats.ts:1085-1088`). The
dashboard therefore stays on the TypeScript provider until either the page is ported or the dashboard
is rewritten against this JSON.

**S4 Port 8800.** Recorded at `serve.rs:147-168` and in `lib.rs`. 6660 is
`LOCAL_AREA_NETWORK_PROVIDER_PORT` (`config/environment/src/port.ts:4`) and is owned by the running
TypeScript provider — including that process's gateway builtins. The consequence nobody should
discover by accident: `packages/typecrate/harness/src/config.rs:36` still defaults to
`http://localhost:6660/providers/private/v1`, so the Rust provider is live but undialed. Nothing in
this port changed a number outside its own crate; the reconciliation is a deliberate cutover decision
(README, "Two implementations, two ports").

**S5 Keyless header.** See the policy section above. The alternative (`Suppress`) is one row-builder
call away and is already exercised end to end against a listener that reads the header.

**S6 Credentials loaded, not ported.** Three guarantees a later stage must keep: pools are ordered
arrays with `default == entries[0]` and nothing re-sorted; the consumer scope is least-privilege,
exactly the 11 vendors (`consumers.ts:45-57`); and the `sentinels` vocabulary is global, so scope
filtering never narrows it (`export-vault.ts:308`). The new state — 503 while the artifact is missing
or refused (`serve.rs:48-50`) — exists so that a git-ignored generated file can never abort a host
process that also serves `/v1/harness`.

**S7 Client-layer departures.** Enumerated one per marker in README's second table; the ones whose
consequence reaches beyond the file that records them: per-row pools (drain budget), keep-last
credential on drain (wire header), the registry's refuse-don't-coerce rule (a deployment that half
worked in TypeScript hard-fails here, by design), the label allow-list (a hostile label is refused at
build rather than printed), attempt/latency/cache-write additions (no parity figure depends on them),
and the locally stated body limit (`serve.rs:92-95`, the same `1 << 20` the services router applies at
`packages/typecrate/services/src/server/mod.rs:85`).

**S8 Counters.** The TypeScript base tracks ~17 members: the 10 lifetime counters
(`model-class.ts:417`-`:495`, first field `inputTokenTotal` at `:417`, last
`measuredDecodeMillisecondTotal` at `:495`), the private `lastRequestTokens` baseline (`:525`), the
three cost getters (`:534`, `:542`, `:548`), the two gate getters (`:378`, `:388`) and the mutable
`upstreamLabel` (`:355`). `Counters` (`accounting.rs:95-147`) models all ten counters plus the two
port additions; `/stats` publishes the ten (inside the 20-key row and the 11-key totals) and the two
additions appear in `totals` only (`serve.rs:1151-1158`). Nothing else is surfaced.

## Parity and test scope

The approved scope is a **behavioural test core, not 1:1 parity** with the TypeScript suite. Counted on
this pass: the TypeScript side is 35 `*.test.ts` files under `packages/agentic/provider/src` with 294
declared cases, plus 8 route-side test files under `runtime/endpoint/provider/**`; the Rust side is
146 test functions (33 lib unit + 113 integration across 5 targets) **plus 2 doc tests**, and
`cargo test --offline --locked` run on this pass (2026-11-19, re-run after the micro-pass added
`sse::tests::the_sentinel_prefix_ends_the_stream_even_with_text_after_it`) reported exactly that:
**148 passed, 0 failed, 0 ignored** (33 lib / 15 accounting / 27 clients / 12 registry /
18 routes / 41 transport / 2 doc tests), exit 0 — so the "146 vs 148" question is the doctest
target, not a missing test. The doctests are the runnable examples in
`src/vault.rs:900` (`load_from_str`) and `src/transport.rs:1677` (`prepare_body`) — the two
lines `cargo test --doc` names. `fmt`, `check` and `clippy -- -D warnings` were also re-run on
this pass and are clean. The per-target breakdown and the gap list are in README's
[Test parity and coverage](README.md#test-parity-and-coverage).

The suite is organised around the acceptance letters recorded in `tests/registry.rs:3-16` (A4–A9), and
its discipline is that every expectation is read out of a TypeScript or Rust source **before** the code
ran (`tests/routes.rs:5-8`), against a real in-process listener rather than a mock
(`tests/transport.rs:3-8`). It does not dial a live upstream, does not run inside the services binary,
and does not exercise Daytona, Lightning, the HTML dashboard, stats persistence or per-row request
shapes for all 29 rows.

Two pre-existing TypeScript failures are recorded so nobody files them as port regressions:
`packages/agentic/provider/src/local/local-basalt.test.ts:51` and `:71` assert zero costs while the
client carries `inputCost = 0.10` / `cacheCost = 0.01` / `outputCost = 0.60`
(`local/local-basalt.ts:118`, `:121`, `:124`). Re-run on this pass: 2 failed, 15 passed. The Rust row
prices the code (`.priced(0.10, 0.01, 0.60)`, `clients.rs:467`), so matching the stale expectation
would have broken parity — the test is what needs updating, in TypeScript.

## Citation drift and doc defects — now closed

Every item below was found by opening the cited line, and every one was re-checked on this pass
(2026-11-19) against the file it names. Unlike the earlier documentation pass that recorded them,
this pass owns the `.rs` files, so each defect has been corrected **in the file that cited it** —
the table states the corrected anchor rather than the stale one. The sweep that produced the list was
mechanical and re-runnable: the auditors in `Temp/ptp-fix/` (`audit-symbols.py`,
`audit-endpoints.py`, `audit-claims.sh`, `audit-authorities.sh`, `audit-cites2.sh`,
`fix-cites4`-`fix-cites6`) resolve each cited identifier or sentence in the cited file and report the
mismatch; the residual output after this pass is only names that are Rust builtins, trait methods,
TypeScript-side camelCase members verified separately, or numbers that are not line numbers.

| # | Defect, as first found | Verified truth | Corrected where |
| --- | --- | --- | --- |
| 1 | `lib.rs` attributed `write('data: [DONE]\n\n')` to `start.ts:1496-1501`, in a 1222-line file with no `DONE` anywhere | the sentinel is the **route's own** frame: `private-chat-completion.ts:163` yields `data: ${JSON.stringify(chunk)}` and `:186` yields `data: [DONE]` after clean exhaustion; the gateway turns each yielded string into a frame at `start.ts:853` (`await stream.write(chunk + '\n\n')`) | `src/sse.rs:28-34` and the `lib.rs` module table; `grep -rn 1496 src` is empty |
| 2 | `lib.rs` named three functions that do not exist in the gateway (`errorSentence`, `errorPayload`, `emitStreamError`) | the only exported frame builder is `buildStreamErrorPayload` (`start.ts:635-705`); the mid-stream frame is written inline in the `streamSSE` catch at `start.ts:860-873` (`console.error` `:861`, `writeSSE` `:866-871`), and nothing follows it — hence no `[DONE]` after an error | `src/lib.rs:14` now names the real function and says in words that the three others are NOT gateway functions; `src/serve.rs:643` keeps one such sentence at the catch site |
| 3 | `client.rs:145-159` justified the unported per-call `options.baseUrl` with `model-class.ts:573` | the file has no `baseUrl` identifier at all (the field is `baseURL`), `:573` is the 429 handler's `console.log`, and `create` takes one parameter (`:922-924`) | `src/client.rs:151-154` states the corrected evidence; the "not ported" conclusion stands |
| 4 | `lib.rs` cited `model-class.ts:567-572` for the `stream_options.include_usage` opt-in | the rule is **per client** in 32 files (samples `nvidia-glm5-flash.ts:76`, `daytona-qwen3-8.ts:498`, `lightning-glm-5-3.ts:112`) and `model-class.ts` has no `stream_options` | `src/lib.rs:12` now says so and names the samples; the Rust row flag `requires_usage_option` (`client.rs:345`) matches all 32 |
| 5 | `accounting.rs` said the two port counters are ignored by "the `/stats` projection" while `serve.rs` publishes them | rows omit them (`serve.rs:1098-1104`); `totals` publishes `attemptTotal` and `consecutiveFailures` (`serve.rs:1151-1158`) | the `accounting.rs` module doc (`:47-53`) now draws exactly that line |
| 6 | the client lineage table cited `withModel` `:895-897`, `create` `:905-907`, `switchBaseUrl` `:929-931`, and `serve.rs` cited `:753-755` for the Responses pass-through | `withModel` `:899-901`, `create` `:922-924`, `switchBaseUrl` `:933-935`, `createResponses` → `responses.create(this.withModel(body))` at `:747-749` | `src/lib.rs:17`, `src/client.rs:40-46` (the corrected `:707`/`:708`/`:588-619` seam cites) and `src/serve.rs` deviation 2 |
| 7 | TypeScript-side drift: `local/local-strata.ts:247` attributes its "[retry] HTTP 429" line to `model-class.ts:551`, which is blank (the line is `:573`) | as stated; the file is not this crate's to edit | left as found — recorded here so a future re-cite starts from `:572-576`; `src/transport.rs:290-292` records the same class of problem for the URL-rule cite |
| 8 | a cite worth keeping: `serve.rs` and `provider-route.rs` cite `start.ts:780` for "one listener per distinct declared port" | `start.ts:777-781` is exactly the port-grouping phase | unchanged — re-verified, no edit |
| 9 | the `RoutingVendor` doc listed eight `merge/merge-*.ts` call sites and then said "the **seven** names are the complete set" | eight call sites, **five** distinct vendors (`telnyx`, `pareto`, `modal`, `deepinfra`, `openai`) — which is why the enum has five variants | `src/lib.rs:17` ("eight `vendor:` injections (five distinct values)") and the `RoutingVendor` doc in `src/client.rs` |
| 10 | `error.rs` cited `start.ts:319-327` for `STREAM_ERROR_CODES` | the doc comment is `:317`, the `as const` array `:318-326` (the six codes `:319-325`), and `:327` is the `StreamErrorCode` alias | `src/error.rs:6` and `:83` |
| 11 | `package.json`'s metadata prose predated the crate: "NO [lib] bin yet", "axum/reqwest are NOT declared", "PORTED TODAY: `src/vault.rs`" | the route bridge exists and is mounted by `packages/typecrate/services`; axum and reqwest are declared; the transport, clients, registry and routes all exist — and the scripts carry `--locked` but **never** `--offline` | `package.json` was rewritten on this pass: `#layout`, `#rust`, `#locked`, `#scope` and `#artifact` now describe what ships, and `#rust`/`#locked` state that `--offline` is per-invocation, matching the sibling crates |

Two things this pass deliberately did **not** claim: the retry budget (row `transport.rs:77`/`:80` in
README's deviation table) and the SSE decoder's three one-directional leniencies (`sse.rs:54-80`) are
deviations, not citation errors, and they are recorded as such in README rather than "fixed" here.

## Cutover: what the vault side needs when this crate becomes the consumer

`config/vault/src/vault/grants.test.ts` is the regression guard that keeps
`VAULT_CONSUMERS['agentic-provider']` equal to what the consumer actually imports, and its scanner
target is `packages/agentic/provider/src` today. Its own header (`:22-26`) states that when the
provider is moved, renamed or ported away, **this file is what must be retargeted** at the new
consumer — for this crate, the site that reads
`config/vault/generated/agentic-provider.json` (`serve.rs:191-194`, `:442`). Until that retargeting
happens, adding a credential need to the Rust rows has no vault-side guard: the artifact would simply
lack the slot, and the registry would refuse the row with a problem naming it (`registry.rs:41-50`) —
loud, but only at first request.

## What a later stage must not re-break

- The **ordering** chain: artifact `entries[0]` → pool head → `CLIENTS` array order → `/models` order,
  with `/stats` re-sorting by id on its own projection only
  (`vault.rs:49-55`, `clients.rs:10-19`, `registry.rs:14-20`, `serve.rs:993-1011`).
- The **verb gate runs before the vault gate**, and both run before any body is read
  (`serve.rs:756-767`, `:884-892`, pinned by
  `tests/routes.rs::the_advertised_any_verb_is_refused_before_readiness_on_every_route`).
- A **non-object JSON body is a 404, never a 400**: a body that fails to parse is folded into `{}`
  by the reference's own `catch`, and a top-level array or scalar it keeps INTACT — either way the
  handler resolves no `model`, so the route's own lookup answers the 404 (`start.ts:815`,
  `private-chat-completion.ts:65-66`). `null` is the one shape that still 500s, because the
  reference destructures `null` inside the handler and that throw reaches the gateway catch
  (`start.ts:908-911`). Validating the body shape, or letting `null` fold too, is the regression
  (`src/serve.rs:793-838`, pinned by
  `tests/routes.rs::a_body_that_is_not_a_json_object_resolves_no_model_and_is_a_404`
  and `::a_null_body_is_the_one_shape_the_gateway_does_not_fold`).
- An **in-band error frame carries no `status` key**: the SDK builds
  `new APIError(undefined, message, undefined, undefined)` (`streaming.js:50`, `error.js:12`), so
  the status that was on the frame stays a retained diagnostic that never reaches the wire and has no
  reader in `src/` (`FailureSignals::frame_status`, `src/error.rs:1035`). Promoting it into the
  payload is the regression
  (`tests/transport.rs::a_mid_stream_error_frame_with_a_status_stays_unknown_and_carries_no_status`).
- The **SSE decoder is one event, one payload**: multi-line `data:` lines join with `\n` and a
  payload that *starts with* `[DONE]` ends the stream (`src/sse.rs:164-165`, `:242-256`), because
  that is what the SDK decoder does (`streaming.js:261`, `:35`). Splitting the stream on `,`
  instead of `\n\n`, or demanding an exact `[DONE]`, is the regression
  (`src/sse.rs::several_data_lines_of_one_event_are_one_payload_joined_with_a_newline`, with the
  prefix rule pinned by `src/sse.rs::the_sentinel_prefix_ends_the_stream_even_with_text_after_it`).
- **Bounded retry is a deviation, not parity.** Never describe `retry.max_retries` as
  OpenAI-SDK-equivalent: the JS path retries 429 forever (`model-class.ts:572-576`) and the
  gateway never retries at all, because the SDK forces `maxRetries: 0` per request
  (`model-class.ts:885-892`).
- The **prompt estimate is over the caller's body**, the request bytes are the rewritten payload
  (`client.rs:39-54`).
- **`requestTotal` counts rotations once** and counts failures (`accounting.rs:106-111`,
  `client.rs:68-70`).
- **Exactly-once settlement** across exhaustion, failure, cancellation and abandonment
  (`transport.rs:102-112`).
- **`Cargo.lock` stays a subset** and axum stays feature-default (`Cargo.toml:146`), or the sibling
  service build and the offline build both move.
- The **no-`charset` JSON content type** and the two SSE headers, byte for byte
  (`serve.rs:227-235`, pinned by `tests/routes.rs`).
- **No credential in any printable path** — `Secret`, the label allow-list, the redaction gates, and
  the two suite-level leak tests (`vault.rs:153-164`, `client.rs:1024-1059`, `error.rs:62-69`,
  `tests/transport.rs::no_credential_reaches_a_diagnostic_or_the_wire_detail`,
  `tests/clients.rs::no_credential_value_reaches_the_failure_vocabulary_or_the_debug_output`).

## Staleness in the Rust top-level docs (reported, not edited)

`rust/README.md` and `rust/MIGRATION.md` were read for the rules this crate follows; two of their
statements no longer describe the repo, and this pass did not touch either file:

1. `rust/README.md:3-5` says "**No TypeScript has been migrated, and no Rust has been compiled** —
   `cargo` is not installed in this environment". Three typecrate crates now compile and test with
   cargo (`packages/typecrate/services`, `packages/typecrate/harness`, and this one), and the harness
   README records a full clean gate run (`packages/typecrate/harness/README.md:260-267`).
2. `rust/MIGRATION.md:185-192` ("Marking a new package") describes only the
   `rust/crates/<scope>-<name>/` + workspace-inheritance flow (steps 1-4, including
   "add its path to `[workspace.dependencies]` so other crates use `.workspace = true`"). That is not
   the shape the three typecrate crates use — a standalone `[workspace]` at the npm directory with
   every field restated (`Cargo.toml:1-25` here) — and `rust/README.md:65-70` does describe the
   standalone escape hatch, so `MIGRATION.md`'s steps are the half that is stale.
