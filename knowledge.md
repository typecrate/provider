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
`src/transport.rs:593` records a "DEVIATION FROM **C2**", and `tests/registry.rs:1` labels the
registry suite "(**C3**)". The map below is read off those labels, the module docs and the
acceptance letters they cite.

1. **C1 (done) — the vault substrate.** `src/vault.rs`: the artifact loader, the five validity
   classes, the classifier, `Secret`, order-preserving pools, all-problems-at-once refusal. Its scope
   note is `package.json` `#scope` ("PORTED TODAY: `src/vault.rs`"), and its contract is
   `config/vault/knowledge.md` "Rust consumption contract".
2. **C2 (done, then corrected) — the outbound transport.** `src/transport.rs` + `src/sse.rs` +
   `src/error.rs`: pools behind forward-only atomic cursors, the retry/429/404 rules, the SSE codec,
   the failure taxonomy. The correction is recorded in-code at `transport.rs:593` (keyless headers
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
6. **C6 (done) — the crate's own process.** `src/bin/server.rs` is the `[[bin]]`
   `typecrate-provider-server` (`Cargo.toml:70-72`): flags, the credential gate, one bind, the banner,
   the shutdown signal and a hand-written `tracing` subscriber. To make that host stand alone,
   `src/serve.rs` also published its gateway half — `with_gateway_headers` (`:758`),
   `is_cors_preflight` (`:786`), `gateway` (`:803`) and the health document — so a container gets the
   cross-origin policy and the preflight `204` without linking `typecrate-services`. Pinned by
   `tests/bin_server.rs` (19) and `tests/gateway.rs` (8).
7. **C7 (done) — pushing that process to a cloud container.** `scripts/deploy-endpoint.ts`,
   `scripts/start-modal.ts`, `scripts/start-daytona.ts` (+ `scripts/entry-guard.ts`) are `vite-node`
   verbs over the **shared** client `@services/daytona/sandbox` (`services/daytona/sandbox/runtime-deploy.ts`),
   i.e. the same client nexus uses; `build:server` / `start:server` / `deploy:endpoint` / `start:modal` /
   `start:daytona` / `typecheck` / `test:scripts` are the npm verbs. This is the first time this package
   has had npm **dependencies** (`@config/vault`, `@services/daytona`) — which is what the
   `.github/workflows/ci.yml` header now has to be careful about.
8. **Still to port** (each is listed in the crate's own "still to port" block, `src/lib.rs:29-38`):
   the Daytona sandbox substrate behind `daytona/daytona-qwen3-8.ts` (S1 in the README ledger), the
   Lightning Chat↔Responses adapter (S2), and the `/stats` HTML dashboard with its cross-session
   buffer (S3, `runtime/endpoint/provider/private/stats/stats-buffer.ts`). Nothing else in the crate
   is a placeholder. **Do not read item 8 against item 7:** hosting *the provider itself* on Daytona (a
   `start:daytona` sandbox) has nothing to do with the `daytona/qwen3.8-27b` **row**, whose upstream base
   URL only exists after a GPU-sandbox boot — the row stays unported and `/models` answers 15 rows.
9. **Open parity decisions** a later stage has to settle, not implement blind: whether a drained row
   should drop its credential header (the drain markers in `tests/clients.rs:1242` and
   `tests/registry.rs:1052`), and whether JSON-only `/stats` is the target shape or a stopgap.

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
  `license` (`:40`), `publish` (`:41`), `[lints.rust]` (`:229-230`) and `[profile.release]`
  (`:237-242`) are restated literally. A maintainer who "simplifies" one of those into inheritance
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
  features = ["std"]` (`Cargo.toml:91-100`) — dropping the `attributes` feature avoids
  `tracing-attributes`, which appears in neither sibling lock — and `axum = "0.8"` with **default
  features** and no `sse` feature (`Cargo.toml:189-205`), because the SSE response is
  `Body::from_stream` + two headers (`serve.rs:568-579`). `tests/transport.rs`'s
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
  `packages/agentic/provider/src` — see "Cutover" below for what still has to move now that this
  crate is the consumer.
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
(`Cargo.toml:229-230`). The lazy `OnceLock` state (`serve.rs:414-435`) also installs the registry
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

- **Per client, not per family** (`client.rs:124-142`). TypeScript keeps its rotation queue at module
  scope (`nvidia/nvidia-glm5-flash.ts:9`, `makora/makora-glm-5-3.ts:5`), so four Nvidia clients share
  one queue and a key burned by one sibling is gone for the others. The Rust transport owns the cursor
  and takes its target by value, so each row rotates its own copy. Order is unchanged (document order,
  never re-sorted); drain *budget* is not: each row drains after its own `len` rotations. A later
  stage must not "fix" this into a shared `Arc` without re-deriving the drain tests.
- **`NextOnEveryRotation` vs `SameThenNext`** (`client.rs:110-122`). NVIDIA shifts the head and returns
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
  `client.rs:144-164`) and both stay routable; only the header differs. Pinned at
  `tests/clients.rs:1242` and `tests/registry.rs:1052`.

### The 404 failover boundary - one shared head, one pinned walk per request

`transport.rs:84-93` returns a 404 as a **typed** verdict and never retries it
(`TransportFailure::is_not_found`, `transport.rs:938`); `client.rs:56-108` states the rule and
`client.rs:856-989` (`dispatch`) runs it, because in TypeScript `switchBaseUrl()` is called by a 404
status handler inside a `while (true)` around `fetch` (`model-class.ts:933` is the method; the wrapper
is the `:557-654` region). Two consequences that must not be re-broken: the rotation loop is **not**
bounded by the retry budget (the SDK's own retries are neutralised upstream with `maxRetries: 0`), and
the accounting wrapper sits **outside** the loop, so a request that rotates five times still counts one
`requestTotal` and settles its prompt split once. The exhausted case is the route's sentence —
`transport::EXHAUSTED_BASE_URLS_MESSAGE` (`transport.rs:204`) — which is the text the Responses route
throws by hand today (`runtime/endpoint/provider/private/responses/private-responses.ts:118`).

**What the 2026-11-25 concurrency fix changed, and why the shape is now load-bearing.** Advancing ONE
shared endpoint cursor by compare-and-swap is correct for one request at a time and wrong for ten:
every concurrent 404 moved the shared selection one place for itself, so ten requests failing on the
same dead relay skipped nine endpoints NOBODY had dialled, and each skipped entry became the upstream
some request adopted without ever collecting a verdict from it. The loop is therefore per request:

- **start** — `position = min(head, last)` is resolved ONCE at dispatch
  (`Transport::endpoint_start`, `transport.rs:1281`, called at `client.rs:876`);
- **pin** — every attempt dials `pool[position]` and hands that entry to the transport as
  `RequestOptions::endpoint` (`transport.rs:1077`, `with_endpoint` at `:1145`), and `Transport::run`
  resolves the dial target exactly once, BEFORE its retry loop (`transport.rs:1483-1498`). The 429
  pause, the timeout and the dead-socket retries therefore cannot move a request to another upstream;
- **verify** — a 404 from `pool[position]` publishes `head := max(head, position + 1)` with a
  `fetch_max` (`Transport::verify_endpoint`, `transport.rs:1308`) and steps that request to
  `position + 1` (`ModelClient::advance_walk`, `client.rs:1073-1123`); `position > last` is the
  exhaustion, where the row records the failure and propagates the raw 404 (`client.rs:929-954`);
- **nothing else moves the head** — `advance_walk` is the only production writer.
  `ModelClient::switch_base_url`'s relay arm (`client.rs:1048`, `:1061`, the TS `switchBaseUrl` parity surface)
  and `Transport::rotate_endpoint` (`transport.rs:1351`) survive as documented **seams** that publish
  one step past the entry the SHARED head names, and `Transport::reset_rotation`
  (`transport.rs:1400`) is a rewind no production path takes. All three say so in their own docs, so a
  reader cannot mistake them for the request path.

The invariant, and the price: for every `j < head`, `pool[j]` was PERSONALLY 404-dialled by some
request, and each request's own dial sequence is contiguous — so no endpoint is skipped unverified at
any level of concurrency. Concurrent requests consequently walk in **lockstep** and dial the same
relays; that is accepted deliberately, because spreading across unverified relays *is* the bug.
Verification is the real dial's verdict: no pre-flight health probe was invented, and the 404-only
trigger, the retry budgets, the accounting and the credential axis are untouched. The credential axis
keeps the claim-a-slot cursor `advance` returns (`transport.rs:1827`, handing back the slot it claimed
rather than a re-read), which is right for a different job: rotating a key takes it out of the pool for
EVERY caller, so each rotation must hand its caller the slot it claimed.

**Attribution follows the dial, not the process.** `dispatch` builds a `ServedEndpoint`
(`client.rs:471-508`) from the SAME entry it puts on the wire and returns it on the answer
(`UpstreamAnswer`, `client.rs:510-530`); `serve.rs:646-653` (`RequestLog::capture`) logs from it, so
`relay=` and the `[failover]` lines name a dial that happened even when another request rotates in
between (`client.rs:1111-1113`). `Transport::upstream_label` (`transport.rs:1325`) and
`ModelClient::upstream_label` (`client.rs:690`) stay as the TS-parity **state** read of the shared
head — correct only while the process runs one request at a time, and their docs now say exactly that
instead of claiming the deployed capture rule outright.

Pinned by `tests/clients.rs::concurrent_requests_walk_the_verified_prefix_and_never_skip_an_endpoint`,
`tests/clients.rs::concurrent_requests_all_walk_a_fully_outaged_pool_and_refuse`,
`tests/clients.rs::a_second_request_starts_where_the_first_one_proved_the_pool_dead`,
`tests/clients.rs::the_switch_base_url_seam_publishes_one_step_per_call`,
`tests/transport.rs::a_pinned_request_dials_the_pin_and_never_the_shared_selection`,
`tests/transport.rs::a_pinned_retry_stays_on_the_pinned_endpoint`, the two inline contentions in
`src/transport.rs` (`advance_hands_each_racer_the_slot_it_claimed`,
`concurrent_verifications_of_one_entry_publish_one_head`), and the timing-free capture case
`src/serve.rs::the_request_log_names_the_endpoint_the_answer_carried_not_the_shared_selection`, which
is the only case that DISCRIMINATES the capture mechanism: it publishes the shared head two entries past
the serving endpoint, so a log that re-read the selection prints `relay-c` while this answer must print
`relay-a`. Every live case answers with the relay the head already names, so none of them can tell the
two implementations apart. The concurrency regression was written FIRST and observed RED against the
unmodified crate before any source line changed.

Related: `rebaseRequestInput` (`model-class.ts:141`) has **no** Rust counterpart, because the retry
rebuilds its URL from the endpoint the request pinned instead of string-patching the previous
attempt's URL (`transport.rs:1483-1498`).

### Retry policy is bounded, and spends the budget differently

`transport.rs:95-117`. The deployed rule, re-verified line by line on this pass rather than assumed: the
429 status handler pauses 3 s and returns `true` with NO attempt counter (`model-class.ts:572-576`
inside the `while (true)` fetch wrapper at `:588-619`), so a relay that always answers 429 holds
a request open indefinitely — the 30-minute per-attempt ceiling (`:624`) does not stop it, because
each attempt restarts the clock. The client is BUILT with `maxRetries: 3` (`:628`) but every
request forces `maxRetries: 0` (`:885-892`), so the SDK's own retries never run, and a status with
no handler is returned un-retried (`:599-600`) — a dropped socket fails on the spot today.

This port bounds EVERY retryable outcome to `1 + max_retries` (`MAX_RETRIES` = 3,
`transport.rs:187`) and, in exchange, spends the budget on the connection and timeout failures
the deployed provider surfaces immediately — justified by the TypeScript's own comment at
`model-class.ts:625-627` (the budget exists for transient drops when an idle proxy connection
is reaped mid cold-start).
The 3 s pause is injectable (`transport.rs:614-620`) so a test proves the pause happened without
waiting for it. **Retry behaviour is explicitly not claimed to match the TypeScript**:
`tests/transport.rs` pins the bounded count, the 404 rotation stop and the never-retried arms
instead of a parity match, and README's deviation rows `transport.rs:95-110` / `:110-117` state the two
asymmetries an operator feels.

### Keyless is a policy, not a constant

`transport.rs:118-131` and `:578-605`. Selecting the `none` sentinel must mean both "this endpoint needs
no credential" (a **usable** state: the model stays discoverable,
`model-class.ts:32-35` and `:378`) and "here is what reaches the wire". The TypeScript is not one
behaviour: `openai@6.42.0` builds the header from `apiKey` for anything that is not `null`
(`node_modules/openai/client.js:233-241`), so every client with `apiKey: 'none'` sends
`Authorization: Bearer none`, while the two local relays additionally pin
`defaultHeaders: { Authorization: null }` (`local/local-strata.ts:161`, `local/local-basalt.ts:147`)
and send nothing. `KeylessHeaderPolicy::SendNone` is `#[default]` (`transport.rs:600`) because that is
what today's relays actually receive; `Suppress` (`:604`) is per-row and used by exactly
`ClientSpec::keyless_relay()` (`clients.rs:97-103`). This is a **correction**, not a preference: the
first cut of the module sent nothing for every keyless credential, which was wrong on every keyless
row except the two relays (`transport.rs:127-131`, `:593`).

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
spelling (`transport.rs:1854`, and `tests/clients.rs::stream_options_reaches_only_a_streaming_request_
that_needs_it`).

### Settlement without `finally`

TypeScript settles a stream in `trackStream`'s `finally` (`model-class.ts:831`, folding through the
block at `:859-878`), which runs on exhaustion, on a mid-stream throw **and** on abandonment. Rust has
no `finally`, so `transport.rs:132-142` settles in two places — the terminal transition in `poll_next`
and `Drop` for abandonment — funnelled through one hook that is *taken*, so it fires exactly once, and
`Drop` additionally cancels the request's child `CancellationToken` so the upstream body stops being
read. Four paths are pinned by name in `tests/clients.rs` (`a_streamed_success_settles_exactly_once`,
`an_undecodable_frame_settles_the_stream_as_a_failure_once`,
`an_abandoned_stream_settles_once_and_is_not_a_failure`, `a_cancelled_stream_settles_once_as_a_failure`)
and in `tests/transport.rs::the_settlement_fires_once_for_clean_failure_and_cancellation`.
`transport.rs:2108` records the one judgement call: an undecodable frame is a fatal stream whose
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

**An in-band error frame's declared status RESCUES a failure nothing else named (deviation,
2026-11-24).** An error that arrives INSIDE the stream (`data: {"error":…}`) is read by the SDK, which
throws `new APIError(undefined, data.error, undefined, response.headers)`
(`node_modules/openai/core/streaming.js:50`); that constructor assigns `this.status = status`
(`node_modules/openai/core/error.js:12`) — the `undefined` it is handed — so the object the deployed
TypeScript gateway classifies has **no numeric status at all**, whatever the upstream wrote in
`error.status`, `readUpstreamStatus` (`start.ts:506-517`) finds nothing, and the frame falls through
to `unknown` with the frozen sentence (`start.ts:685`) and no `status` key (`:699`). This crate used
to reproduce that loss byte for byte. It no longer does: an in-band error object is a real upstream
verdict, and the consumer's retry decision reads a status
(`providerWireFailureOf`, `packages/underload/harness/src/engine/transport/provider-timeouts.ts:666`),
so `FailureSignals::from_upstream_frame` lifts a valid numeric `error.status` and, failing that,
infers one from the size refusal the frame's sentence or `error.code` names or from the error CLASS it
names (`UPSTREAM_TYPE_STATUSES`: `server_error` 500, `model_busy` 409, `invalid_request_error` 400,
`authentication_error` 401, `forbidden` 403, `not_found` 404, `rate_limit_error` 429,
`context_length_exceeded` 400; anything else infers nothing, and a number always wins). What the
correction of the same day fixed is WHERE that value may be used: the three reads go into a fifth
field, `FailureSignals::declared_status`, and only rule 9 of `classify_stream_error` consults it — the
`unknown` fallthrough. The parity field `status` keeps its TS meaning, a status some layer OBSERVED on
a response head, which is why rule 3 (`start.ts:601`) still reads it alone and nothing else. Consequence,
in both directions: `{"type":"server_error","message":"upstream stream terminated"}` — and the same
frame with `status: 503` beside the sentence — is still `connection` with NO `status` key, exactly as
before the deviation, while a frame nothing named becomes `upstream_status` with a real `status` on the
wire and the `unknown` class is the narrow residue that is left — whose sentence still LEADS with
`UNKNOWN_STREAM_ERROR_MESSAGE` (the prefix consumers string-match) and now TAILS the upstream's own
detail, because a relayed size refusal whose wording was thrown away was unreadable. WHY the ordering
is load-bearing rather than cosmetic: the consumer reads `code` before `status`, wire `connection` gets
the short, quickly-terminal dial ladder (`provider-timeouts.ts:714-721`, `:101-106`) and
`upstream_status` under a non-terminal 5xx falls to the codeless 1 + 10 transient ladder
(`:761-790`, `:118-125`) — letting the declared number win bought eleven extra attempts for a failure
the transport had already diagnosed, which is the rule the module header names "never demote a named
transport verdict into a longer retry ladder". `StreamFailure::frame_status` survives every case where
the emitted status and the number the frame wrote legitimately differ — a status rules 1-8 withheld
(errno-shaped `code` OR a wording verdict), and an inferred status the frame never sent — and
`StreamFailure::log_line` prints whichever applies. A `status` is still stamped by a PRE-stream HTTP
status (`TransportError::Status`, the gate at `start.ts:644`). Pinned by
`tests/transport.rs::a_worded_in_band_frame_keeps_its_code_and_emits_no_status` (the named-verdict
half, including the exact wire key set with no `status` member, and the withheld number on the operator
line), `::a_frame_nothing_names_is_rescued_by_the_status_it_declares`, and at the ladder level by
`::the_classifier_respects_the_precedence_ladder`; the accepted and refused `error.status` shapes —
`as_u64`, an integral `as_f64`, and a trimmed ASCII-digit string, since `Value::as_u64` alone dropped
the two shapes real proxies write — are pinned by
`::a_declared_status_is_read_from_every_shape_the_consumer_reads`.

**A message naming a size refusal is a 400, ahead of the class table (deviation D7, 2026-11-24).**
The class table alone answers the local relay's context refusal with **500**, because the relay stamps
`server_error` on every engine `ERR` line (`Temp/basalt-check/serve/handler.py:736-738`) — and a 500
is a verdict the consumer can only retry, which is the wrong remedy for a prompt that is too long:
`isContextOverflowSignal` (`packages/underload/harness/src/engine/transport/provider-timeouts.ts`)
refuses to read matching prose as an overflow behind a 5xx, so the request walks the whole transient
ladder for a fault only a shorter request fixes. So `status_declared_by_size_refusal` sits BETWEEN the
numeric status and the class table and answers 400 when the frame's MESSAGE or its `error.code`
contains any of the four case-insensitive `CONTEXT_OVERFLOW_MARKERS` (`context_overflow`,
`context_no_room`, `exceeds the context`, `leaves no room to answer`) — the two coded words are matched
in BOTH places, because the structured error bodies the same fault produces elsewhere carry them as a
code (`Temp/basalt-check/serve/api/responses.py:42-43`), and reading only the message used to answer
`{"type":"server_error","code":"context_overflow"}` with the class table's 500. WHY 400 IS NOT AN INVENTION: the upstream answers
this identical sentence as a client error on its pre-head path — the overflow text is a `ValueError`
and the handler's `ValueError` arm writes 400 / `invalid_request_error`
(`Temp/basalt-check/serve/handler.py:551-555`), the `/v1/responses` arm labels the same fault
`context_length_exceeded` (`:774-776`), and the sentences themselves are `room_error` /
`overflow_error` (`Temp/basalt-check/serve/prompt.py:15-28`); the mid-stream arm that relays it
in-band carries the comment "engine ERR after the stream started: no 400 now" (`:736-738`), which is
about the transport being unable to re-send a status, not about the request's fault. Precedence is
therefore `error.status` → size refusal → `error.type`, the number first in both directions: where the
upstream states a verdict this crate repeats it, so `{"status":500}` beside overflow wording stays 500.
Two limits are deliberate and pinned: the coded markers rarely fire on the served path, because the
relay strips the code word out of the message (`Temp/basalt-check/serve/engine/process.py:178-179`),
and the per-slot sentence `… exceeds slot S's context (C)` is NOT matched — `prompt.py:86-95` passes
that one through un-rewritten and it contains neither needle, so it keeps the class table's verdict.
Pinned by `tests/transport.rs::an_in_band_context_overflow_frame_keeps_its_wording_on_the_wire` and
`::a_size_refusal_wording_infers_only_a_400_and_a_number_always_outranks_it`, and end to end on the
wire by `tests/routes.rs::a_mid_stream_context_overflow_frame_is_a_400_that_keeps_its_wording` (which
also pins that a frame writing `status: 500` beside the same sentence keeps its 500), and the `code`
half by `tests/transport.rs::a_size_refusal_marker_written_in_error_code_is_a_400`. The consumer's
gate is `isContextOverflowSignal(status, detail)`: it accepts `413`, or `400` + wording, or NO status +
wording, and rejects every other status whatever the prose says
(`provider-timeouts.ts:467-473`). So the 400 is what turns a RETRY into an actionable verdict — but
"unlocks the compaction path" needs the qualifier: the gate's own `CONTEXT_OVERFLOW_PATTERNS`
(`:424-433`) is NARROWER than this crate's four markers, so a sentence only we recognise (the engine's
raw `prompt (N tokens) + max_new (M) exceeds the context (C)`) gets the 400 and lands on the consumer's
terminal `provider-rejected` (`:751-758`) — a terminal refusal in place of eleven retries, not a
smaller prompt. Only wording in BOTH lists becomes compaction, and the gate's wording-alone arm means
the absence of a status was never what hid an overflow it could read.

The three tests that pinned the old rule were rewritten for the new contract, deliberately:
`tests/transport.rs::a_mid_stream_error_frame_with_a_status_is_an_upstream_status_frame`,
`tests/routes.rs::a_mid_stream_error_frame_with_a_status_is_an_upstream_status_frame` and
`tests/transport.rs::the_frame_contract_names_the_target_the_phase_and_the_detail`; the residue and
the inference table are pinned by `an_in_band_server_error_frame_becomes_an_upstream_status_failure`,
`an_in_band_error_class_infers_only_a_listed_status_and_a_number_always_wins`,
`a_status_a_stronger_rule_withholds_is_still_named_in_the_log`,
`an_in_band_context_overflow_frame_keeps_its_wording_on_the_wire` and
`a_content_free_in_band_error_frame_degrades_without_a_new_code`. The 2026-11-24 corrections moved two
more expectations, and each was a genuine encoding of the behaviour being replaced: the residue frames
in `tests/transport.rs::a_mid_stream_error_frame_without_a_status_is_an_unknown_code` and
`tests/routes.rs::a_mid_stream_error_frame_is_one_frame_and_no_sentinel_follows_it` carried
`rate_limit_error`, which joined the status table on this date, so they now carry
`insufficient_quota` — a real OpenAI class the table still refuses to price — and the unmapped-class
example in `an_in_band_error_class_infers_only_a_listed_status_and_a_number_always_wins` moved from
`rate_limit_error` to `usage_limit_exceeded` for the same reason.

### SSE and JSON: key order is contract

`serde_json`'s `preserve_order` feature (`Cargo.toml:78-81`) is what lets `serve.rs` state envelope key
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
- **A real CORS preflight never reaches these handlers** *on the services gateway*: that gateway layer
  answers it with `204` before routing
  (`packages/typecrate/services/src/server/mod.rs:328-337` explains why the predicate needs
  `Access-Control-Request-Method`, `:386-389` is `is_cors_preflight`, and `:398-405` is the `gateway`
  layer that returns `StatusCode::NO_CONTENT` at `:402`). A bare `OPTIONS` does, and gets the `400`.
  The second host does the same thing with **this crate's own copy**: `serve::gateway`
  (`src/serve.rs:803-809`, predicate `:786-791`, headers `:758-780`) is mounted outermost at
  `src/bin/server.rs:580`, because a container payload cannot link the services router — see the next
  section, and the dedup follow-up at the end of this file.

### The standalone host, and pushing it as an anonymous payload

The crate now has two hosts, and the reasoning for the second one is entirely about containers. README
"Deploying it" carries the operator-facing version; the records here are the ones a later stage must not
quietly re-litigate.

**Crate side (C6).**

- **A bin even though the rlib already has a host.** A cloud payload is ONE executable in ONE stage
  directory; pulling `typecrate-services` in would drag its generated route table, its playground
  builtins and its `/v1/harness` gateway along, and would mean a container running Node or a second
  binary to serve a provider (`src/bin/server.rs:10-20`). The dependency edge is one-way
  (`bin → lib`, `:22-27`), so an embedder that runs the route file never starts this executable — the
  two hosts are alternatives, never stacked.
- **The credential gate is FAIL-FAST here and SOFT in the library, deliberately.** The library answers
  503 while the vault is missing (`src/serve.rs`, "the deviation the wire cannot avoid") because it is
  compiled into a process that also serves `/v1/harness`. A container runner probes the app's
  `probePath` (the deploy sends `/health`; spec §8.7), so a soft listener would pass readiness while
  serving nothing but 503s and a bad credential would become a silently-degraded deployment; the bin
  therefore runs the gate **before the bind** and exits 1 with the regeneration command
  (`src/bin/server.rs:29-42`). `--allow-unconfigured` restores the lazy 503 for the cases where booting
  empty is what the operator asked for. This is the single behaviour the bin *adds* to the contract, and
  it is why a deploy does not need a provider-authored gateway route: the bin's own root `/health` is
  the probe target, and it is deliberately NOT part of the `/providers/private/v1` native surface.
- **Exit codes 0 / 1 / 2, and a hand-built tokio runtime.** The sibling convention
  (`packages/typecrate/services/src/bin/server.rs:251-254`): a supervisor restarts 2 and fixes 1. `main`
  builds the runtime by hand and calls `std::process::exit` because
  `[profile.release] panic = "abort"` would otherwise turn a `main -> Result` into an abort status
  instead of the promised code (`src/bin/server.rs:44-50`). An unparseable `PORT` or
  `TYPECRATE_PROVIDER_HOST` counts as **usage** (2), not a server fault: it is an operator-typed value
  (`:119-121`).
- **Default bind is `0.0.0.0`, not the harness bin's `127.0.0.1`** — a container has no client on
  loopback; the probe and the caller both arrive on an interface address (`:96-99`).
- **`GET /health` is bin-only on purpose** (`:109-112`): it is not one of `serve`'s four contract
  routes, and a fifth path constant in the library would change what `packages/typecrate/services`
  **lists** for this package. Consequence an operator has to know: on a Modal box the runner's own routes
  shadow the service's at `/`, `/health`, `/tools`, `/stats`, `/status`, `/logs`
  (the adapter's `THE ENDPOINT IS ONE ORIGIN` block, `modal-sandbox-container.py:10-17`: the box
  reverse-proxies ONE port and the runner owns every path), so that path is not a reliable probe of
  *this* payload through a box.
- **The `tracing` subscriber is hand-written** over `tracing` + `tracing-core`
  (`src/bin/server.rs:64-76`), because `tracing-subscriber` is in no typecrate lockfile and adding it
  would break `tests/transport.rs::the_lock_package_set_is_a_strict_subset_of_the_siblings` — the same
  pressure that keeps axum feature-default (S7). Without a subscriber, `tracing`'s `NoSubscriber`
  answers `enabled(..)` with `false` and the crate's token/failover/retry lines are thrown away.
- **`pub(crate)` in a binary, and a `#[path]` include from `tests/bin_server.rs`.** Flag/environment
  precedence, the gate, the banner labels and the log filter are pure functions so the test target can
  assert them directly; edition 2024 makes `std::env::set_var` unsafe and this crate forbids `unsafe`
  outright, so a test feeds values to the resolver rather than mutating the process environment
  (`src/bin/server.rs:52-62`, the sibling precedent at
  `packages/typecrate/harness/src/bin/server.rs:527-539`).

**Verb side (C7) — what the payload is, and why it is anonymous.**

- **No `x-runtime-profile` header at all**, i.e. the container's `generic` profile
  (`services/daytona/sandbox/runtime-server/src/contract.rs`): the allowed config keys are exactly
  `runCmd, port, probePath, probeTimeoutSec, stopSignal, stopGraceSec, buildCmd, buildTimeoutSec,
  artifactPath, env`, the defaults are `SIGTERM` / `60 s` / an auto-allocated port, and the profile adds
  no key, no signature and no registry lookup. A tar.gz **must** name `runCmd`, because an archive can
  hold anything and the runner refuses to exec blind (`contract.rs`; the client validates it too).
- **The app name and native prefix are TOP-LEVEL metadata, not config.** `RuntimePayload.name` is the
  registry key and `targetPrefix` the native base path (spec §4.7/§8.5); the deploy passes
  `name: 'provider'` EXPLICITLY (omitting it registers `default` at `/app/default/`) and
  `targetPrefix: '/providers/private/v1'`, which is why `/app/provider/models` reaches the crate's own
  `/providers/private/v1/models`. They travel as `x-app-name` / `x-app-target-prefix` for a binary
  upload; neither is a `config` key.
- **Readiness is the bin's own `/health`, probed DIRECTLY on loopback.** The v2 runner probes
  `http://127.0.0.1:<internal port><probePath>` (spec §8.7), so `probePath: '/health'` reaches the
  provider process itself. It must NOT be routed: `/app/provider/health` would become
  `/providers/private/v1/health`, a path the crate does not serve — the usage text and the run summary
  explicitly deny that alias instead of advertising it. (`--probe-path` still allows another loopback
  route.)
- **Credentials are a bundle member, never a `config.env` value.** v2's `AppView` exposes only
  `envKeys` and redacts env VALUES (spec R-5.1/R-5.2) while the full config is persisted in `app.json`,
  and the control plane is only as protected as the runner's admin auth
  (`modal-sandbox-container.py:48-62` `ADMIN AUTH`, and `_print_recipe`'s auth note at `:2097-2109`:
  with no `SANDBOX_ADMIN_TOKEN` set, "anyone with this URL can deploy"). A sandbox gets the token as a
  Modal Secret (collapsed with the runner config on a client without `env`), and every `/health` gate
  requires `authRequired == (an admin token is configured here)`. So a credential in
  `config.env` is enumerable metadata (the key alone names it) and is never the right carrier. The
  anonymity is bidirectional — the container does not know what it runs, and it does not authenticate
  the caller — and the payload's authentication is therefore its own business: the vault document rides
  inside the tar.gz and `env` carries only `TYPECRATE_PROVIDER_VAULT_PATH` as a **relative** path
  (`./agentic-provider.json`, resolved against the runner's stage dir, which is the child's cwd —
  `src/bin/server.rs:156-158`).
- **`config.port` is OMITTED by default; `PORT` is RESERVED.** v2 auto-allocates the internal port and
  injects it as `PORT` (spec §8.3), which the bin honours; `--port <n>` pins one and `--port 0` is the
  explicit spelling of auto. A user `PORT` is a hard `400 config_invalid` (R-8.3/R-8.4 — the v1
  decoupled bind/probe warning is gone), so `reservedEnvErrors` in `scripts/deploy-endpoint.ts` refuses
  `--env PORT=…` at parse time with the fix (`--port`). `config.env` also carries
  `TYPECRATE_PROVIDER_HOST=127.0.0.1` (R-8.5): the gateway is the only way in, and no other interface
  exposes the provider. The internal port in any summary is the registry's loopback port, never a
  public one.
- **`stopSignal: SIGTERM` is safe because the bin drains.** SIGTERM/SIGINT/Ctrl+C stop it after
  in-flight requests finish, with exit 0 (`src/bin/server.rs:186-188`); an anonymous payload's default
  stop signal is SIGTERM precisely because a stranger cannot be assumed to have nexus's worker-pool
  shutdown (`contract.rs`).
- **The endpoint is an argument, never a package constant.** `start:modal` resolves
  `--endpoint`/positional > `TYPECRATE_PROVIDER_ENDPOINT` > `@config/vault` `modalContainer`, while
  nexus's `start-modal.ts:25-26` pins the vault value with no override. The reason is concrete: the box
  this repo owns (`config/vault/src/container/modal-container.ts:1-3`) already serves another workload.
  Under v2 that is no longer a conflict — the registry is multi-app, so the provider registers as
  `provider` beside the peers and a blue-green PUT replaces only that app — but WHICH box receives it is
  still an operator decision (resources, admin auth, blast radius), and a second box has its own
  `https://<source>--<BOX_LABEL>.modal.run` (`modal-sandbox-container.py:343-345`, the `BOX_LABEL`
  assignment with the comment that says why it is env-overridable). The
  wrapper prints which source won, for the same reason. Peers are preserved because the verb never
  touches `/`, a `default` alias, or `DELETE /_provider/sandbox`.
- **Every side effect is injected, and importing a verb starts nothing.** The verbs are pure functions
  over `ProviderDeps` (`scripts/deploy-endpoint.ts`) and `DaytonaDeps`
  (`scripts/start-daytona.ts`), mirroring the client's own `RuntimeDeps` discipline, so
  `vitest` drives all three with no network, no cargo and no Daytona call (101 tests). The entry detector
  lives in `scripts/entry-guard.ts` because **three** files are entry points and two of them import the
  third: nexus's equivalent has no guard, so importing `nexus/scripts/start-modal.ts` performs a deploy
  (`start-modal.ts:25-26`) — a shape that would make this package's own test suite deploy twice.
- **A dry run also skips cargo.** The client's dry-run posture sends nothing (`deployPayload`'s
  `options.dryRun` branch); this verb additionally passes
  `noBuild: flags.noBuild || flags.dryRun` in `buildProviderPayload` (`scripts/deploy-endpoint.ts`) so
  the flag works on a host with no registry access at all — the artifact must already exist, which is
  what makes the offline claim checkable rather than aspirational.
- **Daytona needs its own verb because it CREATES the machine, not because of a two-port rule.**
  `createSandbox({ port })` mints the preview link of the **runner's** port
  (`services/daytona/sandbox/setup-sandbox.ts`, `RUNNER_PORT`), which is the management endpoint, and
  `port` is deliberately left unset there. v2 has ONE origin and serves every app under `/app/<name>/`,
  so the **provider's** base is that same endpoint + `result.path` (`/app/provider/`) — there is no
  `getPreviewLink(<payload port>)` call, no per-port token and no port to publish. The compatibility
  field `providerToken` in the summary is the SAME preview token reported on `endpoint`, because one
  origin means one credential. Lifecycle is the caller's: never delete on success, delete on a failed
  deploy unless `--keep-on-fail`, and `createSandbox` already deletes its own half-built machine. The
  admin bearer (spec §12.3) flows into `createSandbox` in its preferred FILE form and rides on the
  deploy/log/delete calls; it is never printed.
- **One machine-readable last line, on every path.** Human output is prefixed `[provider:deploy]`; the
  final line is a bare JSON object (`buildSummary`), so `tail -1 | jq`
  works for a deploy, a teardown, a dry run and a Daytona create alike. `start:daytona` prints its **own**
  summary last because the client's shape has no room for `sandboxId`; its `port` is the INTERNAL
  registry port and is omitted when not assigned yet.
- **The npm layer is cargo-only no longer, and the docs had to move with it.** `package.json` gained
  `dependencies` (`@config/vault`, `@services/daytona`) and the `#layout` / `#rust` notes that used to
  say "owns no listener of its own" and "no verb needs a vite-node pre-step" now say which half of that
  is still true. The deps are runtime-time for the scripts only — `cargo test` never imports them — which
  is the distinction `.github/workflows/ci.yml` states explicitly, rather than claiming no npm
  dependency exists.


## Deviation records — the reasoning README's table cannot hold

**S1 Daytona.** The row is absent, not disabled: `enabled = false` would still advertise a routable id
whose upstream cannot exist until a sandbox boots, and its endpoint is a sandbox handle rather than a
vault entry (`daytona_api_endpoints/STANDARD` is a placeholder URL). The TypeScript keeps it registered
at `packages/agentic/provider/src/registry.ts:68` and pins it discoverable at
`model-class.test.ts:311-315`. Consequence: `/models` answers 15 rows — the TypeScript's 12 minus
Daytona plus the four Vultr rows this table enables (`model-class.test.ts:278`) — and a caller routing
`daytona/qwen3.8-27b` gets this port's 404. Porting
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

**S4 Port 8800.** Recorded at `serve.rs:146-168` and in `lib.rs`. The TypeScript provider's own
listener stays on `PRIVATE_PROVIDER_PORT` = 6660 (`packages/agentic/provider/src/port.ts`); the
consumer-facing `LOCAL_AREA_NETWORK_PROVIDER_PORT` is 8800
(`config/environment/src/port.ts:4`), and this crate publishes 8800. Consumers migrated to 8800 —
both defaults (`packages/typecrate/harness/src/config.rs:36`,
`packages/underload/harness/src/server/config.ts:272`) dial it — while the TypeScript implementation
stays on 6660 as the behavioural specification and fallback; nothing consumes its 6660 port
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
and no request-body cap (`serve.rs:92-93`: the deployed gateway imposes none, so neither the handler
nor the standalone bin does — `DefaultBodyLimit::disable`).

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
**173 test functions** (33 lib unit + 140 integration across **7** targets) **plus 2 doc tests**, and
`cargo test --workspace --locked` re-run on this pass (2026-11-19, after the `[[bin]]` and gateway
targets were added) reported exactly that: **175 passed, 0 failed, 0 ignored** (33 lib / 0 bin unit /
15 accounting / **19 bin_server** / 27 clients / **8 gateway** / 12 registry / 18 routes / 41 transport /
2 doc tests), exit 0 — so the "173 vs 175" question is the doctest target, not a missing test. The
doctests are the runnable examples in `src/vault.rs:900` (`load_from_str`) and
`src/transport.rs:1868` (`prepare_body`) — the two lines `cargo test --doc` names. `fmt`, `check` and
`clippy -- -D warnings` were re-run on this pass too and are clean. The per-target breakdown and the gap
list are in README's [Test parity and coverage](README.md#test-parity-and-coverage).

**Recount, 2026-11-24 (the in-band status, size-refusal and missing-`model` passes).** The 175 above is
the 2026-11-19 snapshot and is now historical. `cargo test --workspace --locked --no-fail-fast` re-run
at the end of this pass reports **186 passed / 4 failed / 0 ignored**, per target: **34 lib unit** (32
passing, 2 failing) / 0 bin unit / 15 accounting / **19 bin_server** / 27 clients / **8 gateway** /
**12 registry** (10 passing, 2 failing) / **22 routes** / **51 transport** / **2 doc tests**. The count
of test attributes is **188** (`grep -rE '#\[(tokio::)?test(\(|\])' src tests`), 188 + 2 = 190 cases,
of which 186 pass. The four failures are one pre-existing cause, not four faults: the regenerated
`config/vault/generated/agentic-provider.json` carries more `label` members than `vault.rs` pins (53
against 43, and 31 against 28), which fails
`vault::tests::live_artifact_matches_the_documented_shape`,
`vault::tests::a_credential_value_can_never_be_printed`, and the two registry tests that dial the live
artifact's pools (`e_a_one_entry_pool_repeats_its_head_once_and_then_drains`,
`f_a_relay_rotates_its_named_endpoints_in_document_order`). They are outside this crate's behaviour and
the artifact is do-not-touch; `--no-fail-fast` is also required for the tally, because without it cargo
stops at the first failing target. Note also that the doctest target only runs when cargo's configured
`TMPDIR` (`Temp/rust`, `.cargo/config.toml:33`) exists — with it missing, `--doc` errors out with
"failed to create temporary directory" and the two doc tests are silently absent from any total.

**Recount, 2026-11-25 (the per-request endpoint walk).** `cargo test --locked --no-fail-fast` re-run at
the end of that pass from `packages/typecrate/provider` reports **196 passed / 4 failed / 0 ignored**,
per target: **37 lib unit** (35 passing, 2 failing) / 0 bin unit / 15 accounting / **19 bin_server** /
**31 clients** / **8 gateway** / **12 registry** (10 passing, 2 failing) / **22 routes** /
**54 transport** / **2 doc tests**. The count of test attributes is **198**, 198 + 2 = 200 cases, of
which 196 pass. The failures are the SAME four the paragraph above names, and they reproduce with this
pass's `src/` and `tests/` stashed (`git stash push -- src/ tests/` then re-run), which is how they were
confirmed pre-existing rather than caused by the walk: the artifact now carries 45 `label` members where
`vault.rs` pins 43, and 31 where it pins 28. What the pass ADDED is ten cases, all of them about the
walk: the two inline `src/transport.rs` contentions (`advance_hands_each_racer_the_slot_it_claimed`,
`concurrent_verifications_of_one_entry_publish_one_head`), the four `tests/clients.rs` walk cases
(`concurrent_requests_walk_the_verified_prefix_and_never_skip_an_endpoint`,
`concurrent_requests_all_walk_a_fully_outaged_pool_and_refuse`,
`a_second_request_starts_where_the_first_one_proved_the_pool_dead`,
`the_switch_base_url_seam_publishes_one_step_per_call`) and three `tests/transport.rs` cases
(`rotation_walks_the_pools_and_then_reports_exhaustion`, rewritten to the seam's semantics,
`a_pinned_request_dials_the_pin_and_never_the_shared_selection`,
`a_pinned_retry_stays_on_the_pinned_endpoint`), plus the one inline `src/serve.rs` capture case
(`the_request_log_names_the_endpoint_the_answer_carried_not_the_shared_selection`). The correction pass
added no production behaviour: it swapped `behavior == ChatKeylessRelay` for the established
`Behavior::rotates_endpoints` predicate, widened `ServedEndpoint::new` to `pub(crate)` so the capture
case can be built, and documented the two precision points the review raised. The concurrency regression
was written FIRST and
observed RED against the unmodified crate — the pristine `src/` stashed back in, the same probe failing
with `every request must reach an endpoint that answers: ["404 no route", …]` six times over, and the
probe passing again once the walk landed. (Those two runs were captured in that pass's disposable
scratch directory, `Temp/fix-endpoint/`, which is not an artifact; the probe file itself was deleted
after it served that purpose.)
`cargo clippy --all-targets -- -D warnings` and `cargo fmt --check` were re-run for this crate on the
same pass and are clean.

The suite is organised around the acceptance letters recorded in `tests/registry.rs:3-16` (A4–A9), and
its discipline is that every expectation is read out of a TypeScript or Rust source **before** the code
ran (`tests/routes.rs:5-8`), against a real in-process listener rather than a mock
(`tests/transport.rs:3-8`). The two new targets extend that discipline rather than break it:
`tests/bin_server.rs` and `tests/gateway.rs` drive the **bin's own router on a real socket**, so the
fail-fast gate, the `/health` body, the `ANY` refusals and the preflight `204` are wire facts, not
assertions about a hand-built `Next`. Still not covered, and stated so nobody over-reads these figures: no live upstream, no run inside the `typecrate-services` binary (the route-file half is still pinned by literals
and by services' own tests), no `POST /apps` / `PUT /apps/provider` to a live v2 Modal box or Daytona
sandbox, no Daytona or Lightning **rows** (they are absent, not exercised), no HTML dashboard, no stats
persistence, and not all 29 rows exercised individually. The npm verbs are covered separately, by
`yarn vitest run packages/typecrate/provider/scripts` — 101 tests with every side effect injected, so
they need neither network nor cargo.

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
| 3 | `client.rs:175-189` justified the unported per-call `options.baseUrl` with `model-class.ts:573` | the file has no `baseUrl` identifier at all (the field is `baseURL`), `:573` is the 429 handler's `console.log`, and `create` takes one parameter (`:922-924`) | `src/client.rs:181-184` states the corrected evidence; the "not ported" conclusion stands |
| 4 | `lib.rs` cited `model-class.ts:567-572` for the `stream_options.include_usage` opt-in | the rule is **per client** in 32 files (samples `nvidia-glm5-flash.ts:76`, `daytona-qwen3-8.ts:498`, `lightning-glm-5-3.ts:112`) and `model-class.ts` has no `stream_options` | `src/lib.rs:12` now says so and names the samples; the Rust row flag `requires_usage_option` (`client.rs:345`) matches all 32 |
| 5 | `accounting.rs` said the two port counters are ignored by "the `/stats` projection" while `serve.rs` publishes them | rows omit them (`serve.rs:1098-1104`); `totals` publishes `attemptTotal` and `consecutiveFailures` (`serve.rs:1151-1158`) | the `accounting.rs` module doc (`:47-53`) now draws exactly that line |
| 6 | the client lineage table cited `withModel` `:895-897`, `create` `:905-907`, `switchBaseUrl` `:929-931`, and `serve.rs` cited `:753-755` for the Responses pass-through | `withModel` `:899-901`, `create` `:922-924`, `switchBaseUrl` `:933-935`, `createResponses` → `responses.create(this.withModel(body))` at `:747-749` | `src/lib.rs:17`, `src/client.rs:40-46` (the corrected `:707`/`:708`/`:588-619` seam cites) and `src/serve.rs` deviation 2 |
| 7 | TypeScript-side drift: `local/local-strata.ts:247` attributes its "[retry] HTTP 429" line to `model-class.ts:551`, which is blank (the line is `:573`) | as stated; the file is not this crate's to edit | left as found — recorded here so a future re-cite starts from `:572-576`; `src/transport.rs:320-322` records the same class of problem for the URL-rule cite |
| 8 | a cite worth keeping: `serve.rs` and `provider-route.rs` cite `start.ts:780` for "one listener per distinct declared port" | `start.ts:777-781` is exactly the port-grouping phase | unchanged — re-verified, no edit |
| 9 | the `RoutingVendor` doc listed eight `merge/merge-*.ts` call sites and then said "the **seven** names are the complete set" | eight call sites, **five** distinct vendors (`telnyx`, `pareto`, `modal`, `deepinfra`, `openai`) — which is why the enum has five variants | `src/lib.rs:17` ("eight `vendor:` injections (five distinct values)") and the `RoutingVendor` doc in `src/client.rs` |
| 10 | `error.rs` cited `start.ts:319-327` for `STREAM_ERROR_CODES` | the doc comment is `:317`, the `as const` array `:318-326` (the six codes `:319-325`), and `:327` is the `StreamErrorCode` alias | `src/error.rs:6` and `:83` |
| 12 | drift this pass found in its **own** documents and scripts, after the `[[bin]]` and the deploy verbs landed: `package.json` `#layout` "owns no listener of its own", `#rust` "no vite-node pre-step", `README.md`/`knowledge.md` still describing a crate with no process of its own (a "no `[[bin]]`" / "served only by the services router" shape) and stale test totals (**137** and **148**), `.github/workflows/ci.yml:11-14` "the crate has no npm dependencies", and three drifted cites in `scripts/` | the crate owns a listener and has two hosts (bin / route file); the cargo verbs need no pre-step but the three deploy verbs are `vite-node`; the totals are **175** (173 + 2 doctests, per the run recorded under README "Verification"); the crate still has no npm dependencies, but **the package now does** (`@config/vault`, `@services/daytona`, for the scripts only); `contract.rs` is really the `probePath` bullet of the `generic` profile's defaults doc (`contract.rs` as this row is re-pointed today — the TCP-readiness rule, not a function); `modal-sandbox-container.py:1835` was really `:1895-1899`, the child-env builder — named `_runtime_env` when this row was first written and renamed `_child_env` since; **that Python runner no longer exists** (the runtime is the Rust `runtime-server`, T31), so its span is history only and `_child_env` is not a live symbol — and `:4362` is the `BOX_LABEL` assignment (`:4540-4544` when this row was written, `:343-345` today), with the comment that says why it is env-overridable. Both numbers are **spans as of this re-pointing, not permanent**: those two read-only files are being edited by other work in this same session, which is why every pointer here names the symbol (grep `probePath`, `_child_env`, `BOX_LABEL`) rather than trusting a bare number | `README.md` ("What ships", "Deploying it — the three verbs", "Getting a dedicated box", the test table, "Verification", "Two implementations"), `knowledge.md` (port map items 6-7, the route-bridge bullet, the new host/payload section, the parity counts), `package.json` (`#layout`, `#rust`, `#deploy`, `#ts`, `#dependencies`), `package.json` `#artifact`, `.github/workflows/ci.yml` (the header now states what CI runs and what it deliberately cannot), and the three script cites — which now name the **function** as well as the line, so the next renumbering of a file this task only reads is visible instead of silent |

| 13 | the same drift, one pass later, and this time inside **this package's own test and doc history**: the `create opts` bullet of `scripts/start-daytona.test.ts` (the cite sits at `:14` today) cited `setup-sandbox.ts:1565-1590`, the `summary` bullet (the cite sits at `:30` today) cited `runtime-deploy.ts:1579-1591` (both bodies had moved again), the same file's cargo comment (now `:387`) cited `runtime-deploy.ts:874-899`, row 12's own "really" numbers had been overtaken by the same churn, and README's closing drift note cited `scripts/deploy-endpoint.ts:154` and `:437`. Separately: the two `--out` tests asserted `resolve('Temp/out.tar.gz')`, which **cannot fail** — vitest's cwd is the repo root, so the buggy cwd answer and the correct repo-root answer are the same string | `createSandbox` is `setup-sandbox.ts:1565-1587` (the whole arrow, `};` at `:1587`; the `:1586` reported a pass earlier is the `};` that closes the **returned object**), `buildPublicUrl` is `runtime-deploy.ts:1579-1587`, `cargoArtifactCandidates` is `runtime-deploy.ts:874-898` (doc `:874-880` + body `:881-898`), and `RUNNER_PORT` is still `setup-sandbox.ts:234`; the two provider-script comments are `deploy-endpoint.ts:150-157` (`DEFAULT_PORT`) and `:438-445` (`portCollisionNotes`), and the start-modal comment is `start-modal.ts:27-30`; `_child_env` was `:1895-1899` and `BOX_LABEL` `:4540-4544` at that time — both are **history**: the Python runner was replaced by the Rust `runtime-server` (T31), and `BOX_LABEL` is `:343-345` today — so the production scripts' own cites (`:1894-1898`, `:4539-4543`) were then one line low; T36 re-pointed the live ones by symbol (`BOX_LABEL`, `_state_path`); the vitest totals are **86** (83 + 3 new `resolveOutPath` tests), proven against the bug by flipping `join(repoRoot, value)` back to `resolve(value)`: 5 tests fail with received `packages/typecrate/provider/Temp/out.tar.gz`, and the implementation was restored byte-identically (sha256 `2b0f2fd6…a8c6`) | `scripts/start-daytona.test.ts` (the two header cites and the cargo comment, each now naming `createSandbox` / `buildPublicUrl` / `cargoArtifactCandidates`), `scripts/deploy-endpoint.test.ts` (a `describe('resolveOutPath')` block and a `withCwd` fake — the `--out` flow tests now assert the literal `join(REPO_ROOT, …)` while the cwd is faked to the PACKAGE root, which is where `yarn workspace @typecrate/provider deploy:endpoint` actually starts), `README.md` (the `_child_env` span, the **86** totals, the closing drift note re-pointed at the declarations), and `knowledge.md` (the two port-map bullets, the two vitest totals, row 12 above, and this row) |
| 14 | the three deploy verbs still described runtime **v1**: `deploy-endpoint.ts` declared a fixed `config.port: 8800`, no app name/`targetPrefix`, and POSTed `/` with a `--env PORT` warning; `start-daytona.ts` minted a second `getPreviewLink(<payload port>)` URL/token; start-modal's rationale assumed one occupied slot; the deploy docs asserted `GET /status` echoed `config.env` verbatim and readiness was a bare TCP connect | sandbox runtime v2 is a multi-app registry (spec §4/§7): the payload must name its app (`name: 'provider'`; omitted ⇒ the shared `default` alias) and its native base (`targetPrefix: '/providers/private/v1'`), `config.port` is auto-allocated unless `--port` names one, `PORT` is a reserved env key (R-8.3/R-8.4), readiness is the bin's own `/health` probed DIRECTLY on loopback (R-8.7; `/app/provider/health` would 404 under the prefix), `TYPECRATE_PROVIDER_HOST=127.0.0.1` is supplied by the client (R-8.5), the public base is endpoint + `result.path`, and AppView redacts env VALUES (R-5.1/R-5.2) rather than echoing them | `scripts/deploy-endpoint.ts` (v2 payload identity, auto port, `reservedEnvErrors`, `resolveAdminToken`, `teardownApp`/`tailAppLogs`, app-base/health output), `scripts/start-daytona.ts` (one origin, no `getPreviewLink`, internal-only port, admin credential into `createSandbox`), `scripts/start-modal.ts` (coexisting-apps rationale), the three colocated suites (**101** tests, including targetPrefix/loopback-host regressions and the no-second-preview case), and README/knowledge deploy excerpts (the `package.json` `#deploy` note still describes the v1 config: out of this pass's file ownership) |

Two things this pass deliberately did **not** claim: the retry budget (rows `transport.rs:95-110`/`:110-117` in
README's deviation table) and the SSE decoder's three one-directional leniencies (`sse.rs:54-80`) are
deviations, not citation errors, and they are recorded as such in README rather than "fixed" here.

## Follow-ups this pass recorded and did not take

Two pieces of work that this change **made visible** and deliberately left undone, because each edits a
crate or a package outside `packages/typecrate/provider`:

1. **Deduplicate the gateway layer: `packages/typecrate/services/src/server/mod.rs:354-417`.** That
   block holds `PRIVATE_NETWORK_ALLOW_HEADER`, the `*` allow-set, `is_cors_preflight` and the preflight
   `204` layer as **private** items of the services router; `src/serve.rs:747-809` now publishes the same
   behaviour as `PRIVATE_NETWORK_ALLOW_HEADER` (`:747`), `GATEWAY_ALLOW_ALL` (`:753`),
   `with_gateway_headers` (`:758`), `is_cors_preflight` (`:786`) and `gateway` (`:803`), because a
   container payload cannot link the services router and `src/bin/server.rs:580` is the second consumer.
   Two copies of a wire policy is the thing most likely to drift — the services side pins it in
   `packages/typecrate/services/tests/server_http.rs`, and this crate pins its own copy in
   `tests/gateway.rs` — so services should call `typecrate_provider::serve::gateway` instead of its
   private duplicate. Not taken here: it edits the services crate **and** its `tests/server_http.rs`
   pins, and the two suites agree today. `tests/gateway.rs:1-13` records the same decision from the test
   side.
2. **Retarget nexus's scripts onto the same `runtime-deploy` client.** `packages/typecrate/nexus/scripts/`
   still owns a whole profile system, a hub-URL story and its own `deploy-daytona.ts` shell-out
   (`start-modal.ts:25-27` also has no entry guard, so importing that module performs a deploy). This
   package consumed the client instead of duplicating it, so the duplication now sits on the nexus side:
   its verbs could call `cargoReleaseBin` / `packagePayload` / `deployPayload` / `probeEndpoint` and keep
   only its hub-specific payload declaration. Not taken here: nexus's payload is a different workload
   (profile-bearing, port 8085, `SIGINT`), its tests are its acceptance surface, and no behaviour this
   task owns depends on the change.

## Cutover: what the vault side still needs now that this crate is the consumer

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
  Since 2026-11-24 the **status** of that 404 is still the parity to keep and its **message** is a
  recorded deviation (deviation 5, `src/serve.rs:96`): an absent or non-string `model` answers
  `MISSING_MODEL_FIELD_MESSAGE`, naming the field, where the reference interpolates the JS artefact
  `Model 'undefined' not found`. Re-introducing `undefined` is the regression — and so is turning the
  404 into a 400, which is terminal for the consumer either way and would trade parity for nothing. A
  string id that no row owns still answers `Model '<id>' not found` verbatim. The operator half of
  that fault is one `warn!` with the route, the byte count and the body's **top-level key names** only
  (`warn_missing_model_field`, `MISSING_MODEL_KEYS_SHOWN`): message content, tool payloads and
  credential values must never enter it, a body that was not parseable JSON is reported as that
  fact instead of as an invented key list
  (`tests/routes.rs::a_body_naming_no_model_warns_with_key_names_and_never_with_content`), and every
  non-whitespace **control character** is dropped from each key before the list is joined
  (`describe_top_level_keys`) — a caller can send `"\u{1b}[31m"` as a key name, and `flatten_error_detail`
  flattens whitespace, caps length and redacts secrets but has no opinion about the byte that turns an
  ANSI colour on. Whitespace is deliberately left to the flattening, because that IS the reference
  gateway's rule (`src/serve.rs::the_body_key_descriptive_names_the_count_the_sorted_names_and_the_remainder`).
- An **in-band error frame's declared status rescues a failure nothing else named** (since
  2026-11-24): a frame that writes `error.status` (in any shape the deployed readers accept —
  `500`, `500.0`, `"500"`, `" 502 "`), or whose message or `error.code` names a size refusal, or which
  names one of the eight classes in `UPSTREAM_TYPE_STATUSES`, classifies as `upstream_status` and
  stamps the `status` key (`src/error.rs::FailureSignals::from_upstream_frame`). Reverting to the SDK's
  loss of the number — which this crate did until that date, and which made the relay-reported verdicts
  an unactionable `unknown` — is the regression now, and so is widening the inference beyond the closed
  table (a guessed status for an unknown class would invent a verdict the upstream never stated). The
  **precedence inside that rule is the other half of the contract** (D7, 2026-11-24): a numeric
  `error.status` first, then `status_declared_by_size_refusal`, then the class table — never the other
  way round. A status-less frame whose message matches `CONTEXT_OVERFLOW_MARKERS` MUST emit **400**,
  not the 500 the `server_error` class would infer, because the upstream answers that same sentence
  with a client error on its pre-head path (`Temp/basalt-check/serve/handler.py:551-555`) and the
  consumer's overflow gate refuses an overflow behind a 5xx (`isContextOverflowSignal`). Letting the
  class table win, rewriting a frame that DID write `status: 500`, or matching prose outside the four
  markers is the regression. **And the whole declared-status field is a RESCUE, never an override**
  (the correction of the same day): it travels in `FailureSignals::declared_status`, which only rule 9
  of `classify_stream_error` reads, so rules 1-8 — names, errno, wording — decide the code first and a
  frame they named keeps its ABSENT `status` key even when it also writes a number. Demoting
  `{"type":"server_error","message":"upstream stream terminated"}` to `upstream_status` because its
  class or its number says so is the regression that correction closed: the consumer reads `code`
  before `status`, and the swap moves a failure from the short dial ladder
  (`provider-timeouts.ts:714-721`) onto the 1 + 10 transient one (`:761-790`). The frame's
  own number stays separately readable for every case where it legitimately differs from the
  emitted status (`StreamFailure::frame_status`, printed by `StreamFailure::log_line`), and the
  `unknown` class keeps `UNKNOWN_STREAM_ERROR_MESSAGE` as its sentence PREFIX even though the
  upstream's detail now follows it — dropping the constant, or putting the detail before it, breaks
  every consumer that string-matches the legacy frame
  (`tests/transport.rs::a_mid_stream_error_frame_with_a_status_is_an_upstream_status_frame`,
  `::an_in_band_error_class_infers_only_a_listed_status_and_a_number_always_wins`,
  `::an_in_band_context_overflow_frame_keeps_its_wording_on_the_wire` and
  `tests/routes.rs::a_mid_stream_server_error_frame_is_relayed_as_a_status_carrying_refusal`).
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
- **The endpoint head is a verified prefix, never a claim slot.** Nothing may advance it without having
  PERSONALLY dialled the entry it is walking past (`Transport::verify_endpoint`, `transport.rs:1308`,
  is the only production writer and it publishes with `fetch_max`), and no request may re-read the
  shared selection mid-flight — the dial target is resolved once and pinned
  (`transport.rs:1483-1498`). Re-introducing a `advance`-style CAS on this axis, or letting the
  transport re-read `current_endpoint()` inside its retry loop, restores the exact race the 2026-11-25
  pass removed: N concurrent 404s skipping N-1 endpoints nobody dialled. The credential axis keeps the
  CAS on purpose (`transport.rs:1827`) — do not "unify" the two axes.
- The **prompt estimate is over the caller's body**, the request bytes are the rewritten payload
  (`client.rs:39-54`).
- **`requestTotal` counts rotations once** and counts failures (`accounting.rs:106-111`,
  `client.rs:68-70`).
- **Exactly-once settlement** across exhaustion, failure, cancellation and abandonment
  (`transport.rs:132-142`).
- **`Cargo.lock` stays a subset** and axum stays feature-default (`Cargo.toml:189-205`), or the sibling
  service build and the offline build both move.
- The **no-`charset` JSON content type** and the two SSE headers, byte for byte
  (`serve.rs:227-235`, pinned by `tests/routes.rs`).
- **No credential in any printable path** — `Secret`, the label allow-list, the redaction gates, and
  the two suite-level leak tests (`vault.rs:153-164`, `client.rs:1266-1308`, `error.rs:62-69`,
  `tests/transport.rs::no_credential_reaches_a_diagnostic_or_the_wire_detail`,
  `tests/clients.rs::no_credential_value_reaches_the_failure_vocabulary_or_the_debug_output`).
- **The bin's credential gate stays BEFORE the bind** (`src/bin/server.rs:29-42`). Restoring the
  library's lazy 503 there turns a refused vault into a listener that passes a TCP-connect readiness
  probe while serving nothing but 503s — a deploy that reports success and is broken. `--allow-unconfigured`
  is the explicit way to want the soft behaviour.
- **`/health` stays bin-only** (`src/bin/server.rs:109-112`): a fifth path constant in `src/serve.rs`
  changes what `packages/typecrate/services` lists for this package, and the services mount must keep
  answering exactly the four contract routes.
- **The payload stays exactly two members, with those modes** — `typecrate-provider-server` 0755 (the
  runner execs `./<member>`, so the exec bit is not cosmetic) and `agentic-provider.json` 0644 — with
  the app name `provider`, `targetPrefix /providers/private/v1`, a loopback `/health` probe, **no
  `config.port`** unless an operator names one, and **no secret in `config.env`**: v2's AppView exposes
  `envKeys` and redacts values (spec R-5.1/R-5.2), and the full config is persisted in `app.json`, so a
  credential-shaped env key is metadata even when its value is hidden. `scripts/deploy-endpoint.test.ts`
  pins the member table and the config key-for-key; loosening any of those assertions is the regression.
- **Importing a deploy verb must start nothing** (`scripts/entry-guard.ts`). nexus's `start-modal.ts` is
  the counter-example; under `vitest` the guard must win first, or the package's own test suite
  deploys.
- **The endpoint stays a parameter** (argument > `TYPECRATE_PROVIDER_ENDPOINT` > vault): which box
  receives the `provider` app is an operator decision (resources, admin auth, blast radius), and baking
  a URL into the package re-couples this workload to a single shared origin. The deploy is app-scoped,
  so a shared box no longer forces the exclusive use it did under v1's one-slot runner.

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
