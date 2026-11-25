# @typecrate/provider

Rust-native private model provider for the `typecrate` scope: the port of the `@agentic/provider`
**core** plus the four `runtime/endpoint/provider/private/**` service routes, so the same
`/providers/private/v1` surface served **two ways, never stacked**: as this crate's own executable —
`[[bin]] typecrate-provider-server` (`Cargo.toml:70-72`, `src/bin/server.rs`), started by
`start:server` on a dev box and pushed into a cloud container by the three deploy verbs — and as an
`rlib` compiled into `packages/typecrate/services`, which discovers
`endpoint/provider/provider-route.rs` at build time and owns the socket in that host. The bin depends
one-way on the lib, so an embedder that runs the route file never starts this executable
(`src/bin/server.rs:18-27`).

The behavioural spec is the TypeScript source, file by file. The module table below names one
TypeScript origin per Rust module; every deliberate divergence is listed under
[Deviations](#deviations-the-full-list) and carries its reasoning and evidence in
[`knowledge.md`](knowledge.md). `@agentic/provider` is **not deleted** — see
[Two implementations, two ports](#two-implementations-two-ports).

> **House model.** This file follows `packages/typecrate/harness/README.md` — identity +
> "what ships" table (`harness/README.md:1-31`), commands (`:33-44`), running it and environment
> (`:46-78`), the surface (`:80-109`), parity (`:138-154`), deviations (`:156-256`), verification
> (`:258-314`). The operational/gateway half (how a discovered route file lands in another crate's
> router, the verb table, the curl block) is modelled on
> `packages/typecrate/services/README.md:528-597` and `:644-683`. Naming and placement rules come
> from `rust/README.md:35-45` ("Name mapping rule") and `:47-70` ("Where a crate lives").

Native only. `Cargo.toml` is a standalone workspace — its own `[workspace]` table at `Cargo.toml:27`,
because the package sits where the npm package sits (`packages/typecrate/provider`) while the root
workspace globs only `rust/crates/*`; without that boundary cargo walks up and fails with
`current package believes it's in a workspace when it's not` (`rust/README.md:52-61`). Crate name ==
npm name with `@` dropped and `/` replaced by `-`: `@typecrate/provider` → crate
`typecrate-provider` (`Cargo.toml:33`) → `use typecrate_provider::…` (`Cargo.toml:45`,
`src/lib.rs:64`). Edition **2024** (`Cargo.toml:38`), `rust-version = "1.99"` (`:39`),
`publish = false` (`:41`), `unsafe_code = "forbid"` (`Cargo.toml:229-230`), one `rlib` target
(`:49`) plus one `[[bin]] typecrate-provider-server` (`:70-72`). Version `1.0.2` is pinned to the npm line (`Cargo.toml:37` ↔
`package.json:3`) and the equality is a test (`src/lib.rs:131-136`). Artifacts land in the repo-root
`.rust/target` because `../../../.cargo/config.toml:14` sets `target-dir`.

`Cargo.lock` is committed (`.gitignore:17` — "Cargo.lock is NOT ignored"; the `Cargo.toml` header at
`:68`, `:79` and `:88-92` explains why) because every cargo verb runs
`--locked`, and this lockfile's package set is kept a **strict subset** of both sibling typecrate
lockfiles so an offline checkout seeded by either sibling still resolves — that property is a test
(`tests/transport.rs::the_lock_package_set_is_a_strict_subset_of_the_siblings`), and it is why the
dependency feature lists are copied from `packages/typecrate/harness` verbatim
(`Cargo.toml:83`, `:98`, `:157`).

## What ships

_Modelled on `packages/typecrate/harness/README.md:19-31`._

| Area | Module | What it is |
| --- | --- | --- |
| Credentials | `src/vault.rs` | The vault **artifact** loader: `config/vault/generated/<scope>.json` read as data. `Secret` redacts in `Debug`/`Display` (`vault.rs:153-164`), the five validity classes (`vault.rs:180-193`: `Valid` / `Keyless` / `Placeholder` / `Invalid` / `Empty`) and the classifier (`vault.rs:76`, `:85`, `:91`), order-preserving pools (`vault.rs:49-55`), `deny_unknown_fields`, every problem at once. Never reads the environment or a default path. |
| Transport | `src/transport.rs` | OpenAI-compatible outbound HTTP/SSE against upstream relays: a credential pool behind a claim-a-slot cursor and an endpoint pool behind ONE verified-prefix head (`fetch_max`, `:1308`), with the dial target resolved once per call and PINNABLE (`RequestOptions::endpoint`, `:1077`) so no internal retry re-reads shared state (`:1483-1498`); the 429/404/retry rules (**bounded here; the deployed 429 handler retries forever** — see the parity table and deviation rows `transport.rs:95-110`/`:110-117`), the exhausted-pool sentence, the request log facts, `stream_options.include_usage`, the token estimator. Policy constants: 30-minute ceiling `:183`, `max_retries` 3 `:187`, 3 s 429 pause `:190`. |
| Frames | `src/sse.rs` | Incremental **event-boundary** SSE decoder: the `data:` lines of one event accumulate and are emitted as ONE payload joined with `\n` at the blank separator line (what `streaming.js:255-268` does), the `[DONE]` test is a **prefix** match (`sse.rs:164-165`, `streaming.js:35`), a non-`data:` field line carries no payload, `finish()` flushes the event a truncated body left open (`sse.rs:249`), `DONE_SENTINEL` (`sse.rs:87`) / `DONE_FRAME` (`sse.rs:92`), the frame encoders, and the three one-directional leniencies vs the SDK decoder (`sse.rs:54-80`). |
| Errors | `src/error.rs` | The failure vocabulary: the closed `FailureKind` set (`error.rs:91-103`), the `classify_stream_error` port, the SDK-compatible messages, the mid-stream error frame whose wire key order the route tests pin — and the THREE status reads an in-band `{"error":…}` frame gets, in precedence order: the numeric `error.status` it wrote (`FailureSignals::frame_status`), else the 400 its message names as a size refusal (`status_declared_by_size_refusal`, `CONTEXT_OVERFLOW_MARKERS`), else the status its `error.type` states (`UPSTREAM_TYPE_STATUSES`). That is a deviation from the SDK, which hands the caller an `APIError` whose `status` is `undefined` whatever the frame wrote (`node_modules/openai/core/streaming.js:50`, `core/error.js:12`) — see deviation rows `error.rs:39` and `error.rs:53`; redaction + the 200-character cap. |
| Money | `src/pricing.rs` | Dollars per ONE MILLION tokens (`input` / `cache_read` / `output`, plus a carried-but-unpriced `cache_write`) and the cost arithmetic behind every `$X.XX` on this surface. |
| Counters | `src/accounting.rs` | The lifetime counters behind `/stats`: the input/cache continuation diff, the stream settlement, the `Drop`-settled abandonment path, `Totals`, and the two port additions (`accounting.rs:142`, `:146`). |
| Model client | `src/client.rs` | The declarative client: `ClientSpec` (`:345-406`) + `Behavior` (`:264`), the payload rewrite, the 404 failover loop **as a per-request walk** (owned here, not in the transport: the contract at `:56-108`, `dispatch` at `:857-980`, the one head-writer `advance_walk` at `:1081-1114`), the credential-drain shapes, the discoverability gate, the relay-label allow-list (`:1265-1300`, the function at `:1301`). |
| Rows | `config/provider/**/*.json` + `src/clients.rs` | The served set as **one JSON file per model**, grouped **one folder per provider** (`config/provider/nvidia/`, `config/provider/vultr/`, …): 29 files, scanned **recursively** by `build.rs` regardless of file name and sorted by `id`, then compiled into `$OUT_DIR/model_rows.rs` and included as `CLIENTS`. No credentials or endpoint values: `vault.*` names slot REFERENCES, and `src/vault.rs` resolves them from the artifact. The two omissions are stated as an expected diff (`src/clients.rs` `OMITTED`). |
| Registry | `src/registry.rs` | Rows + vault → one shared `ModelClient` per row, in array order, never re-sorted; refusal with a **problem list** instead of coercion (`:49-50`, `:128-168`); the process-wide singleton (`:242`). |
| HTTP | `src/serve.rs` | The four routes: vault gate + lazy process state (`:414-435`), env source resolution (`:488-517`), the verb gate, the SSE relay, the catalog envelope, the stats document and the reset verb. The only module a route file has to name. |
| Bridge | `endpoint/provider/provider-route.rs` | The `ANY` route file `typecrate-services` discovers at build time: four handler wrappers (`:65-85`) and one `service_routes!` block carrying `, port: 8800;` (`:101-117`). No logic. |
| Own process | `src/bin/server.rs` | `[[bin]] typecrate-provider-server` (`Cargo.toml:70-72`): flags, the **fail-fast credential gate before the bind**, one `0.0.0.0` bind, the banner, the SIGTERM/SIGINT drain, a hand-written `tracing` subscriber, and this crate's own `serve::gateway` layer (`src/serve.rs:803`) so a container has CORS and preflight `204` without the services router. The only path it adds to the contract surface is `GET /health` (`:112`). |
| Deploy verbs | `scripts/*.ts` | `deploy-endpoint.ts` (cargo → two-member tar.gz → `POST /apps`, or the blue-green `PUT /apps/provider`, on any sandbox-runtime v2 endpoint), `start-modal.ts` (the same verb, endpoint precedence), `start-daytona.ts` (creates a Daytona sandbox and registers the same bundle in it). Thin declarations over the SHARED client `@services/daytona/sandbox`; guarded so importing them starts nothing (`scripts/entry-guard.ts`). |

## Commands

_Modelled on `packages/typecrate/harness/README.md:33-44`._

From `packages/typecrate/provider`, the cargo verbs at `package.json:16-26` are plain cargo on the
toolchain `rust-toolchain.toml` pins (no `vite-node` pre-step — unlike services, this crate has no
build-time route discovery). The deploy verbs and the two TypeScript gates at `package.json:28-33` are
`vite-node` / `tsc` / `vitest`, hoisted at the monorepo root (never raw `node` — root `AGENTS.md`,
"Runtime / runner policy"). They NEED the monorepo: `@config/vault` and `@services/daytona` are
workspace packages and `tsconfig.json` extends the root config, so none of those five verbs runs in a
standalone checkout — which is exactly what `.github/workflows/ci.yml` states it runs and does not:

```bash
yarn workspace @typecrate/provider fmt          # package.json:19 → cargo fmt -p typecrate-provider
yarn workspace @typecrate/provider fmt:check    # package.json:20 → cargo fmt -p typecrate-provider -- --check
yarn workspace @typecrate/provider lint         # package.json:17 → cargo clippy --workspace --all-targets --locked
yarn workspace @typecrate/provider lint:strict  # package.json:18 → the same, with `-- -D warnings`
yarn workspace @typecrate/provider check        # package.json:15 → cargo check --workspace --all-targets --locked
yarn workspace @typecrate/provider test         # package.json:16 → cargo test --workspace --locked
yarn workspace @typecrate/provider build        # package.json:21 → cargo build --release --locked
yarn workspace @typecrate/provider verify       # package.json:34 → fmt:check && lint:strict && test && build

# the deployable artifact, and the crate's own listener (no services router, no Node):
yarn workspace @typecrate/provider build:server  # package.json:24 → cargo build --release --locked -p typecrate-provider --bin typecrate-provider-server --message-format=json
yarn workspace @typecrate/provider start:server -- --port 0   # package.json:26 → cargo run …; `--` is cargo's separator, so --port reaches the bin

# the three deploy verbs (scripts/*.ts over the SHARED client @services/daytona/sandbox):
yarn workspace @typecrate/provider deploy:endpoint -- --endpoint https://… --dry-run   # package.json:28
yarn workspace @typecrate/provider start:modal     # package.json:29 → the same verb, aimed at a Modal box
yarn workspace @typecrate/provider start:daytona   # package.json:30 → creates a Daytona sandbox of its own

# the TypeScript surface of this package (scripts/*.ts only):
yarn workspace @typecrate/provider typecheck      # package.json:32 → tsc --noEmit (package tsconfig, which excludes **/*.test.ts)
yarn workspace @typecrate/provider test:scripts   # package.json:33 → vitest run scripts (no network, no cargo, no Daytona/Modal call)
```

`--dry-run` on any of the three deploy verbs is **fully offline**: no probe, no DELETE, no POST, no
cargo, and therefore no registry access. It is not a mock either — it assembles the real bundle from the
artifact `build:server` already produced (the artifact must exist), so the member table, the byte count
and the config header it prints are the ones a real deploy sends. The verb that only packages,
`deploy:endpoint --out <path>` with no endpoint, writes the same bundle to disk and deploys nothing.

`check` / `test` / `lint` / `lint:strict` / `build` / `build:server` / `start:server` carry `--locked`
(`package.json:16-26`): the
committed lock must not be re-resolved by a script. `lint` does **not** deny warnings because root
`turbo run test` depends on `lint`; `lint:strict` is the `-D warnings` gate used by `verify` and CI
(the `#lint-vs-lint-strict` note at `package.json:11` states the split — same as
`packages/typecrate/services`, unlike `packages/typecrate/harness`, whose `lint` denies warnings on its
own). `fmt`/`fmt:check` take no lock part, and **no script carries `--offline`** — the eight
scripts match `packages/typecrate/harness` and `packages/typecrate/services` verbatim on this point
(`package.json` `#rust`, `#locked`: `--offline` is explicitly "passed per-invocation rather than
baked into the scripts"). On a machine with no cargo cache (this repo's usual dev host) it is added
to the invocation, and the dependency graph is deliberately a subset of what the sibling typecrate
builds already fetch, so `--offline` succeeds where a fresh resolution would not. Every cargo gate
result recorded in this README was produced that way (`cargo … --offline --locked`); the deploy verbs'
`--dry-run` needs the same treatment only in its cargo leg, which the deploy verbs skip entirely
because a dry run never compiles (`#deploy-verbs`).

## Running it

_Modelled on `packages/typecrate/harness/README.md:46-78` and
`packages/typecrate/services/README.md:644-683`._

### Running it natively — this crate's own process

`start:server` (`package.json:26`) starts the `[[bin]]`: one process, one socket, no services router,
no route discovery and no JS runtime in the artifact (`src/bin/server.rs:12-20` states why the bin
exists when the rlib already has a host — a container payload is ONE executable plus ONE stage
directory).

| What | Resolution | Cited at |
| --- | --- | --- |
| `-h, --host <addr>` | `<ip>`, `<ip>:<port>`, `[<ipv6>]`, `[<ipv6>]:<port>`; default `0.0.0.0`, overridable by `TYPECRATE_PROVIDER_HOST`. An address that names a port wins over `--port`. `0.0.0.0` and not the harness bin's `127.0.0.1`, because a container has no client on loopback. | `src/bin/server.rs:99-107`, `:118-127` |
| `--port <port>` | `--port`, else `PORT`, else `serve::PROVIDER_PORT` (8800). `0` means any free port and the banner prints the port **actually bound**. | `src/bin/server.rs:112-116`, `serve.rs:169` |
| `--allow-unconfigured` | Bind even with a missing / unreadable / refused vault; without it the gate runs **before** the bind and the process exits 1 with the regeneration command. | `src/bin/server.rs:31-45`, `:135-142` |
| `PORT` | The port, unless `--port` names one — the variable every container runner injects, which is why a deploy needs no flag. | `src/bin/server.rs:107`, `:146-150` |
| `TYPECRATE_PROVIDER_HOST` | The bind address, unless `--host` names one. | `src/bin/server.rs:110`, `:151` |
| `TYPECRATE_PROVIDER_VAULT_JSON` / `_PATH` | The vault document inline, then a path; a path is relative to the process's cwd, which for a pushed payload is the stage directory. | `src/bin/server.rs:152-156`, `serve.rs:178-194` |
| `RUST_LOG` | `off…trace`, optionally scoped per target; unset means `info`. | `src/bin/server.rs:157-163`, `:53-64` |
| `--help` | The whole contract above, in the executable's own words; exit 0. | `src/bin/server.rs:117`, `:118-185` |

**Exit codes** (`src/bin/server.rs:51`, `:126-129`): `0` clean shutdown, `1` could not serve (the vault
was refused without `--allow-unconfigured`, or the address was in use), `2` bad usage — including a
`PORT`/`TYPECRATE_PROVIDER_HOST` a runner set to something unparseable, because that is an
operator-typed value. `SIGTERM`, `SIGINT` and `Ctrl+C` stop it and it **drains in-flight requests**
first (`src/bin/server.rs:181-184`), which is what makes `stopSignal: SIGTERM` in the deploy config the
right thing to send.

This host also serves `GET /health` — `{"ok":true,"service":…,"version":…}` — and that path is
**bin-only on purpose**: it is not one of the four contract routes, and adding a fifth path constant to
the library would change what `packages/typecrate/services` lists for this package
(`src/bin/server.rs:113-117`, `:165-170`). Readiness for a deploy therefore does **not** dial it (see
[Deploying it](#deploying-it-the-three-verbs)).

### Running it as a route file — the `typecrate-services` host

The rlib half. `packages/typecrate/services` declares
`typecrate-provider = { path = "../provider" }` (as of this pass,
`packages/typecrate/services/Cargo.toml:92`) and compiles
`packages/typecrate/provider/endpoint/provider/provider-route.rs` into its own crate the same way it
compiles its samples and the `/v1/harness` bridge — a route source by NAME and directory, never by
contents. There the provider has no process of its own: it is started by starting services, and the
gateway supplies the CORS layer, the access log and the multi-listener bind that the bin
does not have:

One build-time caveat comes with that mechanism: cargo's watch set for the scan is deliberately
**narrow** (`packages/typecrate/services/build.rs:9-18`), so a services build that predates this route
file may not notice it. Force the re-scan with `TYPECRATE_SERVICES_RESCAN=<anything> cargo build`
(the variable is `packages/typecrate/services/build_support/mod.rs:157`; the same escape hatch is
documented in `packages/typecrate/services/README.md:826-827`), or use
`yarn workspace @typecrate/services run-native`, which runs the discovery snapshot first
(`packages/typecrate/services/README.md:1009`).

```bash
cd packages/typecrate/services
cargo run --release --bin typecrate-services-server
# → default listener on 0.0.0.0:8000, plus one listener per port a route file declared,
#   derived from the compiled-in route table (packages/typecrate/services/src/server/mod.rs:516,
#   packages/typecrate/services/src/bin/server.rs:10-17)
```

Because 8800 is a **declared** port in that host, it binds on the **same interfaces** as the services default
(`0.0.0.0:8000`, `packages/typecrate/services/src/bin/server.rs:72`), i.e. every interface, and each
extra listener serves `server::app_for(Some(port), state)`
(`packages/typecrate/services/src/server/mod.rs:461`,
`packages/typecrate/services/src/bin/server.rs:665-669`). That is also why it inherits the gateway
header policy wholesale: wildcard CORS and `Access-Control-Allow-Private-Network: true` are added to
every response by one function (`packages/typecrate/services/src/server/mod.rs:358-379`, the `*` value
at `:353`, and the unlisted header's name spelled once at `:343-347`), with **no per-port exclusion
mechanism** on the Rust side. **No request-body ceiling applies to the provider routes on either
host**: the handlers take `Request` and read the body themselves, so the router's
`DefaultBodyLimit` never governs them (`packages/typecrate/services/src/server/mod.rs:85-96`), and
the standalone bin disables axum's own 2 MiB default (`src/bin/server.rs:568-577`).

One CORS nuance an operator has to know before reading the verb-gate rule below: a real **preflight**
(an `OPTIONS` carrying `Access-Control-Request-Method`) is answered by the gateway layer with a
header-only `204` **before routing** (`packages/typecrate/services/src/server/mod.rs:328-337` explains
why the predicate needs that header, `:386-389` is the predicate, `:398-405` is the layer that returns
`204` at `:402`), so it never reaches a provider handler. A bare `OPTIONS` does reach the router, and
on this surface it is refused with the contract's own `400`. The **bin** host reproduces the same
behaviour with this crate's own copy of that layer — `serve::gateway` (`src/serve.rs:803-809`, predicate
`:786-791`, headers `:758-780`, the private-network name `:747-748`), mounted outermost at
`src/bin/server.rs:580` — because a container has no services gateway to inherit. That duplication is
recorded as a follow-up in `knowledge.md`.

### Deploying it — the three verbs

The crate can be **pushed**, not just started. Three `vite-node` verbs under `scripts/` compile the
`[[bin]]`, pack it, and hand it to any endpoint that speaks the runtime contract; none of them is a
container platform SDK, because all three drive the SAME client
(`services/daytona/sandbox/runtime-deploy.ts`, exported from its `index.ts` barrel) that
`@typecrate/nexus` uses — this package declares a workload, it does not re-implement the contract.

| Verb | What it is | Endpoint |
| --- | --- | --- |
| `deploy:endpoint` (`scripts/deploy-endpoint.ts`) | The platform-neutral verb: parse → `cargoReleaseBin` → `packagePayload` → `deployPayload` → summary. Registers the app **`provider`** at `/app/provider/`, declaring `targetPrefix /providers/private/v1`. Takes `--token --admin-token --admin-token-file --port --probe-path --env --vault --out --no-build --artifact --no-teardown --teardown --no-wait --logs --dry-run --help`. | an argument (`<url>` / `--endpoint` / `TYPECRATE_PROVIDER_ENDPOINT`) |
| `start:modal` (`scripts/start-modal.ts`) | That verb, aimed at a Modal box, plus the vault URL as the last-resort default. Thin on purpose (144 lines) — everything except the URL is the generic verb. | `--endpoint` > `TYPECRATE_PROVIDER_ENDPOINT` > `@config/vault` `modalContainer` |
| `start:daytona` (`scripts/start-daytona.ts`) | Creates a Daytona sandbox of its own (`createSandbox`), registers the same bundle in it, and reports the app base on that sandbox's ONE origin (`<endpoint>/app/provider/`). Adds `--public/--private --cpu --mem --disk --ttl --name --keep-on-fail`. | minted by the verb (the sandbox's runner-port preview link) |

`start:modal` keeps the endpoint a **parameter**, unlike `packages/typecrate/nexus/scripts/start-modal.ts:25-26`
(which pins `main([Object.values(modalContainer)[0], …argv])`, so the vault value always wins): the box
this repo owns is **shared**, and under runtime v2 it serves several named apps side by side
(`/app/nexus/`, `/app/provider/`, …), so the deploy is app-scoped and no peer is displaced. Which box
receives the provider is still an operator decision — resources, admin auth and blast radius are the
operator's, not a package constant's. See [Getting a dedicated box](#getting-a-dedicated-box).

**The payload — fixed shape, two members, flat.** `packagePayload` (`runtime-deploy.ts`) builds one
gzip tar with exactly these members and modes, because the container execs the first one and the second
one is its credentials:

```
typecrate-provider-server   mode 0755   the release ELF from build:server
agentic-provider.json       mode 0644   config/vault/generated/agentic-provider.json, verbatim
```

The app identity rides in the two v2 metadata headers — `x-app-name: provider` and
`x-app-target-prefix: /providers/private/v1` — and the config it posts (the `x-runtime-config`
header, no `x-runtime-profile` header at all) is exactly:

```json
{ "runCmd": "./typecrate-provider-server",
  "artifactPath": "typecrate-provider-server", "probeTimeoutSec": 120,
  "stopSignal": "SIGTERM", "probePath": "/health",
  "env": { "TYPECRATE_PROVIDER_VAULT_PATH": "./agentic-provider.json",
           "TYPECRATE_PROVIDER_HOST": "127.0.0.1", "RUST_LOG": "info" } }
```

`config.port` is **absent** unless `--port` names an internal port: v2 auto-allocates one and injects it
as `PORT`, which the bin honours. The app is served at `<endpoint>/app/provider/…`, and the
`targetPrefix` makes `/app/provider/models` reach the crate's native `/providers/private/v1/models`.

`scripts/deploy-endpoint.ts:36-40` states the same object as the contract this file is written against;
`buildPayloadConfig` (`:418-426`) is the code that produces it and
`deploy-endpoint.test.ts` asserts it member by member.

Why each of those choices, in order:

* **No profile header ⇒ the container's anonymous profile.** `generic` is the profile for a payload
  nobody knows anything about (`services/daytona/sandbox/runtime-server/src/contract.rs`): the allowed config
  keys are `runCmd, port, probePath, probeTimeoutSec, stopSignal, stopGraceSec, buildCmd,
  buildTimeoutSec, artifactPath, env`, the defaults are `SIGTERM` / `probeTimeoutSec 60` and an
  auto-allocated port, and it adds no key, no signature and no registry lookup. A tar.gz **must**
  carry `runCmd` (an archive can hold anything, so the runner refuses to guess — `contract.rs`).
  The app NAME and the TARGET PREFIX are top-level payload metadata (spec §4.7/§8.5), never config
  keys: `name` is the registry key, and `targetPrefix` is the native base path the gateway prepends
  after stripping `/app/<name>/`.
* **Readiness is the bin's own `GET /health`, probed on LOOPBACK.** The v2 runner probes
  `http://127.0.0.1:<internal port><probePath>` (spec §8.7), so `probePath: "/health"` reaches the
  bin directly and proves the provider process answered. The route is NOT part of the prefixed native
  surface: going through the gateway would map `/app/provider/health` to
  `/providers/private/v1/health`, which the crate does not serve — hence the usage text and the
  run summary explicitly deny that alias rather than advertising it. `--probe-path` remains available
  for a different loopback route.
* **`config.port` is omitted by default.** v2 auto-allocates an internal port and injects it as
  `PORT` in the child environment; the bin honours `PORT` (`src/bin/server.rs:104`, `:137-139`), so a
  deploy needs no port at all. `--port <n>` pins one for an operator who wants a fixed internal port,
  and `--port 0` is the explicit spelling of auto. `PORT` itself is a RESERVED env key (spec
  R-8.3/R-8.4): the runner rejects a user value with `400 config_invalid`, so the verb refuses
  `--env PORT=…` locally with the fix (`--port`) instead of shipping a payload that cannot boot.
* **The bin binds loopback in a deploy.** `config.env` carries
  `TYPECRATE_PROVIDER_HOST: "127.0.0.1"` (spec R-8.5: the deploy client supplies the app's own
  host-binding variable), so the runner gateway is the only way in and no other interface exposes the
  provider. The crate's native default is still `0.0.0.0` for `start:server`; the deploy overrides it.
* **`stopSignal: SIGTERM`** is the contract's default and the one thing this bin drains cleanly:
  `SIGTERM`, `SIGINT` and Ctrl+C stop it after in-flight requests drain and it exits **0**
  (`src/bin/server.rs:186-188`, verified live).
* **`probeTimeoutSec: 120`** is above the contract's 60 s default because the first boot has to read
  the vault document and build the registry inside the probe window.

**Anonymity cuts both ways, and one rule follows from it.** Modal's box adds no credential layer of
its own — `_box_endpoint` passes `requires_proxy_auth=False` (`:1895-1913`) — and the only gate the
adapter can configure is the runner's own admin auth: `SANDBOX_ADMIN_TOKEN`/`SANDBOX_ADMIN_TOKEN_FILE`
in the deploy environment (module doc `ADMIN AUTH`, `:48-62`; with neither set, anyone who can reach
the URL can use the API). So the *caller* is authenticated by the runner or not at all, and the
provider's upstream credentials are the payload's own business: they ride **inside the bundle**. That
is also why **no secret may go into `config.env`**: the v2 `AppView` exposes only `envKeys` and
redacts every VALUE (spec
R-5.1/R-5.2) while the full config is persisted in `app.json`, so an env value is metadata an operator
(or anything that can read the app record/logs) can enumerate — and the keys alone already describe a
credential. `env` therefore carries only the *relative path* to the document. `cwd` inside the
container is the stage directory, so `./agentic-provider.json` resolves (`src/bin/server.rs:156-158`).
`--env K=V` is for non-secret wiring only.

**Two independent credentials reach the RUNNER, not the provider.** `--token`
(`TYPECRATE_PROVIDER_ENDPOINT_TOKEN`) is the Daytona preview token, sent as `x-daytona-preview-token`
on every call. `--admin-token` / `--admin-token-file` (env
`TYPECRATE_PROVIDER_SANDBOX_ADMIN_TOKEN`, then `SANDBOX_ADMIN_TOKEN`; the `_FILE` pair likewise) is the
runner's §12.3 admin Bearer, sent as `Authorization: Bearer` on mutations and app-log reads. The file
form is preferred, because the value never appears on a command line; `start:daytona` passes that file
straight to `createSandbox` (uploaded 0600, only its path reaches the runner) while using the resolved
value for its own HTTP calls. Neither is ever printed: the plan masks the preview token and reports the
admin credential by presence, and the JSON summary emits `token`/`tokenScope` for the preview token and
`adminAuth: "bearer"` (presence only) for the admin credential.

**The credential gate is what makes a cloud deploy honest.** In the services host the vault gate is
soft (503 per route, because that process also serves `/v1/harness`); in the **bin** it runs **before
the bind** and exits **1** with the regeneration command (`src/bin/server.rs:29-42`). The runner's
readiness probe reaches that same process on loopback, so a soft-503 listener would still pass only if
the process were serving; with the fail-fast gate a bad credential is a *failed* deploy instead of a
silently-degraded one. `--allow-unconfigured` (a bin flag;
`deploy:endpoint --allow-unconfigured` appends it to `runCmd`) restores the lazy 503 when booting empty
is what you asked for. The client-side equivalent for the *credentials file* is the missing-vault error
(`resolveVaultPath` in `scripts/deploy-endpoint.ts`), which refuses to ship at all and prints
`yarn workspace @config/vault export -- --consumer agentic-provider`.

**The app policy is the client's, and it is APP-SCOPED.** The verb handshakes the endpoint with
`GET /health` (a v1 single-slot runner is refused with its own message, R-17.1), reads
`GET /apps/provider`, and then:

* app absent → `POST /apps` registers it;
* app `ready`/`failed` → `PUT /apps/provider` replaces it **blue-green**: the current revision keeps
  serving `/app/provider/` until the new one is ready, and every peer app is untouched;
* app in any other state (queued/building/booting/draining) → the registry refuses a PUT
  (`409 app_not_replaceable`), and the run reports that as `busy` rather than forcing it.

`--no-teardown` turns an existing app into an error; `--teardown` is `DELETE /apps/provider` only —
one app unregistered, the runner and its peers keep serving — and ships nothing. There is **no root
alias** (`/`), no `default` app name, and no routine whole-box reset: `DELETE /_provider/sandbox` is
not exposed by this verb at all. Readiness is polled on `GET /apps/provider`, and a failure attaches
`GET /apps/provider/logs?lines=N`, the only way to see *why* a payload died (bind failure, refused
vault, `ENOEXEC`).

**Two offline postures, and they are different.** `--dry-run` sends nothing and runs no cargo: it
resolves the artifact from the candidate list the client derives from the repo-root
`.cargo/config.toml` `target-dir` (`runtime-deploy.ts:874-899`), so `build:server` must have run first,
and it prints the real bundle and the real config header. `--out <path>` **also** writes the bundle
(works with `--dry-run`, and with no endpoint it is the whole deliverable). A RELATIVE value resolves
against the **repo root** (`resolveOutPath`, `scripts/deploy-endpoint.ts:500`), NOT the process cwd —
under `yarn workspace` the cwd **is** `packages/typecrate/provider`, and an earlier pass recorded an
`--out Temp/…` landing inside that package tree, which root AGENTS.md forbids (scratch goes in
repo-root `Temp/`); absolute paths stay verbatim. Verified with zero outbound network, and `tar -tvzf`:

```
-rwxr-xr-x  3387776  1970-01-01 10:00 typecrate-provider-server   # mode 0755, the release ELF
-rw-r--r--    14956  1970-01-01 10:00 agentic-provider.json       # mode 0644, the credentials
```

(flat, two members, and the mtimes are the gzip epoch — the packer normalises them, so the bundle is
**reproducible**: two runs of the same command produced the same
`sha256 2845ee7faeb2de74ecb60ecfc886cc5ab340df00633f349368eafdd78eefbc5f`, which is what makes
`--out` usable as a build artifact and a reviewable diff).

Every verb prints human lines prefixed `[provider:deploy]` and ends with **exactly one bare JSON line**
(`buildSummary`), so `… | tail -1 | jq` works on a success, a failure, a teardown and a dry run alike.
Exit codes: `0` deployed / planned / app unregistered / bundle written, `1` everything else — Daytona is
never called for a parse error, so a typo cannot cost money.

The recorded run of this pass (host offline, artifact from `build:server`):

```bash
yarn workspace @typecrate/provider deploy:endpoint https://box.modal.run --dry-run   # exit 0, zero network
yarn workspace @typecrate/provider start:daytona -- --dry-run                        # exit 0, zero Daytona calls
```

which printed the member table (`0755 typecrate-provider-server 3387328`,
`0644 agentic-provider.json 14711`, `1616247` bytes gzip), the config JSON and its base64url header,
`readiness: HTTP 2xx on /health via LOOPBACK within 120s — client budget 240000 ms`, the app identity
(`"provider"`, native prefix `/providers/private/v1`), and ended with
`{"mode":"deploy",…,"name":"provider","status":"planned","publicUrl":"https://box.modal.run/app/provider/","path":"/app/provider/","port":0,…}`.
Nothing after that needed the network.

#### Getting a dedicated box

`start:modal`'s last-resort default is the box this repo already owns —
`https://devilz-angelz--container-sandbox.us-east.modal.run`, the single entry of
`config/vault/src/container/modal-container.ts:1-3` — and that box already serves another workload.
Under runtime v2 the provider registers beside it as the app `provider`, so a shared box is no longer
displaced; a second box is still how an operator gets an isolated origin, an isolated admin credential
and an isolated resource budget. A second box is **not** a second copy of the deploy file: as of this
pass the identity constants in `services/modal/deploy/sandbox/modal-sandbox-container.py` are
env-overridable and default to exactly today's values, so one copy of the file drives any number of
independent boxes (the header block that documents them is the `DRIVER CONFIGURATION` section,
`:164-191`).

The env names below are **named by the constant they set**, not only by a line number: that file is
edited concurrently, so each row greps `APP_NAME` / `BOX_LABEL` / `TAG_VALUE` / `IMAGE_NAME` to
re-locate itself. There are exactly four overridable names as of T36 — the spawner label and the
preview-port list are gone with the Python spawner/forwarder they configured:

| Env var | Constant it sets | Default (= today's shared box) |
| --- | --- | --- |
| `SANDBOX_APP_NAME` | `APP_NAME` (`:177`) | `container-sandbox` |
| `SANDBOX_BOX_LABEL` | `BOX_LABEL` (`:345`) | `container-sandbox` |
| `SANDBOX_TAG_VALUE` | `TAG_VALUE` (`:182`; `TAG_KEY = "service"` at `:181` is deliberately not overridable) | `container-sandbox` |
| `SANDBOX_IMAGE_NAME` | `IMAGE_NAME` (`:191`) | `container-sandbox-toolchain` |

Facts an operator needs, each verified against that file:

1. **The URL is derived, not typed.** A Modal web-function deployment answers at
   `https://<source>--<BOX_LABEL>.modal.run` (the `BOX_LABEL` comment at `:343-345` states it, and the
   vault entry above is the live example: source `devilz-angelz`, label `container-sandbox`). Change
   `SANDBOX_BOX_LABEL`, get a new URL.
2. **The names are read at import time** — `modal.App(APP_NAME)` (`:1963`) and the box's
   `web_server(label = BOX_LABEL)` (`_box_endpoint`) are evaluated while the module is imported, so the
   variables have to be in the environment of the process that runs the deploy.
3. **The command shape** is unchanged: `python3 services/modal/deploy/sandbox/modal-sandbox-container.py`
   (or `modal deploy <that file>`), and `--dry-run` on it prints the identity **this** process resolved
   (app, box label, image, the named-images capability, and the two separate admin credentials)
   without touching Modal.
4. **`--cpu` / `--mem` do not size a box.** A web function's resource request is part of the module-level
   declaration, so those flags size a *sandbox*; the CLI says so when you pass them for a box deploy
   (`--cpu`/`--mem` at `:2690-2691`, the sizes note at `:2778-2783`) and the box's numbers are the
   import-time constants (`CPU_CORES` / `MEMORY_MIB`).
5. **The provider's internal port is irrelevant to the public URL.** A box forwards exactly one port —
   the runner's 7777, declared on the Function by `web_server(...)` (`_box_endpoint`) — and serves
   every app through the v2 gateway at `/app/<name>/` (spec §4.1/§4.2), so
   `https://<source>--<label>.modal.run/app/provider/models` reaches the provider's native
   `/providers/private/v1/models` through `targetPrefix`, with no port published and no extra box
   setup. There is no preview-port list, no spawner label and no per-app forwarder to configure: the
   endpoint is ONE origin, the module docstring's `THE ENDPOINT IS ONE ORIGIN` block (`:10-17`) is the
   whole port map, readiness is the bin's `/health` on loopback, and clients use the mount.
6. **Admin auth has TWO carriers, and they are not interchangeable.**
   `SANDBOX_ADMIN_TOKEN` / `SANDBOX_ADMIN_TOKEN_FILE` in the deploy process's **environment** is what
   the import-time box Function secret carries into the box container; a `--token` /
   `--admin-token-file` on the CLI applies to SANDBOX creates only. `--dry-run` prints the two states
   on separate lines for exactly that reason. Sandbox creates carry the value as a Modal `Secret`
   (the whole runner configuration collapses into that one Secret on a client without `env`), the
   value is never printed, and every `/health` gate requires
   `authRequired == (an admin token is configured here)` — a container whose auth disagrees with the
   host credential is terminated (fresh create) or skipped (reuse), never handed back.
7. **The named image is a publish target, not a create input.** `--image`/`deploy_service`/every
   create path build the CURRENT crate+dashboard layers and publish them under `IMAGE_NAME`
   (Modal client ≥ 1.5); containers are created from the built image object, so a publish that is
   behind the checkout can never be what a new sandbox runs. A plain `modal deploy` builds the marker
   Function's image but does **not** publish the name, and a client without the named-images API
   (Modal 1.1.x) prints "no named images; builds on first create/deploy" from `--image` and exits 0.
   `--start` may still REUSE a running idle container, which keeps whatever image it started from —
   pass `--fresh` for one built from current local inputs.
8. **One per-checkout caveat:** `--url` and `--down` read a scratch deploy pointer
   (`Temp/modal-sandbox-container/last-deploy.json`), which two boxes driven from the same checkout
   would share (`_state_path`, `:2157-2164`).

Then point the verb at the new box, without editing the repo:

```bash
yarn workspace @typecrate/provider start:modal -- --endpoint https://<source>--<BOX_LABEL>.modal.run
TYPECRATE_PROVIDER_ENDPOINT=https://<source>--<BOX_LABEL>.modal.run \
    yarn workspace @typecrate/provider start:modal
```

Both forms print which source the endpoint came from, so "which box am I about to replace?" is never a
guess (`scripts/start-modal.ts:102-109`).

### Credentials, and the fact that they arrive at runtime

The TypeScript provider imports its credentials by name (`import { NVIDIA_API_KEYS } from
'@config/vault'`, resolved at bundle time). A Rust process has no such import, so this crate reads
one **generated, git-ignored artifact** at runtime:

```
config/vault/generated/agentic-provider.json
```

Regenerate it with (the exact string the 503 body prints, `src/serve.rs:199-200`):

```bash
yarn workspace @config/vault export -- --consumer agentic-provider
```

That command is the `export` script of `config/vault/package.json:10`, and `--consumer` is
**required** — least privilege is the artifact's whole point, so there is no full-vault default
(`config/vault/scripts/export-vault.ts:285`). `config/vault/generated/` is git-ignored
(`config/vault/.gitignore`), so on a fresh clone the artifact is absent by design.

Where the bytes come from, in precedence order (`src/serve.rs:178`, `:181`, `:191-194`, `:488-517`):

| Source | Selected when | Notes |
| --- | --- | --- |
| `TYPECRATE_PROVIDER_VAULT_JSON` | set and not blank | A whole inline vault document, highest priority. Named after the crate, not the scope: which scope a document was exported for is the artifact's own `consumers` field. |
| `TYPECRATE_PROVIDER_VAULT_PATH` | set and not blank, and no inline document | An explicit artifact path. |
| `config/vault/generated/agentic-provider.json` | neither is set | Resolved from the **provider crate's** own `CARGO_MANIFEST_DIR` (`serve.rs:191-194`), because the route file is compiled into the services binary and a path baked there would describe the services package's neighbourhood. |

An empty or whitespace-only value counts as unset — which is what `set VAR=` (Windows) and
`export VAR=` (POSIX) both leave behind (`serve.rs:513-516`). The precedence is pure and testable
without touching the process environment (`serve.rs:501-511`, unit test
`the_two_configured_values_resolve_in_the_documented_order`), because edition 2024 makes
`std::env::set_var` `unsafe` and this crate forbids `unsafe` outright.

Initialization is **lazy and once**, on the first request that needs it (`serve::process_state`,
`serve.rs:414-435`, a `OnceLock`). A missing, unreadable, unparseable or refused document is not a
startup failure: every route answers **503** whose body names the cause, the artifact tried and the
regeneration command (`serve.rs:260-296`, `:549-559`), and the services binary keeps serving
`/v1/harness` and the samples normally. A registry refusal lists **every** problem, because one vault
rename or one row typo breaks many rows at once (`serve.rs:278-296`, `registry.rs:128-168`).

**No secret can leak through this surface.** Credentials live in `Secret`, whose `Debug` and
`Display` are hand-written redactions (`vault.rs:153-164`); the only credential-derived value the wire
or a log ever sees is a relay **name** (`client.rs:1266-1308`), never a URL and never a value; every
503 body and upstream message is redacted on the way in (`serve.rs:251-296`, `error.rs:62-69`); and
`tests/routes.rs` asserts the test credential appears in no response byte.

## The surface

_Modelled on `packages/typecrate/harness/README.md:80-109` and
`packages/typecrate/services/README.md:550-565`._

The paths are the TypeScript's **verbatim**, so a caller of the deployed surface changes nothing but
the port. All four groups are mounted `ANY` and all four carry `, port: 8800;`
(`endpoint/provider/provider-route.rs:101-117`; the trailer is a bare literal because the
`service_routes!` grammar binds `$port:literal`, precedent
`packages/typecrate/services/examples/endpoints/health/health-route.rs`, grammar
`packages/typecrate/services/src/server/metadata.rs`; `tests/routes.rs::the_mount_is_the_one_the_route_file_declares`
pins `serve::PROVIDER_PORT` against those literals).

| Mount | Verbs the group declares | Any other verb | TypeScript origin |
| --- | --- | --- | --- |
| `ANY /providers/private/v1/chat/completions` | `POST` | `400 {"error":"handler not found"}` | `runtime/endpoint/provider/private/chat-completion/service-route.ts:6-11` (`:8` route, `:10` `POST`) |
| `ANY /providers/private/v1/responses` | `POST` | `400 {"error":"handler not found"}` | `…/responses/service-route.ts:8-13` (`:10` route, `:12` `POST`) |
| `ANY /providers/private/v1/models` | `GET` | `400 {"error":"handler not found"}` | `…/models/service-route.ts:50-55` (`:52` route, `:54` `GET`) |
| `ANY /providers/private/v1/stats` | `GET` (document) + `POST` (`?id=` reset) | `400 {"error":"handler not found"}` | `…/stats/service-route.ts:21-27` (`:23` route, `:25-26` both verbs) |

**`ANY` plus an in-handler verb gate, not a router verb filter.** The TypeScript route objects declare
a verb-keyed handler map, and `asServiceHandler` answers a verb the map does not contain with
`400 {"error":"handler not found"}` (`packages/underload/service/src/server/handler.ts:38-46`, the
refusal itself at `:42`) — not a 405. Mounting `POST`/`GET` only would give axum's body-less `405`
instead and would never deliver an `OPTIONS` to a handler at all, so each group mounts `ANY` and the
handler prints the contract's refusal (`serve.rs:204`, `:758-761`, `:884-886`, `:950-952`; pinned by
`tests/routes.rs::a_verb_the_route_file_does_not_declare_gets_the_gateway_refusal`, which drives
`GET`/`PUT` on the two POST surfaces, `POST`/`DELETE` on `/models` and `PUT` on `/stats`). The gate
runs **before** the vault question, matching the gateway, which dispatches the verb before it calls a
handler.

Gate order on the two POST surfaces (`serve.rs:723-737`, code `:756-871`):

1. verb → `400 {"error":"handler not found"}` (`serve.rs:759-761`);
2. readiness → `503` (the port's own state, `serve.rs:763-767`; the body is not read, so a refused
   route never pays to read it);
3. body cannot be READ — a transport error (`serve.rs:884-891`) — → `500 {"error":"Internal Server
   Error"}`, the gateway's constant for a
   handler throw (`packages/underload/service/src/server/start.ts:908-911`, the body at `:910`),
   with the real reason in the log only. A body that PARSES (`serve.rs:800`) into malformed JSON,
   an array or a scalar is **folded into the empty object** the deployed gateway folds it into
   (`start.ts:815`) and so falls through to item 4 naming no model; `null`
   is the one shape the gateway does not fold, because `const { model } = null` throws
   (`private-chat-completion.ts:65-66`) and takes the same `500` (`serve.rs:801-809`);
4. `body.model` with no row → `404 {"error":"Model '<model>' not found"}`, `error` a **bare string**
   (`private-chat-completion.ts:79-82`, the body at `:81`). The lookup applies **no** discoverability
   filter, so a hidden row stays routable and `enabled` is a catalog switch, not a routing one
   (`serve.rs:836-846`). A `model` that is **absent or not a string** — the fold above, `{}`, or
   `{"model": 7}` — is the same 404 with a different body,
   `{"error":"Model field is required and was missing or not a string in the request body"}`
   ([`MISSING_MODEL_FIELD_MESSAGE`]), plus one `warn!` naming the route, the body's **top-level key
   names** (sorted, capped at `MISSING_MODEL_KEYS_SHOWN`) and its byte count — never a value, never a
   message: deviation row `src/serve.rs:96` states why the deployed `Model 'undefined' not found` was
   dropped;
5. upstream: a settled document goes out verbatim, a stream goes out as `data: <json>` frames
   (`private-chat-completion.ts:163`) ending `data: [DONE]\n\n` (`:186`; the Responses route yields the
   same sentinel at `private-responses.ts:64` — the sentinel is the route's own frame, and no
   `packages/underload/service/src/server/start.ts` line writes it, see the citation defects in
   `knowledge.md`). A failure before the response head
   is the gateway 500; a mid-stream error is **one** frame and no sentinel after it.

Answer shapes:

* `GET /models` → `{"object":"list","data":[…]}`, entries in **registry row order filtered by
  discoverability, never sorted**, keys `id`,`object`,`created`,`owned_by`,`context_length`
  (`serve.rs:893-927`; TS `models/service-route.ts:15-48`, the frozen literals `created: 1677610602`
  `:42`, `owned_by: 'localhost'` `:43`, `context_length: 262144` `:44`). Against the live artifact
  this answers **15 rows**. `enabled` is deliberately not part of an entry: `isDiscoverable` is a
  filter, not a field.
* `GET /stats` → **JSON only**, `{"models":[…],"totals":{…}}`. Rows are every registry row (no
  hide-unused filter) sorted by id with a code-unit compare (`serve.rs:1001-1032`; TS projects then
  sorts at `private-stats.ts:285` and drops all-zero rows at `:276-284` — see
  [Deviations](#deviations-the-full-list) item 3). Each row carries the 20 `ModelStats` keys in the
  TypeScript's own field order (`private-stats.ts:43-102`) plus `enabled` and `discoverable` appended
  last (`serve.rs:1041-1107`); `totals` carries the 11 `StatsTotals` keys
  (`private-stats.ts:142-153`) plus the two port counters (`serve.rs:1111-1161`).
* `POST /stats?id=<routing id>` → `400 {"error":"missing id query parameter"}` when `id` is absent or
  empty (`serve.rs:974-978`; TS `private-stats.ts:1068-1071`); `404 {"error":"unknown model id: <id>"}`
  when no row owns it (`serve.rs:980-982`); otherwise the row's counters **and** its continuation
  baseline are zeroed and the answer is `200 {"reset":"<id>"}` (`serve.rs:984-988`).
* Content types are contract: JSON is `application/json` with **no** `charset` (Hono's `c.json()`),
  a stream is `text/event-stream` + `Cache-Control: no-cache`, and `Connection` /
  `Transfer-Encoding` are left to hyper (`serve.rs:227-235`, `:22-26` of its module doc). No axum
  `sse` **feature** is involved — `Body::from_stream` plus those headers are in axum's default set,
  which is why `packages/typecrate/services` does not have to grow one
  (`tests/transport.rs::axum_sse_response_needs_no_sse_feature`).
* The request log line per completed request is the TypeScript's, in both shapes —
  `MolabEndless: local/basalt | input: 1,234 (56) [$0.13] | output: 78 [$0.03]` when the row names its
  relay, `model: nvidia/kimi-k3 | …` when it does not (`serve.rs:1181-1204`; TS
  `private-chat-completion.ts:49-61`, logged at `:174-186` before the sentinel). Money is formatted
  with `toFixed`'s tie rule, not Rust's round-half-to-even (`serve.rs:1206-1212`).
* Per request, `info` adds exactly ONE more line, at the response head: `chat-completion: serving
  model=local/basalt relay=Markton` (`serve.rs`, `RequestLog::announce`; the endpoint token is the
  route's folder name, `CHAT_COMPLETIONS_ENDPOINT`, and a row whose relays are unnamed prints
  `relay=unlabelled`). It is the line that answers
  "which relay is this request sitting on" while the request is still in flight — the token line above
  only arrives once the body finishes. At `RUST_LOG=typecrate_provider=debug` the same request also
  prints its head (`stream opened …`) and its first chunk (`first chunk after <n>ms …`), which is what
  distinguishes a stream that never opened from one that opened and stalled
  (`RequestLog::stream_opened` / `::first_chunk`).
* A mid-stream failure logs the failure's own line, not a code and a sentence
  (`StreamFailure::log_line`, `serve.rs::Relay`): one greppable line with `code=`, `origin=`
  (upstream vs transport), `chunks=`, the sanitized `upstream=` target, `status=` when one is known,
  `frame_status=` when the frame wrote a number the emitted status does not carry, `errno=` when the
  failure really had one, and the redacted `detail="…"` verbatim — e.g.
  `stream error in chat-completion: code=upstream_status origin=upstream chunks=1 upstream=http://127.0.0.1:8080/v1 status=500 detail="the engine stopped unexpectedly (exit code 1); the next request restarts it")`.
  Every free-text half of it is redacted at capture, so an echoed key cannot reach it, and the whole
  set of lines is asserted through a capturing `tracing` subscriber
  (`tests/routes.rs::the_log_names_the_row_the_relay_and_the_failure_behind_the_frame`).
* A request refused because its body named no usable `model` logs ONE `warn!` — the only line at that
  level on this surface, because the 404 body alone cannot tell an operator whether the client sent
  `{}`, a non-string, or bytes that never parsed (`serve.rs`, `warn_missing_model_field`):
  `chat-completion: refused 404, the body named no usable `model` field; 41 bytes, 2 top-level key(s):
  zz_probe_messages, zz_probe_stream`. Route token, byte COUNT, and the body's **top-level key names**
  — sorted, capped at `MISSING_MODEL_KEYS_SHOWN` with the remainder counted (` (+3 more of 19)`), and
  run through `flatten_error_detail` so no key can forge a second line or smuggle a secret. Every
  non-whitespace **control character** is dropped from each key first (`describe_top_level_keys`,
  2026-11-24): a key name is arbitrary JSON text, and one ESC (`\u{1b}`) in it would otherwise recolour
  the rest of the record in a terminal — while whitespace stays, because collapsing it IS the reference
  gateway's flattening. A body
  that was not JSON says `the body is not parseable JSON (N bytes)`, a JSON array or scalar says
  `the body is JSON but not an object (N bytes)`, and `{}` says `a JSON object with no keys at all` —
  nothing is ever claimed about keys a body did not have. **No message content, no tool payload and no
  header or credential VALUE can reach this line** (asserted against a content canary and the suite's
  credential value by `tests/routes.rs::a_body_naming_no_model_warns_with_key_names_and_never_with_content`).

```bash
curl -s  http://127.0.0.1:8800/providers/private/v1/models          # {"object":"list","data":[…]}
curl -s  http://127.0.0.1:8800/providers/private/v1/stats           # JSON, not the HTML dashboard
curl -s -X POST 'http://127.0.0.1:8800/providers/private/v1/stats?id=local/basalt'   # {"reset":"local/basalt"}
curl -s -X POST -H 'content-type: application/json' \
     -d '{"model":"local/basalt","messages":[{"role":"user","content":"hi"}]}' \
     http://127.0.0.1:8800/providers/private/v1/chat/completions    # the upstream document verbatim
curl -s -X GET http://127.0.0.1:8800/providers/private/v1/models -o /dev/null -w '%{http_code}\n'  # 200
curl -s -X POST http://127.0.0.1:8800/providers/private/v1/models -o /dev/null -w '%{http_code}\n' # 400: /models declares GET only
curl -s  http://127.0.0.1:8000/v1/services/routes                   # the four ANY rows, on port 8800
```

## Credentials: what the artifact guarantees

_Modelled on `packages/typecrate/harness/knowledge.md`'s "Decisions" blocks; the normative upstream
rules are `config/vault/knowledge.md:157` ("Rust consumption contract (normative)")._

**Order is load-bearing, and nothing re-sorts.** Pools are `Vec`, never a map: `entries[0]` is the
live default and the head of the rotation queue (`vault.rs:49-55`). The generator emits
`default = entries[0].value` (`config/vault/src/vault/snapshot.ts`), and the loader **refuses** a
document whose `default` disagrees with its pool head rather than papering over it
(`src/vault.rs:33-34`, unit test `default_disagreeing_with_the_pool_head_is_refused`). So, for every
slot: **pools are ordered arrays, `default == entries[0]`, and neither the loader nor the registry
reorders anything** — which is the whole parity story for "which credential serves the first request"
(`tests/registry.rs::d_the_nvidia_credential_pool_is_the_document_order_and_rotation_walks_it_once`).
The TypeScript equivalent rotates with `Object.keys` + `shift()`
(`packages/agentic/provider/src/nvidia/nvidia-glm5-flash.ts:9`, `:13-18`).

**Least privilege.** The artifact's `consumers` field is exactly `["agentic-provider"]`, and that
scope is a hand-maintained vendor list of exactly the **11** vendors the provider reads
(`config/vault/src/vault/consumers.ts:45-57`; the comment at `:44` already says it: "This is the scope
the Rust port of `@agentic/provider` loads"): `nvidia`, `makora`, `lightning`, `merge`, `sail`,
`vultr`, `token_router`, `open_router`, `daytona`, `strata`, `basalt`. The live artifact matches —
11 vendors, 20 slots, 43 entries (verified against
`config/vault/generated/agentic-provider.json` on this pass). A vault rename or a new vendor import
is therefore visible as a **refusal naming the row**, not as a silent `undefined`: `registry.ts`'s
TypeScript counterpart runs constructors at import time and a missing export surfaces far from its
cause (`registry.rs:41-50`).

**The `sentinels` vocabulary in that document is deliberately GLOBAL, not scope-filtered.** The
exporter filters vendors by scope and re-lists `sentinels` verbatim
(`config/vault/scripts/export-vault.ts:308`; the reason is stated at `:173` — "global vocabulary, not
a secret"). The classifier unions the artifact's three lists with the crate's built-in floors —
`none` keyless (`vault.rs:78-85`), `NotAvailable`/`error`/`null`/`undefined` invalid (`vault.rs:87-91`)
and the `*_PLACEHOLDER` suffix rule (`vault.rs:73-76`) — and never narrows the artifact's set
(`vault.rs:82-84`). A narrowed document cannot silently demote a valid keyless marker.

## Deviations (the full list)

_Modelled on `packages/typecrate/harness/README.md:156-256` (harness states them as a numbered
prose list; this port uses a table because the count is 24 and each needs its own consequence), and
expanded per-item in `knowledge.md`._

### Structural deviations

| # | Deviation | Why | Consequence an operator must know |
| --- | --- | --- | --- |
| S1 | **The Daytona GPU-sandbox lifecycle is not ported**, so the `daytona/qwen3.8-27b` row is **omitted** (`clients.rs:26-35`, `OMITTED` at `:614-619`; `src/lib.rs:52-54`). | Its base URL exists only after a sandbox boots, and its endpoint comes from a sandbox handle, not a vault entry (`daytona_api_endpoints/STANDARD` is a placeholder). A vault-built row would advertise an upstream that cannot answer. The TypeScript keeps it registered at `registry.ts:68`. | `GET /models` lists **15** discoverable rows: the TypeScript's **12** minus Daytona plus the four Vultr rows enabled in this table (`packages/agentic/provider/src/model-class.test.ts:278` pins 12; `:311-315` pins Daytona discoverable). Routing `daytona/qwen3.8-27b` is a `404 Model 'daytona/qwen3.8-27b' not found` here and works there. 29 registered rows vs 30. |
| S2 | **The Lightning Chat↔Responses adapter is not ported** (`clients.rs:36-40`, `OMITTED` at `:620-624`). | The TypeScript does not register the four `lightning/*` clients either — the offering is disabled in `registry.ts`, so every `lightning/*` routing key resolves to nothing. | `/models` and `/stats` lose nothing: omitting them **is** the parity behaviour. If Lightning is ever re-enabled, the adapter has to be ported first. |
| S3 | **`/stats` serves JSON only**, and counters are **process-local** (`serve.rs:54-65`). | The TypeScript answers `GET /stats` with a server-rendered dark-theme HTML document (`runtime/endpoint/provider/private/stats/private-stats.ts:1013-1044`, the `raw` `text/html; charset=utf-8` `Response` at `:1033-1041`, the page's own palette at `:846`), and folds earlier processes' figures in through `stats/stats-buffer.ts`. Neither is ported. | The dashboard is a **consumer** of this JSON, and the TypeScript provider remains authoritative for the page. Consequences of the choice, all deliberate: the hide-unused filter (`private-stats.ts:276-284`) is NOT applied — every row is listed, with `enabled`/`discoverable` beside it; the grouped/family-tile projection (`collectGroupedStats`) and the HTML bar-scale maxima do not exist here; **counters reset on process restart**, and `POST` clears only the live counters (`client.rs`'s `reset_counters`), not a persisted entry. |
| S4 | **Port 8800, not 6660** (`serve.rs:146-168`). | The TypeScript provider's own listener stays on `PRIVATE_PROVIDER_PORT` = `6660` (`packages/agentic/provider/src/port.ts`), so a Rust listener there would collide with it. The consumer-facing `LOCAL_AREA_NETWORK_PROVIDER_PORT` is `8800` (`config/environment/src/port.ts:4`), and this crate publishes that deployed provider. | Consumers now dial `http://localhost:8800/providers/private/v1` (`packages/typecrate/harness/src/config.rs:36`, `packages/underload/harness/src/server/config.ts:272`, and the distribution defaults). The TypeScript provider remains on 6660 as the behavioural spec and fallback; nothing consumes that port. |
| S5 | **Keyless header policy is explicit**, defaulting to parity (`transport.rs:118-131`, `:578-605`). | The deployed TypeScript passes `apiKey: 'none'` and `openai@6.42.0` has no special case for `none`: `bearerAuth` returns a header for anything that is not `null` (`node_modules/openai/client.js:233-241`), so every keyless client sends the literal `Authorization: Bearer none`. The two local relays go one step further and pin `defaultHeaders: { Authorization: null }` (`local/local-strata.ts:161`, `local/local-basalt.ts:147`) and send **no** header. | The port exposes both as `KeylessHeaderPolicy` on the `Target`: **`SendNone` is the default** (`transport.rs:601`) — that is what today's relays actually receive — and `Suppress` (`:604`) is opt-in per row, used by exactly the credential-less local relay rows (`clients.rs:97-103`). A caller that assumed "keyless means no header" would have been wrong on every keyless row except the two relays. |
| S6 | **Credentials are loaded, not ported** (`serve.rs:38-50`, `vault.rs:13-21`). | Named `@config/vault` imports have no Rust equivalent and are not meant to have one; the exported artifact is the whole interface. | Guarantees that come with it: pools are ordered arrays and `default == entries[0]` with nothing re-sorted (above); the consumer scope is least-privilege, exactly the 11 vendors the provider uses (`consumers.ts:45-57`); the `sentinels` vocabulary is global, not scope-filtered (`export-vault.ts:308`, `:173`). The new failure mode is a **503 naming the fix** on a fresh clone, where the TypeScript has no such state (its unresolved credential fails per request with an upstream 401) — recorded at `serve.rs:48-50`. The artifact path is baked from the provider crate's own manifest dir, so it follows the crate, not the executable. |
| S7 | **The client layer is rows, not subclasses; refusals, not coercion; per-row pools.** (`client.rs:31-33`, `:124-142`; `registry.rs:41-50`; `client.rs:1266-1308`; `accounting.rs:52-54`, `:64-66`; `pricing.rs:25-28`; `serve.rs:66-95`; `client.rs:175-190`.) | Each collapsed axis is stated where it is decided: adding a model is one row, the request path is a closed `match`, and a bad document is a fix list instead of `apiKey: undefined`. See the marker table below for each one. | Individually small, jointly the shape of the crate. The ones an operator feels: a bad vault document never half-boots (the registry refuses with every row named); a drained row keeps sending its **last** credential instead of the TypeScript's no-header request; `attemptTotal`/`consecutiveFailures` exist here and have no TypeScript counterpart; latency totals are whole truncated milliseconds; `cache_write` is carried and priced into nothing. |
| S8 | **Counters are the served subset, not every tracked field.** | The TypeScript base tracks ~17 members — 10 lifetime counters (`packages/agentic/provider/src/model-class.ts:417-495`), the private `lastRequestTokens` continuation baseline (`:525`), three derived cost getters (`:534`, `:542`, `:548`), the two gate getters (`:378`, `:388`) and the mutable `upstreamLabel` (`:355`). | This crate models all of those as `Accounting`/`Counters` (`accounting.rs:95-147`) **plus** two port additions (`attempt_total` `:142`, `consecutive_failures` `:146`), but what `/stats` publishes is what the projection and the routes consume: the 10 counters per row (in the 20-key `ModelStats` shape) and the 11-key roll-up. The two port counters appear in `totals` only, never per row (`serve.rs:1098-1104`, `:1151-1158`), and nothing on this surface depends on them for parity — the `knowledge.md` ledger records how the contradicting doc sentence was corrected.

### Every `DEVIATION` marker in the crate, in file order

Evidence: `grep -rn "DEVIATION" --include=*.rs src endpoint tests` in `packages/typecrate/provider`
→ **25 hits** (23 in `src/`, 0 in `endpoint/`, 2 in `tests/`), and each is a row below.

| Marker site | What it deviates | Why | Consequence |
| --- | --- | --- | --- |
| `src/accounting.rs:52` | `attempt_total` + `consecutive_failures` are **port additions**; no TypeScript counterpart. | This port reports attempts and outcomes as values instead of thrown errors, so a supervisor can read a circuit-breaker figure without parsing messages. | Two extra `totals` keys; no parity figure depends on them. (But `accounting.rs:47-48` says the `/stats` projection ignores them while `serve.rs:1151-1158` does publish them in `totals` — a doc defect, see `knowledge.md`.) |
| `src/accounting.rs:64` | Latency totals are whole **truncated** milliseconds, per settlement. | Every span here is an `Instant` diff; TypeScript accumulates `performance.now()` differences, which are fractional. | A fleet total can read a few ms under the Node figure after many sub-ms requests. Nothing in the crate divides two counters together (the dashboard does its own rate math from the paired numerator/denominator — `private-stats.ts:88` states the Prefill formula the page renders), so truncation cannot distort a rate beyond the rounding the page already applies. |
| `src/client.rs:31` | The 30 TypeScript **subclasses** become **JSON rows**. | The interesting variation ("inject `vendor`", "default `reasoning_effort`", "rewrite `max_tokens`", "rotate the key that just failed") is one line per family, scattered over 30 files. | Behaviour is reproduced exactly; only the representation changes. Adding a model is one file in `config/`, and the whole served set is diffable there, which is what makes the ordering guarantees testable. |
| `src/client.rs:124-142` | Credential pools are **per client**, not the module-scope queue per family the TypeScript shares (`nvidia-glm5-flash.ts:9`, `makora-glm-5-3.ts:5`). | The transport owns the cursor and takes its target by value; a family-shared pool would mean re-shaping the transport for one parity detail. | Pool **order** is identical (vault document order, never re-sorted). A key drained by one row no longer drains its siblings: each row drains after its own `len` rotations instead of sharing one budget. |
| `src/client.rs:1266` | The `upstreamLabel` **allow-list** is a port addition. | `model-class.ts:344-350` states the security posture ("a NAME, never a URL, never a key value") as **prose**; nothing in the TypeScript enforces it. | Labels that violate it are refused at registry build (`registry.rs`'s label check, pinned by `tests/registry.rs::i_a_hostile_relay_label_is_refused_without_being_printed`); every label the live artifact publishes passes. A TypeScript-only label the list rejects (credential-shaped, whitespace, `:`) would be accepted there and rejected here. |
| `config/provider/**/*.json` | 30 registered TypeScript clients → **29 rows**. | Daytona omission (S1); the four Lightning clients are unregistered in TypeScript too (S2). | `/models` 15 rows: the TypeScript's 12 minus Daytona plus the four Vultr rows enabled here; the diff is asserted, not remembered (`src/clients.rs` `OMITTED`, `tests/registry.rs::a_…`). |
| `src/error.rs:39` | The classifier reads **data**, not an error graph — and since 2026-11-24 it reads ONE signal the SDK never had: the status an in-band frame declares, by **three** routes, as a **rescue** rather than an override. | TypeScript's `classifyStreamError` walks a thrown `unknown`'s `cause` chain for names, errno, status and message text (`start.ts:472-527`); a Rust failure is already typed. For an in-band `data: {"error":…}` frame the SDK hands the gateway no status at all: `new APIError(undefined, data.error, undefined, headers)` (`node_modules/openai/core/streaming.js:50`, `core/error.js:12`), so whatever the upstream wrote in `error.status` is invisible to the deployed TypeScript, which therefore answers `unknown`. Reproducing that parity made every failure the local relays report in-band — a dead engine (`Temp/basalt-check/serve/handler.py:733-735`), a relayed context refusal — come out as a bare `code: "unknown"` with no status and no reason, which is what the operator could not act on. | `FailureSignals` carries the same four signals plus a **fifth**, `declared_status`, and every rule **and rule order** of `start.ts:581-621` is reproduced. The split is the deviation: the parity field `status` keeps the TS meaning — a status some layer OBSERVED on a response head — so rule 3 is `start.ts:601` verbatim, and what the SDK discards goes in `declared_status`, which rule 3 never reads. `from_upstream_frame` lifts a valid numeric `error.status` (the same 100..=599 range test `readUpstreamStatus` applies, across the shapes BOTH deployed readers accept — `500`, `500.0`, `"500"`, `" 502 "`, `"0503"`; fractions, negatives, out-of-range values and non-digit strings still mean NO status, 2026-11-24), and failing that infers one — first from the **message and `error.code`**, then from the class. The message rule (`status_declared_by_size_refusal`, 2026-11-24) is the one that outranks the table: a sentence naming a size refusal is a **400**, matched against the closed four-marker `CONTEXT_OVERFLOW_MARKERS` (`context_overflow`, `context_no_room`, `exceeds the context`, `leaves no room to answer`, all case-insensitive) — in the frame's `message` AND in its `error.code`, because the structured error bodies the same fault produces elsewhere carry the word as a code (`Temp/basalt-check/serve/api/responses.py:42-43`). WHY: the upstream answers that IDENTICAL message with a client error on its pre-head path — the engine's overflow sentence is a `ValueError` and the handler's `ValueError` arm writes 400 / `invalid_request_error` (`Temp/basalt-check/serve/handler.py:551-555`), and the `/v1/responses` arm calls the same fault `context_length_exceeded` (`:774-776`) — while the mid-stream arm relays it as `server_error` under its own comment "engine ERR after the stream started: no 400 now" (`:736-738`). The class table alone would answer 500, because the relay stamps `server_error` on every engine `ERR` line, and 500 is a status the consumer's overflow gate refuses to read as an overflow (`packages/underload/harness/src/engine/transport/provider-timeouts.ts`, `isContextOverflowSignal`) — so on wording its own pattern list recognises, the 400 replaces the retry ladder with the compaction remedy. It is not a general unlock: that list is NARROWER than this crate's four markers (`:424-433`), so an engine-raw sentence like `prompt (N tokens) + max_new (M) exceeds the context (C)` gets the 400 and lands on the consumer's terminal `provider-rejected` (`:751-758`) — a terminal refusal instead of eleven retries, not a smaller prompt. Failing BOTH, the status the frame's `error.type` states (`UPSTREAM_TYPE_STATUSES`, eight rows since 2026-11-24 — `rate_limit_error` → 429 and `context_length_exceeded` → 400 joined the six, the first because a throttling proxy relays that exact OpenAI class and the second because 400 is the verdict the local server gives the same overflow on its `/v1/responses` path, `Temp/basalt-check/serve/handler.py:774-776` —, nothing inferred outside them). A numeric `error.status` outranks both inferences: where the upstream states a verdict, this crate repeats it. **ALL THREE ARE A RESCUE, NOT AN OVERRIDE** (the 2026-11-24 correction): rules 1-8 — names, errno, wording — run to a verdict FIRST, and only the `unknown` fallthrough consults `declared_status`, at one `if`. So `{"type":"server_error","message":"upstream stream terminated"}` is still `connection` with NO `status` key, and so is an `invalid json` frame under a `server_error` envelope or a timed-out one with `status: 500` beside it. WHY the ordering is not cosmetic: the consumer reads `code` before `status`, wire `connection` gets the short, quickly-terminal dial ladder (`provider-timeouts.ts:714-721`, `:101-106`), and `upstream_status` under a non-terminal 5xx falls to the codeless 1 + 10 transient ladder (`:761-790`, `:118-125`) — an earlier revision of this deviation let the declared number win, and it bought eleven extra attempts for a failure the transport had already diagnosed. A frame nothing named DOES become `upstream_status` with a real `status` key, which is the point of the deviation: the translator treats a non-terminal 5xx on the same transient ladder it used for the `unknown`, so nothing is lost and the status is gained. `StreamFailure::frame_status` survives for the cases where the emitted status and the number the frame wrote differ — both directions, the named-verdict half being what the correction protects — and `StreamFailure::log_line` prints whichever applies. The `unknown` residue keeps `UNKNOWN_STREAM_ERROR_MESSAGE` as the sentence's **prefix** and tails the upstream's detail. The free-text rules stay live because reqwest/hyper messages ARE the text those rules match (`other side closed`, `operation timed out`, `connection reset by peer`). |
| `src/error.rs:108` | The three `*_TEXT_PATTERN` rules (`start.ts:411-417`) become **needle lists**, not regexes. | They are alternations of literal phrases with one optional-character rule (`socket hang ?up`); expanding them is token-for-token equivalent and keeps `regex` out of this crate. | The equivalence is reviewable by reading the tables. `char::is_whitespace` stands in for JS `\s`, which additionally matches `U+FEFF`; no upstream message in this workspace carries it. |
| `src/error.rs:117` | The upstream's own message is stored **already redacted** (at capture), where TypeScript stores raw text and redacts when the frame is built (`start.ts:434-439`). | Crate rule: a credential value must never appear in an error, a log line or a `Debug`. | An upstream that echoes the caller's key back inside an error body cannot put that key into a `{:?}`. Redaction output is byte-identical (`**redacted**`) and the 200-char cap is the cap the gateway applied anyway (`start.ts:346`, `MAX_ERROR_DETAIL_LENGTH`). |
| `src/pricing.rs:25` | `cache_write` is **carried and validated but priced into nothing**. | No TypeScript client has a cache-write rate; inventing a formula would be worse than carrying a zero. | The wire shows no cache-write figure; a row that ever gets a published rate wires it into `Pricing::cost_input` in one place. The rename `cacheCost` → `cache_read` is deliberate: that field prices a cache **read**. |
| `src/registry.rs:49` | A bad row is a **refusal with a problem list**, where the TypeScript proceeds with `apiKey: undefined`. | TS constructors run at import time; a missing vault export surfaces as an unresolved value far from its cause. | One vault rename or row typo is one pass to fix, and the 503 body lists every row. A deployment that "works" in TypeScript (one row quietly unusable) is a hard refusal here. |
| `src/serve.rs:48` | Missing / unreadable / invalid artifact ⇒ **503** with an actionable body. | The TypeScript has no such state: its credentials are compiled in. | A new status the deployed surface never returns, on a fresh clone it is the expected first answer. Nothing aborts the host process. |
| `src/serve.rs:96` | A `model` field that is **absent or not a string** says so, where the deployed relay prints `Model 'undefined' not found` (2026-11-24). | The deployed message is a JS template literal over `body.model`, and the route's destructure (`private-chat-completion.ts:66`) left `undefined` in it. `undefined` is an artefact of the implementation language, not a model id: it named nothing, and in production it was read as a client that had literally sent the string `"undefined"`. | The 404 keeps the same **status** and the same `{"error":"…"}` envelope — both are the deployed verdict, and 404 is terminal for the consumer either way, so changing the status would trade parity for nothing — and only the sentence changes: `MISSING_MODEL_FIELD_MESSAGE`. A model that IS a string but is not served keeps `Model '<id>' not found` verbatim. The upstream's own wording for the same fault is a required-field message (`Temp/basalt-check/serve/handler.py:684-690`, `_unknown_model` with `name is None` → `model is required`), so the new sentence names the FIELD and both ways it can be wrong. The operator half moves to the log: one `warn!` per refused request with the route, the byte count and the body's **top-level key names** — sorted, capped (`MISSING_MODEL_KEYS_SHOWN`), run through `flatten_error_detail` so no key can forge a line or smuggle a secret, and replaced by "the body is not parseable JSON" when the body was not parseable JSON. **No message content, no tool payload, no header or credential value is ever logged there.** Pinned by `tests/routes.rs::a_model_the_registry_does_not_have_is_a_404_naming_the_model_and_never_dials` and `::a_body_that_is_not_a_json_object_resolves_no_model_and_is_a_404`. |
| `src/serve.rs:159` | Listener number **8800**, not 6660 (S4). | 6660 is the TypeScript provider's own `PRIVATE_PROVIDER_PORT`, kept as the behavioural spec and fallback. | Consumers dial 8800 through `LOCAL_AREA_NETWORK_PROVIDER_PORT`; nothing consumes the TypeScript provider's 6660. |
| `src/sse.rs:54` | **Three one-directional leniencies** vs the SDK's `SSEDecoder`: every line and payload is trimmed, a body that ends without the blank separator still delivers the event it started, and an event with no `data:` field produces nothing. | The decoder now matches the authoritative rule — one event, one payload, joined with `\n` at the event boundary (`streaming.js:255-268`) — and the three leftovers are cases where the SDK **drops or rejects** bytes: the SDK strips only a trailing `\r` and one leading space (`:252-254`, `:274-276`) and splits the field name at the first colon (`:273`); it emits an event only when it reads an empty line (`:255-268` with `:212-216` — the end-of-body flush feeds the leftover lines to that same decoder and yields only what comes back), so a truncated body loses its last event there; and a data-less event reaches its caller as `data: ''` and fails `JSON.parse` (`:41-48`). | The direction is **not the same for all three**: leniency 3 can only deliver LESS, while leniencies 1 and 2 can deliver a chunk the deployed decoder never produced — an event not closed by a blank line is relayed HERE and dropped THERE, so a caller of this decoder **can observe one extra chunk**, and an indented `data:` line is a chunk here and nothing there. What no leniency does is invent a payload the deployed RELAY would not have emitted: every chunk handed over is one the TypeScript put on the wire, and the whole difference is in what the inbound decoder keeps. `src/sse.rs` pins each one by name, including the trimmed-field case that would be a different field name under the SDK rule, and the `[DONE]` **prefix** rule with non-whitespace text after the sentinel (`the_sentinel_prefix_ends_the_stream_even_with_text_after_it`), which trimming cannot hide. |
| `src/transport.rs:95-110` | The 429 retry loop is **bounded** to `1 + 3` attempts; the deployed 429 handler pauses 3 s, returns `true` and never counts attempts (`model-class.ts:572-576`), so its loop is `while (true)`. | An unbounded 429 loop against a relay that always answers 429 is an unbounded request — the 30-minute ceiling does not stop it, because each attempt restarts the clock (`model-class.ts:624`, `:628`). The budget is also NOT available to the deployed provider for anything else: `maxRetries: 0` is forced per request (`:885-892`), and a status with no handler returns the response un-retried (`:599-600`). | Exhaustion is `TransportFailure::retries_exhausted` carrying the resume state; a persistently rate-limiting upstream fails here instead of parking a worker forever. **Retry behaviour is explicitly not claimed to be identical** — `tests/transport.rs` pins the bounded count, the 404 rotation stop and the never-retried arms, not a parity match with the TypeScript's loop. |
| `src/transport.rs:110-117` | Connection and timeout failures are **also retried** here. | The TypeScript neutralises the SDK's own retries (`maxRetries: 0`, `model-class.ts:885-892`), so a dropped socket fails on the spot today — but its own comment (`:625-627`) says the budget exists for transient connection drops when an idle proxy connection is reaped mid cold-start. | A flaky socket succeeds here where the deployed provider surfaces the failure. Same budget, different spend; the difference is a deliberate deviation, not an accident of implementation, and it shares the one bounded budget described in the row above (`transport.rs:95-110`). |
| `src/transport.rs:127` | Keyless header policy: **default `SendNone`** (this corrects the module's first cut, which sent nothing for every keyless credential). | Sending nothing was right only for the two relays that null the header and wrong for every other keyless row (S5). | Wire parity with the deployed surface; both arms are pinned end to end against a listener that reads the real header (`tests/clients.rs::a_keyless_relay_sends_no_header_while_a_keyless_pool_sends_bearer_none`). |
| `src/transport.rs:320` | The URL-normalisation rule's cited source **does not exist**: the brief cited `model-class.ts:322-327`, which is the `id` field's doc comment; the only URL surgery in that file is `rebaseRequestInput` at `:141-164`. | The rule itself is what the deployed endpoints and the Daytona client do, so it is implemented as specified and the citation is corrected here rather than silently satisfied. | `normalize_base_url` is this port's own rule (trim + scheme handling), not a port of a TypeScript function. Verified independently: `rebaseRequestInput` is at `model-class.ts:141`. |
| `src/transport.rs:593` | "DEVIATION FROM C2": the transport previously hard-coded the no-header half for **every** keyless credential. | That silently diverged from the deployed provider on every keyless row except the local relays. | The behaviour is now a typed policy; the deviation lives in code, not in a comment. |
| `src/transport.rs:2108` | An undecodable `data:` line is a **fatal stream whose origin is `Transport`**, while the failure code still says `decode`. | Our decoder is the component that can no longer continue, so "fatal stream" is the honest verdict; consumers branch on the code, which matches the SDK's `SyntaxError` class. | A supervisor sees a fatal stream where TypeScript would surface a thrown decode error inside the iterator. The wire frame is unchanged. |
| `src/transport.rs:2447` | Output-token extraction returns `u64`, **truncating toward zero**; TypeScript returns `number`, so a fraction survives. | Every counter the stats projection feeds is an integer, and relays report integers in practice. | A fractional usage figure is truncated here and accumulated as-is there — sub-token drift only. |
| `src/transport.rs:2464` | The token estimator counts **UTF-16 code units**, not bytes. | JS `.length` counts code units; Rust's `len()` counts bytes, so a non-ASCII body would estimate differently than `ceil(len/4)` in the deployed estimator (`model-class.ts:82-85`). | Parity with the deployed figures for CJK/emoji bodies; a naive byte count would over-estimate. |
| `tests/clients.rs:1399` | Pin: a drained pool **re-sends its last credential**; the TypeScript sends no header after `apiKey = ''`. | The transport's exhaustion fallback is documented behaviour — the cursor stays on the last entry, because `local-strata.ts:244-250` returns false without clearing `baseURL`. | A drained row keeps authenticating (better on relays that reject anonymous traffic) and stays routable; discoverability hides it either way. |
| `tests/registry.rs:1052` | Same deviation, registry side: the row keeps its **last credential label** after the drain. | As above; the row is unreachable through the catalog anyway. | `switch_base_url()` reports the drain as sticky (`false`), the label remains nameable for diagnostics, and the catalog stays correct. |

Prose-only deviations that are not marked with a `DEVIATION` token, so a grep for the token alone
under-counts them: the numbered serve deviations plus the parity notes between them
(`serve.rs:54-93`: JSON-only stats, the row-owned upstream path, the verified malformed-body and
pre-stream-500 parity, no request-body cap), the
"still to port" list (`lib.rs:29-38`), the two omitted client surfaces (`lib.rs:49-56`,
`clients.rs:21-43`), and the unported per-call `options.baseUrl`
(`client.rs:175-189` — `knowledge.md` records how its stale `options.baseUrl` evidence was corrected).

## Parity with `@agentic/provider`

_Modelled on `packages/typecrate/harness/README.md:138-154` (capability-by-capability table)._

| Capability | TypeScript (`@agentic/provider` + its routes) | This build |
| --- | --- | --- |
| Registered rows | 30 (`packages/agentic/provider/src/registry.ts:49-206`, Daytona at `:68`) | **29** (`config/provider/**/*.json`); Daytona omitted (S1), the 4 Lightning clients unregistered on both sides (S2) |
| Discoverable rows | **12**, pinned at `model-class.test.ts:278`; Daytona is one of them (`:311-315`) | **15** against the live artifact — Daytona is omitted, and the four Vultr rows are enabled in this table. Pinned by `tests/registry.rs::c_the_live_artifact_builds_every_row_and_the_discoverable_subset_matches_the_baseline` |
| Row order | the `privateClients` array, appended "to preserve all existing registry indices" | **alphabetical by `id`**, computed by `build.rs` at compile time and never re-sorted (`registry.rs:14-20`) |
| Route paths / verbs / bodies / statuses | the four `service-route.ts` files + `handler.ts:42` + `start.ts:910` | verbatim, pinned by `tests/routes.rs` |
| A body that is not a usable JSON object | a body that fails to parse is folded into `{}` by `start.ts:815`, while an array or a scalar the reference keeps INTACT — either way `model` resolves to nothing and the route's own lookup answers `404` (`private-chat-completion.ts:65-66`, `:75-83`); `null` is the one shape NOT folded, because the destructure throws and the gateway answers `500 {"error":"Internal Server Error"}` (`start.ts:908-911`) | the **status** and the fall-through are parity, on both POST surfaces. The reference's sentence interpolates the JS artefact `Model 'undefined' not found`; this port reports the missing field instead — deviation row `src/serve.rs:96`. Pinned by `tests/routes.rs::a_body_that_is_not_a_json_object_resolves_no_model_and_is_a_404` and `::a_null_body_is_the_one_shape_the_gateway_does_not_fold` |
| Port | `6660` (`PRIVATE_PROVIDER_PORT`, `packages/agentic/provider/src/port.ts`) | **8800** (S4; `LOCAL_AREA_NETWORK_PROVIDER_PORT`, `config/environment/src/port.ts:4`) |
| `/stats` answer | server-rendered HTML page (`private-stats.ts:1013-1041`) + cross-session buffer + grouped tiles | **JSON projection only**, live counters only (S3) |
| Keyless upstreams | `Authorization: Bearer none`, except the two local relays that null the header | same, as an explicit per-row policy defaulting to parity (S5) |
| Failover | 404 rotates credentials (Makora re-sends its head; NVIDIA advances) or relays, until `switchBaseUrl()` says no | same two drain shapes on the credential axis (`client.rs:110-122`), and on the relay axis a **per-request walk** over a shared verified-prefix head: `start = min(head, last)` once, one PINNED entry per attempt, `head := max(head, position + 1)` only from a 404 this request dialled (`client.rs:56-108`, `:856-989`, `:1073-1123`; `transport.rs:1281`/`:1308`). Loop owned by the client, `requestTotal` unchanged by rotations, and the lockstep cost of the invariant documented there |
| Retry | 429 pauses 3 s and retries **forever** (`model-class.ts:572-576`); the SDK's own retries are neutralised per request (`:885-892`), so a connection/timeout failure is not retried today, and a status with no handler is returned as-is (`:599-600`) | bounded `1 + 3` for every retryable outcome, connection/timeout included — **not claimed identical** (`transport.rs:94-117`, rows `transport.rs:95-110`/`:110-117` in the deviation table) |
| Counters | 10 tracked + baseline + 3 cost getters | those 10 + 2 port additions (S8) |
| Cost | dollars/M as JS numbers | `f64`, same `(tokens × price) / 1_000_000` shape (`pricing.rs:30-35`) |
| Daytona sandbox | registered, endpoint from a sandbox handle | absent (S1) |
| Lightning adapter | implemented, unregistered | absent (S2) |
| Stats HTML + buffer + tiles | shipped | absent (S3) |

## Test parity and coverage

_Modelled on `packages/typecrate/harness/README.md:258-314` (commands + a per-target count table)._

**Approved scope: a behavioural test core, not 1:1 parity with the TypeScript suite.** The TypeScript
side is **35 `*.test.ts` files** under `packages/agentic/provider/src` declaring **294 cases**
(counted on this pass with `find src -name '*.test.ts' | wc -l` and a
`grep -rcE '^\s*(test|it)(\.\w+)*\('` over those files; the orchestration brief's "~270 / 34" is the
same order of magnitude). The Rust side is a per-stage behavioural core around the acceptance letters
the suite names (`tests/registry.rs:3-16`: A4 pool order/rotation, A5 drain + discoverability, A6
relay labels, A7 catalog reconciliation, A8 the whole row table, A9 public shapes + refusals).

| Target | Tests | What it pins |
| --- | --- | --- |
| lib unit tests (`src/lib.rs` 2, `src/serve.rs` 9, `src/sse.rs` 9, `src/vault.rs` 15, `src/transport.rs` 2) | **37** | the version/`lib`-name handshake (2), the artifact-path constant, the token line in both shapes, the `toFixed` tie rule, the frozen missing-`model` sentence that replaced the JS-interpolated `undefined` label (deviation `src/serve.rs:96`), the body-key list line — sorted names, the 16-name cap, the four honest shapes, and NON-WHITESPACE control characters stripped so a hostile key cannot inject a terminal escape (2026-11-24, N6) —, query parsing, 503 redaction, env precedence, and the request-log relay attribution — 9 in `src/serve.rs`, the ninth pinning the SERVED endpoint's name rather than the shared selection —, the SSE decoder (9): a frame split at any offset, a burst of several frames, the several `data:` lines of one event as ONE payload joined with `\n`, a field line that is not `data:` carrying no payload, the sentinel matched as a PREFIX while a payload merely containing it is not, the sentinel prefix ending the stream even with NON-whitespace text after it (the case trimming cannot hide, and the only one that fails if the prefix rule is replaced by equality), `finish()` flushing the event a truncated body left open, CRLF boundaries plus the trimmed-field leniency, and being finished after the sentinel; and the vault contract: live-artifact shape, document order, `default == head`, the five validity classes, `Secret` unprintability, `deny_unknown_fields`, all-problems-at-once, missing-file IO class (15); and the two inline `src/transport.rs` contentions (`advance_hands_each_racer_the_slot_it_claimed` `:2503`, `concurrent_verifications_of_one_entry_publish_one_head` `:2569`) |
| `tests/accounting.rs` | **15** | the counters as data: a zero ledger with no baseline, the continuation diff (first request / appended turns / an identical body fully cached / a shrunk body restarting the conversation), reset clearing counters and baseline together, non-streaming settlement with and without a usage block, a failed request still settling its wall clock, the phase-split pairing on the first payload, a stream without usage keeping the decode denominator clean, a token-less stream still settling, cost read at the pinned lifetime rates, a cached prefix priced only at the cache rate, and `Totals` summing field by field |
| `tests/transport.rs` | **54** | header writing (keyless both ways, keyed, custom override, canonical order), `stream_options` in/out/null branches, URL rebasing in four branches, 404 as a typed failover verdict, SDK-fallback and HTML-error-body messages, the 429 pause, the 1+3 budget, connection failure after the budget, abort constant, rotation → exhaustion, resume ledger, the deployed exhausted-pool sentence, SSE framing (split/coalesced frames, sentinel absence, mid-stream error classes, **an in-band error frame that carries a status becoming `upstream_status` with that `status` on the wire** (2026-11-24), **the `server_error` class the local relay sends inferring 500 with the engine's reason preserved, a class outside the closed table inferring nothing, and a written number always beating the class**, **a status-less message naming a size refusal inferring 400 ahead of the class table — every marker row asserted as data, the per-slot wording pinned as the deliberate non-match, and a written number beating THAT rule in both directions (`status: 500` beside overflow prose stays 500)** (2026-11-24), **a status a stronger rule withheld still being named as `frame_status` in the operator log line**, a relayed context-window refusal keeping its wording in the wire `message` **and now carrying the 400 the consumer's overflow gate needs beside it**, **a declared status RESCUING rather than overriding (2026-11-24, C1): a worded in-band frame keeps `connection`/`decode`/`timeout` and emits NO `status` key even when it also names a class or writes a number — the withheld number still surfacing as `frame_status` on the operator line — while a frame nothing else named takes the verdict it declared**, **`error.status` read from every shape the deployed readers accept (`500`, `500.0`, `"500"`, `" 502 "`, `"0503"`, plus the refusals: fractions, negatives, out-of-range in either shape, and non-digit strings) (C3)**, **the size-refusal marker read from `error.code` as well as from the message, including the `{"type":"server_error","code":"context_overflow"}` frame that used to infer 500, and an unrelated `code` leaving the class table alone (C4)**, and **the class table's two 2026-11-24 rows asserted as data (`rate_limit_error` → 429, `context_length_exceeded` → 400) with `usage_limit_exceeded` pinned as the class it refuses to price (C2)**, a content-free `{"error":{}}` degrading to the frozen constant with no new code spelling, **a pre-stream HTTP status stamping the status the response head carried**, an undecodable body as a terminal decode failure with its sentence asserted, drop stops reads), settlement exactly once, usage absent-vs-zero, the TTFT stamp, axum-without-`sse`-feature, no credential in any diagnostic, the classifier ladder, text hygiene, the token estimate, the lockfile subset, **the seam walk restated in verified-prefix-head terms (with the clone-shares-the-head, not-the-walk-position note), a request that PINS an endpoint dialling the pin and never the shared selection, and a pinned request whose 429 retry stays on that pin** |
| `tests/clients.rs` | **31** | the wire as the client sends it: pinned `model`, `stream_options` only where needed, `vendor` overwrite, `reasoning_effort` branches (absent/null/empty/`none`), `max_tokens` cap, `stream` normalisation, prompt estimate over the CALLER's body, keyless header both arms, relay-label attribution, the two drain shapes, drained-row header, no rotation for plain rows, relay rotation, invalid pool head still the head, exactly-once settlement (success / undecodable / abandoned / cancelled), the 429 retry inside the transport, the discoverability × validity matrix, no credential in the failure vocabulary or `Debug`, the label allow-list, and that each row builder changes exactly its own field, **the per-request endpoint walk: concurrent requests walking the verified prefix and never dialling an unverified relay (the relay past the first viable one stays undialled by anyone), every request walking a fully-outaged pool and refusing with the 404 it personally collected, a second request starting where the first proved the pool dead (plus the rewind seam resetting it), and the `switch_base_url` seam publishing exactly one step per call** |
| `tests/registry.rs` | **12** | the 29-row table in registry order, the whole projection of every row, the live-artifact build + the 15 discoverable ids + 14 hidden, hidden-but-routable and unknown-id `None`, the nvidia document-order rotation, the one-entry makora repeat-then-drain, the named relay rotation, a hostile label refused without being printed, a nested credential group, a miswired row refused by name, one attributed problem list per row, and the process registry installing once and aggregating every ledger (the artifact-dependent cases run against the live artifact when it is on disk, and print a loud `NOT CHECKED … ran ZERO assertions` line naming the artifact, the regeneration command and the `--nocapture` re-run when it is not — the skip helper is `tests/registry.rs:188-208`) |
| `tests/routes.rs` | **22** | the mount literals incl. `PROVIDER_PORT == 8800`, the verb gate on all four routes, the verb gate ahead of readiness, the 503 naming the regeneration command, a settled completion verbatim, the 404 naming the model and dialing nothing, **a body that is not a JSON object being folded into the empty object the gateway parses — 404 on both POST surfaces, its body now naming the missing `model` FIELD rather than the JS artefact `undefined` (deviation `src/serve.rs:96`), and every non-string `model` shape landing on that same sentence while a string id keeps `Model '<id>' not found` —**, **a `null` body being the one shape the gateway does not fold (500, its exact body)**, the pre-head upstream 500 leaking nothing, frame-by-frame stream copy terminating with the sentinel, an early-stopping upstream, a mid-stream error frame with and without a status — the one WITHOUT one asserting the frozen `unknown` message with the upstream's sentence tailed after it, the one WITH one asserting `upstream_status`, its `status` key and the target-naming sentence, the `server_error` shape the local relay actually sends relaying a 500 plus the engine's reason, and **the mid-stream context refusal arriving as `server_error` with no number: `upstream_status` + `status: 400` with the engine's sentence preserved byte for byte in `message`, and a frame that DID write `status: 500` beside the same sentence keeping its 500** —, the Responses relay, the catalog envelope hiding placeholder rows, the JSON-only stats document, the reset verb incl. the ids it refuses, **and the operator log lines a request produces — the head line naming row and relay, the two `debug!` flight lines, the failure line with its code, origin, chunk count, status and the upstream's reason, and the missing-`model` warn naming the route, the byte count and the SORTED top-level key names of the body (with its four honest shapes: object, no-keys, non-object JSON, not-JSON, and both route tokens) while a credential value and a message canary are asserted absent from every captured line — all captured through a `tracing` subscriber** |
| `tests/bin_server.rs` | **19** | the standalone host's own rules: the default is a reachable listener on the declared port (`0.0.0.0:8800`), the environment supplies the address when no flag does, a flag beats that environment, an address that already names a port wins over `--port`, an unusable `PORT`/`host`/option is bad **usage** (exit 2) not a server fault, `--help` short-circuits resolution but never hides an unknown option, the flags parse into ONE precedence chain, **a refused vault refuses the bind and names the regeneration command**, the banner names the vault input without printing it and reports URLs a client can open, `HELP` documents every line `start` prints, the bin serves `/health` plus the four contract routes on a real socket, every contract route is `ANY` so the handler prints the verb refusal, **the contract routes answer 503 while `/health` still answers 200**, an over-limit body takes the gateway's 500 rather than a local 413, and the log filter (bare level, `target-prefix=level`, a bad directive, allow-by-target-then-level, one event = one line) |
| `tests/gateway.rs` | **8** | the gateway surface `src/serve.rs` publishes for a host that is NOT `typecrate-services` (`with_gateway_headers` `:758`, `is_cors_preflight` `:786`, `gateway` `:803`, `health_document`) against a real listener: a preflight is the method **plus** the `Access-Control-Request-Method` header and neither alone, the policy answers every origin/verb/header plus `Access-Control-Allow-Private-Network: true`, it replaces a policy a handler tried to narrow, the `/health` body is the exact document a probe reads, a preflight answers `204` with the policy and no body, a bare `OPTIONS` reaches the route and takes the gateway's `400`, a routed answer, a fallback and an uncatalogued path all carry the same policy, and a preflight is answered even while the vault is down |
| doc tests (`cargo test`'s doctest target) | **2** | the two runnable examples in module docs: `src/vault.rs:900` (`load_from_str`) and `src/transport.rs:1868` (`prepare_body`) — exactly the two lines `cargo test --doc` names |
| **Total** | **200** | 198 attributed `#[test]` / `#[tokio::test]` functions **+ 2 doc tests** — 200 cases, which is exactly what `cargo test` reports (196 passing + the 4 vault-artifact failures named below) |

> **The 198-vs-200 question, settled.** A static count of test **attributes**
> (`grep -rE '#\[(tokio::)?test(\(|\])' src tests` → **198**: 2 + 9 + 9 + 2 + 15 in the lib unit
> tests and 15 + 19 + 31 + 8 + 12 + 22 + 54 across the seven integration targets) is **not** the
> number `cargo test` prints, because cargo adds the doctest target. The run the 2026-11-25 pass
> performed (`cargo test --locked --no-fail-fast`, from `packages/typecrate/provider`)
> reported, per target: **35 passed / 2 failed** lib unit (37 there) · **0** `src/bin/server.rs`
> unit · **15** `tests/accounting.rs` · **19** `tests/bin_server.rs` · **31** `tests/clients.rs` ·
> **8** `tests/gateway.rs` · **10 passed / 2 failed** `tests/registry.rs` (12 there) · **22**
> `tests/routes.rs` · **54** `tests/transport.rs` · **2** doc tests =
> **196 passed, 4 failed, 0 ignored**. The four are ONE pre-existing cause outside this crate's
> behaviour — the regenerated `config/vault/generated/agentic-provider.json` carries more `label`
> members than `vault.rs` pins (45 against 43, 31 against 28), which fails
> `vault::tests::live_artifact_matches_the_documented_shape`,
> `vault::tests::a_credential_value_can_never_be_printed` and the two registry tests that dial the
> live artifact's pools — and the artifact is do-not-touch. So the two figures never disagree:
> 198 + 2 = 200. Two mechanics are worth knowing before any count is trusted: the doctest target
> only runs when cargo's configured `TMPDIR` (`Temp/rust`, `.cargo/config.toml:33`) exists, so a
> missing one silently removes 2 from every total; and `--no-fail-fast` is required, because without
> it the run stops at the first failing target and reports a partial tally. The
> `src/bin/server.rs` unittest target exists and reports 0 — the bin's rules are
> tested from `tests/bin_server.rs`, which pulls the file in with `#[path]` (that file's own header
> explains why: edition 2024 makes writing the process environment `unsafe`, so a test feeds values
> to the pure resolver instead of calling `set_var`).

**Covered, plainly:** the credential contract (shape, order, validity classes, redaction), pool and
endpoint rotation order and drain behaviour, the payload rewrite per family, the header and retry and
failover rules of the outbound leg, SSE framing and settlement exactly-once semantics, the failure
taxonomy and classifier ladder, the wire shape of all four routes including the refusals, and the
row table's identity/pricing/visibility data.

**Not covered (gaps, stated so nobody over-reads the 148):**

* **No live upstream.** Every HTTP-level test drives an in-process `axum` listener on `127.0.0.1:0`
  (`tests/routes.rs:135`, `tests/transport.rs:3-8`), and the domain suites (`tests/registry.rs`,
  `tests/accounting.rs`, the lib unit tests) build registries from in-memory documents instead. Nothing
  here dials a real relay, and the real-vault-artifact tests build registries, they do not dial.
* **No end-to-end services mount.** The discovered route file is pinned by literals and by
  `services`' own port test, but no provider test runs inside the `typecrate-services` binary, so the
  CORS/private-network decoration, the per-port listener and the `/v1/services/routes` listing for
  these four rows are proven only on the services side (`packages/typecrate/services/tests/server_http.rs`,
  `packages/typecrate/services/tests/discovery.rs`, and
  `packages/typecrate/services/src/server/mod.rs:798-825`, which pins `declared_ports()` to
  `[8010, 8800]` and counts the four route rows that declare 8800 — as of this pass, that file is
  edited concurrently).
* **No HTTP client-level stats/dashboard consumer test**, no HTML dashboard parity tests (S3), no
  Daytona or Lightning coverage (S1/S2 — they are absent, not exercised), no WebSocket/streaming
  client (this surface has none), and no cross-process stats persistence (S3).
* **Not every row is exercised individually.** The 29 rows are pinned as **data** (identity, pricing,
  visibility, behaviour, pools) in `tests/registry.rs` and `tests/clients.rs`; the per-family request
  shapes are exercised by family, not 29 times over — the same distinction the harness crate draws for
  its tool catalogue.
* **Real-clock timing is a proxy**: the 429 pause is asserted by measuring an injected short pause, not
  the deployed 3 s (`transport.rs:614-620`).

**Pre-existing TypeScript failures this work does not fix** (verified on this pass with
`yarn vitest run src/local/local-basalt.test.ts` from `packages/agentic/provider`: **2 failed | 15
passed**):

| Failing assertion | Why it fails | Not a port regression, because |
| --- | --- | --- |
| `packages/agentic/provider/src/local/local-basalt.test.ts:51` — `expect(client.inputCost).toBe(0)` got `0.1` | The Basalt client carries real rates today: `inputCost = 0.10`, `cacheCost = 0.01`, `outputCost = 0.60` (`src/local/local-basalt.ts:118`, `:121`, `:124`). The test still asserts the offering's original free pricing. | The Rust row prices the **code**: `.priced(0.10, 0.01, 0.60)` (`clients.rs:467`). Porting the stale expectation would have cost parity. |
| `…:71` — `expect(client.costInputTotal).toBe(0)` got `0.11` | Same cause, the getter at `model-class.ts:534` applied to a seeded token total. | As above. |

## Two implementations, two ports

`@agentic/provider` is **deliberately kept** — the port was specified as "duplicate the
functionality", not "replace it". Both implementations serve the same `/providers/private/v1` surface,
on separate ports: the TypeScript provider stays on 6660, the deployed Rust provider listens on 8800,
and consumers dial 8800:

| | TypeScript | Rust |
| --- | --- | --- |
| Code | `packages/agentic/provider/src` + `runtime/endpoint/provider/private/**` | `packages/typecrate/provider/src` + its `endpoint/provider/provider-route.rs` |
| Listener | 6660, its own `@underload/service` process (`PRIVATE_PROVIDER_PORT`; kept as the behavioural spec and fallback, not consumed) | 8800 in **either** host: this crate's own `[[bin]]` process (started natively or pushed as a cloud payload), or inside `typecrate-services` when the route file is the host |
| Extra routes on that port | its `/playground*` and `/v1/services/*` builtins belong to the TS gateway on 6660; on the Rust side those builtins live on the services default listener and stay there (`endpoint/provider/provider-route.rs:56-59`). The bin adds exactly one path of its own, `GET /health` (`src/bin/server.rs:112`) | |
| Authority today | behavioural spec and fallback only | **authoritative** for deployed consumers — the harness and every other consumer default dial `http://localhost:8800/providers/private/v1` (`harness/src/config.rs:36`, `packages/underload/harness/src/server/config.ts:272`) |

What remains before the TypeScript implementation can be retired:

1. **Port the stats surface the dashboard needs** — the HTML page, the `stats-buffer.ts` cross-session
   fold, and the grouped tiles (S3) — or move the dashboard to consume this JSON and accept a
   restart-resets-counters difference.
2. **Port the missing substrates**: the Daytona sandbox handle (S1) and, if Lightning is re-enabled,
   the Chat↔Responses adapter (S2).
3. **Re-key the vault grant guard.** `config/vault/src/vault/grants.test.ts` is the regression guard
   that the `agentic-provider` scope equals what the consumer actually imports, and it still scans
   `packages/agentic/provider/src`. Its own header says that when the provider is ported away, that
   file is what must be retargeted at the new consumer — for this crate, the site that reads
   `config/vault/generated/agentic-provider.json` (`grants.test.ts:22-26`).
4. **Decide the parity questions the ledger leaves open**: whether a row may keep sending its last
   credential after a drain (S7), whether bounded 429 retries are the intended answer (transport
   markers), and whether the JSON-only `/stats` is the target shape.

## Verification

_Modelled on `packages/typecrate/harness/README.md:258-314`._

The four cargo gates, from `packages/typecrate/provider`. All four were **run again on 2026-11-24**,
after the in-band status, size-refusal and missing-`model` passes landed. They were run
without `--offline` and needed no network, because the dependency graph is a subset of what the sibling
typecrate builds already resolved (`tests/transport.rs::the_lock_package_set_is_a_strict_subset_of_the_siblings`);
on a host with no cargo cache, add `--offline` per invocation (`package.json` `#locked` — no script
carries it):

```bash
cargo fmt -p typecrate-provider -- --check                    # exit 0 — clean
cargo check --workspace --all-targets --locked                # exit 0 — clean
cargo clippy --workspace --all-targets --locked -- -D warnings # exit 0 — no warnings
cargo test --locked --no-fail-fast                            # 196 passed, 4 failed, exit 101
```

That last line is the honest outcome, not a clean one: the four failures are the pre-existing vault
artifact drift described below, not a behaviour fault, and they were failing before this pass and are
failing after it. The `cargo test` line breaks down per target as **35 passed / 2 failed** lib unit (37
there) + **0** `src/bin/server.rs` unit +
**15** `tests/accounting.rs` + **19** `tests/bin_server.rs` + **31** `tests/clients.rs` + **8**
`tests/gateway.rs` + **10 passed / 2 failed** `tests/registry.rs` (12 there) + **22**
`tests/routes.rs` + **54** `tests/transport.rs` + **2** doc tests — the 198 + 2 arithmetic settled in
[Test parity and coverage](#test-parity-and-coverage). Artifacts land in the repo-root `.rust/target`
(shared with `packages/typecrate/services`); this pass hit no cargo lock contention.

The two TypeScript gates are separate, and they only exist **inside the monorepo** (they import
`@config/vault` and `@services/daytona` and extend the root `tsconfig.json`) — which is exactly what
`.github/workflows/ci.yml` records about what CI can and cannot run:

```bash
yarn tsc --noEmit -p packages/typecrate/provider              # exit 0 (from the repo root)
yarn vitest run packages/typecrate/provider/scripts           # 101 passed / 3 files, no network, no cargo
npx eslint <the four scripts/*.ts>                            # 0 problems (`*.test.ts` is eslint-ignored,
                                                              #   eslint.config.mjs:17)
```

The deploy path was verified **as far as an offline host can take it**: `deploy:endpoint <url>
--dry-run` and `start:daytona -- --dry-run` both exit 0 and print the real 1,616,247-byte bundle, the
member table with its modes, the v2 app identity (`provider`, `targetPrefix /providers/private/v1`), the
config JSON and its base64url header (the run is quoted under
[Deploying it](#deploying-it-the-three-verbs)). What was **not** performed on this pass: a real
`POST /apps` / `PUT /apps/provider` against a live v2 box (that needs the network), so live provider
conformance is unverified here — the v2 routes, headers and polling are pinned on the client side
(`yarn vitest run services/daytona/sandbox`, which includes the v2 conformance suites) and in
`services/daytona/sandbox-specs.md`, which this task only reads.

Everything else this pass ran was read-only: `git status`/`grep`/`find`/`sed`/`stat`. The v1 wording this
pass removed from the verbs is called out in `knowledge.md`'s ledger (row 14), and every deploy-shaped
sentence in this file now names the v2 route, the app name, the `targetPrefix` or the loopback probe
rather than a v1 slot/port. Script-side pointers name the **declaration** (`DEFAULT_PROBE_PATH`,
`APP_NAME`, `TARGET_PREFIX`, `resolveAdminToken`) instead of a line, because the shared client and the
Modal container file are edited elsewhere in this session and renumber again; a number goes silently
wrong, a symbol name stays greppable.
