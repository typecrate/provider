//! The four provider service routes: request validation, SSE assembly, JSON envelopes, counters.
//!
//! # Typecrate port - source lineage
//!
//! One module, four handlers, each with one TypeScript origin. The paths are kept VERBATIM, so a
//! caller of the deployed TypeScript surface changes nothing but the port:
//!
//! | Rust handler         | Mount                                         | TypeScript origin                                                                                                                                                                                                                                                                                                                                                                                     |
//! |----------------------|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//! | [`chat_completions`] | `POST /providers/private/v1/chat/completions` | `runtime/endpoint/provider/private/chat-completion/{service-route.ts,private-chat-completion.ts}` - the id lookup and its 404 (`:75-83`), the pinned-model swap (`:91`), the streaming branch (`:101-193`), the frame sequence `data: <json>` then `data: [DONE]` (`:161-186`), the relay label captured at the response head (`:106-130`), the request log line (`:49-61`), the non-streaming branch (`:201-225`) |
//! | [`responses`]        | `POST /providers/private/v1/responses`        | `runtime/endpoint/provider/private/responses/{service-route.ts,private-responses.ts}` - the local model gate (`:70-83`), `toResponsesStream` (`:56-65`), the 404 failover loop and its terminal condition (`:106-121`, `:133-147`) |
//! | [`models`]           | `GET /providers/private/v1/models`            | `runtime/endpoint/provider/private/models/service-route.ts:15-48` - the `{object:"list", data:[…]}` envelope and its literals (`created: 1677610602`, `owned_by: "localhost"`, `context_length: 262_144`), the `isDiscoverable` filter, the order-preserving projection |
//! | [`stats`]            | `GET` + `POST /providers/private/v1/stats`    | `runtime/endpoint/provider/private/stats/{service-route.ts,private-stats.ts}` - `collectStats` (`:197-341`, the row shape and the `totals` roll-up) and `privateStatsReset` (`:1063-1090`, the `?id=` reset and its three status codes) |
//!
//! The verb-dispatch half of the contract belongs to the gateway, not to the handler file:
//! `asServiceHandler` (`packages/underload/service/src/server/handler.ts:38-46`) answers a verb the
//! route does not declare with `400 {"error":"handler not found"}`, and the dispatcher maps any
//! throw out of a handler to `500 {"error":"Internal Server Error"}` while logging the real reason
//! (`packages/underload/service/src/server/start.ts:908-911`). Both are reproduced here, in the same
//! direction: the message a handler fails with is for the log, never for the wire.
//!
//! The SSE response headers are the gateway's own, from `streamSSE`
//! (`node_modules/hono/dist/helper/streaming/sse.js`): `Content-Type: text/event-stream` and
//! `Cache-Control: no-cache` are set here; the same function also writes `Connection: keep-alive`
//! and `Transfer-Encoding: chunked`, and on HTTP/1.1 hyper writes both of those itself for a
//! streamed body, so they are left to the transport rather than duplicated by hand.
//!
//! # Why the route file is thin and this module is not
//!
//! `endpoint/provider/provider-route.rs` is discovered by
//! `packages/typecrate/services/build.rs` and compiled as a module of the SERVICES crate
//! (`packages/typecrate/services/build_support/mod.rs` "Route file contract"), so everything in it
//! is type-checked against services' `axum` build and its `crate::` paths mean `typecrate_services`.
//! Keeping the logic here means the contract is owned by the crate that owns the registry, is
//! testable in-process without the services binary, and is served identically by any host that
//! links this rlib.
//!
//! # The deviation the wire cannot avoid: credentials arrive at runtime
//!
//! The TypeScript provider has its credentials COMPILED IN (named `import { NVIDIA_API_KEYS } from
//! '@config/vault'` specifiers, resolved at build time), so its routes can never be "not ready". A
//! Rust process has no such import, so this layer loads the exported vault artifact lazily, once
//! ([`process_state`]), and the routes answer **503** with the exact regeneration command while that
//! artifact is missing, unreadable or refused. Nothing here can abort the host process: a vault
//! problem is a per-request verdict, so the services binary that also hosts `/v1/harness` boots
//! normally with no artifact on disk.
//!
//! DEVIATION (deliberate, recorded): missing/unreadable/invalid artifact ⇒ `503` with an actionable
//! body, where the TypeScript has no such state (an unresolved credential would fail per request,
//! with an upstream 401 rather than a local 503).
//!
//! # Deviations recorded here, in the order a reader meets them
//!
//!   1. **`/stats` is JSON only.** The TypeScript answers `GET /stats` with a server-rendered HTML
//!      document (`private-stats.ts:1013-1041`, the `raw` Response passthrough). This cut publishes
//!      the same projection as JSON and nothing else, so the dashboard is a consumer of data rather
//!      than a string of template bytes. Consequences of that choice, all deliberate:
//!        * the hide-unused filter (`:276-284`) is NOT applied - every row is listed, with `enabled`
//!          and `discoverable` beside it, because a JSON consumer filters for itself and an omitted
//!          row would be indistinguishable from a broken registry;
//!        * `enabled`/`discoverable` are appended as the LAST two keys of each row, so the preceding
//!          keys stay exactly the `ModelStats` shape and order (`:43-102`);
//!        * the cross-session buffer (`stats-buffer.ts`) is out of scope, so no earlier process's
//!          figures are folded in, and `POST` resets only the live counters
//!          ([`crate::client::ModelClient::reset_counters`]) rather than a persisted entry too.
//!   2. **The upstream path of the Responses surface comes from the row, not from the route.** The
//!      TypeScript calls `client.createResponses(…)`, which always posts to `{base}/responses`
//!      (`packages/agentic/provider/src/model-class.ts:747-749`, verified). This crate has ONE call point,
//!      [`crate::client::ModelClient::create`], whose upstream path is the row's
//!      [`crate::client::Behavior::path`] - `chat/completions` for every registered row today,
//!      `responses` only for [`crate::client::Behavior::ResponsesPassthrough`]. What IS reproduced:
//!      the local model gate, the body forwarded untouched apart from the pinned model clamp, the
//!      404 failover with its terminal condition (owned by [`crate::client::ModelClient`]'s dispatch
//!      loop instead of by a route-level `while (true)`), and `[DONE]` after clean exhaustion.
//!   3. **Not a deviation - recorded so the next reader does not re-open it.** A pre-stream upstream
//!      failure is `500 {"error":"Internal Server Error"}` whatever the upstream status was, and a
//!      body that fails to parse (or parses to an array or a scalar) is a `404` naming NO model. Both
//!      are the deployed gateway's own verdicts, verified against it: the endpoint never
//!      catches a transport error (`private-chat-completion.ts:101-104` / `:196-200` leave
//!      `await client.create(body)` uncaught), so the throw reaches the gateway's catch, whose whole
//!      body is `console.error` + `context.json({ error: 'Internal Server Error' }, 500)`
//!      (`packages/underload/service/src/server/start.ts:908-911`); and `start.ts:815` is
//!      `try { body = await context.req.json(); } catch { body = {}; }`, so a malformed body IS the
//!      empty object whose `model` reads `undefined` in the route's destructure
//!      (`private-chat-completion.ts:66`) and 404s from its own lookup (`:78-83`). The one shape the
//!      gateway does not fold is `null`, which throws out of that destructure and so takes the same
//!      500 as a transport error. `tests/routes.rs` pins all four wire outcomes. What the port adds
//!      is invisible on the wire: the pre-stream status is also *classified* (
//!      [`crate::error::TransportError::kind`]) and logged, where the TypeScript keeps it
//!      only on the thrown `APIError` (`node_modules/openai/core/error.js:12`) that the gateway
//!      discards.
//!      The 404 TEXT of that case is deviation 5, not this one: the status and the fall-through are
//!      still parity, only the interpolated `<model>` is not.
//!   4. **There is no request-body cap.** PARITY, not a deviation: the deployed gateway imposes none,
//!      and neither does this module - a body of any size is read in full.
//!   5. **A request whose `model` field is absent or not a string says so, instead of naming
//!      `undefined`** (2026-11-24). DEVIATION (deliberate, recorded): the deployed relay answers
//!      `404 {"error":"Model 'undefined' not found"}`, because its message is a JS template literal
//!      and the destructure left `body.model` as `undefined`
//!      (`private-chat-completion.ts:66`, `:78-83`). `undefined` is an artefact of THAT
//!      implementation language: it is not a model id, and it does not tell the caller what was wrong
//!      with the request - in production it was misread as a harness that had literally sent the
//!      string `"undefined"`. The upstream's own wording for the same fault is a REQUIRED-field
//!      message (`Temp/basalt-check/serve/handler.py:684-690` `_unknown_model` answers
//!      `model is required`), so this route reports the fault as a fault:
//!      [`MISSING_MODEL_FIELD_MESSAGE`]. Kept unchanged, because both are the deployed verdict and
//!      neither depends on the interpolation: the **404** itself, and
//!      `Model '<id>' not found` for an id that IS a string but is not served - a terminal verdict
//!      for the consumer either way, so changing the status would buy nothing and cost parity.
//!      The reason a warn log accompanies it (body key names, never values) is on
//!      [`warn_missing_model_field`].
//!
//! # The secret rule this module is responsible for
//!
//! Nothing here can print a credential. The only credential-derived value the wire or the log ever
//! sees is a relay NAME ([`crate::client::ModelClient::upstream_label`], gated by
//! [`crate::client::upstream_label_is_safe`]); the upstream failure sentences are redacted at their
//! source ([`crate::error::StreamFailure`] and [`crate::error::TransportError`]); the vault loader's
//! diagnostics are redacted at theirs (`vault.rs:947-999`); and every refusal string that reaches a
//! 503 body passes [`crate::error::redact_secrets`] once more on the way in. `tests/routes.rs`
//! asserts the test credential appears in no response byte.

use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures_util::{Stream, StreamExt};
use serde_json::{Map, Value, json};

use crate::accounting::{Counters, Totals};
use crate::client::ModelClient;
use crate::error::{flatten_error_detail, redact_secrets};
use crate::registry::Registry;
use crate::sse::{DONE_FRAME, data_frame};
use crate::transport::{ResponseOrStream, TransportFailure, UpstreamStream};
use crate::vault::{VaultConfig, VaultError, VaultFailure};

// ---------------------------------------------------------------------------
// The mount, the port, and the constants the wire is made of
// ---------------------------------------------------------------------------

/// The shared namespace of all four routes (`private/constant.ts:1 BASE_URL`), verbatim.
pub const MOUNT: &str = "/providers/private/v1";

/// `chat-completion/service-route.ts:8`: `` `${BASE_URL}/chat/completions` ``.
pub const CHAT_COMPLETIONS_ROUTE: &str = "/providers/private/v1/chat/completions";

/// `responses/service-route.ts:10`: `` `${BASE_URL}/responses` ``.
pub const RESPONSES_ROUTE: &str = "/providers/private/v1/responses";

/// `models/service-route.ts:52`: `` `${BASE_URL}/models` ``.
pub const MODELS_ROUTE: &str = "/providers/private/v1/models";

/// `stats/service-route.ts:23`: `` `${BASE_URL}/stats` ``, served for both `GET` and `POST`.
pub const STATS_ROUTE: &str = "/providers/private/v1/stats";

/// The listener these four groups declare.
///
/// The TypeScript declares `port: PRIVATE_PROVIDER_PORT` on all four route objects
/// (`runtime/endpoint/provider/private/{chat-completion,responses,models,stats}/service-route.ts`,
/// value `6660` from `packages/agentic/provider/src/port.ts`), and the TypeScript gateway binds one
/// listener per distinct declared port (`packages/underload/service/src/server/start.ts:780`) - the
/// same listener model the services server implements. This crate publishes **8800** instead: the
/// TypeScript provider keeps its own 6660 listener as the behavioural spec and fallback, while the
/// deployed Rust provider is the one consumers dial. The consumer-facing
/// `LOCAL_AREA_NETWORK_PROVIDER_PORT` is `8800` (`config/environment/src/port.ts:4`), and
/// `packages/typecrate/harness/src/config.rs:36` /
/// `packages/underload/harness/src/server/config.ts:272` both default to it.
///
/// DEVIATION (deliberate, recorded): the listener number is not the TypeScript's. The paths, verbs,
/// bodies and status codes are.
///
/// The route file still has to WRITE the number as a bare literal (`, port: 8800;`): the
/// `service_routes!` grammar binds `$port:literal`, so a Rust `const` cannot be named there -
/// `packages/typecrate/services/examples/endpoints/health/health-route.rs` (the `}, port: 8010;`
/// group) is the precedent and `packages/typecrate/services/src/server/metadata.rs` the grammar. This
/// constant is the single place the value is documented in Rust, and `tests/routes.rs` asserts the
/// route file's literal agrees with it.
pub const PROVIDER_PORT: u16 = 8800;

/// Inline vault document, highest priority. Named after the crate rather than after the consumer
/// scope: the value IS a vault artifact, and which scope it was exported for is the artifact's own
/// `consumers` field.
pub const VAULT_JSON_ENV: &str = "TYPECRATE_PROVIDER_VAULT_JSON";

/// Explicit artifact path, second priority, honoured when the inline document is absent.
pub const VAULT_PATH_ENV: &str = "TYPECRATE_PROVIDER_VAULT_PATH";

/// The artifact this crate reads when neither environment variable is set, resolved relative to
/// THIS crate's manifest.
///
/// That choice is load-bearing for the discovered route file: `provider-route.rs` is compiled into
/// `typecrate-services`, so a path baked in THERE would describe the services package's
/// neighbourhood. `env!("CARGO_MANIFEST_DIR")` expands where it is written, and it is written here,
/// so the constant always names the provider crate's own view of
/// `config/vault/generated/agentic-provider.json`, however far the final executable is started from.
pub const DEFAULT_VAULT_ARTIFACT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../config/vault/generated/agentic-provider.json"
);

/// The command the artifact is regenerated with, quoted verbatim in every refusal
/// (`config/vault/package.json:8` declares `export`, and `scripts/export-vault.ts:285` requires
/// `--consumer`). It is a command line, not a secret, so it is safe to print.
pub const REGENERATION_COMMAND: &str =
    "yarn workspace @config/vault export -- --consumer agentic-provider";

/// `asServiceHandler`'s own sentence for a verb the route does not declare (`handler.ts:42`), which
/// an `ANY` mount has to reproduce by itself.
pub const HANDLER_NOT_FOUND: &str = "handler not found";

/// The gateway's constant body for any handler throw (`start.ts:910`).
pub const INTERNAL_ERROR: &str = "Internal Server Error";

/// `private-stats.ts:1070` - the reset verb without a usable `?id=`.
pub const MISSING_ID_ERROR: &str = "missing id query parameter";

/// The 404 body for a request that never named a model at all - deviation 5 in the module header.
///
/// WHY THIS SENTENCE AND NOT THE DEPLOYED ONE: the deployed relay prints
/// `Model 'undefined' not found`, where `undefined` is what the TypeScript destructure left behind
/// (`private-chat-completion.ts:66`) and nothing more - it is a JS rendering artefact, not a model
/// id, and an operator reading it in production took it for a literal id named `undefined`. The
/// upstream's own answer to the same fault states the FIELD instead
/// (`Temp/basalt-check/serve/handler.py:684-690`, `_unknown_model` with `name is None` writes
/// `model is required`), which is the shape this sentence keeps: a required field, and the two ways
/// it can be wrong. It is deliberately not the upstream's exact bytes - this crate's other wire
/// messages are its own (`INTERNAL_ERROR`, `HANDLER_NOT_FOUND`), and "missing OR not a string" is
/// precisely what this route accepts and the upstream never has to say.
pub const MISSING_MODEL_FIELD_MESSAGE: &str =
    "Model field is required and was missing or not a string in the request body";

/// How many top-level body key names [`warn_missing_model_field`] may name, at most.
///
/// WHY A CAP AT ALL: the key list is the whole diagnostic, and a caller can send a body with hundreds
/// of keys - an unbounded line is an unbounded log record, and the tail of a long key list is not
/// what identifies a malformed client. Sixteen is comfortably more than either request surface
/// actually uses (`model`, `messages`, `stream`, `max_tokens`, and a handful of sampling fields), so a
/// truncated list still names the fault, and the line says how many names it left out.
pub const MISSING_MODEL_KEYS_SHOWN: usize = 16;

/// `models/service-route.ts:42` - the catalog's frozen creation epoch.
pub const MODEL_CREATED_EPOCH: u64 = 1_677_610_602;

/// `models/service-route.ts:43` - the catalog's frozen owner.
pub const MODEL_OWNED_BY: &str = "localhost";

/// The `StreamErrorContext.endpoint` value the gateway would supply for this stream route
/// (`start.ts:861,868` + `:624`, `:647`: the route's FOLDER name). It is only observable through an
/// error frame's `message` when the upstream target is unknown, because the transport always knows
/// the target - see [`crate::error::StreamFailure::sentence`].
pub const CHAT_COMPLETIONS_ENDPOINT: &str = "chat-completion";

/// The folder name of the Responses route, same rule as [`CHAT_COMPLETIONS_ENDPOINT`].
pub const RESPONSES_ENDPOINT: &str = "responses";

/// The media type Hono's `c.json()` writes (`node_modules/hono/dist/helper/response/json.js`). No
/// `charset` is claimed, because the TypeScript does not claim one and a header byte is contract.
pub const JSON_CONTENT_TYPE: &str = "application/json";

/// The SSE media type `streamSSE` sets.
pub const SSE_CONTENT_TYPE: &str = "text/event-stream";

/// The `Cache-Control` `streamSSE` sets, which is what stops an intermediary from caching a stream.
pub const SSE_CACHE_CONTROL: &str = "no-cache";

// ---------------------------------------------------------------------------
// The readiness of the serving surface
// ---------------------------------------------------------------------------

/// Why the serving surface cannot answer yet, in text that is safe to print.
///
/// The message is deliberately actionable: the artifact is generated and git-ignored, so "no such
/// file" on a fresh clone is the expected first failure, and the fix is one command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unavailable {
    /// The whole refusal: cause first, regeneration command last.
    message: String,
}

impl Unavailable {
    /// A refusal built from a cause and the artifact that was tried.
    ///
    /// `cause` goes through [`redact_secrets`] on the way in: a serde diagnostic can echo an
    /// offending scalar (`invalid type: string "sk-live-…"`), and this type has to stay printable
    /// even if a lower layer starts quoting values. The artifact path and the command are appended
    /// UNREDACTED - a repository path is not a credential, and rewriting one would make the fix
    /// unreadable.
    #[must_use]
    pub fn artifact(artifact: &Path, cause: impl AsRef<str>) -> Self {
        Self {
            message: format!(
                "the private provider vault artifact is unavailable: {} ({}); regenerate it with: \
                 {}",
                redact_secrets(cause.as_ref().trim()),
                artifact.display(),
                REGENERATION_COMMAND
            ),
        }
    }

    /// A refusal for a document that parsed and was still refused - a vault validator problem, or a
    /// registry that could not bind its rows to it.
    ///
    /// Every problem is listed, because one vault rename or one row typo breaks many rows at once and
    /// a one-problem message would need one restart per row.
    #[must_use]
    pub fn problems(artifact: &Path, causes: &[String]) -> Self {
        // Redaction per line rather than over the joined text: the marker must never swallow the
        // newline that separates two problems.
        let lines: Vec<String> = causes
            .iter()
            .map(|cause| format!("  - {}", redact_secrets(cause.trim())))
            .collect();

        Self {
            message: format!(
                "the private provider registry could not be built: {} problem(s):\n{}\n\
                 Regenerate the artifact with: {} ({})",
                causes.len(),
                lines.join("\n"),
                REGENERATION_COMMAND,
                artifact.display()
            ),
        }
    }

    /// The printable refusal, exactly as the 503 body carries it.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for Unavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

/// Where the vault document comes from for one build of the serving surface.
///
/// Ambient input lives ONLY in this layer - the binary/route boundary - never in
/// [`crate::vault`] or [`crate::registry`], which stay injected-input by contract
/// (`config/vault/knowledge.md` "Loader shape": the loader is injected, never ambient).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VaultSource {
    /// A document captured in [`VAULT_JSON_ENV`].
    Inline(String),
    /// A path named in [`VAULT_PATH_ENV`].
    Path(PathBuf),
    /// [`DEFAULT_VAULT_ARTIFACT`], i.e. nothing was configured.
    DefaultArtifact,
}

impl VaultSource {
    /// The identity printed in a refusal: a real path, or the variable that stood in for one.
    #[must_use]
    fn artifact(&self) -> PathBuf {
        match self {
            // An inline document has no file; naming the variable tells the operator which input
            // refused rather than inventing a path.
            Self::Inline(_) => PathBuf::from(VAULT_JSON_ENV),
            Self::Path(path) => path.clone(),
            Self::DefaultArtifact => PathBuf::from(DEFAULT_VAULT_ARTIFACT),
        }
    }
}

/// Whether the routes can serve, and from which registry.
///
/// `Ready` OWNS its registry instead of borrowing [`Registry::process()`] so the state can be built
/// from a synthetic row set in a test, and so no caller can observe half of a half-installed
/// process singleton. A [`ModelClient`] clone is identity-preserving, so the rotation cursors and
/// the counters stay process-wide even though the registry handle is a clone.
#[derive(Clone, Debug)]
pub enum ServeState {
    /// The registry is built; every route answers from it.
    Ready(Registry),
    /// The vault or the registry refused to build; every route answers 503.
    Unavailable(Unavailable),
}

impl ServeState {
    /// The registry, when the surface is ready.
    #[must_use]
    pub fn registry(&self) -> Option<&Registry> {
        match self {
            Self::Ready(registry) => Some(registry),
            Self::Unavailable(_) => None,
        }
    }

    /// The refusal, when the surface is not ready.
    #[must_use]
    pub fn unavailable(&self) -> Option<&Unavailable> {
        match self {
            Self::Unavailable(reason) => Some(reason),
            Self::Ready(_) => None,
        }
    }

    /// Whether requests are served rather than refused.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }
}

/// Builds the state from one vault document, touching neither the filesystem nor the environment.
///
/// A validator or registry refusal becomes [`ServeState::Unavailable`] carrying EVERY problem (both
/// layers collect a problem list rather than failing on the first), so the 503 body is a fix list.
#[must_use]
pub fn state_from_vault(vault: &VaultConfig, artifact: &Path) -> ServeState {
    match Registry::try_new(vault) {
        Ok(registry) => ServeState::Ready(registry),
        Err(refusal) => {
            ServeState::Unavailable(Unavailable::problems(artifact, refusal.problems()))
        }
    }
}

/// Builds the state from one explicit source. Never panics: every IO, parse and validation refusal
/// becomes [`ServeState::Unavailable`].
#[must_use]
pub fn state_from_source(source: &VaultSource) -> ServeState {
    let artifact = source.artifact();

    match read_source(source) {
        Err(refusal) => ServeState::Unavailable(artifact_refusal(&artifact, &refusal)),
        Ok(vault) => state_from_vault(&vault, &artifact),
    }
}

/// The process-wide serving state, built ONCE on first use.
///
/// Lazy on purpose: this code is compiled into the services binary, and a missing git-ignored
/// artifact must not stop that binary from starting - it also serves `/v1/harness` and the samples.
/// When the document IS accepted, the registry is installed process-wide through
/// [`Registry::initialize_process`] as well, so any other consumer in the process sees the same rows,
/// counters and rotation cursors; the returned handle is a clone of that same instance.
#[must_use]
pub fn process_state() -> &'static ServeState {
    static PROCESS_STATE: OnceLock<ServeState> = OnceLock::new();

    PROCESS_STATE.get_or_init(|| {
        let source = vault_source_from_env();
        let artifact = source.artifact();

        let vault = match read_source(&source) {
            Ok(vault) => vault,
            Err(refusal) => {
                return ServeState::Unavailable(artifact_refusal(&artifact, &refusal));
            }
        };

        match Registry::initialize_process(&vault) {
            Ok(registry) => ServeState::Ready(registry.clone()),
            Err(refusal) => {
                ServeState::Unavailable(Unavailable::problems(&artifact, refusal.problems()))
            }
        }
    })
}

/// Reads one source into a validated document, without deciding what a refusal means.
fn read_source(source: &VaultSource) -> Result<VaultConfig, VaultError> {
    match source {
        VaultSource::Inline(document) => crate::vault::load_from_str(document),
        VaultSource::Path(path) => crate::vault::load_from_path(path),
        VaultSource::DefaultArtifact => crate::vault::load_from_path(DEFAULT_VAULT_ARTIFACT),
    }
}

/// One vault refusal to one printable cause, keeping the "there is no artifact" case distinct from
/// "the artifact is wrong" - the missing-vs-drift split `VaultFailure` exists to carry.
fn artifact_refusal(artifact: &Path, refusal: &VaultError) -> Unavailable {
    if refusal.failure() == VaultFailure::Io {
        return Unavailable::artifact(
            artifact,
            // The TypeScript has no equivalent sentence, because its credentials are compiled in.
            format!(
                "no vault artifact was found at this path - it is a generated, git-ignored file, \
                 and a Rust process reads it at runtime instead of importing it at build time ({})",
                artifact.display()
            ),
        );
    }

    Unavailable::artifact(artifact, refusal_message(refusal))
}

/// One flat sentence for a vault refusal: the failure class, then every problem.
///
/// [`crate::vault`] already redacts its parse diagnostics, so this is composition for the reader and
/// not a second redaction pass - [`Unavailable`] still redacts on the way in.
fn refusal_message(refusal: &VaultError) -> String {
    let class = match refusal.failure() {
        VaultFailure::Io => "the artifact could not be read",
        VaultFailure::Json => "the artifact is not a vault document this crate can parse",
        VaultFailure::Validator => "the artifact parsed and the vault validator refused it",
    };

    if refusal.problem_count() == 0 {
        return class.to_owned();
    }

    format!("{class}: {}", refusal.problems().join("; "))
}

/// Reads the configured source without deciding anything else about the process.
///
/// Precedence is the one [`VaultSource`] documents: inline document, then explicit path, then the
/// crate-relative artifact. An empty or whitespace-only value counts as unset - which is what
/// `set VAR=` on Windows and `export VAR=` on POSIX both leave behind.
#[must_use]
pub fn vault_source_from_env() -> VaultSource {
    // Reading, not interpreting: the interpretation lives in [`vault_source_from_values`] so the
    // precedence can be tested without mutating a process-global environment (in edition 2024
    // `std::env::set_var` is `unsafe`, and this crate forbids `unsafe` outright - a test that wanted
    // to exercise precedence by setting variables would have to break that rule).
    let document = std::env::var(VAULT_JSON_ENV).ok();
    let path = std::env::var(VAULT_PATH_ENV).ok();

    vault_source_from_values(document.as_deref(), path.as_deref())
}

/// The two configured values -> the source, in the documented precedence.
#[must_use]
pub fn vault_source_from_values(document: Option<&str>, path: Option<&str>) -> VaultSource {
    if let Some(document) = configured(document) {
        return VaultSource::Inline(document.to_owned());
    }

    if let Some(path) = configured(path) {
        return VaultSource::Path(PathBuf::from(path));
    }

    VaultSource::DefaultArtifact
}

/// One candidate value, with an empty or whitespace-only value read as unset.
fn configured(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.trim().is_empty())
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

/// A JSON response with the gateway's media type and an explicit status.
///
/// `serde_json`'s `preserve_order` feature is why the envelopes below can state their key order at
/// all: the catalog and the error frames are compared as BYTES by their consumers, and a
/// `BTreeMap`-backed object would reorder them.
fn json(status: StatusCode, document: Value) -> Response {
    // A `Value` built from `json!`/`Map` cannot fail to serialise; the fallback keeps that total
    // without an `expect` on a request path.
    let bytes = serde_json::to_vec(&document).unwrap_or_else(|_| b"null".to_vec());

    (
        status,
        [(header::CONTENT_TYPE, JSON_CONTENT_TYPE)],
        Body::from(bytes),
    )
        .into_response()
}

/// The `{ "error": "<one string>" }` shape every refusal on this surface uses.
///
/// It is the TypeScript's shape for both the model 404 (`private-chat-completion.ts:78-83`,
/// `{ error: \`Model '${model}' not found\` }`) and the throw 500 (`start.ts:910`): `error` is a
/// BARE STRING, not an object, which is exactly what a byte-level consumer can tell apart.
fn error(status: StatusCode, message: impl AsRef<str>) -> Response {
    json(status, json!({ "error": message.as_ref() }))
}

/// The 503 every route answers while the vault is unavailable.
fn unavailable(reason: &Unavailable) -> Response {
    // One log line per refused request is right here: the state is process-wide and permanent until
    // the process restarts, so a refused route has to be explainable from the log alone.
    tracing::warn!(
        target: "typecrate_provider::serve",
        "provider route refused: {reason}"
    );

    error(StatusCode::SERVICE_UNAVAILABLE, reason.message())
}

/// The SSE response: `stream`'s frames, with the headers `streamSSE` writes.
///
/// No axum `sse` FEATURE is involved: `Body::from_stream` and these headers are in axum's DEFAULT
/// set, which `tests/transport.rs::axum_sse_response_needs_no_sse_feature` proves end to end. That
/// is also why `packages/typecrate/services` does not have to grow one - it declares `json,query,ws`
/// and compiles this module into its own binary.
fn sse(stream: UpstreamStream, endpoint: &'static str, log: RequestLog) -> Response {
    // The head is about to be committed, so this is the last point where the request is still
    // describable as "alive and not yet producing". Debug-level on purpose: the operator switches to
    // `RUST_LOG=typecrate_provider=debug` precisely to find a stream that never produced a chunk.
    log.stream_opened(endpoint);

    let body = Body::from_stream(Relay::new(stream, endpoint, log));

    // A builder that cannot fail: the two header values are validated statics and the status is a
    // literal. The fallback keeps the total without an `expect`.
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, SSE_CONTENT_TYPE)
        .header(header::CACHE_CONTROL, SSE_CACHE_CONTROL)
        .body(body)
        .unwrap_or_else(|_| error(StatusCode::OK, INTERNAL_ERROR))
}

// ---------------------------------------------------------------------------
// The stream relay: `data: <json>` per chunk, one terminal frame, nothing after it
// ---------------------------------------------------------------------------

/// The per-request facts the request log line needs, captured at the response HEAD.
///
/// The label is captured HERE and never re-read after the stream drains, for the reason the
/// TypeScript gives at `private-chat-completion.ts:106-122`: the registry clients are process-wide
/// singletons whose endpoint cursor rotates, so reading `upstreamLabel` later would attribute this
/// request to whichever relay the process happens to sit on when the last chunk lands. The token
/// figures, by contrast, are read at settlement, because only then are they final.
#[derive(Clone, Debug)]
struct RequestLog {
    /// The executing row, whose counters the line prints.
    client: ModelClient,
    /// The relay NAME that served this request, `None` when the row does not name its relays.
    label: Option<String>,
}

impl RequestLog {
    /// Captures the attribution at the instant the response head has arrived.
    fn capture(client: &ModelClient) -> Self {
        Self {
            client: client.clone(),
            label: client.upstream_label(),
        }
    }

    /// The relay half every line prints: the vault entry LABEL, or the word that says the row does
    /// not name its relays. A label is user-visible configuration already screened by
    /// [`crate::client::upstream_label_is_safe`], never a URL and never a credential value.
    fn relay(&self) -> &str {
        self.label.as_deref().unwrap_or("unlabelled")
    }

    /// Names the row and the relay that served the request, at the response head.
    ///
    /// This is the "what is going on" line: until it prints, an operator with a request hanging in
    /// front of a relay has NOTHING to grep for, because the token line below only arrives when the
    /// body finishes and the failure line only when it dies. One `info!` per request, emitted at the
    /// head for both the settled and the streaming shape, and it can only name the relay the request
    /// actually used for the reason the struct's doc gives.
    fn announce(&self, endpoint: &str) {
        tracing::info!(
            target: "typecrate_provider::serve",
            "{endpoint}: serving model={} relay={}",
            self.client.id(),
            self.relay()
        );
    }

    /// The first `debug!` flight line: the head is committed, so the request is alive.
    ///
    /// `RUST_LOG=typecrate_provider=debug` turns these into a trace of one request; at the default
    /// `info` they cost nothing, which is what keeps the added output to the one line above.
    fn stream_opened(&self, endpoint: &str) {
        tracing::debug!(
            target: "typecrate_provider::serve",
            "{endpoint}: stream opened model={} relay={}",
            self.client.id(),
            self.relay()
        );
    }

    /// The second `debug!` flight line: the upstream has produced its first chunk, measured from the
    /// instant the head arrived. Absent this line, a stream that opened and then stalled looks
    /// identical to one that never opened.
    fn first_chunk(&self, endpoint: &str, opened: Instant) {
        tracing::debug!(
            target: "typecrate_provider::serve",
            "{endpoint}: first chunk after {}ms model={} relay={}",
            opened.elapsed().as_millis(),
            self.client.id(),
            self.relay()
        );
    }

    /// Emits the one line the TypeScript prints per completed request.
    ///
    /// Called on CLEAN exhaustion only, and before the sentinel is written - the order of
    /// `private-chat-completion.ts:174-186` (`console.log(…)` then `yield 'data: [DONE]'`). The
    /// counters are settled by then, because the transport runs its settlement hook as its stream
    /// terminates, i.e. before the consumer sees the stream end.
    fn emit(&self) {
        let counters = self.client.counters();

        tracing::info!(
            target: "typecrate_provider::serve",
            "{}",
            format_token_line(
                self.client.id(),
                counters.input_token_total,
                counters.cache_token_total,
                self.client.input_cost(),
                counters.output_token_total,
                self.client.output_cost(),
                self.label.as_deref()
            )
        );
    }
}

/// The provider's own view of one upstream stream: chunk frames, then exactly one terminal frame.
///
/// The terminal rule is the whole reason this type exists:
///   * clean exhaustion -> the request log line, then `data: [DONE]\n\n`;
///   * a stream failure -> one classified error frame, and NO sentinel after it
///     (`start.ts:860-873`: the frame is written and the stream then closes normally).
///
/// A failure is therefore a FRAME, never a body error: the socket closes cleanly, exactly as the
/// TypeScript gateway's `streamSSE` does after `writeSSE({ data: JSON.stringify(buildStreamErrorPayload(…)) })`
/// (`packages/underload/service/src/server/start.ts:866-871`, verified — the frame is built inline at
/// the catch site; the gateway has no `errorPayload` helper). The classifier
/// already ran - in [`crate::transport`], through [`crate::error::classify_stream_error`] - so the
/// frame this crate emits carries the closed code set and the `param: null` / omitted-`status`
/// discipline without any route-level re-classification.
struct Relay {
    /// The upstream chunk stream, kept until it ends: the settlement hook lives inside it, and
    /// dropping it early is what cancels the upstream read.
    upstream: UpstreamStream,
    /// The route's folder name, for an error frame's fallback subject.
    endpoint: &'static str,
    /// The facts of the request log line.
    log: RequestLog,
    /// Set once a terminal frame has been produced, so the sentinel and an error frame can never
    /// both reach the wire and nothing at all follows the terminal frame.
    finished: bool,
    /// When this relay was built, i.e. when the SSE head was committed. Only the `debug!` first-chunk
    /// line reads it, and it exists so a stalled stream is distinguishable from a stream that never
    /// opened.
    opened: Instant,
    /// Set on the first upstream chunk, so the flight line is printed once and not per frame.
    delivered: bool,
}

impl Relay {
    fn new(upstream: UpstreamStream, endpoint: &'static str, log: RequestLog) -> Self {
        Self {
            upstream,
            endpoint,
            log,
            finished: false,
            opened: Instant::now(),
            delivered: false,
        }
    }
}

impl Stream for Relay {
    /// `io::Error` is the shape `Body::from_stream` accepts. It is never actually produced: every
    /// upstream failure becomes a frame, so the body ends cleanly.
    type Item = Result<Bytes, io::Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.finished {
            return Poll::Ready(None);
        }

        match this.upstream.poll_next_unpin(context) {
            // A chunk is RE-SERIALISED, never echoed as upstream bytes: the TypeScript does the same
            // (`private-chat-completion.ts:163 JSON.stringify(chunk)`), and `preserve_order` is what
            // keeps the upstream's own key order intact through the round trip.
            Poll::Ready(Some(Ok(chunk))) => {
                if !this.delivered {
                    this.delivered = true;
                    this.log.first_chunk(this.endpoint, this.opened);
                }

                Poll::Ready(Some(Ok(Bytes::from(data_frame(&chunk)))))
            }
            Poll::Ready(Some(Err(failure))) => {
                // The gateway logs the real error before it writes the frame (`start.ts:861`); the
                // frame itself carries only the classified sentence. The line is the failure's own
                // [`crate::error::StreamFailure::log_line`], which is what the deployed gateway
                // lacked: it printed only the code and the sentence, so an `unknown` came out as
                // `stream error in chat-completion for unknown: Upstream stream terminated` and
                // named neither the relay, nor the upstream's own verdict, nor how much had already
                // reached the caller.
                tracing::error!(
                    target: "typecrate_provider::serve",
                    "{}",
                    failure.log_line(Some(this.endpoint))
                );

                this.finished = true;

                Poll::Ready(Some(Ok(Bytes::from(data_frame(
                    &failure.payload(Some(this.endpoint)),
                )))))
            }
            Poll::Ready(None) => {
                // Clean exhaustion: the line first, then the sentinel, then nothing.
                this.log.emit();
                this.finished = true;

                Poll::Ready(Some(Ok(Bytes::from(DONE_FRAME))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// The gateway surface a standalone host needs: cross-origin policy, preflight, readiness
// ---------------------------------------------------------------------------
//
// WHY THIS LIVES HERE AND NOT IN THE BIN: the four handlers are `pub` for exactly one reason -
// so a host that is not `typecrate-services` can mount them. A host that mounts them still has
// to answer CORS the same way, or the same request is `200` behind the services router and
// opaque-origin-blocked in front of a container. These items are that "same way", stated once.
// They mirror `packages/typecrate/services/src/server/mod.rs:354-417` byte for byte in behaviour
// (header set, preflight condition, `204`); the duplication is deliberate and temporary - the
// follow-up is for services to call these and delete its private copies, which is out of scope
// here because it edits the services crate and its `tests/server_http.rs` pins.
//
// WHY `any(...)` MAKES THE PREFLIGHT RULE NECESSARY: the mount delivers every verb to the
// handlers (see `endpoint/provider/provider-route.rs`, "Why every group is `ANY`"), so a plain
// `OPTIONS /providers/private/v1/models` is a ROUTED request the handler refuses with `400
// {"error":"handler not found"}` - the TypeScript's own verb-miss verdict - and NOT a preflight.
// Only an `OPTIONS` carrying `Access-Control-Request-Method` is a preflight, and only that one
// short-circuits to `204` here. The same pair gates hono's `cors()`.

/// The crate's own name on the wire, i.e. `[package] name` verbatim. [`crate::CRATE_NAME`] is the
/// same identifier spelled for Rust (`_`), which is what `use` paths and log targets need; a
/// readiness body names the PACKAGE, so the hyphenated spelling lives here.
pub const SERVICE_NAME: &str = "typecrate-provider";

/// `Access-Control-Allow-Private-Network` is not in the `http` crate's header table (it is a
/// Chrome-specified response header that never became an RFC), so its name is spelled once here.
/// `HeaderName::from_static` is a `const fn`, so the lowercase-normalised name is built at compile
/// time and never re-parsed per request.
pub const PRIVATE_NETWORK_ALLOW_HEADER: HeaderName =
    HeaderName::from_static("access-control-allow-private-network");

/// The value the three `Access-Control-Allow-*` headers carry: this host answers every origin,
/// every verb and every header, exactly as the TS gateway does (`allowMethods: ['*']`,
/// `allowHeaders: ['*']`).
pub const GATEWAY_ALLOW_ALL: &str = "*";

/// Adds the gateway's four cross-origin headers to `response`, replacing anything a handler set
/// for them so the advertised policy is the transport's, not one endpoint can quietly narrow.
#[must_use]
pub fn with_gateway_headers(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static(GATEWAY_ALLOW_ALL),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(GATEWAY_ALLOW_ALL),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(GATEWAY_ALLOW_ALL),
    );
    // The PNA answer is `true` (not an echo): a client that did not ask for it ignores the header,
    // and a client that did must not have to re-negotiate after a redirect.
    headers.insert(
        PRIVATE_NETWORK_ALLOW_HEADER,
        HeaderValue::from_static("true"),
    );

    response
}

/// A CORS preflight, i.e. an `OPTIONS` that declares the verb it wants to send next
/// (`Access-Control-Request-Method`) - see the block comment on this section for why the METHOD
/// ALONE is not the test.
#[must_use]
pub fn is_cors_preflight(request: &Request) -> bool {
    request.method() == Method::OPTIONS
        && request
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
}

/// The outermost layer of any router that mounts these handlers: a preflight `204` before
/// routing, otherwise the routed response with [`with_gateway_headers`] applied to it.
///
/// `Next`'s default body/state parameters are right here precisely because a host wires this
/// layer AFTER `.with_state`, so it never has to name the state type - which is what lets the
/// same layer wrap a `Router<ServeState>` (a test) and a `Router<()>` (the bin).
///
/// SSE survives this layer untouched: only `Response::headers_mut()` is touched, and a streamed
/// body passes through `Next::run` without being buffered, so the `text/event-stream` frames
/// still reach the socket incrementally.
pub async fn gateway(request: Request, next: Next) -> Response {
    if is_cors_preflight(&request) {
        // Body-less by definition (`204` means "no body", and the CORS answer IS the response),
        // so no media type is claimed and no body is drained.
        return with_gateway_headers(StatusCode::NO_CONTENT.into_response());
    }

    with_gateway_headers(next.run(request).await)
}

/// The body of the readiness route a standalone listener answers: `{ok, service, version}`.
///
/// `ok` is unconditional, and that is the honest shape: this endpoint reports that a process is
/// up and speaking HTTP, which is what a payload-agnostic TCP/HTTP readiness probe can ask.
/// Whether the vault artifact was accepted is a different question with a different answer -
/// every contract route says `503` with the regeneration command while it was not - and
/// `src/bin/server.rs` refuses to bind at all in that state unless `--allow-unconfigured` was
/// passed, so a `200` here is never a listener that is up but serving nothing.
///
/// Lives in the LIBRARY rather than in the bin because the bin's `/health` is its own route and
/// is NOT one of [`MOUNT`]'s four - adding it to the route constants would change what
/// `packages/typecrate/services` lists for this package.
#[must_use]
pub fn health_document() -> Value {
    json!({
        "ok": true,
        "service": SERVICE_NAME,
        "version": crate::version(),
    })
}

// ---------------------------------------------------------------------------
// The two POST surfaces
// ---------------------------------------------------------------------------

/// `POST /providers/private/v1/chat/completions`.
///
/// The gate order is the TypeScript's, and each refusal is reachable only where the TypeScript can
/// also reach it:
///   1. a verb the route does not declare -> `400 {"error":"handler not found"}` (`handler.ts:42`);
///   2. the vault unavailable -> `503` (the port's own state);
///   3. a body that cannot be READ (transport error) -> `500
///      {"error":"Internal Server Error"}` — the port's own verdict; a body that PARSES to
///      nothing usable (malformed JSON, an array, a scalar) is folded into the empty object the
///      gateway folds it into and therefore falls through to gate (4), while a body of `null` takes
///      the same `500` as an unreadable body because the TypeScript's destructure throws on it
///      (deviation 3 states the whole verification);
///   4. an id no row owns -> `404 {"error":"Model '<model>' not found"}`, `error` a bare string; a
///      body whose `model` is absent or not a string -> the same 404 with
///      [`MISSING_MODEL_FIELD_MESSAGE`] and one `warn!` naming the body's key names (deviation 5);
///   5. upstream: a settled document goes out verbatim, a stream goes out as frames, and a stream
///      that dies mid-flight gets one classified frame with no sentinel after it.
///
/// The pinned-model swap the TypeScript performs at the route level (`:91 body.model =
/// client.model`) lives inside [`crate::client::ModelClient::rewrite`] here, so the payload sent and
/// the counters recorded are produced by the same clamp.
pub async fn chat_completions(state: &ServeState, request: Request) -> Response {
    completion(state, request, CHAT_COMPLETIONS_ENDPOINT).await
}

/// `POST /providers/private/v1/responses`.
///
/// Same gate order as [`chat_completions`]. What differs is what the upstream sees (deviation 2: the
/// path comes from the row's `Behavior`) and what the frames mean to the caller - the Responses-side
/// frames are the upstream's own SDK events, forwarded untouched apart from the re-serialisation
/// every frame goes through here and in `toResponsesStream`.
pub async fn responses(state: &ServeState, request: Request) -> Response {
    completion(state, request, RESPONSES_ENDPOINT).await
}

/// The handler body both POST surfaces share, because the two TypeScript handlers differ only in
/// which client entry point they call and in which folder name an error frame reports.
async fn completion(state: &ServeState, request: Request, endpoint: &'static str) -> Response {
    // (1) The verb test belongs to the handler, because the mount is `ANY` - that is what keeps the
    // TypeScript's `400 {"error":"handler not found"}` instead of axum's body-less 405.
    if request.method() != Method::POST {
        return error(StatusCode::BAD_REQUEST, HANDLER_NOT_FOUND);
    }

    // (2) Readiness, before any body is read: a refused route must not pay to read the body.
    let registry = match state {
        ServeState::Ready(registry) => registry,
        ServeState::Unavailable(reason) => return unavailable(reason),
    };

    let (_, body) = request.into_parts();

    // (3) The body, part one: the READ. An unreadable body has no TypeScript counterpart at all
    // (the gateway has no body cap either), so it stays the gateway's constant 500 with the real
    // reason in the log.
    let Ok(bytes) = axum::body::to_bytes(body, usize::MAX).await else {
        tracing::error!(
            target: "typecrate_provider::serve",
            "{endpoint}: the request body could not be read"
        );

        return error(StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR);
    };

    // (3) The body, part two: the PARSE. A body that fails to parse, or parses to an array or a
    // scalar, is NOT a failure here - it is the EMPTY OBJECT the deployed gateway folds it into.
    // `start.ts:815` is `try { body = await context.req.json(); } catch { body = {}; }`, and the
    // non-object shapes read `undefined` for `model` exactly as an empty object does
    // (`const { model } = [1,2]` / `= 7` / `= "text"` are all `undefined` in JavaScript), so the
    // TypeScript relay answers its model 404 from its own lookup
    // (`private-chat-completion.ts:75-83`). PARITY, not a deviation: the VERDICT is produced by
    // falling through into gate (4) with an empty document, and gate (4) still answers 404. Only the
    // WORDING of that 404 deviates, and only because `undefined` is a JS artefact - deviation 5.
    //
    // `null` is the one shape the gateway does NOT fold, because `const { model } = null` THROWS
    // (`TypeError: Cannot destructure property 'model'`) out of the handler body
    // (`private-chat-completion.ts:65-66`), and a throw out of a handler is the gateway's
    // `500 {"error":"Internal Server Error"}` (`start.ts:908-911`). That is the answer kept here.
    //
    // The parse diagnostic is logged at DEBUG with the crate's redaction rules applied (a request
    // body can echo a credential), so the fault stays observable without the body ever reaching the
    // wire.
    //
    // The parse OUTCOME is kept apart from the document: gate (4) reports a missing `model`
    // differently depending on whether the body had keys to name at all (deviation 5), and once the
    // fold below has run, an empty document can no longer tell those three cases apart.
    let mut shape = BodyShape::Object;
    let document = match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Null) => {
            tracing::debug!(
                target: "typecrate_provider::serve",
                "{endpoint}: a JSON `null` body throws in the TypeScript, so the gateway's 500 stands"
            );

            return error(StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR);
        }
        Ok(document) if document.is_object() => document,
        Ok(_) => {
            // A body that parsed into an array or a scalar: no credential can be quoted by naming
            // its shape, so only the byte count is logged.
            tracing::debug!(
                target: "typecrate_provider::serve",
                "{endpoint}: a non-object body is the empty object the deployed gateway parses it \
                 into ({} bytes)",
                bytes.len()
            );

            shape = BodyShape::NotAnObject;

            Value::Object(Map::new())
        }
        Err(error) => {
            // `flatten_error_detail` is the crate's redaction + 200-character cap, and a serde
            // diagnostic can quote an offending scalar, so it goes through here before the log.
            tracing::debug!(
                target: "typecrate_provider::serve",
                "{}: the request body is not JSON ({})",
                endpoint,
                redact_secrets(&flatten_error_detail(&error.to_string()))
            );

            shape = BodyShape::NotJson;

            Value::Object(Map::new())
        }
    };

    // (4) Routing. The key is `body.model` matched against the row id with NO discoverability
    // filter: a hidden row stays routable, which is what makes `enabled` a catalog switch and not a
    // routing switch (`crate::registry::Registry::find`). TWO 404s live here and they are kept apart
    // on purpose, because they name two different faults:
    //   * the body DID name a model string and no row owns it -> the deployed
    //     `Model '<id>' not found` verbatim. That message IS the reason, and the id in it is real.
    //   * the body named no usable `model` at all -> deviation 5: the field is absent, or is present
    //     but not a string (a number, `null`, an object), or the body was folded into the empty
    //     document because it was not JSON. The deployed relay prints the JS artefact
    //     `Model 'undefined' not found` here; it names no model and explains nothing, so the body
    //     names the FIELD instead and the `warn!` records what did arrive. No separate branch is
    //     needed for "absent" vs "present but not a string": `model` is either in the key list that
    //     line prints or it is not, which IS the distinction, and guessing it from the shape alone
    //     would be the invented key list the no-content rule forbids.
    let Some(requested) = document.get("model").and_then(Value::as_str) else {
        warn_missing_model_field(endpoint, shape, &document, bytes.len());

        return error(StatusCode::NOT_FOUND, MISSING_MODEL_FIELD_MESSAGE);
    };

    let Some(client) = registry.find(requested) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("Model '{requested}' not found"),
        );
    };

    // (5) The upstream call. The client owns the rewrite, the accounting and the 404 failover loop,
    // so that loop's terminal condition (`private-responses.ts:117-119`) arrives as one failure here
    // instead of needing a second loop at the route.
    let answer = match client.create(&document).await {
        Ok(answer) => answer,
        Err(failure) => return upstream_throw(endpoint, &failure),
    };

    // The attribution is read NOW: the response head has arrived, which is the only instant that can
    // name the relay this request used.
    let log = RequestLog::capture(client);

    // The one extra line per request the operator asked for: it names the row and the relay while the
    // request is still in flight, so a stream that hangs is attributable before it settles. Both
    // response shapes get it, and at the default `info` level nothing else here is added per request
    // (the flight lines are `debug!`, and the token line still carries the settlement).
    log.announce(endpoint);

    match answer {
        ResponseOrStream::Response(completion) => {
            // The settled case: the line, then the completion document verbatim (`:201-225`).
            log.emit();

            json(StatusCode::OK, completion)
        }
        ResponseOrStream::Stream(stream) => sse(stream, endpoint, log),
    }
}

// ---------------------------------------------------------------------------
// `GET /models`
// ---------------------------------------------------------------------------

/// `GET /providers/private/v1/models` - the OpenAI-compatible catalog.
///
/// The envelope, the literals, the discoverability filter and the ORDER are the contract
/// (`models/service-route.ts:15-48`): `data` is the registry in row order, filtered, never sorted,
/// and every entry is the OpenAI model object. `enabled` is deliberately NOT part of an entry - the
/// entry shape stays the OpenAI one, which is why `isDiscoverable` is a filter and not a field.
pub async fn models(state: &ServeState, request: Request) -> Response {
    if request.method() != Method::GET {
        return error(StatusCode::BAD_REQUEST, HANDLER_NOT_FOUND);
    }

    let registry = match state {
        ServeState::Ready(registry) => registry,
        ServeState::Unavailable(reason) => return unavailable(reason),
    };

    let data: Vec<Value> = registry
        .clients()
        .iter()
        .filter(|client| client.is_discoverable())
        .map(models_entry)
        .collect();

    // Key order is `object` then `data` (`models/service-route.ts:18` and `:38`). A `Map` is written explicitly
    // rather than `json!`, so the order the wire carries is the order stated here.
    let mut envelope = Map::new();
    envelope.insert("object".to_owned(), Value::from("list"));
    envelope.insert("data".to_owned(), Value::Array(data));

    json(StatusCode::OK, Value::Object(envelope))
}

/// One catalog entry, in the order the TypeScript writes it: `id`, `object`, `created`, `owned_by`,
/// `context_length`.
///
/// `context_length` comes from the ROW rather than from a second literal, and the two agree for every
/// live row: [`crate::client::DEFAULT_CONTEXT_LENGTH`] is the same `262_144`
/// `models/service-route.ts:44` hard-codes.
fn models_entry(client: &ModelClient) -> Value {
    let mut entry = Map::new();
    entry.insert("id".to_owned(), Value::from(client.id()));
    entry.insert("object".to_owned(), Value::from("model"));
    entry.insert("created".to_owned(), Value::from(MODEL_CREATED_EPOCH));
    entry.insert("owned_by".to_owned(), Value::from(MODEL_OWNED_BY));
    entry.insert(
        "context_length".to_owned(),
        Value::from(u64::from(client.spec().context_length)),
    );

    Value::Object(entry)
}

// ---------------------------------------------------------------------------
// `GET` + `POST /stats` - JSON only
// ---------------------------------------------------------------------------

/// `GET` + `POST /providers/private/v1/stats`.
///
/// `GET` answers the projection ([`stats_document`]); `POST` is the per-model reset verb keyed by
/// `?id=`, in the three status codes `privateStatsReset` uses (`private-stats.ts:1063-1089`):
///   * no `id`, or an empty one -> `400 {"error":"missing id query parameter"}`;
///   * an id no row owns -> `404 {"error":"unknown model id: <id>"}`;
///   * a row -> its counters zeroed, `200 {"reset":"<id>"}`.
///
/// The buffer half of the TypeScript reset (`clearStatsBufferEntry(id)`) has no counterpart here:
/// cross-session persistence is out of scope for this cut, so a reset is process-local and a restart
/// starts from zeros anyway (deviation 1).
pub async fn stats(state: &ServeState, request: Request) -> Response {
    let method = request.method().clone();

    // The verb test comes FIRST, as it does on the two POST surfaces: the gateway answers a verb the
    // route object does not declare from its dispatch table, before the handler body - and therefore
    // before any vault question - can run (`handler.ts:38-46`).
    if !matches!(method, Method::GET | Method::POST) {
        return error(StatusCode::BAD_REQUEST, HANDLER_NOT_FOUND);
    }

    let registry = match state {
        ServeState::Ready(registry) => registry,
        ServeState::Unavailable(reason) => return unavailable(reason),
    };

    let (parts, body) = request.into_parts();
    let query = parts.uri.query().map(str::to_owned);
    // The reset verb never reads a body, but it is drained rather than left unconsumed: an
    // unconsumed body on a kept-alive connection is a protocol question the handler should answer
    // explicitly, and the drain is streamed rather than buffered, so it imposes no size cap.
    let mut stream = body.into_data_stream();
    while stream.next().await.is_some() {}

    match method {
        Method::GET => json(StatusCode::OK, stats_document(registry)),
        _ => reset(registry, query.as_deref()),
    }
}

/// The `POST` half of the stats route.
fn reset(registry: &Registry, query: Option<&str>) -> Response {
    let Some(id) = query_value(query, "id").filter(|id| !id.is_empty()) else {
        // `!isString(id) || id.length === 0` is the same guard (`private-stats.ts:1068-1071`): a
        // present-but-empty `?id=` 400s rather than 404ing on a match against nothing.
        return error(StatusCode::BAD_REQUEST, MISSING_ID_ERROR);
    };

    let Some(client) = registry.find(&id) else {
        return error(StatusCode::NOT_FOUND, format!("unknown model id: {id}"));
    };

    // The live half of `resetTotals` (`model-class.ts:507-519`): the counters AND the
    // `lastRequestTokens` baseline, both inside `Accounting::reset`.
    client.reset_counters();

    json(StatusCode::OK, json!({ "reset": id }))
}

/// The whole `GET /stats` document: `{ "models": [...], "totals": { ... } }`.
///
/// `models` is every row of the registry, projected and then sorted by id with a code-unit compare -
/// the order `collectStats` produces (`private-stats.ts:285`) and NOT the registry's row order, which
/// `/models` does keep. The hide-unused filter is skipped (deviation 1).
///
/// `totals` is the roll-up of those same rows (`:289-316`), with `totalCost` summed from the per-row
/// brackets rather than from the aggregate: each row prices its own tokens at its own rates
/// (`:229-231`), which is also why [`Registry::aggregate`] deliberately carries no cost.
#[must_use]
pub fn stats_document(registry: &Registry) -> Value {
    let mut rows: Vec<(&'static str, Value)> = registry
        .clients()
        .iter()
        .map(|client| (client.id(), stats_row(client)))
        .collect();

    // `private-stats.ts:285` uses a `<`/`>` code-unit compare, and `Ord for str` is the same
    // lexicographic byte order, so the two agree with no locale involved. `sort_by_key` is stable, as
    // `Array#sort` is in V8, so two rows that compare equal keep their registry order.
    rows.sort_by_key(|(id, _)| *id);

    let mut totals = Totals::default();
    let mut total_cost = 0.0_f64;

    for client in registry.clients() {
        totals.add(&client.counters());
        total_cost += client.total_cost();
    }

    let mut document = Map::new();
    document.insert(
        "models".to_owned(),
        Value::Array(rows.into_iter().map(|(_, row)| row).collect()),
    );
    document.insert(
        "totals".to_owned(),
        stats_totals(&totals.counters, total_cost),
    );

    Value::Object(document)
}

/// One stats row, in the field ORDER of `ModelStats` (`private-stats.ts:43-102`), with `enabled` and
/// `discoverable` appended as the last two keys.
///
/// The row is keyed by the ROUTING id - the same slug the chat and responses routes resolve by - not
/// by the upstream wire id, which is what keeps a dashboard row and a 404 message naming one model
/// (`private-stats.ts:44-51` says so explicitly).
#[must_use]
pub fn stats_row(client: &ModelClient) -> Value {
    let counters = client.counters();
    let cost = client.pricing().cost(
        counters.input_token_total,
        counters.cache_token_total,
        counters.output_token_total,
    );
    let pricing = client.pricing();

    let mut row = Map::new();
    row.insert("id".to_owned(), Value::from(client.id()));
    row.insert("model".to_owned(), Value::from(client.model()));
    row.insert("provider".to_owned(), Value::from(client.provider()));
    row.insert(
        "inputTokens".to_owned(),
        Value::from(counters.input_token_total),
    );
    row.insert(
        "cacheTokens".to_owned(),
        Value::from(counters.cache_token_total),
    );
    row.insert(
        "outputTokens".to_owned(),
        Value::from(counters.output_token_total),
    );
    row.insert("requests".to_owned(), Value::from(counters.request_total));
    row.insert("inputCost".to_owned(), Value::from(cost.input));
    row.insert("cacheCost".to_owned(), Value::from(cost.cache));
    row.insert("outputCost".to_owned(), Value::from(cost.output));
    row.insert("totalCost".to_owned(), Value::from(cost.total));
    row.insert("inputPrice".to_owned(), Value::from(pricing.input));
    row.insert("outputPrice".to_owned(), Value::from(pricing.output));
    row.insert("cachePrice".to_owned(), Value::from(pricing.cache_read));
    row.insert(
        "milliseconds".to_owned(),
        Value::from(counters.millisecond_total),
    );
    row.insert(
        "ttftMilliseconds".to_owned(),
        Value::from(counters.ttft_millisecond_total),
    );
    row.insert(
        "generationMilliseconds".to_owned(),
        Value::from(counters.generation_millisecond_total),
    );
    row.insert(
        "measuredPrefillTokens".to_owned(),
        Value::from(counters.measured_prefill_token_total),
    );
    row.insert(
        "measuredDecodeTokens".to_owned(),
        Value::from(counters.measured_decode_token_total),
    );
    row.insert(
        "measuredDecodeMilliseconds".to_owned(),
        Value::from(counters.measured_decode_millisecond_total),
    );
    // The port's two additions, AFTER the TypeScript block, so the TypeScript's row shape and order
    // stay readable inside the JSON and a consumer can ignore the tail.
    row.insert("enabled".to_owned(), Value::from(client.enabled()));
    row.insert(
        "discoverable".to_owned(),
        Value::from(client.is_discoverable()),
    );

    Value::Object(row)
}

/// The fleet roll-up, in the field ORDER of `StatsTotals` (`private-stats.ts:141-153`), with the two
/// port counters appended for the same reason they are appended in [`stats_row`].
fn stats_totals(counters: &Counters, total_cost: f64) -> Value {
    let mut totals = Map::new();
    totals.insert(
        "inputTokens".to_owned(),
        Value::from(counters.input_token_total),
    );
    totals.insert(
        "cacheTokens".to_owned(),
        Value::from(counters.cache_token_total),
    );
    totals.insert(
        "outputTokens".to_owned(),
        Value::from(counters.output_token_total),
    );
    totals.insert("requests".to_owned(), Value::from(counters.request_total));
    totals.insert("totalCost".to_owned(), Value::from(total_cost));
    totals.insert(
        "milliseconds".to_owned(),
        Value::from(counters.millisecond_total),
    );
    totals.insert(
        "ttftMilliseconds".to_owned(),
        Value::from(counters.ttft_millisecond_total),
    );
    totals.insert(
        "generationMilliseconds".to_owned(),
        Value::from(counters.generation_millisecond_total),
    );
    totals.insert(
        "measuredPrefillTokens".to_owned(),
        Value::from(counters.measured_prefill_token_total),
    );
    totals.insert(
        "measuredDecodeTokens".to_owned(),
        Value::from(counters.measured_decode_token_total),
    );
    totals.insert(
        "measuredDecodeMilliseconds".to_owned(),
        Value::from(counters.measured_decode_millisecond_total),
    );
    totals.insert(
        "attemptTotal".to_owned(),
        Value::from(counters.attempt_total),
    );
    totals.insert(
        "consecutiveFailures".to_owned(),
        Value::from(u64::from(counters.consecutive_failures)),
    );

    Value::Object(totals)
}

// ---------------------------------------------------------------------------
// Small, pure helpers the contract depends on
// ---------------------------------------------------------------------------

/// The request log line: `formatTokenLine` (`private-chat-completion.ts:49-61`) reproduced.
///
/// ```text
/// MolabEndless: local/basalt | input: 1,234 (56) [$0.13] | output: 78 [$0.03]   # relay named
/// model: nvidia/kimi-k3      | input: 0 (0) [$0.00] | output: 0 [$0.00]         # no relay
/// ```
///
/// The greppable `model:` token is kept for every provider that does not name its relays, so the
/// deployment's log pattern never changes and `undefined:` can never print. Thousands are grouped as
/// `toLocaleString('en-US')` groups them; dollars get two decimals with the `toFixed(2)` TIE RULE
/// ([`to_fixed_two`]), the only place this port formats money for a human. A label is a vault entry
/// NAME, never a URL and never a credential value ([`crate::client::upstream_label_is_safe`] is what
/// enforces it).
#[must_use]
pub fn format_token_line(
    model: &str,
    total_input: u64,
    total_cache: u64,
    input_cost: f64,
    total_output: u64,
    output_cost: f64,
    upstream_label: Option<&str>,
) -> String {
    format!(
        "{}{model} | input: {} ({}) [${}] | output: {} [${}]",
        match upstream_label {
            // `upstreamLabel ? `${upstreamLabel}: ` : 'model: '` - the prefix OR the token, never
            // both and never neither.
            Some(label) => format!("{label}: "),
            None => "model: ".to_owned(),
        },
        group_thousands(total_input),
        group_thousands(total_cache),
        to_fixed_two(input_cost),
        group_thousands(total_output),
        to_fixed_two(output_cost)
    )
}

/// Formats a dollar figure exactly as `Number.prototype.toFixed(2)` does, TIE RULE included.
///
/// The tie rule is the whole reason this helper exists: ECMA-262's `toFixed` picks the LARGER
/// candidate when a value sits exactly on a cent boundary, while Rust's `{:.2}` rounds half to EVEN.
/// A cost CAN land exactly there - the figures are `tokens × price / 1_000_000`, and a dyadic price
/// like $0.25/M over 500_000 tokens is exactly `0.125` - where the deployed line prints `$0.13` and
/// `{:.2}` would print `$0.12`. The log line is the contract, so the JS rule wins.
///
/// The decision is taken on the DECIMAL EXPANSION rather than on `value * 100.0`: that multiply is
/// itself inexact (`0.07 * 100.0` is `7.000000000000001`), while a tie can only exist for a value
/// whose exact rational IS a three-decimal number. `{:.30}` is correctly rounded, so such a value
/// renders as `…X5` followed by only zeros and every other value is decidable by comparing its tail
/// against `5`-then-zeros. Nothing is nudged by an epsilon, so a figure one ulp below a tie stays
/// below it (which is exactly what JS does: `(0.015).toFixed(2)` is `"0.01"`, because the stored
/// double is just under the tie).
///
/// Negative figures keep the JS shape - sign first, then the magnitude rounded the same way
/// (`(-0.125).toFixed(2)` is `"-0.13"`). Costs here are never negative (both `Pricing` rates and the
/// token counters are unsigned), but the total stays correct if that stops being true.
#[must_use]
fn to_fixed_two(value: f64) -> String {
    let (sign, magnitude) = if value.is_sign_negative() {
        ("-", value.abs())
    } else {
        ("", value)
    };

    // `inf`/`nan` have no decimal expansion to reason about, and `{:.2}` is the only total answer
    // for them; a figure too large for the cent arithmetic falls back the same way.
    // Bound the expansion so the two halves of the borrow outlive the parse below.
    let expanded = format!("{magnitude:.30}");
    let Some((whole, fraction)) = expanded.split_once('.') else {
        return format!("{sign}{magnitude:.2}");
    };
    let Ok(units) = whole.parse::<u128>() else {
        return format!("{sign}{magnitude:.2}");
    };
    let Some(base) = units.checked_mul(100) else {
        return format!("{sign}{magnitude:.2}");
    };

    let digits = fraction.as_bytes();
    let digit = |index: usize| u128::from(digits.get(index).copied().unwrap_or(b'0') - b'0');
    let kept = digit(0) * 10 + digit(1);
    let tail = digits.get(2..).unwrap_or(&[]);

    let round_up = match tail.first() {
        // Above the boundary: an ordinary round-up.
        Some(byte) if *byte > b'5' => true,
        // Exactly ON the boundary (`5` then only zeros): JS picks the larger candidate.
        Some(b'5') => tail[1..].iter().all(|byte| *byte == b'0'),
        _ => false,
    };

    let total = base + kept + u128::from(round_up);

    // Whole part, the decimal point, then exactly two cent digits — the shape `toFixed(2)` prints.
    format!("{sign}{}.{:02}", total / 100, total % 100)
}

/// Groups a count the way `Number.prototype.toLocaleString('en-US')` does (`1234` -> `1,234`), the
/// only formatting rule that call contributes to the log line.
#[must_use]
pub fn group_thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, character) in digits.char_indices() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }

        grouped.push(character);
    }

    grouped
}

/// What the request body ACTUALLY was, as the parse step found it (deviation 5).
///
/// Why a type rather than a string: the three arms below are the only answers gate (4) can honestly
/// give about a body that named no model, and the distinction is the diagnostic - "keys: messages,
/// max_tokens" and "the body was not parseable JSON" point at two different broken clients, while an
/// invented `keys:` list for a body that had no keys at all would point at a fault that is not there.
/// `Value::Null` never reaches this type: that arm answers 500 and returns before the gate
/// ([`completion`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BodyShape {
    /// Valid JSON object: its top-level key names are the diagnostic.
    Object,
    /// Valid JSON that is not an object - `[1,2]`, `7`, `"text"`. Parseable, but it has no keys.
    NotAnObject,
    /// Not JSON at all - `""`, `not json`, truncated bytes. Nothing may be claimed about its keys.
    NotJson,
}

/// One `warn!` for the missing-`model` 404, naming only what an operator needs and nothing private.
///
/// WHY THIS LINE EXISTS AT ALL: the 404 body now says the FIELD was wrong (
/// [`MISSING_MODEL_FIELD_MESSAGE`]), which is the right thing to tell a CALLER but leaves the operator
/// with two indistinguishable faults - a client that sent `{}` and a client whose body never parsed.
/// The route name plus the key names tells them apart, which is the whole reason the fault was
/// misdiagnosed in production the first time.
///
/// HARD CONSTRAINT - NAMES, NEVER CONTENT. Only three things are ever printed: the route token
/// ([`CHAT_COMPLETIONS_ENDPOINT`] / [`RESPONSES_ENDPOINT`]), the byte COUNT, and the top-level KEY
/// NAMES of the JSON object. No message text, no tool payload, no header or credential VALUE: a
/// chat body is user content and this route serves requests that may not even be authorised. Key
/// names are user-supplied bytes, so the joined list goes through [`flatten_error_detail`] - the
/// crate's whitespace flattener, secret redactor and 200-character cap in one call - which also stops
/// a key named `"a\ninfo: forged"` from writing a second log line.
///
/// WHY `warn` AND NOT `info`: this is a request the gateway refused for a reason the sender has to
/// fix, and at the crate's default `info` level the operator asked for one line per SUCCESSFUL request
/// only - a fault that explains itself still has to be visible.
fn warn_missing_model_field(endpoint: &str, shape: BodyShape, body: &Value, bytes: usize) {
    // Each shape gets the honest sentence for itself; two of them deliberately claim NOTHING about
    // keys, which is the point of tracking the parse outcome separately from the folded document.
    let what = match shape {
        BodyShape::NotJson => format!("the body is not parseable JSON ({bytes} bytes)"),
        BodyShape::NotAnObject => format!("the body is JSON but not an object ({bytes} bytes)"),
        BodyShape::Object => describe_top_level_keys(body, bytes),
    };

    tracing::warn!(
        target: "typecrate_provider::serve",
        "{endpoint}: refused 404, the body named no usable `model` field; {what}"
    );
}

/// The `{n} bytes, keys[...]` half of [`warn_missing_model_field`]'s line.
///
/// Every key is FREE OF NON-WHITESPACE CONTROL CHARACTERS before it is printed (see the strip below),
/// because the body is untrusted input and the line goes to a terminal; the cap, the whitespace
/// flattening and the redaction come from [`flatten_error_detail`].
///
/// The sort is what makes the line reproducible: `serde_json` preserves INSERTION order by default
/// (the `preserve_order` feature is what the crate's own frame-key assertions rely on), so an
/// unsorted list would put a different key first for every client and defeat a grep.
#[must_use]
fn describe_top_level_keys(body: &Value, bytes: usize) -> String {
    let Some(entries) = body.as_object() else {
        // Unreachable today - `BodyShape::Object` is only set from an `is_object()` arm - and it
        // still degrades to a byte count rather than an `unwrap`.
        return format!("the body is not a JSON object ({bytes} bytes)");
    };

    // A JSON key is arbitrary text, and `flatten_error_detail` (below) only collapses whitespace, caps
    // the length and redacts secrets - it does NOT touch control characters. A caller can therefore
    // send `"me\u{0}ssage"` or `"mo\u{1b}[31mdel"`, and the ESC would reach the terminal inside the
    // printed list and recolour or erase the rest of the record (2026-11-24 review, N6). Every
    // NON-WHITESPACE control character is dropped here, before the list is joined: Unicode `Cc` minus
    // the whitespace the parity rule already handles. WHITESPACE IS DELIBERATELY LEFT ALONE, because
    // `flatten_error_detail` collapsing `\n`/`\t`/`\r` into a single space is the deployed gateway's
    // behaviour (`start.ts` flattens `\s`) and it is pinned above by the newline-in-a-key assertion -
    // stripping the newline instead would print `zzinfo` where the gateway prints `zz info`.
    // DROPPING rather than substituting a marker is deliberate: the terminal is what is being protected
    // (an ESC is what turns `[31m` into a colour), and the printable remainder stays in the line as the
    // literal garbage it is, so the operator still sees that the caller sent a strange key. Two keys
    // that differ only in control characters collapse to the same printed name, which the count beside
    // the list still reports.
    let mut names: Vec<String> = entries
        .keys()
        .map(|key| {
            key.chars()
                .filter(|character| !character.is_control() || character.is_whitespace())
                .collect()
        })
        .collect();
    names.sort();

    let shown = names.len().min(MISSING_MODEL_KEYS_SHOWN);
    // `flatten_error_detail` is the log-injection guard and the length cap described on
    // [`warn_missing_model_field`]; it runs on the JOINED list so one pathological key cannot push
    // the rest out of the record on its own.
    let listed = flatten_error_detail(&names[..shown].join(", "));

    // A body of `{}` has no names to blame, and saying `keys[]=` would read like a formatting bug.
    if names.is_empty() {
        return format!("{bytes} bytes, a JSON object with no keys at all");
    }

    // The remainder is counted, never invented: an operator reading a capped line has to be able to
    // tell it from a complete one.
    let hidden = names.len() - shown;
    let tail = if hidden == 0 {
        String::new()
    } else {
        // The list is capped at [`MISSING_MODEL_KEYS_SHOWN`], so `hidden` is the count BEYOND that
        // cap and the total is always `shown + hidden`.
        format!(" (+{hidden} more of {})", names.len())
    };

    format!("{bytes} bytes, {shown} top-level key(s): {listed}{tail}")
}

/// One query value, read the way Hono's `req.query()` reads it: the FIRST value for a repeated key,
/// `+` as a space, `%XX` decoded.
///
/// `?id=a&id=b` therefore resolves to `a`, matching the TypeScript, and `id=local%2Fstrata` resolves
/// to `local/strata`. Routing ids are plain slugs today, so decoding only matters when a caller sends
/// an escape - doing it anyway keeps a 404 message naming the id the caller actually meant.
#[must_use]
pub fn query_value(query: Option<&str>, name: &str) -> Option<String> {
    let query = query?;

    for pair in query.split('&') {
        // A pair with no `=` is a valueless flag (`?flag`), not this key, and it must not end the
        // scan: `?flag&id=x` still has to find `id`.
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };

        if decode_form_value(key) == name {
            return Some(decode_form_value(value));
        }
    }

    None
}

/// Decodes one `application/x-www-form-urlencoded` token (`+` -> space, `%XX` -> byte).
fn decode_form_value(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' => {
                // A `%` needs TWO hex digits to be an escape; a stray one stays literal, which is
                // what `URLSearchParams` does with it. The bounds check and the two digit decodes
                // are ONE condition because a let-chain is only legal inside `if`/`while` - which
                // is also what keeps the push below from needing an `unwrap`.
                if index + 2 < bytes.len()
                    && let (Some(high), Some(low)) =
                        (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
                {
                    out.push(high * 16 + low);
                    index += 3;
                } else {
                    out.push(b'%');
                    index += 1;
                }
            }
            other => {
                out.push(other);
                index += 1;
            }
        }
    }

    // A decoded byte sequence can be invalid UTF-8 (an arbitrary percent-escape); a lossy conversion
    // keeps the message printable instead of failing the request.
    String::from_utf8_lossy(&out).into_owned()
}

/// One hex digit, `None` when the byte is not one.
fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// The 500 a pre-stream upstream failure produces, with the real verdict in the log.
///
/// The wire body is the gateway's constant (deviation 3). The log line carries the classified code
/// and the failure's own sentence, both redacted at their source (`crate::error::StreamFailure` and
/// [`crate::error::TransportError`]), plus the failure kind - and the attempt ledger names endpoints
/// and relay labels only, never a credential value.
fn upstream_throw(endpoint: &str, failure: &TransportFailure) -> Response {
    tracing::error!(
        target: "typecrate_provider::serve",
        "request error in {}: {} ({})",
        endpoint,
        failure.message(),
        failure.kind().as_str()
    );

    error(StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        DEFAULT_VAULT_ARTIFACT, MISSING_MODEL_FIELD_MESSAGE, MISSING_MODEL_KEYS_SHOWN,
        REGENERATION_COMMAND, Unavailable, VaultSource, describe_top_level_keys, format_token_line,
        group_thousands, query_value, to_fixed_two, vault_source_from_values,
    };

    #[test]
    fn the_default_artifact_is_the_provider_crate_s_own_view_of_config_vault() {
        // The one property the discovered route file cannot get right on its own: the path is baked
        // from THIS crate's manifest, so it sits under the provider package and ends in the artifact.
        assert!(
            DEFAULT_VAULT_ARTIFACT.ends_with(
                "/packages/typecrate/provider/../../../config/vault/generated/agentic-provider.json"
            ),
            "the artifact path is provider-crate relative: {DEFAULT_VAULT_ARTIFACT}"
        );
    }

    #[test]
    fn the_token_line_is_the_deployed_format_in_both_its_shapes() {
        assert_eq!(
            format_token_line(
                "local/basalt",
                1_234,
                56,
                0.125,
                78,
                0.034,
                Some("MolabEndless")
            ),
            "MolabEndless: local/basalt | input: 1,234 (56) [$0.13] | output: 78 [$0.03]"
        );
        assert_eq!(
            format_token_line("nvidia/kimi-k3", 0, 0, 0.0, 0, 0.0, None),
            "model: nvidia/kimi-k3 | input: 0 (0) [$0.00] | output: 0 [$0.00]"
        );
        // Thousands, at the widths the deployment's own lines reach.
        assert_eq!(group_thousands(10_000_000), "10,000,000");
        assert_eq!(group_thousands(1_000), "1,000");
        assert_eq!(group_thousands(999), "999");
        assert_eq!(group_thousands(0), "0");
    }

    #[test]
    fn money_rounds_the_way_tofixed_rounds_it() {
        // The exact ties, i.e. the cases this helper EXISTS for: JS picks the larger candidate,
        // Rust's `{:.2}` picks the even one and would print `0.12` / `0.62` here.
        assert_eq!(to_fixed_two(0.125), "0.13");
        assert_eq!(to_fixed_two(0.625), "0.63");
        // Where the two rules agree, they agree - pinned so the helper is never "simplified" back
        // into `{:.2}`, which would look identical on these and only differ on the two above.
        assert_eq!(to_fixed_two(0.375), "0.38");
        assert_eq!(to_fixed_two(0.875), "0.88");
        // A decimal LITERAL that looks like a tie is not one: the stored double of 0.015 and of
        // 2.675 sits just BELOW the boundary, and JS rounds both down. An epsilon nudge would break
        // these two while fixing nothing.
        assert_eq!(to_fixed_two(0.015), "0.01");
        assert_eq!(to_fixed_two(2.675), "2.67");
        // The figures the deployed lines actually carry, plus the carry across the decimal point and
        // the negative shape of `toFixed` (sign first, magnitude rounded the same way).
        assert_eq!(to_fixed_two(0.0), "0.00");
        assert_eq!(to_fixed_two(0.034), "0.03");
        assert_eq!(to_fixed_two(1234.567), "1234.57");
        assert_eq!(to_fixed_two(9.999), "10.00");
        assert_eq!(to_fixed_two(-0.125), "-0.13");
        // Total, never a panic: a figure with no decimal expansion falls back to the plain format.
        assert_eq!(to_fixed_two(f64::INFINITY), "inf");
    }

    /// The operator line for a body that named no model. The JS interpolation this test used to pin
    /// (`model_label` -> `Model 'undefined' not found`) is GONE on purpose, 2026-11-24: `undefined`
    /// was a rendering artefact of the deployed TypeScript's template literal, not a model id, and it
    /// told the reader nothing. See deviation 5 in this module's header and
    /// [`MISSING_MODEL_FIELD_MESSAGE`].
    #[test]
    fn the_missing_model_sentence_is_pinned_as_data() {
        assert_eq!(
            MISSING_MODEL_FIELD_MESSAGE,
            "Model field is required and was missing or not a string in the request body",
            "the wire sentence, byte for byte; a rewording here is a wire change"
        );
    }

    /// The key list is the whole diagnostic, so its ORDER, its CAP and its remainder count are all
    /// contract. `serde_json` preserves insertion order (the `preserve_order` feature), so an
    /// unsorted list would name a different key first for every client and defeat a grep.
    #[test]
    fn the_body_key_descriptive_names_the_count_the_sorted_names_and_the_remainder() {
        // Insertion order is the reverse of the sort, so the expectation proves the sort ran.
        assert_eq!(
            describe_top_level_keys(
                &json!({ "zz_stream": false, "zz_messages": [], "zz_model_like": 7 }),
                27
            ),
            "27 bytes, 3 top-level key(s): zz_messages, zz_model_like, zz_stream"
        );

        // The one shape with nothing to name: saying `keys[]=` would read like a formatting bug.
        assert_eq!(
            describe_top_level_keys(&json!({}), 2),
            "2 bytes, a JSON object with no keys at all"
        );

        // The defensive arm. `BodyShape::Object` is only ever set from an `is_object()` guard, so a
        // non-object reaching here means a future caller forgot the guard - and it still degrades to
        // a sentence rather than an `unwrap`.
        assert_eq!(
            describe_top_level_keys(&json!(7), 1),
            "the body is not a JSON object (1 bytes)"
        );

        // The cap, as data: 19 keys named `k00`..`k18`, of which `MISSING_MODEL_KEYS_SHOWN` (16) are
        // listed in sort order and the remaining 3 are counted, not invented.
        let mut wide = serde_json::Map::new();
        for index in 0..19 {
            wide.insert(format!("k{index:02}"), Value::Bool(true));
        }
        let listed: Vec<String> = (0..16).map(|index| format!("k{index:02}")).collect();

        assert_eq!(
            describe_top_level_keys(&Value::Object(wide), 88),
            format!(
                "88 bytes, {} top-level key(s): {} (+3 more of 19)",
                MISSING_MODEL_KEYS_SHOWN,
                listed.join(", ")
            )
        );

        // A key NAME is caller bytes. A newline in one must not forge a second log line, and
        // `flatten_error_detail` is what flattens it (the same call also redacts and caps).
        let mut injected = serde_json::Map::new();
        injected.insert("zz\ninfo: forged".to_owned(), Value::Bool(true));
        injected.insert("zz_probe".to_owned(), Value::Bool(true));

        assert_eq!(
            describe_top_level_keys(&Value::Object(injected), 9),
            "9 bytes, 2 top-level key(s): zz info: forged, zz_probe"
        );

        // The HALF `flatten_error_detail` cannot cover: it collapses whitespace, caps the length and
        // redacts credential shapes, but a NUL or an ESC is neither whitespace nor a secret, and an ESC
        // reaching a terminal rewrites the rest of the record. Non-whitespace control characters are
        // therefore stripped per key, so the NUL vanishes and the ANSI escape loses the byte that made
        // it an escape — its printable remainder stays visible as the garbage it is (`mo[31mdel`), and
        // nothing can recolour the line. The exact string below IS the assertion: a surviving
        // `\u{1b}[31m` or a surviving `\u{0}` fails it. The newline case above keeps the flattened
        // form, because that half IS the gateway's rule.
        let mut hostile = serde_json::Map::new();
        hostile.insert("me\u{0}ssage".to_owned(), Value::Bool(true));
        hostile.insert("mo\u{1b}[31mdel".to_owned(), Value::Bool(true));

        assert_eq!(
            describe_top_level_keys(&Value::Object(hostile), 24),
            "24 bytes, 2 top-level key(s): message, mo[31mdel"
        );
    }

    #[test]
    fn query_values_are_read_the_way_the_router_reads_them() {
        assert_eq!(
            query_value(Some("id=local/strata"), "id").as_deref(),
            Some("local/strata")
        );
        assert_eq!(
            query_value(Some("id=local%2Fstrata"), "id").as_deref(),
            Some("local/strata")
        );
        assert_eq!(query_value(Some("id=a&id=b"), "id").as_deref(), Some("a"));
        assert_eq!(query_value(Some("id="), "id").as_deref(), Some(""));
        assert_eq!(query_value(Some("other=1"), "id"), None);
        assert_eq!(query_value(None, "id"), None);
        // A valueless flag must not end the scan, and a stray `%` stays literal.
        assert_eq!(query_value(Some("flag&id=x"), "id").as_deref(), Some("x"));
        assert_eq!(query_value(Some("id=a%b"), "id").as_deref(), Some("a%b"));
        // A `+` is a space in a form value.
        assert_eq!(query_value(Some("id=a+b"), "id").as_deref(), Some("a b"));
    }

    #[test]
    fn an_unavailable_message_names_the_command_and_redacts_a_credential_shaped_cause() {
        let reason = Unavailable::artifact(
            std::path::Path::new("/tmp/agentic-provider.json"),
            "invalid type: string \"sk-live-9f3a7c21be4d0000feed\", expected u32",
        );
        let message = reason.message();

        assert!(message.contains(REGENERATION_COMMAND), "{message}");
        assert!(message.contains("/tmp/agentic-provider.json"), "{message}");
        assert!(
            !message.contains("sk-live-9f3a7c21be4d0000feed"),
            "a credential-shaped value leaked into a printable message: {message}"
        );
    }

    #[test]
    fn the_two_configured_values_resolve_in_the_documented_order() {
        // Nothing ambient is touched: the precedence is a pure function of the two values, so this
        // runs in parallel with every other test and cannot be perturbed by a caller's environment.
        assert_eq!(
            vault_source_from_values(Some("{}"), Some("/tmp/some-artifact.json")),
            VaultSource::Inline("{}".to_owned())
        );
        // A whitespace-only inline document is unset, so the path wins - `set VAR=` on Windows and
        // `export VAR=` on POSIX both leave exactly this behind.
        assert_eq!(
            vault_source_from_values(Some("   "), Some("/tmp/some-artifact.json")),
            VaultSource::Path(std::path::PathBuf::from("/tmp/some-artifact.json"))
        );
        assert_eq!(
            vault_source_from_values(Some(""), Some("/tmp/other.json")),
            VaultSource::Path(std::path::PathBuf::from("/tmp/other.json"))
        );
        assert_eq!(
            vault_source_from_values(None, Some("")),
            VaultSource::DefaultArtifact
        );
        assert_eq!(
            vault_source_from_values(None, None),
            VaultSource::DefaultArtifact
        );
    }
}
