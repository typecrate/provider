//! The outbound transport substrate: one OpenAI-compatible HTTP/SSE client over a rotating
//! upstream target.
//!
//! TypeScript origin: `packages/agentic/provider/src/model-class.ts` - the `StandardClient`
//! TRANSPORT layer only (`:557-654` the status-handler transport and its `while (true)` loop,
//! `:624` the 30-minute ceiling, `:628` the retry budget, `:634-636` the 404 handler that calls
//! `switchBaseUrl`, `:885-892` the `maxRetries: 0` that neutralises the SDK's own retries, and the
//! credential posture at `:39-60` / `:378-390`) - plus the per-vendor failover it delegates to,
//! whose shape is `packages/agentic/provider/src/local/local-strata.ts:243-261` (`switchBaseUrl`
//! shifts an endpoint queue and re-points `baseURL` AND `upstreamLabel` in lockstep) and
//! `packages/agentic/provider/src/nvidia/nvidia-glm5-flash.ts:80-85` (the same method rotating
//! CREDENTIALS instead). The wire mechanics follow the only in-repo Rust precedent for outbound
//! OpenAI-compatible streaming, `packages/typecrate/harness/src/transport.rs` (`:222-262` the
//! header merge and the cancel-raced send, `:303-354` the SSE body loop, `:356-367` the
//! cancellation-outranks-everything rule and the truncation guard).
//!
//! WHAT THIS MODULE IS NOT. No model clients, no registry, no routes, no `/stats`, and no counter
//! arithmetic: the accounting FIGURES reach the caller through [`SettlementHook`] and the caller
//! decides what to add to what. That split is what lets the client task own
//! `inputTokenTotal`/`cacheTokenTotal`/`ttftMillisecondTotal` (the TS instance fields at
//! `model-class.ts:411-457`) without this layer duplicating them.
//!
//! ONE SHARED HEAD, ONE PINNED WALK PER REQUEST (`&self` everywhere)
//! -----------------------------------------------------------------
//! The TypeScript clients are process-wide singletons whose `baseURL` and `apiKey` FIELDS mutate
//! in place (`model-class.ts:352-355` says `upstreamLabel` is "Mutable, NOT readonly";
//! `local-strata.ts:253-257` assigns `this.baseURL` / `this.upstreamLabel` inside `switchBaseUrl`;
//! `nvidia-glm5-flash.ts:81` assigns `this.apiKey`). The Rust equivalent has to rotate behind
//! `&self`, because the routes will hold one transport per model and share it across requests. So
//! the pools are immutable `Vec`s and the SELECTION is a pair of [`AtomicUsize`]s that only ever
//! move forward, shared through an [`Arc`] on `Clone` - a clone of a transport is the SAME
//! singleton, not a second queue.
//!
//! THE TWO AXES ARE NOT THE SAME KIND OF STATE, AND THE DIFFERENCE IS THE CONCURRENCY FIX.
//!   * The ENDPOINT cursor is a VERIFIED-PREFIX HEAD, not a claim slot. `head == j` states one
//!     fact about the pool: some request has PERSONALLY dialled `pool[j]` and received its verdict,
//!     and that verdict was the failover trigger (HTTP 404). The dial IS the probe - this port has no
//!     pre-flight health check to invent, so "verified" means exactly "someone got the real answer".
//!     Each request keeps its OWN position: it resolves `start = min(head, last)` once
//!     ([`Transport::endpoint_start`]), then dials `pool[start]`, `pool[start + 1]`, ... and pins the
//!     entry it dials for the whole [`Transport::post`] call, so none of its internal retries (the
//!     429 pause, a timeout, a dead socket) can change the upstream under it. Only a 404 that THIS
//!     request collected moves the head, and it moves it with `fetch_max`
//!     ([`Transport::verify_endpoint`]).
//!   * The CREDENTIAL cursor stays a claim-a-slot cursor, advanced by [`advance`]: the credential
//!     families hand the NEXT caller a key that has been taken out of the pool
//!     (`nvidia-glm5-flash.ts:13-18`, `makora-glm-5-3.ts:6-11`), which IS a per-rotation claim
//!     rather than a verification.
//!
//! Why the head is `fetch_max` and the old endpoint cursor was a compare-and-swap: a CAS-advance
//! hands each of N concurrent 404s its OWN step, so ten requests failing on the same dead relay
//! move the shared selection ten places and nine of them dial endpoints nobody had proved dead.
//! `fetch_max` is idempotent and commutative, so N requests that all 404 at `pool[j]` publish the
//! same head (`j + 1`), and the endpoints between them are dialled by every one of them. The cost is
//! stated plainly: concurrent requests WALK IN LOCKSTEP and dial the same endpoints instead of
//! spreading across the pool - deliberate, because spreading is precisely what used to skip
//! unverified endpoints. The invariant that buys is [`Transport::verify_endpoint`]'s: for every
//! `j < head`, `pool[j]` was personally 404-verified by some request, and every request's dial
//! sequence is contiguous.
//!
//! Why a forward-only cursor rather than a drained `VecDeque`: the TypeScript `shift()` consumes
//! the queue, so a rotated-past endpoint is never retried again in the process, and a cursor
//! reproduces exactly that. Keeping the pool intact is what lets the ledger, `/stats` and
//! [`Transport::reset_rotation`] still name every entry. On exhaustion the selection FALLS BACK to
//! the LAST entry rather than to "nothing", because `local-strata.ts:244-250` returns false WITHOUT
//! clearing `baseURL`: the endpoint that was set last stays in force. A fully-outaged pool therefore
//! leaves the head one PAST `last` (every entry has been proved dead), and every read clamps to
//! `last` - [`Self::selected_index`] is the single place that clamp lives.
//!
//! THE FOUR DECISIONS THE BRIEF ASKED FOR
//! ---------------------------------------
//! 1. RESULT TYPE. [`ResponseOrStream`] mirrors the branch `model-class.ts:166-171` documents and
//!    makes at runtime with `isAsyncIterable` (declared `:170-171`, branched at `:723-733`: a settled
//!    object or an async iterable) as a compile-time enum. A stream item is `Result<Value, StreamFailure>`:
//!    TypeScript THROWS from inside the async iterator (`private-chat-completion.ts:163-168`
//!    rethrows the upstream object untouched, precisely to keep the `status`/`cause.code` the
//!    gateway's classifier reads) and a Rust `Stream` has no exception channel, so the failure
//!    arrives as a terminal `Err` item and the stream is fused to `None` after it. The observable
//!    behaviour is the same - a `for await` body ends by exception, a Rust consumer's next item IS
//!    the exception - and it keeps `[DONE]` off the wire after a failure, because the sentinel is
//!    the CALLER's frame and the caller sees `Err` instead of clean exhaustion
//!    ([`sse::stream_body`] documents the placement rule).
//!
//! 2. 404 IS NOT RETRIED HERE. It is a typed outcome the caller acts on:
//!    [`TransportError::Status`] with `status == 404` plus the [`Attempt`] ledger is exactly what
//!    TS hands the route loop (`private-responses.ts:106-121`), and the caller then moves to the entry
//!    its OWN walk is on (`crate::client::ModelClient::dispatch` publishes the verification with
//!    [`Transport::verify_endpoint`] and re-pins the next entry) and re-runs the WHOLE attempt with
//!    the previous [`AttemptState`] handed back in through
//!    [`RequestOptions::with_resume`] - the "the whole attempt is re-run" property of the TS fetch
//!    wrapper (`model-class.ts:592-618`). `rebaseRequestInput` (`model-class.ts:141-164`) has no
//!    Rust counterpart because the retry rebuilds its URL from the endpoint the request PINNED instead
//!    of string-patching the URL the previous attempt used.
//!
//! 3. THE RETRY BUDGET IS BOUNDED. The exact TypeScript rule, verified line by line: the fetch
//!    wrapper is `while (true)` (`model-class.ts:593-618`), and it retries ONLY when a status
//!    handler exists AND returns truthy — `if (!handler) return response` (`:599-600`) means every
//!    status with no handler (400, 401, 403, 500, 503, …) is handed to the SDK untouched, and
//!    `if (!shouldRetry) return response` (`:608-609`) ends the loop when a handler refuses. Only
//!    two handlers exist by default: 429 pauses 3 s and returns `true`, so it retries FOREVER with
//!    no attempt counter (`:572-576`), and 404 rotates the base URL and stops when the pool is
//!    exhausted (`:634-635` + `switchBaseUrl` `:933-935`). The SDK's own retries are neutralised per
//!    request (`:885-892` forces `maxRetries: 0`), so a dropped or timed-out socket is NOT retried
//!    today even though the client is built with `maxRetries: 3` and a 30-minute `timeout`
//!    (`:624-628`). This port bounds every retryable outcome to `1 + max_retries` attempts (3, from
//!    `model-class.ts:628`) and reports exhaustion as [`TransportFailure::retries_exhausted`]
//!    carrying the [`Resume`] state. DEVIATION (deliberate): an unbounded 429 loop against a relay
//!    that always answers 429 is an unbounded request - the 30-minute ceiling does not stop it,
//!    because each attempt restarts the clock - and bounding it is the difference between a slow
//!    upstream and a stuck worker. DEVIATION (deliberate): connection and timeout failures are ALSO
//!    retried here. TS neutralises the SDK's own retries (`:885-892` forces `maxRetries: 0`), so a
//!    dropped socket fails on the spot today - but the comment at `:625-627` says the retry budget
//!    EXISTS precisely for "transient connection drops (e.g. UND_ERR_SOCKET 'other side closed')
//!    that happen when an idle proxy connection is reaped mid cold-start", which is what this port
//!    spends it on. `tests/transport.rs` pins the bounded count, the 404 rotation stop, and the
//!    never-retried status arm; the retry behaviour is NOT claimed to be identical to the TypeScript.
//!
//! 4. KEYLESS IS A POLICY, NOT A CONSTANT. Selecting the `none` sentinel must both (a) mean "this
//!    endpoint needs no credential" (it is a USABLE state: [`Credential::is_usable`], and the model
//!    stays discoverable - `model-class.ts:32-35`) and (b) decide what reaches the wire. The
//!    TypeScript half is not one behaviour: `openai@6.42.0` has no special case for `none`
//!    (`node_modules/openai/client.js:232-241` builds the header from `apiKey` unconditionally), so
//!    every client that passes `apiKey: 'none'` sends the literal `Authorization: Bearer none`, while
//!    the two local relays additionally pin `defaultHeaders: { Authorization: null }`
//!    (`local-strata.ts:161`, `local-basalt.ts:147`) and send NO header. This port exposes both as
//!    [`KeylessHeaderPolicy`] on the [`Target`], DEFAULTING TO `SendNone` because that is what today's
//!    relays actually receive. DEVIATION (deliberate, and a correction to this module's first cut):
//!    the transport previously sent NOTHING for every keyless credential, which was right only for the
//!    two relays that null the header and wrong for every other keyless row. `tests/clients.rs` pins
//!    both arms end to end against a listener that reads the real header.
//!
//! CANCEL SAFETY, AND THE ABSENCE OF `finally`
//! --------------------------------------------
//! TypeScript settles a stream's accounting in `trackStream`'s `finally` block
//! (`model-class.ts:859-878`), which runs on normal exhaustion, on a mid-stream throw AND on
//! abandonment - and abandonment IS the request's end for the duration counters
//! (`model-class.ts:799-801`). Rust has no `finally`, so [`StreamBody`] settles in two places: the
//! terminal transition inside `poll_next` (exhaustion, failure, cancellation) and `Drop` for
//! everything abandoned. Both funnel through one `settle` that TAKES the hook, so the hook fires
//! exactly once per stream, and `Drop` additionally cancels the request's child
//! [`CancellationToken`] so the upstream body stops being read instead of idling on a socket nobody
//! is draining.
//!
//! SECRETS NEVER LEAVE THE REDACTED WORLD. A credential is held as a [`Secret`] and reaches the
//! wire only through [`Secret::expose`] inside the header builder. Nothing else here can print one:
//! [`Attempt`] records a credential's index, LABEL and [`VaultValidity`] (all public
//! configuration), [`Transport`] derives `Debug` over types whose only secret-bearing field is a
//! [`Secret`], and every upstream message reaching an error has been through
//! `error::flatten_error_detail` (redaction + the 200-character cap). tests/transport.rs pins that
//! end to end against a listener that sees the real header.

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::{FailureKind, StreamFailure, TransportError, flatten_error_detail};
use crate::sse::{SseDecoder, SseEvent};
use crate::vault::{Secret, VaultEntry, VaultValidity, classify};

// ---------------------------------------------------------------------------
// The policy constants, one per TypeScript source line
// ---------------------------------------------------------------------------

/// The cold-start ceiling: `timeout: 30 * 60 * 1000` (`model-class.ts:624`).
///
/// Private clouds scale containers down when idle, so a request that hits a cold container can take
/// minutes while the model checkpoint loads; the ceiling is deliberately enormous, and the SDK's own
/// default timeout is what it replaces.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// The retry budget: `maxRetries: 3` (`model-class.ts:628`), i.e. at most 4 attempts per logical
/// request. See the module doc for what the budget covers in this port.
pub const MAX_RETRIES: usize = 3;

/// The 429 backoff: `await scriptPause(3000)` (`model-class.ts:572-576`).
pub const RATE_LIMIT_PAUSE: Duration = Duration::from_secs(3);

/// The one line the retry path prints, so a pause is not invisible in the terminal
/// (`model-class.ts:573`, whose literal hard-codes `3000ms`). Printed through `tracing::info!`
/// rather than `console.log`, and with the ACTUAL pause interpolated, because this port makes the
/// pause injectable.
pub const RATE_LIMIT_RETRY_LOG: &str =
    "[retry] HTTP 429 rate limited — waiting {millis}ms before retrying";

/// The terminal condition of a base-URL failover that ran out of base URLs.
///
/// The brief quoted `Upstream returned 404 for every known base URL.`; the string the TypeScript
/// routes actually throw is this one, byte-identical in both branches
/// (`private-responses.ts:118` and `:144`), so the port carries the deployed wording.
pub const EXHAUSTED_BASE_URLS_MESSAGE: &str =
    "Service unavailable: all base URLs are exhausted (404 Not Found).";

/// The Chat Completions path (`chat.completions.create` -> `POST {baseURL}/chat/completions`;
/// `harness/src/transport.rs:141-147` composes the same URL).
pub const CHAT_COMPLETIONS_PATH: &str = "chat/completions";

/// The native Responses path (`private-responses.ts` speaks it through `client.responses.create`).
pub const RESPONSES_PATH: &str = "responses";

/// The model-catalog path, used by the sandbox engines' discovery
/// (`daytona/daytona-qwen3-8.ts:336`, `fetch(\`${link.url}/v1/models\`)`).
pub const MODELS_PATH: &str = "models";

/// The keyless credential marker (`vault::KEYLESS_SENTINELS`, whose authority is
/// `model-class.ts:32-35`: `none` means "this endpoint needs no credential" and the model stays
/// discoverable - it is not a failure).
pub const KEYLESS_CREDENTIAL: &str = "none";

// ---------------------------------------------------------------------------
// The endpoint pool: base URLs, plus the relay NAMES that label them
// ---------------------------------------------------------------------------

/// One upstream endpoint: a base URL normalised to the form a path is appended to, and the
/// operator-facing relay NAME.
///
/// `label` reproduces `upstreamLabel`'s SECURITY POSTURE (`model-class.ts:345-350`): it may only
/// ever hold a NAME from the endpoint record - never the endpoint URL, never a credential value -
/// because it is echoed to browser devtools by the chat-completion token line
/// (`private-chat-completion.ts:174-184`, the line being `formatTokenLine` at `:49-61`). \
/// The vault entry label IS that name: `vault.rs` documents
/// `VaultEntry::label` as the source map key that `local-strata.ts:13` publishes as `upstreamLabel`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    base_url: String,
    label: Option<String>,
}

impl Endpoint {
    /// Wraps a base URL, applying the `/v1` rebasing rule at construction
    /// ([`normalize_base_url`]), so every later read - selection, ledger, error text - names the URL
    /// that will actually be dialed.
    #[must_use]
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: normalize_base_url(base_url),
            label: None,
        }
    }

    /// The same base URL with the relay NAME the log line prints (`upstreamLabel`).
    #[must_use]
    pub fn labeled(base_url: &str, label: &str) -> Self {
        Self {
            base_url: normalize_base_url(base_url),
            label: Some(label.to_owned()),
        }
    }

    /// One endpoint out of a vault ENDPOINT-slot entry: `value` is the URL, `label` is the relay name.
    ///
    /// [`VaultEntry::value_str`] (not `Secret::expose`) is deliberate: `slots.ts:vaultSlotSecret`
    /// says only `keys` slots are secret, so an endpoint value is public configuration and
    /// `value_str` is the accessor the loader reserves for exactly that case.
    #[must_use]
    pub fn from_entry(entry: &VaultEntry) -> Option<Self> {
        let base_url = entry.value_str()?;
        let label = entry.label();

        Some(if label.is_empty() {
            Self::new(base_url)
        } else {
            Self::labeled(base_url, label)
        })
    }

    /// The normalised base URL (`https://host/v1`, no trailing slash).
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The relay NAME - never a URL, never a credential.
    #[must_use]
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// The full request URL for one API path: `{base}/{path}` with exactly one separator
    /// (`harness/src/transport.rs:141-147` normalises the same way once per client).
    #[must_use]
    pub fn request_url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url.trim_end_matches('/'), path)
    }
}

/// Rebases a configured base URL onto the request-ready form.
///
/// The rule, in evaluation order. It is the convention every vault endpoint value already follows
/// (10 of the 11 vendors in `config/vault/generated/agentic-provider.json` carry `/v1`), and the one
/// client that appends the segment by hand is `daytona/daytona-qwen3-8.ts:500-502`,
/// `this.baseURL = \`${endpoint}/v1\``:
///   1. an AUTHORITY containing `api.openai.com` is left ALONE - that is the SDK's own default base
///      (`node_modules/openai/client.js:137`, `https://api.openai.com/v1`), which already carries the
///      version segment; matching on the authority means a path segment named `api.openai.com` under
///      some other host cannot trigger the exemption;
///   2. a path ending in `/v1` is left alone (the vault's own spelling);
///   3. a path ending in `/v1/` is left alone apart from the trailing slash (a configured value may
///      carry one, and the SDK trims it too);
///   4. otherwise `/v1` is APPENDED, PRESERVING ANY PATH PREFIX: `https://host/api` ->
///      `https://host/api/v1`, `https://host` -> `https://host/v1`.
///
/// Surrounding whitespace is trimmed first: the SDK's URL parser ignores it, and
/// `rebaseRequestInput` (`model-class.ts:148-157`) trims for the same reason - a configured
/// `' https://host/v1'` must not become a prefix mismatch later.
///
/// DEVIATION (deliberate): the brief cites `model-class.ts:322-327`, and that file contains no such
/// rule (`:322-333` is the `id` field's doc comment; the only URL surgery in the file is
/// `rebaseRequestInput` at `:141-164`, which swaps one base prefix for ANOTHER during a retry). The
/// rule is implemented exactly as specified - it is what the deployed endpoints and the Daytona
/// client do - and the citation is corrected here rather than silently satisfied.
#[must_use]
pub fn normalize_base_url(base_url: &str) -> String {
    let trimmed = base_url.trim();

    // Split into `scheme://authority` + `path`, the only two parts the rule reads. The query and
    // fragment halves are kept out of the rebasing decision and re-attached verbatim.
    let (scheme, rest) = trimmed
        .split_once("://")
        .map_or((None, trimmed), |(scheme, rest)| (Some(scheme), rest));

    let boundary = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(boundary);
    let query_at = tail.find(['?', '#']).unwrap_or(tail.len());
    let (path, query) = tail.split_at(query_at);

    // Branch 1: the SDK-owned host.
    if authority.contains("api.openai.com") {
        return trimmed.trim_end_matches('/').to_owned();
    }

    let base = match scheme {
        Some(scheme) => format!("{scheme}://{authority}{path}"),
        // A scheme-less value (a bare `host:port/v1` from a test or a local engine): the rule is
        // identical, and the caller's scheme-less form is preserved as given.
        None => format!("{authority}{path}"),
    };

    let base = format!("{}{query}", base.trim_end_matches('/'));

    if path.ends_with("/v1") || path.ends_with("/v1/") {
        return base;
    }

    format!("{base}/v1")
}

// ---------------------------------------------------------------------------
// The credential pool: values, validity, and the keyless sentinel
// ---------------------------------------------------------------------------

/// One selectable credential.
///
/// The three states a caller can be in, and what each sends:
///   * a real value              -> `Authorization: Bearer <value>`;
///   * the `none` keyless marker -> NO `Authorization` header at all (the second half of the `none`
///     semantics the SDK does not implement - see the module doc);
///   * no credential at all (an empty pool) -> also no header, and the CALLER decides whether to
///     issue the request: the TS SDK refuses to CONSTRUCT a client with an empty key
///     (`node_modules/openai/client.js:142-144`, "Missing credentials"), and `isValidApiKey('')`
///     (`model-class.ts:52-60`) is what keeps such a model out of the catalog.
///
/// `validity` is CARRIED rather than re-derived, so the client layer can reproduce
/// `hasValidApiKey` / `isDiscoverable` (`model-class.ts:378-390`) without re-reading the value, and
/// so an unusable pool entry (the live `open_router` pool is 100% unusable) is visible in the ledger
/// without a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    /// `None` means "send no header": either the keyless marker or an absent credential.
    value: Option<Secret>,
    validity: VaultValidity,
    /// The vault entry label: the public NAME of this pool entry, safe to log.
    label: Option<String>,
}

impl Credential {
    /// A credential from a vault entry, REUSING the vault's own verdict (`VaultEntry::validity`)
    /// instead of re-implementing the classifier.
    ///
    /// A GROUP entry (`value: null`, data in `children()`) has no credential to select, so it becomes
    /// a header-less entry; a caller building a pool expands groups through
    /// `VaultSlot::usable()`/`VaultEntry::children()` first.
    #[must_use]
    pub fn from_entry(entry: &VaultEntry) -> Self {
        let label = entry.label();

        Self {
            value: entry.value().cloned(),
            validity: entry.validity(),
            label: (!label.is_empty()).then(|| label.to_owned()),
        }
    }

    /// A bare value the caller already holds (a constructor argument, a test fixture). The verdict
    /// comes from the vault classifier, so `'none'` becomes keyless and `''` / `error` /
    /// `NotAvailable` become unusable exactly as the loader reports them - the `model-class.ts:39-60`
    /// contract, ported by reuse.
    #[must_use]
    pub fn new(value: &Secret) -> Self {
        Self {
            value: Some(value.clone()),
            validity: classify(value.expose()),
            label: None,
        }
    }

    /// The keyless credential: a marker, not a value, and the reason no header is sent.
    #[must_use]
    pub const fn keyless() -> Self {
        Self {
            value: None,
            validity: VaultValidity::Keyless,
            label: None,
        }
    }

    /// The vault verdict this credential carries.
    #[must_use]
    pub const fn validity(&self) -> VaultValidity {
        self.validity
    }

    /// Whether a client may call its provider with this credential (`Valid` or `Keyless` only: the
    /// gate `model-class.ts:378-390` applies, expressed by the vault's own classifier).
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        self.validity.is_usable()
    }

    /// `true` when this credential is the keyless marker (no header).
    #[must_use]
    pub const fn is_keyless(&self) -> bool {
        matches!(self.validity, VaultValidity::Keyless)
    }

    /// The public NAME of this credential's pool entry, for the ledger and the logs.
    #[must_use]
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// The `Authorization` header VALUE, or `None` when no header may be sent.
    ///
    /// This is the one place the value is read for the wire. The shape matches the SDK
    /// (`node_modules/openai/client.js:236-240`: `Authorization: Bearer ${apiKey}`, and NOTHING when
    /// the key is null).
    ///
    /// KEYLESS CARRIES NO POLICY HERE. A keyless credential has no header value of its own: whether
    /// the wire sees `Bearer none` or no header at all is the TARGET's call, made by
    /// [`KeylessHeaderPolicy`] in [`Transport::authorization`] - read that type for why the default is
    /// the `Bearer none` half. A credential that merely LOOKS like the sentinel
    /// (`Credential::new(&Secret::new("none"))`) reaches the same policy branch, because
    /// [`VaultValidity::Keyless`] is what decides it, not the value.
    #[must_use]
    pub fn bearer(&self) -> Option<String> {
        if self.is_keyless() {
            return None;
        }

        self.value
            .as_ref()
            .map(|secret| format!("Bearer {}", secret.expose()))
    }

    /// The un-redacted value, for a caller that must move it somewhere loud.
    #[must_use]
    pub fn expose(&self) -> Option<&str> {
        self.value.as_ref().map(Secret::expose)
    }
}

/// The upstream target one transport speaks to: an ordered endpoint pool, an ordered credential
/// pool, and the keyless flag.
///
/// ORDER IS LOAD-BEARING and the pools are `Vec` for the same reason `vault.rs` keeps them as
/// `Vec`: entry `0` is the live default and the head of the rotation queue (`vault.rs`'s module doc,
/// `config/vault/knowledge.md` "Ordering is load-bearing", and the TS consumers that rotate with
/// `Object.keys` + `shift()` at `nvidia-glm5-flash.ts:9-18`).
#[derive(Clone, Debug, Default)]
pub struct Target {
    endpoints: Vec<Endpoint>,
    credentials: Vec<Credential>,
    /// Whether this whole target needs no credential. Derived from the pool (any keyless entry makes
    /// the target keyless) and overridable for a target whose pool is empty BY CONSTRUCTION, as the
    /// local sandbox relays are (`local-strata.ts:156`, `apiKey: 'none'`).
    keyless: bool,
    /// What a keyless selection sends in `Authorization` - see [`KeylessHeaderPolicy`] for why the
    /// default is the `Bearer none` half. `Target::default()` and [`Target::new`] both land on
    /// `KeylessHeaderPolicy::default()` (`SendNone`), so a target built by any caller gets the
    /// deployed-SDK behaviour unless it opts into [`KeylessHeaderPolicy::Suppress`].
    keyless_header: KeylessHeaderPolicy,
}

impl Target {
    /// One target from the two pools, with the keyless flag DERIVED and the keyless header policy at
    /// its default ([`KeylessHeaderPolicy::SendNone`]).
    #[must_use]
    pub fn new(endpoints: Vec<Endpoint>, credentials: Vec<Credential>) -> Self {
        let keyless = credentials.iter().any(Credential::is_keyless);

        Self {
            endpoints,
            credentials,
            keyless,
            keyless_header: KeylessHeaderPolicy::default(),
        }
    }

    /// Overrides the derived keyless flag: the "this relay needs no credential at all" switch for a
    /// target that carries no credential entries.
    #[must_use]
    pub const fn keyless(mut self, keyless: bool) -> Self {
        self.keyless = keyless;

        self
    }

    /// Chooses the keyless `Authorization` spelling (see [`KeylessHeaderPolicy`]). Only consulted
    /// when the SELECTED credential is keyless; a pool that carries a real value always sends it.
    #[must_use]
    pub const fn keyless_header(mut self, policy: KeylessHeaderPolicy) -> Self {
        self.keyless_header = policy;

        self
    }

    /// The keyless `Authorization` spelling in force for this target.
    #[must_use]
    pub const fn keyless_header_policy(&self) -> KeylessHeaderPolicy {
        self.keyless_header
    }

    /// The endpoint pool, in the document's own order.
    #[must_use]
    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    /// The credential pool, in the document's own order.
    #[must_use]
    pub fn credentials(&self) -> &[Credential] {
        &self.credentials
    }

    /// Whether the target needs no credential at all.
    #[must_use]
    pub const fn is_keyless(&self) -> bool {
        self.keyless
    }

    /// The entries a caller may actually select - the vault's own usability gate. The live
    /// `open_router` pool is 100% unusable, so an EMPTY iterator is a normal input, and the client
    /// layer (not this layer) decides whether to build a client at all.
    // No `#[must_use]`: clippy's `double_must_use` rejects it on an `impl Iterator` return, whose
    // liveness is already enforced by the `Iterator` bound itself.
    pub fn usable_credentials(&self) -> impl Iterator<Item = &Credential> {
        // `iter()` yields `&Credential`, so the predicate receives `&&Credential`: a closure, not
        // the method path, which expects `&Credential`.
        self.credentials
            .iter()
            .filter(|credential| credential.is_usable())
    }
}

/// What the `none` keyless marker sends in `Authorization`.
///
/// WHY THIS IS A POLICY AND NOT A FIXED CHOICE. Two facts pull in opposite directions, and only one
/// of them can be the default:
///   * `openai@6.42.0` has NO special case for the sentinel - `node_modules/openai/client.js:232-241`
///     builds the header from `apiKey` unconditionally - so every deployed TypeScript client that
///     passes `apiKey: 'none'` (`daytona/daytona-qwen3-8.ts:451`, and the two local relays before
///     they nulled the header) puts the literal `Authorization: Bearer none` on the wire.
///   * The two local relays go one step further and pin `defaultHeaders: { Authorization: null }`
///     (`local/local-strata.ts:161`, `local/local-basalt.ts:147`), which the SDK's `validateHeaders`
///     accepts and `bearerAuth` drops, so THOSE two send no header at all (verified in the TypeScript
///     comment at `local-strata.ts:152-155`: "fetch capture shows the header absent").
///
/// [`SendNone`] is the DEFAULT because it is what today's relays actually receive; the credential-free
/// spelling is opt-in per target (one row, and it is the credential-less local offering).
/// DEVIATION FROM C2 (deliberate, and the reason the type exists): the transport previously hard-coded
/// the "no header" half for EVERY keyless credential, which silently diverged from the deployed
/// provider on every keyless row except the local relays. The behaviour is now selectable, the
/// deviation is recorded here instead of in a comment, and `tests/clients.rs` pins both arms.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeylessHeaderPolicy {
    /// Send `Authorization: Bearer none` - the value the deployed SDK sends (`SendNone`).
    #[default]
    SendNone,
    /// Send NO `Authorization` header at all - the `defaultHeaders: { Authorization: null }` spelling
    /// of `local-strata.ts:161` / `local-basalt.ts:147`.
    Suppress,
}

/// The request policy: the ceiling, the budget, and the injectable backoff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    /// The per-attempt ceiling ([`REQUEST_TIMEOUT`] by default).
    pub timeout: Duration,
    /// Retries beyond the first attempt ([`MAX_RETRIES`] by default).
    pub max_retries: usize,
    /// The pause before a 429 retry ([`RATE_LIMIT_PAUSE`] by default).
    ///
    /// INJECTABLE ON PURPOSE: the pause is the one part of the parity contract a test cannot wait
    /// 3 s per case to prove, so a suite sets 5 ms and still asserts that the pause HAPPENED -
    /// tests/transport.rs::`a_429_pauses_before_it_retries` measures the elapsed span, which is the
    /// observable half of the rule.
    pub rate_limit_pause: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Self::standard()
    }
}

impl Policy {
    /// The TS-declared budget: 30-minute ceiling, 3 retries, 3 s backoff.
    #[must_use]
    pub const fn standard() -> Self {
        Self {
            timeout: REQUEST_TIMEOUT,
            max_retries: MAX_RETRIES,
            rate_limit_pause: RATE_LIMIT_PAUSE,
        }
    }

    /// Replaces only the backoff - the test seam described on the field.
    #[must_use]
    pub const fn with_rate_limit_pause(mut self, pause: Duration) -> Self {
        self.rate_limit_pause = pause;

        self
    }
}

/// `stream_options` for a streaming request (`nvidia-glm5-flash.ts:64-79`, and the identical
/// `makora/makora-glm-5-3.ts` rule).
///
/// Emitted ONLY on a streaming request that opted in, and ABSENT - never `null` - otherwise: the
/// OpenAI contract rejects `stream_options` on a non-streaming request
/// (`nvidia-glm5-flash.ts:75-77`), and the TS `undefined` value disappears from the serialized body
/// entirely, which is the property [`prepare_body`] reproduces. Without the opt-in no chunk carries
/// `usage`, so `outputTokenTotal` would stay at 0 for every streamed request
/// (`nvidia-glm5-flash.ts:69-74`).
///
/// Every optional field carries `skip_serializing_if`, so an unset field cannot reach the wire as
/// `null` - the same reason the error frame omits `status`/`errno` (see `error.rs`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamOptions {
    /// Ask the relay to attach its `usage` block to the trailing chunk.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_usage: Option<bool>,
}

impl StreamOptions {
    /// The only spelling any client in this repo sends: `{ include_usage: true }`.
    #[must_use]
    pub const fn with_usage() -> Self {
        Self {
            include_usage: Some(true),
        }
    }
}

// ---------------------------------------------------------------------------
// The result types
// ---------------------------------------------------------------------------

/// The boxed chunk stream a streaming request hands out.
///
/// The item type is the deliberate decision recorded in the module doc: `Result<Value,
/// StreamFailure>` carries BOTH the chunk value and the error channel, because TS throws from inside
/// the async iterator and a Rust stream has no other way to say "the iteration ended by exception".
/// After an `Err` item the stream is fused, so a consumer's loop ends exactly once.
pub type UpstreamStream = futures_util::stream::BoxStream<'static, Result<Value, StreamFailure>>;

/// A settled response object, or a stream of chunk objects - the two shapes
/// `model-class.ts:166-171` distinguishes at runtime with `isAsyncIterable`, distinguished here at
/// compile time.
///
/// `Debug` is hand-written because the boxed stream is not `Debug`: printing one would have to poll
/// it, and the whole contract of this type is that only the consumer may poll it. The variant name is
/// all a diagnostic can honestly say, and it is enough for the `{:?}` in a failing assertion.
pub enum ResponseOrStream {
    /// A non-streaming answer: the upstream JSON document, untouched.
    Response(Value),
    /// A streaming answer: chunk VALUES (re-serialised, never upstream raw bytes -
    /// `private-chat-completion.ts:163` re-stringifies each chunk, which is what makes the
    /// `serde_json` `preserve_order` feature load-bearing for key order).
    Stream(UpstreamStream),
}

impl ResponseOrStream {
    /// `true` for the streaming variant.
    #[must_use]
    pub fn is_stream(&self) -> bool {
        matches!(self, Self::Stream(_))
    }

    /// `true` for the settled variant.
    #[must_use]
    pub fn is_response(&self) -> bool {
        matches!(self, Self::Response(_))
    }

    /// The settled document, `None` for a stream.
    #[must_use]
    pub fn response(&self) -> Option<&Value> {
        match self {
            Self::Response(value) => Some(value),
            Self::Stream(_) => None,
        }
    }

    /// Takes the settled document, `None` for a stream.
    #[must_use]
    pub fn into_response(self) -> Option<Value> {
        match self {
            Self::Response(value) => Some(value),
            Self::Stream(_) => None,
        }
    }

    /// Takes the chunk stream, `None` for a settled document.
    #[must_use]
    pub fn into_stream(self) -> Option<UpstreamStream> {
        match self {
            Self::Stream(stream) => Some(stream),
            Self::Response(_) => None,
        }
    }
}

impl std::fmt::Debug for ResponseOrStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Response(value) => formatter.debug_tuple("Response").field(value).finish(),
            // The stream is NOT polled here: `Debug` must never consume an item.
            Self::Stream(_) => formatter.write_str("Stream(<chunk stream>)"),
        }
    }
}

/// What one attempt of one request touched, in the order it was touched.
///
/// The anti-leak contract is STRUCTURAL: a credential appears as an INDEX, an optional entry LABEL
/// and an optional [`VaultValidity`] - never as a value. An endpoint appears as its base URL and its
/// relay NAME, both public configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attempt {
    /// The 1-based attempt number within the logical request (resumed attempts included).
    pub index: usize,
    /// The normalised base URL this attempt was sent to.
    pub endpoint_url: String,
    /// The relay NAME that served it (`upstreamLabel` parity: a name, never a URL, never a key).
    pub endpoint_label: Option<String>,
    /// The position of the selected credential in the pool.
    pub credential_index: Option<usize>,
    /// The credential's vault entry label, when the pool carried one.
    pub credential_label: Option<String>,
    /// The vault verdict for the selected credential.
    pub credential_validity: Option<VaultValidity>,
    /// The status the upstream answered with; `None` when no verdict arrived.
    pub status: Option<StatusCode>,
    /// How the attempt ended; `None` when it succeeded.
    pub kind: Option<FailureKind>,
    /// Wall-clock time this attempt itself took, backoff pause excluded.
    pub elapsed: Duration,
}

impl Attempt {
    /// The endpoint this attempt used - the value a 404 failover rotates AWAY from.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint_url
    }

    /// Whether this attempt reached a settled answer.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.kind.is_none()
    }
}

/// The state one request accumulates across attempts - the payload of [`Resume::Resumed`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AttemptState {
    /// Attempts made so far, including any carried in by [`RequestOptions::with_resume`].
    pub attempts: usize,
    /// The whole ledger, in order.
    pub attempted: Vec<Attempt>,
    /// Total wall-clock time, including every backoff pause: TS measures the same span with one
    /// `performance.now()` captured at the accounting wrapper's entry (`model-class.ts:710`).
    pub elapsed: Duration,
    /// Chunk frames already delivered, for a request that failed mid-stream.
    pub chunks: u64,
    /// Whether a generated payload already reached the consumer, i.e. whether TTFT is stamped.
    pub first_payload: bool,
}

impl AttemptState {
    /// Whether the NEXT attempt may carry this state - which is what "resume" means here.
    #[must_use]
    pub fn resume(&self) -> Resume {
        if self.chunks == 0 && !self.first_payload {
            Resume::New
        } else {
            Resume::Resumed(self.clone())
        }
    }
}

/// The retry-decision shape the brief calls `ResumedOrNew`: what a re-run carries over, and when it
/// starts from scratch.
///
/// WHAT "RESUME" MEANS IN THIS PORT, precisely:
///   * the attempt LEDGER is extended, not reset, so a rotated re-run can still name every endpoint
///     and credential the request already burned through - which is what lets the terminal
///     [`EXHAUSTED_BASE_URLS_MESSAGE`] report the whole attempt set the way the TS route loop reports
///     it (`private-responses.ts:106-121`);
///   * `chunks` / `first_payload` are preserved, so the rotated attempt must NOT re-stamp TTFT and
///     must NOT re-add the request-token baseline for text already on the wire: a re-run that
///     re-stamped would double-count the request and report a second first token that never happened
///     (`model-class.ts:854-856` stamps once, and only the caller can hold that invariant across two
///     transport calls);
///   * `elapsed` accumulates across the whole logical request, because TS measures ONE span from the
///     accounting wrapper's entry (`model-class.ts:710-713`), spanning internal retries AND the 429
///     pauses;
///   * nothing about the RESPONSE resumes: the body is replayed as-is, which is the TS property at
///     `model-class.ts:594-596` ("Each loop reuses the same request body and headers, which is safe
///     for the JSON request bodies emitted by these clients"). The upstream call itself starts over,
///     because a model generation cannot be resumed mid-stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resume {
    /// Carry the state below into the next attempt (a mid-stream or partial-delivery failure).
    Resumed(AttemptState),
    /// Nothing reached the consumer and nothing was stamped: the retry is a fresh request.
    New,
}

/// A request-side failure plus the ledger behind it.
///
/// The route layer gets everything it needs from this one type: `status()` (its `status ?? 500`),
/// `kind()`, `message()`, `is_not_found()` (the `is404` gate), `attempted()` (which endpoints were
/// tried) and `resume()` (whether a rotated re-run may carry the ledger).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportFailure {
    error: TransportError,
    state: AttemptState,
    /// `true` when the retry budget ran out: the caller may still rotate a credential and re-run with
    /// [`TransportFailure::resume`], and only THEN give up.
    retries_exhausted: bool,
}

impl TransportFailure {
    /// A failure the caller acts on directly (a non-2xx verdict, a cancellation, an unbuildable
    /// request).
    #[must_use]
    pub fn new(error: TransportError, state: AttemptState) -> Self {
        Self {
            error,
            state,
            retries_exhausted: false,
        }
    }

    /// The same failure after the retry budget was spent.
    #[must_use]
    pub fn exhausted(error: TransportError, state: AttemptState) -> Self {
        Self {
            error,
            state,
            retries_exhausted: true,
        }
    }

    /// The terminal failover outcome: every known base URL answered 404 and the pool is exhausted
    /// (`private-responses.ts:117-119` and `:143-145`).
    ///
    /// It carries a 404 status so the routes keep rendering what they render today for that
    /// condition, with the exhausted-pool sentence as the message half.
    #[must_use]
    pub fn all_base_urls_exhausted(state: AttemptState) -> Self {
        Self::new(
            TransportError::Status {
                status: StatusCode::NOT_FOUND,
                detail: EXHAUSTED_BASE_URLS_MESSAGE.to_owned(),
            },
            state,
        )
    }

    /// The typed error half.
    #[must_use]
    pub fn error(&self) -> &TransportError {
        &self.error
    }

    /// Consumes the failure to reach the error alone.
    #[must_use]
    pub fn into_error(self) -> TransportError {
        self.error
    }

    /// The verdict to forward (`None` -> the route's 500 branch).
    #[must_use]
    pub fn status(&self) -> Option<StatusCode> {
        self.error.status()
    }

    /// The closed code for this failure.
    #[must_use]
    pub fn kind(&self) -> FailureKind {
        self.error.kind()
    }

    /// The message a consumer string-matches (the SDK's own sentence).
    #[must_use]
    pub fn message(&self) -> String {
        self.error.message()
    }

    /// The failover gate: `error instanceof APIError && error.status === 404`.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        self.error.is_not_found()
    }

    /// Whether the budget, not the upstream, ended this request.
    #[must_use]
    pub const fn retries_exhausted(&self) -> bool {
        self.retries_exhausted
    }

    /// The full state a rotated re-run resumes from.
    #[must_use]
    pub fn state(&self) -> &AttemptState {
        &self.state
    }

    /// Every endpoint this request touched, in order - the "attempted endpoints" ledger.
    #[must_use]
    pub fn attempted(&self) -> &[Attempt] {
        &self.state.attempted
    }

    /// What the next attempt may carry over.
    #[must_use]
    pub fn resume(&self) -> Resume {
        self.state.resume()
    }
}

impl std::fmt::Display for TransportFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message())
    }
}

impl std::error::Error for TransportFailure {}

/// A failure that never reached an attempt - the one case where the ledger is legitimately empty.
///
/// `prepare_body` (serialisation, the `stream` flag) can fail BEFORE the first endpoint is picked,
/// and the `?` in [`Transport::post`] relies on this conversion: the empty [`AttemptState`] it builds
/// is exactly what `resume()` then reports as [`Resume::New`], because nothing was delivered.
impl From<TransportError> for TransportFailure {
    fn from(error: TransportError) -> Self {
        Self::new(error, AttemptState::default())
    }
}

// ---------------------------------------------------------------------------
// Per-call options, and the settlement contract
// ---------------------------------------------------------------------------

/// The accounting facts one stream settles with, handed to a [`SettlementHook`] EXACTLY ONCE - on
/// exhaustion, on failure, on cancellation, or on abandonment.
///
/// TS settles the same facts in `trackStream`'s `finally` (`model-class.ts:859-878`), and the rule
/// that they are recorded TOGETHER is what makes the dashboard's rate figures valid, so they arrive
/// as one struct rather than as separate callbacks. The ARITHMETIC stays the client layer's: these
/// are spans and figures, not counters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamSettlement {
    /// How the stream ended.
    pub outcome: StreamOutcome,
    /// Chunk frames that reached the consumer.
    pub chunks: u64,
    /// Request entry -> settlement: the span TS folds into `millisecondTotal`.
    pub elapsed: Duration,
    /// Request entry -> first generated payload, i.e. TTFT. `None` when no payload arrived, and
    /// `None` is NOT zero: TS records no phase split at all in that case (`model-class.ts:869-877`).
    pub time_to_first_token: Option<Duration>,
    /// First generated payload -> settlement. Same `None` rule.
    pub generation: Option<Duration>,
    /// The LAST `usage.completion_tokens` / `usage.output_tokens` seen on any chunk; `None` means the
    /// upstream never reported one.
    pub output_tokens: Option<u64>,
    /// Whether a usage block was SEEN at all. An explicit `0` is KNOWN (it adds 0 tokens and still
    /// counts its span in the decode denominator) while ABSENT usage must not inflate that
    /// denominator - `outputUsageKnown` at `model-class.ts:838-839`, and the reason this flag is
    /// separate from the figure above.
    pub usage_reported: bool,
    /// Whether the `[DONE]` sentinel actually arrived. `false` together with
    /// [`StreamOutcome::Completed`] means the body ended early: the provider surface still emits its
    /// own `[DONE]` after a clean-looking exhaustion (`private-chat-completion.ts:186`), so this is a
    /// signal for the caller, not a failure of this layer.
    pub sentinel: bool,
    /// The failure that ended the stream, if one did.
    pub failure: Option<StreamFailure>,
}

/// How a stream ended, and therefore which settlement rule applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamOutcome {
    /// The body was exhausted, with or without the sentinel (see [`StreamSettlement::sentinel`]).
    Completed,
    /// A failure item was delivered (upstream error frame, socket death, undecodable frame).
    Failed,
    /// The caller's cancellation token fired: the TS path that raises `APIUserAbortError`.
    Cancelled,
    /// The consumer dropped the stream. TS has no such name - abandonment runs the same `finally`
    /// and IS the request's end (`model-class.ts:799-801`), which is why `Drop` performs it here.
    Abandoned,
}

/// The hook a caller installs to receive exactly one [`StreamSettlement`].
///
/// `Send + Sync`, not just `Send`: `RequestOptions` carries this box across an `.await` inside the
/// request future, and axum's `Handler` bound requires that future to be `Sync` as well as `Send`
/// (the router boxes the response body, and the boxed body is `Send + Sync`). A `+ Send`-only box made
/// every `serve::` handler unmountable — `fn(Request) -> ...: Handler<_, _>` is not satisfied at the
/// mount site — which is what `packages/typecrate/provider/tests/routes.rs` and
/// `packages/typecrate/services` both failed on. Every hook the client layer builds captures only
/// `Arc`/`Copy` accounting state, so the stronger bound costs nothing at the call sites.
pub type SettlementHook = Box<dyn FnOnce(StreamSettlement) + Send + Sync>;

/// Per-call options: the cancellation signal, extra headers, the settlement hook, the endpoint this
/// call is pinned to, and the state a resumed attempt carries.
#[derive(Default)]
pub struct RequestOptions {
    /// Extra headers. A custom header REPLACES the generated one of the same name, which is why the
    /// merge runs through a map and each name is written exactly once
    /// (`harness/src/transport.rs:222-247`).
    pub headers: BTreeMap<String, String>,
    /// The caller's cancellation signal. Each request takes a CHILD token from it, so dropping one
    /// stream cancels that stream's upstream read without cancelling the caller's whole context.
    pub signal: CancellationToken,
    /// The settlement hook for a streaming request. A SETTLED response never calls it: the caller
    /// already has that request's span in [`AttemptState::elapsed`], which is where TS settles the
    /// non-streaming half of `millisecondTotal` (`model-class.ts:723-730`).
    pub on_settle: Option<SettlementHook>,
    /// The ledger a rotated re-run resumes from - [`TransportFailure::state`], handed back.
    pub resume: Option<AttemptState>,
    /// The endpoint THIS call must dial, pinned by the caller.
    ///
    /// `crate::client` sets it once per attempt from the request's own walk position, and
    /// [`Transport::run`] reads it exactly once per call: the pin holds across every internal retry,
    /// so a 429 pause or a socket retry cannot silently move the request to a different upstream.
    /// `None` is the unpinned shape - model discovery (`get_json`), and any suite that drives the
    /// transport directly - which dials the shared selection as it stands when the CALL starts and
    /// keeps dialling that same entry for every retry of that call.
    pub endpoint: Option<Endpoint>,
}

impl std::fmt::Debug for RequestOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The hook is a boxed closure with nothing useful to print; naming its presence is all a
        // diagnostic needs. Nothing in this type can hold a credential anyway: an `Endpoint` is a
        // normalised URL plus a public relay NAME, both of which the log lines already print.
        formatter
            .debug_struct("RequestOptions")
            .field("headers", &self.headers)
            .field("signal", &self.signal)
            .field(
                "on_settle",
                &self.on_settle.as_ref().map(|_| "SettlementHook"),
            )
            .field("resume", &self.resume)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl RequestOptions {
    /// The defaults: no extra headers, an uncancelled token, no hook, fresh state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attaches the caller's cancellation signal.
    #[must_use]
    pub fn with_signal(mut self, signal: CancellationToken) -> Self {
        self.signal = signal;

        self
    }

    /// Installs the settlement hook the accounting layer needs.
    #[must_use]
    pub fn with_settlement(mut self, hook: SettlementHook) -> Self {
        self.on_settle = Some(hook);

        self
    }

    /// Adds one header, replacing a generated header of the same name.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());

        self
    }

    /// Carries a previous attempt's state into this call - the resumed re-run after a rotation.
    #[must_use]
    pub fn with_resume(mut self, state: AttemptState) -> Self {
        self.resume = Some(state);

        self
    }

    /// Pins this call to one endpoint, for every attempt it makes.
    ///
    /// This is how a request keeps its upstream to itself: the transport reads the pin once and
    /// never re-reads the shared selection while the call is in flight, so the caller's walk position
    /// (not whatever another concurrent request published) decides what gets dialled
    /// (see the module section "ONE SHARED HEAD, ONE PINNED WALK PER REQUEST").
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = Some(endpoint);

        self
    }
}

// ---------------------------------------------------------------------------
// The transport
// ---------------------------------------------------------------------------

/// The outbound OpenAI-compatible transport: one shared HTTP client, one policy, one rotating
/// upstream target.
///
/// `Clone` shares the rotation cursors through an `Arc`, so a cloned transport is the SAME singleton
/// - which is how the TypeScript clients behave: process-wide, one credential/base-URL pair that
///   mutates in place for every request the process makes.
///
/// What is shared is deliberately NARROW on the endpoint axis: a clone shares only the
/// verified-prefix HEAD (`endpoint_head`), i.e. the bookkeeping of which entries some request has
/// PERSONALLY dialled and received the failover verdict from. Where a request IS in its own walk is
/// per-request state, never shared, and every dial is pinned to the entry the walker chose. That is
/// the property that stops a burst of concurrent requests from skipping endpoints no one ever
/// dialled; `rotate_endpoint` and `reset_rotation` are the documented seams that still move the
/// shared head outside a request's own walk.
#[derive(Clone, Debug)]
pub struct Transport {
    http: Client,
    policy: Policy,
    target: Target,
    /// The endpoint VERIFIED-PREFIX head - the in-place `baseURL` mutation of the TypeScript
    /// original, weakened from "the selection every request dials" to "which entries have been
    /// proved dead, by someone, on purpose".
    endpoint_head: Arc<AtomicUsize>,
    /// The credential selection cursor - the in-place `apiKey` mutation. This one IS a shared
    /// claim-a-slot cursor, because taking a credential out of the pool takes it out for everyone.
    credential_cursor: Arc<AtomicUsize>,
}

impl Transport {
    /// The TS-declared policy over one target.
    #[must_use]
    pub fn new(target: Target) -> Self {
        Self::with_policy(target, Policy::standard())
    }

    /// One policy over one target, with a default pooled reqwest client.
    #[must_use]
    pub fn with_policy(target: Target, policy: Policy) -> Self {
        Self::with_http_client(target, policy, Client::new())
    }

    /// The test seam: a caller-supplied HTTP client, so a suite can point one transport at an
    /// in-process listener (`harness/src/transport.rs:140-148` is the same seam).
    #[must_use]
    pub fn with_http_client(target: Target, policy: Policy, http: Client) -> Self {
        Self {
            http,
            policy,
            target,
            endpoint_head: Arc::new(AtomicUsize::new(0)),
            credential_cursor: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The target this transport speaks to.
    #[must_use]
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// The policy in force.
    #[must_use]
    pub const fn policy(&self) -> &Policy {
        &self.policy
    }

    /// The endpoint currently selected; `None` only when the pool is empty.
    #[must_use]
    pub fn current_endpoint(&self) -> Option<Endpoint> {
        Self::selected(&self.endpoint_head, &self.target.endpoints)
    }

    /// The credential currently selected; `None` when the pool is empty.
    #[must_use]
    pub fn current_credential(&self) -> Option<Credential> {
        Self::selected(&self.credential_cursor, &self.target.credentials)
    }

    /// The index currently selected in the endpoint pool (`None` for an empty pool). Exposed because
    /// the ledger and `/stats` both name entries by position as well as by label.
    #[must_use]
    pub fn endpoint_index(&self) -> Option<usize> {
        Self::selected_index(&self.endpoint_head, &self.target.endpoints)
    }

    /// The index currently selected in the credential pool.
    #[must_use]
    pub fn credential_index(&self) -> Option<usize> {
        Self::selected_index(&self.credential_cursor, &self.target.credentials)
    }

    /// The endpoint pool's entry at one index, `None` outside the pool.
    ///
    /// This is what a per-request walk pins from: the pools are immutable, so the entry at an index
    /// IS that index, and a caller that holds the value never has to touch the shared head again to
    /// dial it.
    #[must_use]
    pub fn endpoint_at(&self, index: usize) -> Option<Endpoint> {
        self.target.endpoints.get(index).cloned()
    }

    /// The endpoint pool's last valid index, `None` for an empty pool.
    #[must_use]
    pub fn endpoint_last_index(&self) -> Option<usize> {
        self.target.endpoints.len().checked_sub(1)
    }

    /// The raw verified-prefix head, which may legitimately sit ONE PAST the last index - that is
    /// what a fully-outaged pool looks like: every entry dialled, every entry 404ed.
    ///
    /// Every SELECTION reads it clamped (see [`Self::endpoint_index`] and [`Self::endpoint_start`]);
    /// this raw form exists for the walk arithmetic and for the suites that assert the head landed on
    /// the index the contract says it must.
    #[must_use]
    pub fn endpoint_head(&self) -> usize {
        self.endpoint_head.load(Ordering::SeqCst)
    }

    /// Where a NEW request's walk starts: the head, clamped into the pool. `None` for an empty pool,
    /// which is the "target has no endpoint" case the caller still has to answer.
    ///
    /// A request resolves this ONCE, at dispatch start, and from then on its own arithmetic owns its
    /// position. Re-reading it per attempt is exactly the race this type no longer has: a shared
    /// re-read hands a request whatever the last concurrent failure selected.
    #[must_use]
    pub fn endpoint_start(&self) -> Option<usize> {
        Self::selected_index(&self.endpoint_head, &self.target.endpoints)
    }

    /// Records that the CALLER personally dialled `pool[position]` and received the failover verdict
    /// from it, publishing `head := max(head, position + 1)`.
    ///
    /// What it RETURNS is `max(head_before_this_fetch_max, position + 1)`: the highest value this call
    /// could have produced, which is a LOWER BOUND on the head immediately after it - a concurrent
    /// publish of a later entry may land between the `fetch_max` and any later read, so the caller must
    /// not treat the return value as "the head as of this call". Nothing in the crate needs the exact
    /// post-publish head; only the published value matters.
    ///
    /// `fetch_max` is the whole point: the publish is idempotent and commutative, so a hundred
    /// concurrent requests that all 404 at the same entry agree on ONE head, where the previous
    /// claim-a-slot cursor advanced a hundred times and every step after the first skipped an
    /// endpoint nobody had dialled. It never rewinds and never wraps, so the INVARIANT holds for the
    /// life of the process:
    ///   * for every `j < head`, `pool[j]` was personally 404-verified by SOME request;
    ///   * each request's own dial sequence is contiguous, because a request moves from `position` to
    ///     `position + 1` only after collecting that position's verdict.
    ///
    /// Those two statements together are the guarantee the user asked for: no endpoint is skipped
    /// unverified, no matter how many requests arrive at once. The cost is stated plainly - concurrent
    /// requests walk in lockstep and dial the same entries instead of spreading across the pool - and
    /// spreading is exactly what used to skip.
    #[must_use]
    pub fn verify_endpoint(&self, position: usize) -> usize {
        // `fetch_max` returns the value BEFORE this call, so the new head is `max(old, position + 1)`;
        // re-loading afterwards would race with a concurrent publish and report someone else's number.
        self.endpoint_head
            .fetch_max(position + 1, Ordering::SeqCst)
            .max(position + 1)
    }

    /// The relay NAME in force right now. `upstreamLabel` parity includes its CAPTURE RULE: read it
    /// at the response head, not after the stream drains, because the shared client may rotate in
    /// between (`private-chat-completion.ts:106-122`).
    ///
    /// What it cannot promise is the second half of that rule under concurrency: this is a read of the
    /// SHARED head, so it names the entry the process sits on, which is the entry a request dialled
    /// only when that request is the only one running. A request's own attribution comes from the
    /// endpoint IT pinned (`crate::client::ServedEndpoint`), never from a re-read like this one.
    #[must_use]
    pub fn upstream_label(&self) -> Option<String> {
        self.current_endpoint().and_then(|endpoint| endpoint.label)
    }

    /// Moves the shared endpoint head past the entry it currently names and yields the entry the head
    /// lands on - `switchBaseUrl()` for the endpoint-rotating clients
    /// (`local-strata.ts:243-261`). `None` means nothing remains to dial, which is the condition the
    /// route loops turn into [`EXHAUSTED_BASE_URLS_MESSAGE`].
    ///
    /// SEAM, not the production path. Production rotation is per request, inside
    /// `crate::client::ModelClient::dispatch`, and publishes past the entry THAT request dialled;
    /// this call publishes past the entry the SHARED head names, which is the same thing only while
    /// the caller is alone. It stays public because the parity surface (`switchBaseUrl`) and the
    /// suites that walk a whole pool need it, and it is documented as a seam exactly like
    /// [`Transport::reset_rotation`] is.
    ///
    /// Its exhaustion boundary is NOT the client seam's. Here, `position >= last` answers `None`
    /// WITHOUT publishing, so the last entry stays unverified in the head; the client-side seam
    /// (`crate::client::ModelClient::switch_base_url`'s relay arm, which runs
    /// `crate::client::ModelClient::advance_walk`) publishes that last verification - `head := len`,
    /// one past the pool - and only then answers `false`. That asymmetry is deliberate: the client
    /// arm reports a walk that DID collect the last entry's 404, so the head must say so, while this
    /// call is a pure "is anything left to dial?" answer and never invents a verification it has not
    /// been paid for. A caller that needs the head to reach `len` must use the client seam (or the
    /// production walk), not this one.
    #[must_use]
    pub fn rotate_endpoint(&self) -> Option<Endpoint> {
        // One personal 404 at the entry the head names: publish past it, then hand back what the head
        // now points at. `endpoint_start` is the clamped read, so a head that is already past the end
        // answers `None` instead of dialling past the pool.
        let position = self.endpoint_start()?;
        let last = self.endpoint_last_index()?;

        if position >= last {
            return None;
        }

        // The publish is the point here; the returned head belongs to whoever reads it next.
        let _ = self.verify_endpoint(position);

        self.endpoint_at(position + 1)
    }

    /// Advances to the next credential - `switchBaseUrl()` for the credential-rotating clients
    /// (`nvidia-glm5-flash.ts:80-85`). `None` means no credential remains.
    ///
    /// The credential axis keeps the claim-a-slot cursor, and correctly so: rotating a credential pool
    /// takes a key OUT of the pool for every caller, so each rotation must hand its caller the slot IT
    /// claimed. That is what [`advance`] returns (see its doc).
    #[must_use]
    pub fn rotate_credential(&self) -> Option<Credential> {
        // Same shape as `Transport::rotate_endpoint` used to have: index from `advance`, entry from
        // the pool.
        advance(&self.credential_cursor, &self.target.credentials)
            .and_then(|index| self.target.credentials.get(index).cloned())
    }

    /// Whether another endpoint rotation is possible, i.e. whether a failover could still help.
    #[must_use]
    pub fn endpoints_exhausted(&self) -> bool {
        self.endpoint_head.load(Ordering::SeqCst) + 1 >= self.target.endpoints.len()
    }

    /// Whether another credential rotation is possible.
    #[must_use]
    pub fn credentials_exhausted(&self) -> bool {
        self.credential_cursor.load(Ordering::SeqCst) + 1 >= self.target.credentials.len()
    }

    /// Rewinds both cursors to the pool heads: a process-level re-initialisation, and the seam a test
    /// uses to observe rotation from the beginning.
    ///
    /// NOT part of the TypeScript parity surface - a live process never rewinds a drained pool, and
    /// nothing calls it in production. On the endpoint axis it also erases the head's "personally
    /// verified" bookkeeping, which is only true of a pool that has been re-probed.
    pub fn reset_rotation(&self) {
        self.endpoint_head.store(0, Ordering::SeqCst);
        self.credential_cursor.store(0, Ordering::SeqCst);
    }

    /// One streaming or settled request, decided by the body's own `stream` field - the single source
    /// of truth that `create({ ...body, stream })` has in TS, where the flag is normalised once so the
    /// `stream_options` decision and the payload cannot disagree (`nvidia-glm5-flash.ts:57-63`).
    ///
    /// # Errors
    ///
    /// A [`TransportFailure`] for every way an attempt can fail: a non-2xx verdict (404 included, as
    /// the failover trigger), a timeout, a connection death, a cancellation, an undecodable body, or a
    /// request that could not be built. The ledger rides along.
    pub async fn post(
        &self,
        path: &str,
        body: &Value,
        options: RequestOptions,
    ) -> Result<ResponseOrStream, TransportFailure> {
        let (bytes, streaming) = prepare_body(body)?;

        self.run(Method::POST, path, Some(bytes), streaming, options)
            .await
    }

    /// One `GET {base}/{path}` settled document - model discovery against a sandbox engine
    /// (`daytona-qwen3-8.ts:336`), and the same header/auth/retry pipeline as [`Transport::post`].
    ///
    /// # Errors
    ///
    /// See [`Transport::post`].
    pub async fn get_json(
        &self,
        path: &str,
        options: RequestOptions,
    ) -> Result<Value, TransportFailure> {
        self.run(Method::GET, path, None, false, options)
            .await?
            .into_response()
            .ok_or_else(|| {
                TransportFailure::new(
                    TransportError::Decode {
                        detail: "a GET request answered with a stream".to_owned(),
                    },
                    AttemptState::default(),
                )
            })
    }

    /// The index of one cursor inside one pool, clamped to the last entry (the exhaustion fallback).
    fn selected_index<T>(cursor: &AtomicUsize, pool: &[T]) -> Option<usize> {
        if pool.is_empty() {
            return None;
        }

        Some(cursor.load(Ordering::SeqCst).min(pool.len() - 1))
    }

    /// The entry one cursor selects, with the exhaustion fallback documented on [`advance`].
    fn selected<T>(cursor: &AtomicUsize, pool: &[T]) -> Option<T>
    where
        T: Clone,
    {
        Self::selected_index(cursor, pool).and_then(|index| pool.get(index).cloned())
    }

    /// The retry loop: build, send, classify, and decide whether to try again.
    async fn run(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        streaming: bool,
        mut options: RequestOptions,
    ) -> Result<ResponseOrStream, TransportFailure> {
        // A resumed re-run EXTENDS the ledger of the request it came from; a fresh call starts one.
        let mut state = options.resume.clone().unwrap_or_default();
        // The budget covers the LOGICAL request, resumed attempts included, so a rotated retry cannot
        // reset the ceiling on itself.
        let budget = 1 + self.policy.max_retries;
        let child = options.signal.child_token();

        // THE DIAL TARGET IS RESOLVED ONCE, BEFORE THE RETRY LOOP. A pin the caller supplied wins; an
        // unpinned caller (model discovery via `get_json`, or a suite driving the transport directly)
        // dials the shared selection AS IT STANDS WHEN THIS CALL STARTS. Either way, no iteration of
        // the loop re-reads shared state, so the 429 pause and the timeout/connection retries all stay
        // on the upstream the request started on - the property the per-request walk in
        // `crate::client` depends on.
        let Some(endpoint) = options.endpoint.clone().or_else(|| self.current_endpoint()) else {
            return Err(TransportFailure::new(
                TransportError::Build {
                    detail: "the upstream target has no endpoint".to_owned(),
                },
                state,
            ));
        };

        loop {
            let credential = self.current_credential();
            let credential_index = self.credential_index();
            let url = endpoint.request_url(path);
            let started = Instant::now();

            let outcome = self
                .send(
                    &method,
                    &url,
                    credential.as_ref(),
                    body.clone(),
                    streaming,
                    &options,
                    &child,
                )
                .await;

            let elapsed = started.elapsed();
            state.attempts += 1;
            state.elapsed += elapsed;

            let record = |state: &mut AttemptState,
                          status: Option<StatusCode>,
                          kind: Option<FailureKind>| {
                state.attempted.push(Attempt {
                    index: state.attempts,
                    endpoint_url: endpoint.base_url.clone(),
                    endpoint_label: endpoint.label.clone(),
                    credential_index,
                    credential_label: credential
                        .as_ref()
                        .and_then(Credential::label)
                        .map(str::to_owned),
                    credential_validity: credential.as_ref().map(Credential::validity),
                    status,
                    kind,
                    elapsed,
                });
            };

            let response = match outcome {
                Ok(response) => response,
                Err(error) => {
                    record(&mut state, error.status(), Some(error.kind()));

                    // Only the transient verdicts consume the retry budget. A cancellation is final
                    // (the caller asked to stop), and a build or decode verdict is deterministic for
                    // this body, so re-sending it would repeat the same answer.
                    let transient = matches!(
                        error,
                        TransportError::Timeout { .. } | TransportError::Connection { .. }
                    );

                    if transient && state.attempts < budget {
                        continue;
                    }

                    return Err(if transient {
                        TransportFailure::exhausted(error, state)
                    } else {
                        TransportFailure::new(error, state)
                    });
                }
            };

            let status = response.status();
            // The ledger records a `kind` ONLY for a failed attempt: a success is `None`, and any
            // non-2xx verdict is the same `upstream_status` code `TransportError::kind()` reports
            // for a `Status` failure (src/error.rs:725-731), so the ledger and the error view of the
            // same attempt can never disagree.
            let kind = if status.is_success() {
                None
            } else {
                Some(FailureKind::UpstreamStatus)
            };
            record(&mut state, Some(status), kind);

            // 404: the failover trigger, returned WITHOUT an internal retry. The caller rotates and
            // re-runs the whole attempt (`private-responses.ts:106-121`), and there is no
            // `rebaseRequestInput` to do because the retry rebuilds its URL from the rotated endpoint.
            if status == StatusCode::NOT_FOUND {
                return Err(TransportFailure::new(
                    TransportError::Status {
                        status,
                        detail: error_detail(response).await,
                    },
                    state,
                ));
            }

            // 429: the pause, then the retry (the handler at `model-class.ts:572-577`).
            if status == StatusCode::TOO_MANY_REQUESTS {
                let error = TransportError::Status {
                    status,
                    detail: error_detail(response).await,
                };

                if state.attempts < budget {
                    tracing::info!(
                        "[retry] HTTP 429 rate limited — waiting {}ms before retrying",
                        self.policy.rate_limit_pause.as_millis()
                    );

                    if self.policy.rate_limit_pause > Duration::ZERO {
                        sleep_or_cancel(self.policy.rate_limit_pause, &child).await;
                    }

                    continue;
                }

                return Err(TransportFailure::exhausted(error, state));
            }

            if !status.is_success() {
                return Err(TransportFailure::new(
                    TransportError::Status {
                        status,
                        detail: error_detail(response).await,
                    },
                    state,
                ));
            }

            // 2xx streaming: the stream is handed out as soon as the HEAD arrives, exactly like the
            // SDK's `create` resolving for `stream: true` (`private-chat-completion.ts:107-118`).
            if streaming {
                let body = StreamBody::new(response, child.clone(), url, options.on_settle.take());

                return Ok(ResponseOrStream::Stream(body.boxed()));
            }

            // 2xx settled: the body is read under the REMAINDER of the attempt's ceiling, so the
            // 30-minute bound covers head + body together the way the SDK's own timeout does.
            let remaining = self.policy.timeout.saturating_sub(elapsed);

            return match read_document(response, &child, remaining).await {
                Ok(value) => Ok(ResponseOrStream::Response(value)),
                Err(error) => Err(TransportFailure::new(error, state)),
            };
        }
    }

    /// Builds the headers and races one send against the cancellation signal and the ceiling.
    ///
    /// EIGHT arguments by design, and they are not splittable without losing the point: the URL, the
    /// credential, the header set and the accept kind are all derived per attempt, and passing them
    /// separately is what keeps a retry from reusing a previous attempt's headers. Same allowance the
    /// sibling crate takes (`harness/src/store/mod.rs:1039`).
    #[allow(clippy::too_many_arguments)]
    async fn send(
        &self,
        method: &Method,
        url: &str,
        credential: Option<&Credential>,
        body: Option<Vec<u8>>,
        streaming: bool,
        options: &RequestOptions,
        child: &CancellationToken,
    ) -> Result<reqwest::Response, TransportError> {
        let headers = self.headers(credential, streaming, options)?;

        let mut request = self.http.request(method.clone(), url).headers(headers);

        // A GET carries no body at all; attaching an empty one would advertise
        // `Content-Length: 0` alongside the JSON content type.
        if let Some(bytes) = body {
            request = request.body(bytes);
        }

        tokio::select! {
            // `biased` keeps the abort first, as in the harness precedent
            // (`harness/src/transport.rs:251-261`).
            biased;

            () = child.cancelled() => Err(TransportError::Aborted {
                detail: format!("operation was aborted before a response head from {url}"),
            }),
            outcome = tokio::time::timeout(self.policy.timeout, request.send()) => match outcome {
                Err(_) => Err(TransportError::Timeout {
                    detail: format!(
                        "request to {url} exceeded {}ms",
                        self.policy.timeout.as_millis()
                    ),
                }),
                Ok(Err(error)) => Err(classify_send_error(&error)),
                Ok(Ok(response)) => Ok(response),
            },
        }
    }

    /// The `Authorization` value for the selected credential, applying the target's
    /// [`KeylessHeaderPolicy`] - the single place that decision is made.
    ///
    /// Three cases, in evaluation order:
    ///   * no credential selected (an empty pool) -> no header, whatever the policy says. A target
    ///     with nothing to send must not invent the sentinel;
    ///   * a KEYLESS selection -> the policy decides: [`KeylessHeaderPolicy::SendNone`] emits
    ///     `Bearer none` (the value the sentinel entry carries, falling back to
    ///     [`KEYLESS_CREDENTIAL`] when the marker carries no value at all, as `Credential::keyless()`
    ///     does), [`KeylessHeaderPolicy::Suppress`] emits nothing;
    ///   * a real value -> `Bearer <value>`, the SDK's own shape (`openai/client.js:236-240`).
    ///
    /// The value is read through [`Secret::expose`] only inside this function, so the sentinel name is
    /// the sole credential-shaped string this layer can ever put on the wire.
    #[must_use]
    fn authorization(&self, credential: Option<&Credential>) -> Option<String> {
        let credential = credential?;

        if credential.is_keyless() {
            return match self.target.keyless_header_policy() {
                KeylessHeaderPolicy::SendNone => Some(format!(
                    "Bearer {}",
                    credential.expose().unwrap_or(KEYLESS_CREDENTIAL)
                )),
                KeylessHeaderPolicy::Suppress => None,
            };
        }

        credential.bearer()
    }

    /// The generated headers plus the caller's, merged so a custom header REPLACES the generated one
    /// of the same name (`harness/src/transport.rs:222-247`: reqwest's `.header()` APPENDS, so the
    /// merge runs through a map and each name is written exactly once).
    fn headers(
        &self,
        credential: Option<&Credential>,
        streaming: bool,
        options: &RequestOptions,
    ) -> Result<HeaderMap, TransportError> {
        // `HeaderName` implements `Eq`/`Hash` but NOT `Ord`, so the "write each name once, in a
        // stable order" step runs over a sorted Vec of pairs instead of a BTreeMap.
        let mut headers: Vec<(HeaderName, HeaderValue)> = Vec::new();

        // Decision 5: `Content-Type: application/json` always, and `Accept: text/event-stream` on
        // streaming requests. The map is ordered by header name, which keeps a built request stable
        // (`harness/src/transport.rs:224-228` makes the same argument).
        let generated = [
            (
                "accept",
                if streaming {
                    "text/event-stream"
                } else {
                    "application/json"
                },
            ),
            ("content-type", "application/json"),
        ];

        for (name, value) in generated {
            put_header(
                &mut headers,
                HeaderName::from_bytes(name.as_bytes()).map_err(|error| TransportError::Build {
                    detail: format!("generated header name {name} is invalid: {error}"),
                })?,
                HeaderValue::from_str(value).map_err(|error| TransportError::Build {
                    detail: format!("generated header value is invalid: {error}"),
                })?,
            );
        }

        // The credential half, and the whole reason `keyless` is a distinct state: the target's
        // [`KeylessHeaderPolicy`] decides whether the sentinel sends `Bearer none` (what the deployed
        // SDK sends, `node_modules/openai/client.js:232-241`) or no header at all (what the local
        // relays pin with `defaultHeaders: { Authorization: null }`, `local-strata.ts:161`).
        if let Some(value) = self.authorization(credential) {
            let value = HeaderValue::from_str(&value).map_err(|_| TransportError::Build {
                // The credential VALUE is never printed: the message names the field and the
                // credential's public label only.
                detail: format!(
                    "the credential header is not header-encodable ({})",
                    credential
                        .and_then(Credential::label)
                        .unwrap_or("unnamed credential")
                ),
            })?;

            headers.push((AUTHORIZATION, value));
        }

        for (name, value) in &options.headers {
            put_header(
                &mut headers,
                HeaderName::from_bytes(name.as_bytes()).map_err(|error| TransportError::Build {
                    detail: format!("header name {name} is invalid: {error}"),
                })?,
                HeaderValue::from_str(value).map_err(|error| TransportError::Build {
                    detail: format!("header value for {name} is invalid: {error}"),
                })?,
            );
        }

        // Sorted by canonical header name, so the same logical request always produces the same
        // byte-level request (the determinism property tests/transport.rs asserts on captured
        // headers).
        headers.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));

        Ok(headers.into_iter().collect())
    }
}

/// Writes ONE header value under `name`, replacing a previously generated value of the same name.
///
/// This is the merge rule reqwest's `.header()` does NOT give for free (it APPENDS, so a caller's
/// `accept` would sit next to the generated one and the upstream would see two): every name is
/// written exactly once, caller last. `harness/src/transport.rs:222-247` solves the same problem
/// through a map; the map here is a sorted `Vec` because `HeaderName` has no `Ord`.
fn put_header(headers: &mut Vec<(HeaderName, HeaderValue)>, name: HeaderName, value: HeaderValue) {
    if let Some(slot) = headers.iter_mut().find(|(existing, _)| *existing == name) {
        slot.1 = value;
    } else {
        headers.push((name, value));
    }
}

/// Claims the next slot of one rotation cursor, and reports exhaustion when there is nothing left.
///
/// `try_update` is the atomic step, and the value IT hands back is the PREVIOUS one; the slot this
/// call claimed is therefore `previous + 1`, and that is what this function returns. Re-loading the
/// cursor afterwards - this function's previous shape - is the race that handed two racing callers the
/// same slot: between the winning compare-and-swap and the re-load another thread can advance past it,
/// so both callers collect the LATER value and the entry neither of them claimed is never served. A
/// `fetch_max`-style publish cannot help here, because the credential axis genuinely hands out one
/// entry per rotation (see [`Transport::rotate_credential`]).
///
/// On exhaustion the cursor stays on the LAST entry: `local-strata.ts:244-250` returns false WITHOUT
/// clearing `baseURL`, so the entry that was selected last stays in force.
#[must_use]
fn advance<T>(cursor: &AtomicUsize, pool: &[T]) -> Option<usize>
where
    T: Clone,
{
    let last = pool.len().checked_sub(1)?;

    // `Ok(previous_value)` on a won claim, `Err(_)` when the closure returned `None` (the cursor is on
    // `last`, so there is no next slot). `try_update` re-runs the closure after a lost race, so every
    // caller holding `Ok` claimed a distinct `previous` and therefore a distinct slot - and
    // `previous < last` makes `previous + 1` a valid index.
    let previous = cursor
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            (current < last).then_some(current + 1)
        })
        .ok()?;

    Some(previous + 1)
}

// ---------------------------------------------------------------------------
// Request-shape helpers
// ---------------------------------------------------------------------------

/// Serialises the request body and normalises the `stream` / `stream_options` pair.
///
/// `stream_options` is emitted ONLY when `stream` is true. A non-streaming request that carries it
/// loses the key ENTIRELY - never `"stream_options": null` - because that is what the TS
/// `stream_options: stream ? { include_usage: true } : undefined` produces once `JSON.stringify`
/// drops the undefined key (`nvidia-glm5-flash.ts:79`), and because the OpenAI contract rejects the
/// field on a settled request (`nvidia-glm5-flash.ts:75-77`).
///
/// The caller's [`Value`] is never mutated: the caller owns it, and it is also the document whose
/// serialized length the token estimate is taken from.
///
/// Returns the bytes to send and whether this request is a stream.
///
/// # Errors
///
/// [`TransportError::Build`] if the document cannot be serialized.
///
/// # Examples
/// ```
/// use serde_json::json;
/// use typecrate_provider::transport::prepare_body;
///
/// // A non-streaming body never carries `stream_options`, not even as `null`.
/// let (bytes, streaming) = prepare_body(
///     &json!({ "model": "m", "stream": false, "stream_options": { "include_usage": true } }),
/// )
/// .expect("a document serializes");
/// let document: serde_json::Value = serde_json::from_slice(&bytes).expect("the bytes are JSON");
///
/// assert!(!streaming);
/// assert_eq!(document.get("stream_options"), None);
/// ```
pub fn prepare_body(body: &Value) -> Result<(Vec<u8>, bool), TransportError> {
    let mut document = body.clone();

    let streaming = document
        .as_object()
        .and_then(|map| map.get("stream"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if let Some(map) = document.as_object_mut() {
        // The `!streaming` half covers both spellings of absence, and the explicit-`null` half keeps
        // a streaming body from emitting a null option object either.
        if !streaming || map.get("stream_options") == Some(&Value::Null) {
            map.remove("stream_options");
        }
    }

    serde_json::to_vec(&document)
        .map(|bytes| (bytes, streaming))
        .map_err(|error| TransportError::Build {
            detail: format!("the request body could not be serialized: {error}"),
        })
}

/// Maps a reqwest send failure onto the taxonomy.
///
/// The precedence mirrors `classifyStreamError`: an explicit timeout verdict first (reqwest's
/// `is_timeout` is the direct analogue of `APIConnectionTimeoutError`), then a decode failure, then a
/// builder/body failure, and everything else that prevented a response head is a CONNECTION failure -
/// which is what `APIConnectionError` means in the SDK, and the class a supervisor treats as fatal
/// (`error::StreamOrigin::Transport`).
fn classify_send_error(error: &reqwest::Error) -> TransportError {
    // Redacted and capped on the way in: reqwest's Display names the request URL, and a URL can
    // carry userinfo, so this is the guarantee rather than a precaution.
    let detail = flatten_error_detail(&error.to_string());

    if error.is_timeout() {
        return TransportError::Timeout { detail };
    }

    if error.is_decode() {
        return TransportError::Decode { detail };
    }

    if error.is_builder() || error.is_body() {
        return TransportError::Build { detail };
    }

    // `is_status()` cannot be reached from here: a response head means `Ok`. Everything else is the
    // socket, and the socket is a connection failure.
    TransportError::Connection { detail }
}

/// Renders the message half of a non-2xx answer exactly as `APIError.makeMessage` would
/// (`node_modules/openai/core/error.js:21-38`), then flattens and redacts it.
///
/// The three TypeScript branches, in order: the body's `error.message` string; else
/// `JSON.stringify(error)` when the `error` member exists at all (a plain STRING member is
/// stringified WITH its quotes, which is what JS does); else the raw response text. An empty body
/// yields an empty detail, which [`TransportError::message`] renders as
/// `{status} status code (no body)`.
async fn error_detail(response: reqwest::Response) -> String {
    let text = response.text().await.unwrap_or_default();

    if text.is_empty() {
        return String::new();
    }

    let Ok(document) = serde_json::from_str::<Value>(&text) else {
        // Not JSON at all - a proxy's HTML error page is the common case: the raw text is the
        // message, exactly as `makeMessage` falls back to `options.responseText`.
        return flatten_error_detail(&text);
    };

    let message = match document.get("error") {
        Some(error @ Value::Object(_)) => error
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| error.to_string(), ToOwned::to_owned),
        // A non-null, non-object `error` member: JS stringifies the whole member. `null` and an
        // absent member fall through to the raw text, exactly as
        // `error ? JSON.stringify(error) : message` decides.
        Some(error @ (Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Array(_))) => {
            error.to_string()
        }
        Some(Value::Null) | None => text,
    };

    flatten_error_detail(&message)
}

/// Reads and parses a settled 2xx body under the remaining ceiling.
async fn read_document(
    response: reqwest::Response,
    child: &CancellationToken,
    remaining: Duration,
) -> Result<Value, TransportError> {
    tokio::select! {
        biased;

        () = child.cancelled() => Err(TransportError::Aborted {
            detail: "operation was aborted while reading the response body".to_owned(),
        }),
        outcome = tokio::time::timeout(remaining, response.bytes()) => match outcome {
            Err(_) => Err(TransportError::Timeout {
                detail: "the response body was not delivered inside the request ceiling".to_owned(),
            }),
            Ok(Err(error)) => Err(TransportError::Connection {
                detail: flatten_error_detail(&error.to_string()),
            }),
            Ok(Ok(bytes)) if bytes.is_empty() => Err(TransportError::Decode {
                // TS parity: the SDK's parser fails on an empty body too, so a 200 that carries no
                // document is a decode failure, not an empty answer.
                detail: "response body is empty".to_owned(),
            }),
            Ok(Ok(bytes)) => serde_json::from_slice(&bytes).map_err(|error| TransportError::Decode {
                detail: flatten_error_detail(&error.to_string()),
            }),
        },
    }
}

/// Sleeps the backoff, but a cancellation ends the pause at once: a caller that gave up during a 429
/// pause must not have to wait it out to be heard.
async fn sleep_or_cancel(pause: Duration, child: &CancellationToken) {
    tokio::select! {
        biased;

        () = child.cancelled() => (),
        () = tokio::time::sleep(pause) => (),
    }
}

// ---------------------------------------------------------------------------
// The stream body: SSE in, chunk values out, accounting settled on the way out
// ---------------------------------------------------------------------------

/// The raw upstream byte stream, boxed: `reqwest::Response::bytes_stream().boxed()`.
///
/// The alias exists because the type appears in the struct field and in the constructor, and it is
/// dense enough that clippy's `type_complexity` limit rejects it inline. `reqwest::Error` stays in the
/// item type: the pump maps it into [`StreamFailure`] itself, so nothing upstream of [`StreamBody`]
/// ever names it.
type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

/// The state behind [`UpstreamStream`].
///
/// Three jobs, in one place so they cannot disagree:
///   * pump the SSE body through [`SseDecoder`] and deliver whole chunk VALUES;
///   * keep the accounting facts TS keeps in `trackStream` (`model-class.ts:836-878`): frames
///     delivered, the first generated payload, the last usage block, the entry instant;
///   * settle exactly once - on the terminal transition, or on `Drop`.
struct StreamBody {
    /// The upstream byte stream, `None` once the body has been fully consumed.
    bytes: Option<ByteStream>,
    decoder: SseDecoder,
    /// Chunk values decoded but not yet delivered, so one TCP segment carrying three frames yields
    /// three items across three polls.
    queued: VecDeque<Result<Value, StreamFailure>>,
    /// The end-of-stream marker, queued BEHIND the buffered frames so nothing is dropped when the
    /// sentinel arrives in the same segment as the last data frame.
    end: Option<End>,
    /// The target the error frame names. TS has to thread this onto the thrown object with a Symbol
    /// because its gateway cannot see the upstream (`annotateStreamErrorUpstream`,
    /// `start.ts:536-547`); here it is simply what the transport dialed.
    upstream: String,
    /// The request's child token: cancelled by the caller's signal, and cancelled BY `Drop`.
    cancel: CancellationToken,
    started: Instant,
    chunks: u64,
    first_payload: Option<Instant>,
    usage: Option<u64>,
    usage_reported: bool,
    settlement: Option<SettlementHook>,
    /// The terminal item has been handed out; the stream is fused from here.
    fused: bool,
}

/// Why the stream is ending.
#[derive(Clone, Debug)]
enum End {
    /// The `[DONE]` sentinel arrived, or the body ended without one. Both are a clean end for this
    /// layer - the provider surface emits its own `[DONE]` after either, and
    /// [`StreamSettlement::sentinel`] reports which happened.
    Clean,
    /// A failure item is about to be delivered.
    Failed(StreamFailure),
}

/// What one poll of the upstream byte stream produced, taken OUT of the borrow so the arms below can
/// mutate the rest of the state.
enum Pulled {
    Chunk(Bytes),
    Broken(reqwest::Error),
    Closed,
}

impl StreamBody {
    fn new(
        response: reqwest::Response,
        cancel: CancellationToken,
        upstream: String,
        settlement: Option<SettlementHook>,
    ) -> Self {
        Self {
            bytes: Some(response.bytes_stream().boxed()),
            decoder: SseDecoder::new(),
            queued: VecDeque::new(),
            end: None,
            upstream,
            cancel,
            started: Instant::now(),
            chunks: 0,
            first_payload: None,
            usage: None,
            usage_reported: false,
            settlement,
            fused: false,
        }
    }

    /// Folds one decoded payload into the accounting facts and returns the chunk value, in the order
    /// TS reads them (`model-class.ts:845-857`: usage first, then the payload stamp, then the yield).
    fn absorb(&mut self, payload: &str) -> Result<Value, StreamFailure> {
        let chunk: Value = serde_json::from_str(payload).map_err(|error| {
            // A `data:` line that is not JSON: the SDK re-raises `SyntaxError` for the same body, and
            // the code is `decode`. DEVIATION (deliberate): the ORIGIN is `Transport`, because our
            // decoder is the component that can no longer continue - so a supervisor sees a fatal
            // stream, which is the honest verdict for an undecodable body. The CODE still says
            // `decode`, which is what a consumer branches on.
            StreamFailure::transport(
                &TransportError::Decode {
                    detail: flatten_error_detail(&error.to_string()),
                },
                self.chunks,
                Some(&self.upstream),
            )
        })?;

        // A gateway that attaches a provider error to an otherwise-200 stream
        // (`harness/src/transport.rs:445-455`): the upstream DID reach the model, so this is an
        // UPSTREAM failure and the supervisor must not treat it as fatal.
        if let Some(error) = chunk.get("error").filter(|value| !value.is_null()) {
            return Err(StreamFailure::upstream(
                error,
                self.chunks,
                Some(&self.upstream),
            ));
        }

        // Both usage spellings at both placements: `completion_tokens` for Chat Completions,
        // `output_tokens` for the Responses surface - the latter nested inside a terminal
        // `response.completed` event (`readOutputTokens` + `trackStream:846-853`).
        let reported = chunk.get("usage").and_then(output_tokens_of).or_else(|| {
            chunk
                .get("response")
                .and_then(|response| response.get("usage"))
                .and_then(output_tokens_of)
        });

        if let Some(tokens) = reported {
            self.usage = Some(tokens);
            self.usage_reported = true;
        }

        // The TTFT stamp happens BEFORE delivery, so it measures arrival and not consumer processing
        // (`model-class.ts:816-819`), and it short-circuits once stamped - the O(1)-per-chunk rule the
        // TS comment asks for.
        if self.first_payload.is_none() && has_generated_payload(&chunk) {
            self.first_payload = Some(Instant::now());
        }

        Ok(chunk)
    }

    /// Hands the settlement to the caller - once, because the hook is taken here.
    fn settle(&mut self, outcome: StreamOutcome, failure: Option<StreamFailure>) {
        let Some(hook) = self.settlement.take() else {
            return;
        };

        let now = Instant::now();

        let (time_to_first_token, generation) = self.first_payload.map_or((None, None), |first| {
            (
                Some(first.duration_since(self.started)),
                Some(now.duration_since(first)),
            )
        });

        hook(StreamSettlement {
            outcome,
            chunks: self.chunks,
            elapsed: now.duration_since(self.started),
            time_to_first_token,
            generation,
            output_tokens: self.usage,
            usage_reported: self.usage_reported,
            sentinel: self.decoder.is_finished(),
            failure,
        });
    }

    /// Queues the end-of-stream marker, dropping anything buffered after a failure.
    fn end_with(&mut self, end: End) {
        if matches!(end, End::Failed(_)) {
            // Nothing after an error frame is trustworthy: TS would have thrown out of the iterator
            // at exactly this point.
            self.queued.clear();
        }

        self.end = Some(end);
    }
}

impl Stream for StreamBody {
    type Item = Result<Value, StreamFailure>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if this.fused {
                return Poll::Ready(None);
            }

            // Cancellation outranks every other reading of the stream: an aborted run reports an
            // abort, not a truncation a retry loop would re-send
            // (`harness/src/transport.rs:356-363`). The CHILD token carries the caller's cancellation
            // too, so one check covers both halves.
            if this.cancel.is_cancelled() {
                let failure = StreamFailure::transport(
                    &TransportError::Aborted {
                        detail: "operation was aborted during the stream".to_owned(),
                    },
                    this.chunks,
                    Some(&this.upstream),
                );

                this.fused = true;
                this.settle(StreamOutcome::Cancelled, Some(failure.clone()));

                return Poll::Ready(Some(Err(failure)));
            }

            // Buffered frames first: they were decoded from bytes that already arrived, so they are
            // delivered before any terminal decision.
            if let Some(item) = this.queued.pop_front() {
                // Counted BEFORE the item leaves, exactly as the gateway counts a frame before writing
                // it (`start.ts:846,851`), so a failure reported after N frames names N.
                if item.is_ok() {
                    this.chunks += 1;
                } else {
                    this.fused = true;

                    let failure = item.as_ref().err().cloned();
                    this.settle(StreamOutcome::Failed, failure);
                }

                return Poll::Ready(Some(item));
            }

            // Nothing buffered: honour a queued end-of-stream marker.
            if let Some(end) = this.end.take() {
                this.fused = true;

                return match end {
                    End::Clean => {
                        this.settle(StreamOutcome::Completed, None);

                        Poll::Ready(None)
                    }
                    End::Failed(failure) => {
                        this.settle(StreamOutcome::Failed, Some(failure.clone()));

                        Poll::Ready(Some(Err(failure)))
                    }
                };
            }

            // Nothing buffered and no decision queued: pull more bytes. The result is COPIED OUT of
            // the borrow so the arms below may touch the rest of the state.
            let pulled = match this.bytes.as_mut() {
                None => return Poll::Ready(None),
                Some(stream) => match stream.poll_next_unpin(context) {
                    Poll::Ready(Some(Ok(chunk))) => Pulled::Chunk(chunk),
                    Poll::Ready(Some(Err(error))) => Pulled::Broken(error),
                    Poll::Ready(None) => Pulled::Closed,
                    Poll::Pending => return Poll::Pending,
                },
            };

            match pulled {
                Pulled::Chunk(chunk) => {
                    for event in this.decoder.push(&chunk) {
                        match event {
                            SseEvent::Data(payload) => match this.absorb(&payload) {
                                Ok(value) => this.queued.push_back(Ok(value)),
                                Err(failure) => {
                                    this.end_with(End::Failed(failure));

                                    break;
                                }
                            },
                            // The sentinel is not a chunk: it ends the body, and the CALLER's `[DONE]`
                            // frame is what the browser sees (`sse::stream_body`).
                            SseEvent::Done => {
                                this.end = Some(End::Clean);

                                break;
                            }
                        }
                    }

                    // Loop again: there may now be a frame ready.
                }
                Pulled::Broken(error) => {
                    // The body died mid-stream. `classify_send_error` decides which of
                    // `timeout` / `aborted` / `connection` it was, and those three have to stay
                    // distinguishable because the supervisor's fatal rule reads exactly that split.
                    let failure = StreamFailure::transport(
                        &classify_send_error(&error),
                        this.chunks,
                        Some(&this.upstream),
                    );

                    this.end_with(End::Failed(failure));
                }
                Pulled::Closed => {
                    // The body ended. An unterminated tail is still a frame if it looks like one
                    // (`harness/src/transport.rs:343-354`), and a body that never sent the sentinel is
                    // still a CLEAN end for the provider surface - the TS relay emits its own `[DONE]`
                    // after any exhausted loop (`private-chat-completion.ts:163-186`). A caller that
                    // wants the truncation guard reads [`StreamSettlement::sentinel`].
                    for event in this.decoder.finish() {
                        match event {
                            SseEvent::Data(payload) => match this.absorb(&payload) {
                                Ok(value) => this.queued.push_back(Ok(value)),
                                Err(failure) => {
                                    this.end_with(End::Failed(failure));

                                    break;
                                }
                            },
                            SseEvent::Done => this.end = Some(End::Clean),
                        }
                    }

                    if this.end.is_none() && this.queued.is_empty() {
                        this.end = Some(End::Clean);
                    }
                }
            }
        }
    }
}

impl Drop for StreamBody {
    fn drop(&mut self) {
        // Rust has no `finally`: everything that must settle on ABANDONMENT settles here. The terminal
        // paths already took the hook, so a completed stream never settles twice.
        if !self.fused {
            this_settle_abandoned(self);
        }

        // Stop reading upstream promptly. Dropping the byte stream closes the connection, and
        // cancelling the child token is what makes the stop OBSERVABLE to anything racing on it (and
        // to a caller that kept its own clone of the token).
        self.cancel.cancel();
    }
}

/// `Drop` cannot move out of `&mut self`, so the abandoned settlement is built through a helper that
/// takes the hook out of the field.
fn this_settle_abandoned(body: &mut StreamBody) {
    body.settle(StreamOutcome::Abandoned, None);
}

// ---------------------------------------------------------------------------
// The stream-accounting ports (pure, and the two the settlement depends on)
// ---------------------------------------------------------------------------

/// Whether one chunk carries a model-GENERATED payload - `hasGeneratedPayload`
/// (`model-class.ts:206-241`), ported rule for rule.
///
/// Lifecycle and metadata traffic must not fake a first token, so the inclusion matrix IS the
/// contract:
///   * a native Responses event (it carries a `type` string, which chat chunks never do) counts only
///     for the four delta families, and only with a NON-EMPTY `delta`;
///   * a chat chunk counts on a non-empty `delta.content`, a non-empty `delta.reasoning_content`, or a
///     `delta.tool_calls[]` entry with a non-empty function NAME or non-empty ARGUMENTS (an empty
///     array and a name-less / arguments-less placeholder do not count);
///   * role-only deltas, empty strings, usage-only trailing chunks and finish-metadata chunks do not
///     count, and neither does any `response.created` / `response.completed` style event.
#[must_use]
pub fn has_generated_payload(chunk: &Value) -> bool {
    // The Responses surface is discriminated by its `type` string; a chat chunk never carries one, so
    // the branch is unambiguous (`model-class.ts:209-219`).
    if let Some(kind) = chunk.get("type").and_then(Value::as_str) {
        let generated = matches!(
            kind,
            "response.output_text.delta"
                | "response.reasoning_summary_text.delta"
                | "response.reasoning_text.delta"
                | "response.function_call_arguments.delta"
        );

        return generated
            && chunk
                .get("delta")
                .and_then(Value::as_str)
                .is_some_and(|delta| !delta.is_empty());
    }

    let Some(choices) = chunk.get("choices").and_then(Value::as_array) else {
        return false;
    };

    choices.iter().any(|choice| {
        // `if (!delta) continue`: in JSON that is a null or absent delta. An object delta proceeds,
        // and so does a non-object non-null one whose every field reads as missing - the same verdict
        // either way, so the null gate is the whole rule.
        let Some(delta) = choice.get("delta").filter(|value| !value.is_null()) else {
            return false;
        };

        let non_empty = |field: &str| {
            delta
                .get(field)
                .and_then(Value::as_str)
                .is_some_and(|text| !text.is_empty())
        };

        if non_empty("content") || non_empty("reasoning_content") {
            return true;
        }

        delta
            .get("tool_calls")
            .and_then(Value::as_array)
            .is_some_and(|calls| {
                calls.iter().any(|call| {
                    let function = call.get("function");

                    function
                        .and_then(|function| function.get("name"))
                        .and_then(Value::as_str)
                        .is_some_and(|name| !name.is_empty())
                        || function
                            .and_then(|function| function.get("arguments"))
                            .and_then(Value::as_str)
                            .is_some_and(|arguments| !arguments.is_empty())
                })
            })
    })
}

/// Reads the output-token figure out of one usage block - `readOutputTokens`
/// (`model-class.ts:128-132`).
///
/// The two spellings are the two upstream surfaces: `completion_tokens` for Chat Completions (and the
/// Lightning adapter's trailing chunk) and `output_tokens` for the Responses API. A missing or
/// non-numeric value is `None`, so the caller leaves the running total untouched instead of adding a
/// corrupted figure.
///
/// DEVIATION (deliberate): TS returns `number`, so a fractional figure survives; this returns `u64`
/// and truncates toward zero, because every counter the stats projection feeds is an integer, and
/// relays report integers in practice.
#[must_use]
pub fn output_tokens_of(usage: &Value) -> Option<u64> {
    let tokens = usage
        .get("completion_tokens")
        .or_else(|| usage.get("output_tokens"))?;

    tokens
        .as_u64()
        .or_else(|| tokens.as_f64().map(|value| value.trunc() as u64))
}

/// Estimates a request's tokens without a tokenizer - `estimateRequestTokens`
/// (`model-class.ts:82-85`): `ceil(serialized.length / 4)`.
///
/// DEVIATION (deliberate, and one the brief's `ceil(len/4)` shorthand hides): JS `.length` counts
/// UTF-16 CODE UNITS, while Rust's `len()` counts BYTES, so a body with non-ASCII text would estimate
/// differently. Counting code units is what keeps the figure equal to the deployed estimator: a BMP
/// character is one unit, an astral character is two (a surrogate pair), exactly as in JS.
#[must_use]
pub fn estimate_request_tokens(request: &Value) -> u64 {
    let units: u64 = request
        .to_string()
        .chars()
        .map(|character| if u32::from(character) > 0xFFFF { 2 } else { 1 })
        .sum();

    units.div_ceil(4)
}

// ---------------------------------------------------------------------------
// Unit tests for the two atomic primitives
// ---------------------------------------------------------------------------
//
// These live INSIDE the module because `advance` is private, and because both tests are about the
// arithmetic of one cursor: no HTTP, no listener, no timing assumptions beyond "these threads meet at
// a barrier". The behaviour-level counterpart of each lives in `tests/transport.rs` and
// `tests/clients.rs`, where the same invariants are asserted through real dials.

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    /// `advance` hands every racer the slot IT claimed: the returned indices are distinct, they cover
    /// `1..=last` completely, the losers all answer `None`, and the cursor lands on `last`.
    ///
    /// This is the exactness half of the concurrency fix. The previous implementation returned
    /// `cursor.load()` AFTER a won claim, so two racers could both report the later slot: one index
    /// then appeared twice in `slots`, one index never appeared at all, and the pool entry behind the
    /// missing index was never served to anyone. The assertion below is that exact multiset, so the
    /// regression fails on a duplicate or a gap rather than on a fuzzy property.
    #[test]
    fn advance_hands_each_racer_the_slot_it_claimed() {
        // The pool entries ARE their own indices, so a returned slot index and the entry it names are
        // the same number and the test can state the answer exactly.
        let pool: Vec<usize> = (0..16).collect();
        let last = pool.len() - 1;
        // A SHARED BORROW of the pool, not a move: `scope.spawn` closures live for the scope, so each
        // racer can take the same `&[usize]` (which is `Copy`) instead of consuming the `Vec`.
        let entries: &[usize] = &pool;
        let cursor = Arc::new(AtomicUsize::new(0));
        // More racers than slots: 24 threads race for `last` = 15 claims, so 9 of them must be the
        // exhaustion answers, and every one of them must still be a DISTINCT claim.
        let racers = 24_usize;
        let barrier = Arc::new(Barrier::new(racers));

        let claims: Vec<Option<usize>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..racers)
                .map(|_| {
                    let cursor = Arc::clone(&cursor);
                    let barrier = Arc::clone(&barrier);

                    scope.spawn(move || {
                        // Everyone waits here, then walks into the same compare-and-swap together.
                        barrier.wait();

                        advance(&cursor, entries)
                    })
                })
                .collect();

            // Every thread is SPAWNED before the first one is joined: joining inside the map above
            // would park the scope thread in front of a barrier the unspawned racers have not
            // reached yet.
            handles
                .into_iter()
                .map(|handle| handle.join().expect("the racing thread did not panic"))
                .collect()
        });

        // The multiset of claimed slots, and the count of exhaustion answers.
        let mut slots: Vec<usize> = claims.iter().copied().flatten().collect();
        let exhausted = claims.iter().filter(|claim| claim.is_none()).count();

        slots.sort_unstable();
        slots.dedup();

        assert_eq!(
            slots,
            (1..=last).collect::<Vec<usize>>(),
            "every slot 1..={last} is claimed by exactly one racer - no duplicate, no gap"
        );
        assert_eq!(
            exhausted,
            racers - last,
            "the racers beyond the last slot are the exhaustion answers"
        );
        assert_eq!(
            cursor.load(Ordering::SeqCst),
            last,
            "the cursor ends on the last entry, the exhaustion fallback the TS original documents"
        );
    }

    /// `verify_endpoint` is a PUBLISH, not a claim: N racers that all verified the same entry agree on
    /// one head (`entry + 1`), where the old endpoint cursor moved N times and N-1 of those steps
    /// skipped an entry nobody had dialled.
    #[test]
    fn concurrent_verifications_of_one_entry_publish_one_head() {
        let pool: Vec<Endpoint> = (0..8)
            .map(|index| Endpoint::labeled(&format!("https://{index}.example/v1"), "relay"))
            .collect();
        let transport = Transport::new(Target::new(pool, Vec::new()));
        let barrier = Arc::new(Barrier::new(16));

        let heads: Vec<usize> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16)
                .map(|_| {
                    let transport = transport.clone();
                    let barrier = Arc::clone(&barrier);

                    scope.spawn(move || {
                        barrier.wait();

                        // All sixteen requests dial index 3 and get the 404 verdict from it.
                        transport.verify_endpoint(3)
                    })
                })
                .collect();

            // Spawned before joined, for the barrier's sake (see the test above).
            handles
                .into_iter()
                .map(|handle| handle.join().expect("the racing thread did not panic"))
                .collect()
        });

        assert!(
            heads.iter().all(|head| *head == 4),
            "sixteen concurrent 404s at index 3 publish head 4 exactly, saw {heads:?}"
        );
        assert_eq!(
            transport.endpoint_head(),
            4,
            "and the shared head is that same value"
        );
        assert_eq!(
            transport.endpoint_start(),
            Some(4),
            "so the next request starts at the first entry NOBODY has verified"
        );
    }
}
