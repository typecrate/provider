//! The declarative model client: one row per served model, one shared singleton per row.
//!
//! # Typecrate port - source lineage and deviations
//!
//! TypeScript origin: `packages/agentic/provider/src/model-class.ts`, plus the
//! per-provider differences the subclasses encode (`makora/makora-glm-5-3.ts`,
//! `nvidia/nvidia-glm5-flash.ts`, `local/local-strata.ts`,
//! `local/local-basalt.ts`, `merge/merge-*.ts`, `sail/*`, `vultr/*`,
//! `token-router/*`, `open-router/*`).
//!
//! ## WHY A ROW INSTEAD OF A CLASS
//!
//! The TypeScript expresses the served model set as thirty subclasses, each
//! overriding a handful of fields (`model`, `id`, `provider`, `enabled`,
//! `inputCost`...) and, when the provider needs it, `create()` and
//! `switchBaseUrl()`. That works, but it scatters the actual variation over
//! thirty files, and the interesting differences - "inject `vendor: 'pareto'`",
//! "default `reasoning_effort` to `max`", "rewrite `max_tokens` to 32768",
//! "rotate credentials, returning the key that just failed" - are hidden inside
//! method bodies that only differ in one line.
//!
//! This port collapses each axis of variation into a variant of [`Behavior`] or a
//! field of [`ClientSpec`], so a model is DATA: one [`ClientSpec`] row, dispatched
//! statically at request time. The consequences are deliberate:
//!   * adding a model is one row in `clients.rs`, not a new type;
//!   * the whole served set is diffable in one file, which is what makes the
//!     ordering guarantees of `crate::registry` testable;
//!   * no `dyn Trait` and no vtable: [`Behavior`] is a closed enum, so the request
//!     path is a `match` the compiler can prove exhaustive.
//!
//! DEVIATION (structural, and the point of the task): subclasses become rows. The
//! behaviour each subclass encodes is reproduced exactly; only its representation
//! changes.
//!
//! ## What `create()` does, and in which order
//!
//! Mirrors `accountUpstreamCall` (`model-class.ts:700-734`) wrapped around the
//! subclass `create()` body, in the TypeScript's own order:
//!   1. the prompt side of the accounting settles FIRST, over the CALLER's body
//!      (`recordRequestTokens(body)` at `:707` runs before `withModel` rewrites
//!      the payload), then `requestTotal += 1` (`:708`);
//!   2. the payload is rewritten (`withModel` + the provider's own `create()`
//!      rewrites - see [`ModelClient::rewrite`]);
//!   3. the request is issued, and the 404 failover loop that the TypeScript hides
//!      inside its `fetch` wrapper (`model-class.ts:588-619`, the loop body at
//!      `:593-618`, the default handler at `:634-635`) runs here;
//!   4. one clock read settles the elapsed span, the output usage, and (for a
//!      stream) the phase split, in [`crate::accounting`].
//!
//! Step 1's asymmetry is load-bearing and easy to lose: the token ESTIMATE is
//! computed over the caller's body while the bytes SENT are the rewritten payload.
//! `recordRequestTokens` is handed `body`, not the pinned body
//! (`model-class.ts:707` vs `:733`), so the estimate the dashboard divides by is
//! the pre-rewrite figure. Both bodies are kept here for exactly that reason.
//!
//! ## The 404 failover loop lives here, not in the transport
//!
//! In the TypeScript, `switchBaseUrl()` is invoked by a `404` status handler
//! installed inside a `while (true)` loop around `fetch`
//! (`model-class.ts:661-663` and `:591-614`). Two consequences follow, and both
//! are reproduced:
//!   * the rotation loop is NOT bounded by the retry budget (`maxRetries: 3`
//!     governs the SDK's own retries, which the client disables with
//!     `maxRetries: 0` at `:636-643`); it runs until `switchBaseUrl()` returns
//!     false. [`crate::transport`] keeps its budget for 429 and transient
//!     failures only, and returns a 404 as a settled failure, which is what lets
//!     this layer own the rotation;
//!   * the accounting wrapper sits OUTSIDE that loop, so a request that rotates
//!     five times still counts ONE request and settles its prompt split once
//!     (`model-class.ts:707-708` runs once per `create()`).
//!
//! On exhaustion the TypeScript route turns the still-404 response into
//! `Service unavailable: all base URLs are exhausted (404 Not Found).` - the
//! message the transport already owns
//! ([`crate::transport::TransportFailure::all_base_urls_exhausted`],
//! [`crate::transport::EXHAUSTED_BASE_URLS_MESSAGE`]). This layer returns the raw
//! 404 [`crate::transport::TransportFailure`] and lets the route decide, which is
//! the layer boundary the transport was built around.
//!
//! ## Two credential-drain shapes, not one
//!
//! The two credential-rotating families rotate DIFFERENTLY, and the difference is
//! observable in the number of wasted retries:
//!   * `nvidia/nvidia-glm5-flash.ts:13-18` shifts the pool head away and returns
//!     the NEW head, so every rotation moves to a fresh key;
//!   * `makora/makora-glm-5-3.ts:6-11` shifts the head off and returns THE REMOVED
//!     entry, so the first rotation re-sends on the key that just failed, and only
//!     later rotations advance. With Makora's single live key that is exactly one
//!     wasted retry before the pool drains.
//!
//! Both are modelled, as [`CredentialAdvance`], because collapsing them would
//! change which credential a request is served with - the property A4 pins.
//!
//! ## Credential pools are per client, not per family
//!
//! DEVIATION (deliberate, recorded): the TypeScript keeps its rotation queue at
//! MODULE scope, so it is shared by every sibling of a family
//! (`nvidia/nvidia-glm5-flash.ts:9`, `makora/makora-glm-5-3.ts:5`, and the note in
//! `registry.ts` above the client array). Four Nvidia clients therefore share one
//! six-key queue: a key drained by one sibling is gone for the others too.
//!
//! Here the pool belongs to the client ([`crate::transport::Transport`]'s cursor),
//! so every row rotates its own copy. Consequences, stated plainly:
//!   * each pool's ORDER is identical to the TypeScript's (vault document order,
//!     never re-sorted) - which is what A4 asserts;
//!   * a drain in one row no longer drains its siblings; each row drains after its
//!     own `len` rotations instead of sharing one budget.
//!
//! Per-row pools were chosen over a family-shared `Arc` because the transport owns
//! the cursor and takes its `Target` by value: sharing would mean re-shaping the
//! transport for one parity detail, and no live row has more than six credentials
//! to begin with.
//!
//! ## Discoverability flips when a pool drains
//!
//! `isDiscoverable` is a GETTER in the TypeScript (`model-class.ts:388-390`:
//! `enabled && hasValidApiKey`), read fresh by the catalog route on every request,
//! "so a credential that changes on the instance (e.g. a key pool draining to an
//! empty string) is reflected immediately" (`:375-377`). The drain is what sets
//! `apiKey = ''` (`makora-glm-5-3.ts:86`, `nvidia-glm5-flash.ts:83`), and `''` is
//! not a valid key. [`ModelClient::is_discoverable`] reproduces that: it is
//! computed, never cached, and a drained client is hidden while staying routable.
//!
//! What "staying routable" means ON THE WIRE is where this port deviates, and the
//! difference is measurable: the TypeScript assigns `apiKey = ''`, which the OpenAI
//! SDK treats as falsy, so a drained request goes out with NO `Authorization` header
//! at all. Here the credential pool is a cursor over an immutable pool, and
//! [`crate::transport`] documents its exhaustion fallback - the cursor stays on the
//! LAST entry, because `local-strata.ts:244-250` returns false without clearing
//! `baseURL` on the endpoint axis. A drained row therefore keeps sending its last
//! credential. Hidden either way (the flag is what A5 asserts), and routable either
//! way; only the header differs, and re-sending a key that worked beats an
//! unauthenticated request on providers that reject anonymous traffic. The `clients`
//! suite pins the header a drained row actually sends.
//!
//! ## Credential values never leave through this layer's diagnostics
//!
//! Nothing here prints a credential. Rotation logs name the client id and the
//! vault LABEL ([`ModelClient::switch_base_url`]), and the credential type the
//! transport hands back redacts itself (`crate::vault::Secret`). That is the
//! TypeScript's own posture, which prints relay names for the same reason
//! (`local-strata.ts:246-249`: "`endpoint` is an `Object.keys()` entry (a name),
//! never a URL and never a key value").
//!
//! ## Not ported at this layer: a per-call base-URL override
//!
//! There is nothing to port: the TypeScript has no per-call base-URL option either. `create` takes
//! ONE parameter (`model-class.ts:922-924`), and every subclass override matches that arity; the base
//! URL lives on the client (`this.baseURL`, captured at `:604` and rebased onto a rotated endpoint at
//! `:617` via `rebaseRequestInput` `:141-164`) and moves only through `switchBaseUrl()` (`:933-935`).
//! An earlier cut of this doc justified the omission with an `options.baseUrl` at `model-class.ts:573`
//! — that line is the 429 handler's `console.log`, and the file has no `baseUrl` identifier at all
//! (the field is `baseURL`), so the conclusion stands on the arity evidence above and the stale cite
//! is recorded here rather than repeated. Nothing in this package passes such an option, so the Rust
//! entry point is [`ModelClient::create`] with no override parameter. If a route task ever needs
//! one, the knob belongs on the call, not on the row: rebuild the transport's
//! `Target` with [`crate::transport::Endpoint::new`] for that request only, and
//! accept that a fresh transport restarts the endpoint cursor at the pool head (the
//! TypeScript's rotated client likewise holds no per-request pool position).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::accounting::{Accounting, Counters, StreamRecord, Totals};
use crate::error::TransportError;
use crate::pricing::Pricing;
use crate::transport::{
    self, CHAT_COMPLETIONS_PATH, KeylessHeaderPolicy, RequestOptions, ResponseOrStream,
    StreamOptions, StreamSettlement, Transport, TransportFailure,
};

/// The gateway routing target a Merge-family row asks for.
///
/// `merge/merge-*.ts` injects a `vendor` field into EVERY request body, after the
/// spread, so a caller-supplied value cannot bypass the gateway's routing contract
/// (`merge-glm-5-3-flash.ts:87`, `merge-glm-5-3.ts:84`, `merge-kimi-k3.ts:69`,
/// `merge-kimi-k2-6.ts:57`, `merge-mimo-2-6.ts:96`, `merge-ds4-flash.ts:103`,
/// `merge-gpt-6-1-sol.ts:101`, `merge-gpt-6-luna.ts:99`). Those eight call sites use **five**
/// distinct names — `deepinfra` (three files), `openai` (two), `telnyx`, `pareto` and `modal` (one
/// each) — and that set is the complete vocabulary those files emit, which is why the enum below has
/// exactly five variants. A sixth vendor would be a new row variant, not a string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutingVendor {
    /// `vendor: 'telnyx'`, the full-price GLM-5.3 path (`merge-glm-5-3.ts:84`).
    Telnyx,
    /// `vendor: 'pareto'`, the cheap Flash path (`merge-glm-5-3-flash.ts:87`).
    Pareto,
    /// `vendor: 'modal'`, the private-cloud path (`merge-kimi-k3.ts:69`).
    Modal,
    /// `vendor: 'deepinfra'`, the open-weight path (`merge-mimo-2-6.ts:96`).
    DeepInfra,
    /// `vendor: 'openai'`, the first-party path (`merge-gpt-6-1-sol.ts:101`).
    OpenAI,
}

impl RoutingVendor {
    /// The exact wire spelling the TypeScript writes.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Telnyx => "telnyx",
            Self::Pareto => "pareto",
            Self::Modal => "modal",
            Self::DeepInfra => "deepinfra",
            Self::OpenAI => "openai",
        }
    }
}

/// How a credential pool advances when a request rotates.
///
/// See the module section "Two credential-drain shapes, not one".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialAdvance {
    /// Shift the failed head away and serve the NEW head - Nvidia's `getNewKey`
    /// (`nvidia-glm5-flash.ts:13-18`: `keyList.shift()` then `keyList[0]`).
    NextOnEveryRotation,
    /// Serve the entry that was just shifted OFF - Makora's `getNewKey`
    /// (`makora-glm-5-3.ts:6-11`: `const key = keyList.shift(); return KEYS[key]`).
    /// The first rotation therefore re-sends on the credential that just failed;
    /// the pool still shrinks by one either way, so the drain is not postponed.
    SameThenNext,
}

/// The request shape one row speaks, and how it fails over.
///
/// Closed set: `crate::clients` covers every registered TypeScript client with
/// these five variants, which is the evidence that the abstraction is complete.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Behavior {
    /// OpenAI-compatible Chat Completions against one fixed upstream, with a real
    /// credential and NO failover (`switchBaseUrl` returns false - every
    /// `vultr/*.ts`, `sail/*.ts`, `token-router/*.ts`, and
    /// `open-router-glm-5-3-flash.ts`).
    ChatPlain,
    /// [`Self::ChatPlain`] plus the gateway's mandatory `vendor` field (the whole
    /// `merge/*` family).
    ChatRouted {
        /// The vendor the gateway routes this model to.
        vendor: RoutingVendor,
    },
    /// Chat Completions against a pool of CREDENTIAL-LESS relays, rotating the BASE
    /// URL on a 404 and naming each relay (`local/strata`, `local/basalt` -
    /// `local-strata.ts:242-261`, `local-basalt.ts:231-251`). These are the only
    /// rows that publish an `upstreamLabel`, because they are the only rows whose
    /// endpoints are individually named in the TypeScript.
    ChatKeylessRelay,
    /// Chat Completions against one fixed upstream whose CREDENTIAL pool rotates on
    /// a 404 (`makora/*` with [`CredentialAdvance::SameThenNext`], `nvidia/*` with
    /// [`CredentialAdvance::NextOnEveryRotation`]).
    ChatCredentialDrain {
        /// Which drain shape the family uses.
        advance: CredentialAdvance,
    },
    /// The `StandardClient` base `create()` verbatim: the body is model-clamped and
    /// forwarded to the native Responses API untouched (`model-class.ts:922-924`).
    ///
    /// The TypeScript registers no row that uses it today - every registered
    /// client overrides `create()` - but the base class defines it, and
    /// `createResponses` (`model-class.ts:747-749`) is the Responses-surface entry
    /// point a route uses for clients that pin nothing. Keeping it as a variant is
    /// what makes the row table able to express "no chat rewrite at all", which is
    /// also the only way to observe `requires_usage_option: false` end to end.
    ResponsesPassthrough,
}

impl Behavior {
    /// The path the request is posted to: `chat/completions` for the four chat
    /// shapes, `responses` for the passthrough (`model-class.ts:922-924`).
    #[must_use]
    pub const fn path(&self) -> &'static str {
        match self {
            Self::ResponsesPassthrough => transport::RESPONSES_PATH,
            Self::ChatPlain
            | Self::ChatRouted { .. }
            | Self::ChatKeylessRelay
            | Self::ChatCredentialDrain { .. } => CHAT_COMPLETIONS_PATH,
        }
    }

    /// Whether this shape rotates its BASE URL across a named pool, and therefore
    /// whether an `upstreamLabel` exists at all.
    #[must_use]
    pub const fn publishes_relay_label(&self) -> bool {
        matches!(self, Self::ChatKeylessRelay)
    }

    /// Whether exhausting this shape's pool must flip the row to undiscoverable
    /// (`model-class.ts:388-390` reading `hasValidApiKey` fresh).
    #[must_use]
    pub const fn drains_credentials(&self) -> bool {
        matches!(self, Self::ChatCredentialDrain { .. })
    }

    /// Whether a 404 can be answered by rotating an ENDPOINT rather than a
    /// credential - the two failover axes the TypeScript splits by override.
    #[must_use]
    pub const fn rotates_endpoints(&self) -> bool {
        matches!(self, Self::ChatKeylessRelay)
    }
}

/// One served model, declared.
///
/// Every field is one the TypeScript expresses as a subclass override, and the
/// mapping is one-to-one (`model-class.ts` field declarations plus the per-family
/// `create()` bodies). Strings are `&'static str` because a row is a compile-time
/// constant, which is also what keeps the registry's ordering guarantee cheap: no
/// allocation per row, nothing to re-sort.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClientSpec {
    /// The routing and catalog id, `${provider}/${base_model}` (`readonly id`).
    pub id: &'static str,
    /// The display provider name the stats projection and log line print
    /// (`readonly provider`).
    pub provider: &'static str,
    /// The upstream wire model, clamped into every request
    /// (`readonly model` + `withModel`, `model-class.ts:899-901`).
    pub model: &'static str,
    /// The vault VENDOR this row reads its pools from - e.g. `"nvidia"`,
    /// `"strata"`. Distinct from [`Self::provider`], which is a display name.
    pub vault_vendor: &'static str,
    /// The credential slot inside that vendor, or `None` for a row whose upstream
    /// needs no credential at all (the two local relays).
    pub credential_slot: Option<&'static str>,
    /// The endpoint slot inside that vendor. Every row has one; a row that does not
    /// rotate uses only the slot's default value.
    pub endpoint_slot: &'static str,
    /// Per-million rates, in dollars (`readonly inputCost` / `cacheCost` /
    /// `outputCost`).
    pub pricing: Pricing,
    /// The catalog opt-out (`readonly enabled`, `model-class.ts:368`).
    pub enabled: bool,
    /// Whether a streaming request gets `stream_options: { include_usage: true }`.
    ///
    /// True for every chat-native row that reports usage on a trailing chunk
    /// (`nvidia-glm5-flash.ts:79`, and the identical rule in every other chat
    /// family). False for the Responses passthrough, whose native surface carries
    /// usage inside the terminal event instead. Absent - never `null` - when the
    /// row does not need it; [`transport::prepare_body`] removes a null spelling.
    pub requires_usage_option: bool,
    /// A `max_tokens` the row OVERWRITES on every request.
    ///
    /// `local-strata.ts:216` and `local-basalt.ts:204` rewrite it to
    /// `MAX_CONTEXT_LENGTH / 8` = 32768 because opencode's explicit cap would
    /// otherwise pre-empt the engine's output budget. `None` leaves the caller's
    /// value alone.
    pub forced_max_tokens: Option<u64>,
    /// The `reasoning_effort` a row substitutes when the caller gave none
    /// (`const effort = isInvalid(...) || === '' ? '<default>' : ...` in every
    /// reasoning family). `None` means the row never injects the field.
    pub default_reasoning_effort: Option<&'static str>,
    /// Whether an explicit `'none'` counts as "not provided" and gets the default.
    ///
    /// Only the three GPT-family rows do this (`merge-gpt-6-1-sol.ts:92`,
    /// `merge-gpt-6-luna.ts:90`, `token-router-sol.ts:74`); every other family
    /// forwards an explicit `'none'` untouched, because for those providers it is a
    /// real instruction.
    pub coerce_none_effort: bool,
    /// How the `Authorization` header is built when the selection is keyless.
    /// See [`KeylessHeaderPolicy`]: the two local relays pin
    /// `defaultHeaders: { Authorization: null }` (`local-strata.ts:161`,
    /// `local-basalt.ts:147`) and so send NO header, while every other keyless
    /// selection sends `Bearer none` the way `openai@6.42.0` does.
    pub keyless_header: KeylessHeaderPolicy,
    /// The request shape and failover axis.
    pub behavior: Behavior,
    /// The context length the catalog advertises (`262144` for every live row, the
    /// constant `local-strata.ts:49` and `local-basalt.ts:49` derive
    /// `max_tokens` from).
    pub context_length: u32,
}

impl ClientSpec {
    /// A row that speaks Chat Completions against one fixed upstream, the shape
    /// most rows use. Named constructor so a row in `crate::clients` reads as
    /// intent rather than as a struct literal with fourteen fields.
    #[must_use]
    pub const fn plain(
        id: &'static str,
        provider: &'static str,
        model: &'static str,
        vault_vendor: &'static str,
        credential_slot: &'static str,
        endpoint_slot: &'static str,
    ) -> Self {
        Self {
            id,
            provider,
            model,
            vault_vendor,
            credential_slot: Some(credential_slot),
            endpoint_slot,
            pricing: Pricing::free(),
            enabled: true,
            requires_usage_option: true,
            forced_max_tokens: None,
            default_reasoning_effort: None,
            coerce_none_effort: false,
            keyless_header: KeylessHeaderPolicy::SendNone,
            behavior: Behavior::ChatPlain,
            context_length: DEFAULT_CONTEXT_LENGTH,
        }
    }

    /// A credential-LESS relay row: no credential slot exists for these upstreams, and
    /// no `Authorization` header is sent at all.
    ///
    /// This is a constructor rather than a modifier on [`Self::plain`] because
    /// [`Self::credential_slot`] is `Option`: a `plain` row has to pass a slot name it
    /// then throws away, and a placeholder argument invites a reader to think the slot
    /// is used.
    #[must_use]
    pub const fn relay(
        id: &'static str,
        provider: &'static str,
        model: &'static str,
        vault_vendor: &'static str,
        endpoint_slot: &'static str,
    ) -> Self {
        Self {
            credential_slot: None,
            endpoint_slot,
            keyless_header: KeylessHeaderPolicy::Suppress,
            behavior: Behavior::ChatKeylessRelay,
            ..Self::plain(id, provider, model, vault_vendor, "", endpoint_slot)
        }
    }
}

/// The context length every live row advertises, and the value the TypeScript's
/// catalog route hard-codes (`runtime/endpoint/provider/private/models/
/// service-route.ts`, `context_length: 262_144`) - the same number
/// `local-strata.ts:49` divides by eight to get its output limit.
pub const DEFAULT_CONTEXT_LENGTH: u32 = 262_144;

/// One served model: the shared, process-wide client its rows describe.
///
/// `Clone` is identity-preserving: a clone points at the SAME rotation cursors,
/// counters, and drain flag, because the inside is one `Arc`. That is the property
/// the TypeScript's process-wide client array exists for (`registry.ts`: the
/// rotation lists "are already module-level provider definitions, so per-request
/// instance freshness never isolates failover state - one shared array keeps every
/// consumer aligned with those rotations").
#[derive(Clone, Debug)]
pub struct ModelClient {
    inner: Arc<Inner>,
}

/// The mutable core: rotation state plus the accounting ledger.
#[derive(Debug)]
struct Inner {
    spec: ClientSpec,
    transport: Transport,
    accounting: Accounting,
    /// Set once the credential pool has been drained past its last entry, which is
    /// the Rust spelling of the TypeScript's `this.apiKey = ''`
    /// (`makora-glm-5-3.ts:86`, `nvidia-glm5-flash.ts:83`). Sticky: the TypeScript's
    /// module-level queue never refills either, so a drained row stays hidden for
    /// the rest of the process.
    drained: AtomicBool,
    /// Rotations this row has performed, which is what makes
    /// [`CredentialAdvance::SameThenNext`] reproducible: Makora's first rotation
    /// re-sends on the credential the cursor already holds, and only the second
    /// advances it.
    rotations: AtomicUsize,
}

impl ModelClient {
    /// The row plus the transport its pools were built into.
    ///
    /// `crate::registry` is what builds the transport from a vault document; this
    /// constructor is also the seam a suite uses to point one row at an in-process
    /// listener without a vault at all (`tests/clients.rs`).
    #[must_use]
    pub fn new(spec: ClientSpec, transport: Transport) -> Self {
        Self {
            inner: Arc::new(Inner {
                spec,
                transport,
                accounting: Accounting::new(),
                drained: AtomicBool::new(false),
                rotations: AtomicUsize::new(0),
            }),
        }
    }

    // -- identity -----------------------------------------------------------

    /// The routing and catalog id.
    #[must_use]
    pub fn id(&self) -> &'static str {
        self.inner.spec.id
    }

    /// The display provider name.
    #[must_use]
    pub fn provider(&self) -> &'static str {
        self.inner.spec.provider
    }

    /// The upstream wire model.
    #[must_use]
    pub fn model(&self) -> &'static str {
        self.inner.spec.model
    }

    /// The declared row.
    #[must_use]
    pub fn spec(&self) -> &ClientSpec {
        &self.inner.spec
    }

    /// The pinned rates.
    #[must_use]
    pub fn pricing(&self) -> &Pricing {
        &self.inner.spec.pricing
    }

    /// The catalog opt-out flag. NOT the same as discoverability, which also needs
    /// a usable credential (`model-class.ts:388-390`).
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.inner.spec.enabled
    }

    /// The path requests are posted to.
    #[must_use]
    pub fn path(&self) -> &'static str {
        self.inner.spec.behavior.path()
    }

    // -- discoverability ----------------------------------------------------

    /// Whether the catalog may list this model.
    ///
    /// `enabled && credential_ok`, computed on every call and never cached
    /// (`model-class.ts:378-390`). Three credential states:
    ///   * a row with no credential slot at all (the local relays) is KEYLESS, and a
    ///     keyless upstream counts as valid - the TypeScript's `apiKey` is `'none'`
    ///     there, which `isValidApiKey` accepts (`model-class.test.ts:224,301,308`);
    ///   * a row with a pool is valid while the CURRENTLY selected entry is usable -
    ///     which is how `open-router` is hidden today: its one entry classifies
    ///     `invalid`, so `hasValidApiKey` is false while `enabled` stays true
    ///     (`model-class.test.ts:288-296`);
    ///   * a row whose pool has drained is invalid whatever the current entry is,
    ///     because the TypeScript has by then set `apiKey = ''`
    ///     (`nvidia-glm5-flash.ts:83`), and `''` fails the same predicate.
    #[must_use]
    pub fn is_discoverable(&self) -> bool {
        if !self.inner.spec.enabled || self.inner.drained.load(Ordering::SeqCst) {
            return false;
        }

        self.credential_is_valid()
    }

    /// Whether this row can currently call its provider with a usable credential.
    fn credential_is_valid(&self) -> bool {
        // A row with no credential slot is a deliberate no-credential upstream, not
        // a broken one; see the `is_discoverable` doc.
        if self.inner.spec.credential_slot.is_none() {
            return true;
        }

        self.inner
            .transport
            .current_credential()
            .is_some_and(|credential| credential.is_usable())
    }

    // -- relay attribution --------------------------------------------------

    /// The relay NAME serving this client right now, for the log line
    /// (`private-chat-completion.ts:106-122` captures `client.upstreamLabel` at the
    /// response head for exactly that reason).
    ///
    /// `None` for every row whose endpoints are not individually named upstream:
    /// the TypeScript sets `upstreamLabel` only in the two local relays, so the
    /// other rows print `model: <id>` rather than `STANDARD: <id>`. That is the
    /// behaviour being preserved - the vault's non-local endpoint slots all carry
    /// one entry labelled `STANDARD`/`Standard`/`Default`, and publishing those
    /// labels would rename every log line in the deployment.
    #[must_use]
    pub fn upstream_label(&self) -> Option<String> {
        if !self.inner.spec.behavior.publishes_relay_label() {
            return None;
        }

        self.inner.transport.upstream_label()
    }

    /// The endpoint URL in force, which is what the TypeScript reports as
    /// `client.baseURL` (`private-chat-completion.ts:124-130`).
    #[must_use]
    pub fn base_url(&self) -> Option<String> {
        self.inner
            .transport
            .current_endpoint()
            .map(|endpoint| endpoint.base_url().to_owned())
    }

    /// The credential's vault LABEL in force, `None` when the pool is empty or the
    /// entry carries no label. Labels are safe to print; values never are.
    #[must_use]
    pub fn credential_label(&self) -> Option<String> {
        self.inner
            .transport
            .current_credential()
            .and_then(|credential| credential.label().map(str::to_owned))
    }

    // -- accounting ---------------------------------------------------------

    /// The lifetime counters, the figures `/stats` projects
    /// (`private-stats.ts:197-284`).
    #[must_use]
    pub fn counters(&self) -> Counters {
        self.inner.accounting.counters()
    }

    /// Fresh-input-plus-cache dollars (`costInputTotal`).
    #[must_use]
    pub fn input_cost(&self) -> f64 {
        self.inner.accounting.input_cost(self.pricing())
    }

    /// Output dollars (`costOutputTotal`).
    #[must_use]
    pub fn output_cost(&self) -> f64 {
        self.inner.accounting.output_cost(self.pricing())
    }

    /// The two brackets added (`costTotal`).
    #[must_use]
    pub fn total_cost(&self) -> f64 {
        self.inner.accounting.total_cost(self.pricing())
    }

    /// Zero this row's counters and its cache baseline (`resetTotals`).
    pub fn reset_counters(&self) {
        self.inner.accounting.reset();
    }

    /// The fleet roll-up of one row's counters (see [`Totals`]).
    #[must_use]
    pub fn totals(&self) -> Totals {
        let mut totals = Totals::default();
        totals.add(&self.counters());

        totals
    }

    /// Rewinds the rotation cursors to the pool heads.
    ///
    /// NOT part of the TypeScript parity surface - a live process never rewinds a
    /// drained pool - it is the seam that lets a suite observe rotation from the
    /// beginning without building a second client. It does not clear
    /// [`Self::is_discoverable`]'s drain flag, so a test that needs the pre-drain
    /// state rebuilds the client.
    pub fn reset_rotation(&self) {
        self.inner.transport.reset_rotation();
        self.inner.rotations.store(0, Ordering::SeqCst);
    }

    // -- the call point -----------------------------------------------------

    /// One upstream call: `client.create(body)` (`model-class.ts:652-653`).
    ///
    /// # Errors
    ///
    /// The [`TransportFailure`] the attempt settled with, unchanged - including the
    /// raw 404 after every relay and credential has been exhausted, which is the
    /// value the route layer turns into a 503.
    pub async fn create(&self, body: &Value) -> Result<ResponseOrStream, TransportFailure> {
        self.create_with_signal(body, CancellationToken::new())
            .await
    }

    /// [`Self::create`] with the caller's cancellation signal attached, so dropping
    /// the returned stream or cancelling the token stops the upstream read
    /// (`model-class.ts`'s abort path, which the transport renders as
    /// `APIUserAbortError`).
    ///
    /// # Errors
    ///
    /// See [`Self::create`].
    pub async fn create_with_signal(
        &self,
        body: &Value,
        signal: CancellationToken,
    ) -> Result<ResponseOrStream, TransportFailure> {
        // One clock read at the entry, the way `accountUpstreamCall` captures
        // `startedAt` before the transport runs (`model-class.ts:710`). Every
        // settlement figure derives from this instant, which is what keeps the
        // phase split and the total describing the same span.
        let started = Instant::now();

        // STEP 1 - the prompt side, over the CALLER's body. `recordRequestTokens`
        // receives `body`, not the pinned payload (`model-class.ts:707`), so the
        // estimate is the pre-rewrite figure while the bytes sent are the rewritten
        // ones. `record_request` also performs `requestTotal += 1` (`:708`), which
        // is why it happens here and not after the response.
        let prefill = self
            .inner
            .accounting
            .record_request(transport::estimate_request_tokens(body));

        // STEP 2 - the payload rewrite.
        let payload = self.rewrite(body)?;

        // STEP 3 - the failover loop, owned by this layer (see the module section).
        self.dispatch(&payload, signal, prefill, started).await
    }

    /// STEP 3: send, and rotate on a 404 until the failover axis is exhausted.
    async fn dispatch(
        &self,
        payload: &Value,
        signal: CancellationToken,
        prefill: u64,
        started: Instant,
    ) -> Result<ResponseOrStream, TransportFailure> {
        let mut resume: Option<transport::AttemptState> = None;
        let path = self.path();

        loop {
            // One attempt per `post` call. `attempt_total` is a PORT addition, so it
            // is defined here as the attempts this layer can actually see; the
            // transport's own 429 retries are not visible on a success path.
            self.inner.accounting.record_attempt(1);

            // The settlement hook is installed per attempt: a rotated attempt owns a
            // different stream, and only the attempt that reaches a 2xx head can
            // settle anything. A settled (non-streaming) response never calls it -
            // this layer folds those figures itself.
            let mut options = RequestOptions::new().with_signal(signal.clone());

            if let Some(state) = resume.clone() {
                options = options.with_resume(state);
            }

            let client = self.clone();
            options = options.with_settlement(Box::new(move |settlement| {
                client.settle_stream(prefill, started, &settlement);
            }));

            let outcome = self.inner.transport.post(path, payload, options).await;

            let error = match outcome {
                Ok(answer) => {
                    // A settled response has no settlement hook, so its usage and
                    // elapsed span fold in here - the non-streaming half of
                    // `accountUpstreamCall` (`model-class.ts:724-730`).
                    if let ResponseOrStream::Response(document) = &answer {
                        // `readOutputTokens(usage)` receives the USAGE OBJECT, not the response
                        // (`model-class.ts:724`), and that is `transport::output_tokens_of`'s
                        // contract too - `StreamBody::absorb` passes it `chunk.usage`. The Option
                        // reaches `record_response` intact, because the TypeScript guards on
                        // `!== undefined` (`:725`): an absent usage block adds NOTHING rather
                        // than a zero that would masquerade as a measured figure.
                        let output_tokens =
                            document.get("usage").and_then(transport::output_tokens_of);

                        self.inner
                            .accounting
                            .record_response(output_tokens, elapsed_millis(started));
                    }

                    self.inner.accounting.record_success();

                    return Ok(answer);
                }
                Err(failure) => failure,
            };

            // A 404 is the failover trigger (`model-class.ts:634-636`: the default
            // 404 handler IS `this.switchBaseUrl()`). Anything else - 429 after the
            // transport's own budget, a timeout, a dead socket - settles here and
            // propagates untouched, exactly as the TypeScript re-throws the original
            // error object (`model-class.ts:716-720`).
            if !error.is_not_found() || !self.switch_base_url() {
                self.inner.accounting.record_failure();
                // "A failed attempt still consumed wall-clock time" (`:680-683`).
                self.inner
                    .accounting
                    .record_elapsed(elapsed_millis(started));

                return Err(error);
            }

            // Carry the ledger into the rotated attempt: the next stream must not
            // re-stamp time-to-first-token, and the terminal failure must still be
            // able to name every endpoint the request burned through
            // (`transport::AttemptState`'s contract).
            resume = Some(error.state().clone());
        }
    }

    /// The stream half of the settlement, driven by the transport's hook.
    ///
    /// EXACTLY-ONCE is the transport's guarantee (`transport::SettlementHook` runs on
    /// exhaustion, failure, cancellation, and abandonment, including via `Drop`), and
    /// this is the only fold point, so a stream can neither double-count nor escape
    /// the counters. That covers the abandonment case the TypeScript handles by
    /// running `trackStream`'s `finally` when a consumer walks away
    /// (`model-class.ts:862-879`).
    fn settle_stream(&self, prefill: u64, started: Instant, settlement: &StreamSettlement) {
        let record = StreamRecord::new(settlement, elapsed_millis(started));

        self.inner.accounting.record_stream(prefill, &record);

        // The port's success/failure counters. `Abandoned` is deliberately neither:
        // a consumer that stops reading early is not a provider failure, and
        // counting it as one would let a client's disconnect trip the row's
        // failure signal for every other caller.
        match settlement.outcome {
            transport::StreamOutcome::Completed => self.inner.accounting.record_success(),
            transport::StreamOutcome::Failed | transport::StreamOutcome::Cancelled => {
                self.inner.accounting.record_failure();
            }
            transport::StreamOutcome::Abandoned => {}
        }
    }

    // -- failover -----------------------------------------------------------

    /// Advance to the next upstream credential or base URL, and say whether one
    /// remains (`switchBaseUrl()`, `model-class.ts:933-935` and its overrides).
    ///
    /// `false` means the row's failover axis is exhausted, which the route turns
    /// into a 503. The three shapes:
    ///   * no failover at all for `ChatPlain` / `ChatRouted` /
    ///     `ResponsesPassthrough` - the TypeScript's `override switchBaseUrl()
    ///     { return false }` in every `merge`, `vultr`, `sail`, `token-router`, and
    ///     `open-router` file;
    ///   * endpoint rotation for a keyless relay, logging one line per rotation
    ///     exactly as `local-strata.ts:250,258` and `local-basalt.ts:238,249` do;
    ///   * credential rotation for a draining row, which SETS `drained` when the
    ///     pool runs out - the `apiKey = ''` assignment those families make.
    ///
    /// Logging stays relay-only for parity: the credential families print nothing on
    /// rotation in the TypeScript (`nvidia-glm5-flash.ts:82-85`), and only names and
    /// labels are ever printed, never a credential value.
    #[must_use]
    pub fn switch_base_url(&self) -> bool {
        match self.inner.spec.behavior {
            Behavior::ChatKeylessRelay => self.rotate_endpoint(),
            Behavior::ChatCredentialDrain { advance } => self.rotate_credential(advance),
            Behavior::ChatPlain | Behavior::ChatRouted { .. } | Behavior::ResponsesPassthrough => {
                false
            }
        }
    }

    /// The relay axis: advance to the next named endpoint.
    fn rotate_endpoint(&self) -> bool {
        let Some(endpoint) = self.inner.transport.rotate_endpoint() else {
            // The TypeScript's own exhaustion line, same register as its 429 retry
            // line (`local-strata.ts:246-248`).
            tracing::info!(
                "[failover] {} exhausted \u{2014} no upstream relays remain",
                self.id()
            );

            return false;
        };

        // The label is the vault entry's NAME - never the URL, never a credential
        // value (`local-strata.ts:254-256` says so explicitly).
        tracing::info!(
            "[failover] {} -> {}",
            self.id(),
            endpoint.label().unwrap_or(endpoint.base_url())
        );

        true
    }

    /// The credential axis, per the family's drain shape.
    fn rotate_credential(&self, advance: CredentialAdvance) -> bool {
        // One counter, one increment per rotation, read BEFORE the increment so the
        // value names this rotation's ordinal (0-based).
        //
        // Makora's `getNewKey` returns the entry it SHIFTED OFF, so the first
        // rotation re-sends on the credential the cursor already points at; the
        // pool has still shrunk, so the second rotation is a different credential (or
        // the drain). A cursor that only ever moves forward needs this ordinal to
        // reproduce that one-step lag.
        let rotation = self.inner.rotations.fetch_add(1, Ordering::SeqCst);

        if advance == CredentialAdvance::SameThenNext && rotation == 0 {
            return true;
        }

        if self.inner.transport.rotate_credential().is_some() {
            return true;
        }

        // The pool is empty: `apiKey = ''` (`nvidia-glm5-flash.ts:83`,
        // `makora-glm-5-3.ts:86`), which the `drained` flag stands in for. Sticky,
        // because a drained module-level queue never refills either.
        self.inner.drained.store(true, Ordering::SeqCst);

        false
    }

    // -- payload rewrite ----------------------------------------------------

    /// The request rewrite: `withModel` plus the provider's own `create()` body.
    ///
    /// Order matters and follows the TypeScript's object spread order, because a
    /// later key OVERWRITES an earlier one:
    ///   1. `model` - always the row's, never the caller's (`withModel`,
    ///      `model-class.ts:899-901`: `this.model || body.model`);
    ///   2. `reasoning_effort` - defaulted when absent, `null`, or `''`, and also
    ///      when it is `'none'` on the three rows that treat `'none'` as "not
    ///      provided". Every OTHER explicit value is forwarded verbatim, including a
    ///      non-string, because the TypeScript's ternary only inspects those three
    ///      spellings;
    ///   3. `max_tokens` - the local relays overwrite the caller's cap with the
    ///      one-eighth-of-context constant (`local-strata.ts:216`);
    ///   4. `vendor` - after the spread, so a caller cannot bypass the gateway's
    ///      routing contract (`merge-glm-5-3-flash.ts:87`);
    ///   5. `stream` - normalised to a real boolean (`const stream =
    ///      Boolean(body.stream)`), which is what makes the injected
    ///      `stream_options` and the payload agree by construction;
    ///   6. `stream_options` - only on a streaming request, only for a row that
    ///      needs it, and ABSENT rather than `null` otherwise.
    ///
    /// A non-object body is refused rather than coerced: the TypeScript's `{ ...body,
    /// model }` silently produces `{ model }` for a string body, which then fails
    /// opaquely upstream, and this port's rule is to name the problem at the layer
    /// that sees it.
    pub fn rewrite(&self, body: &Value) -> Result<Value, TransportFailure> {
        let Some(map) = body.as_object() else {
            return Err(TransportError::Build {
                detail: format!(
                    "model '{}' expects a JSON object body, got {}",
                    self.id(),
                    value_kind(body)
                ),
            }
            .into());
        };

        let mut document = map.clone();

        document.insert("model".to_owned(), json!(self.model()));

        // The Responses passthrough is the base class's raw forward: NOTHING else is
        // touched, so a caller speaking that surface keeps its own field order,
        // stream flag, and usage options (`model-class.ts:922-924`).
        if matches!(self.inner.spec.behavior, Behavior::ResponsesPassthrough) {
            return Ok(Value::Object(document));
        }

        if let Some(default) = self.inner.spec.default_reasoning_effort {
            let given = document.get("reasoning_effort");

            if self.effort_needs_default(given) {
                document.insert("reasoning_effort".to_owned(), json!(default));
            }
        }

        if let Some(max_tokens) = self.inner.spec.forced_max_tokens {
            document.insert("max_tokens".to_owned(), json!(max_tokens));
        }

        if let Behavior::ChatRouted { vendor } = self.inner.spec.behavior {
            document.insert("vendor".to_owned(), json!(vendor.as_str()));
        }

        // `Boolean(body.stream)` normalises ANY JSON value to a real boolean -
        // including the non-obvious ones JS treats as truthy, e.g. the string
        // `"false"`. Reproducing JS truthiness here (rather than `as_bool()`) is what
        // keeps the payload and the `stream_options` decision in sync with the
        // deployed behaviour for those inputs; `prepare_body` then reads the boolean
        // this line wrote, so the two can never disagree.
        let streaming = js_truthy(document.get("stream"));
        document.insert("stream".to_owned(), json!(streaming));

        if streaming && self.inner.spec.requires_usage_option {
            let options = serde_json::to_value(StreamOptions::with_usage()).unwrap_or_else(|_| {
                // A one-field struct with `Some(true)` cannot fail to serialize; the
                // literal keeps that total without an `unwrap` on the request path.
                json!({ "include_usage": true })
            });

            document.insert("stream_options".to_owned(), options);
        }

        Ok(Value::Object(document))
    }

    /// Whether the caller's `reasoning_effort` counts as "not provided".
    fn effort_needs_default(&self, given: Option<&Value>) -> bool {
        match given {
            // Absent or `null` is `isInvalid(...)` in the TypeScript's condition.
            None | Some(Value::Null) => true,
            Some(Value::String(value)) => {
                // `=== ''` is the second clause; the third clause (`=== 'none'`)
                // exists only on the three GPT-family rows.
                value.is_empty() || (self.inner.spec.coerce_none_effort && value == "none")
            }
            // Any other JSON type is an explicit value the ternary forwards.
            Some(_) => false,
        }
    }
}

/// Validates one candidate relay label, the gate `upstreamLabel` needs.
///
/// THE SECURITY CLAIM THIS IMPLEMENTS: `model-class.ts:337-350` documents
/// `upstreamLabel` as a value that is "an `Object.keys()` entry (a name), never a
/// URL and never a key value", and the label reaches the request log line and the
/// gateway's error annotation. That claim is documentation, not code - nothing in
/// the TypeScript validates it - so this port enforces it and records the gap.
///
/// DEVIATION (deliberate, recorded): the allow-list below is the closest faithful
/// enforcement of that documented claim. It is a port addition, not a port of
/// existing TypeScript code, and it is deliberately narrow: it must reject
/// credential-shaped and hostile-looking labels while accepting every label the live
/// vault actually carries (`MolabEndless`, `MarktonWillonLatest`, `STANDARD`,
/// `DevilAngel`, ...).
///
/// The rules, in order:
///   1. empty or whitespace-only is rejected - a blank label renders `": id"` and
///      cannot attribute anything;
///   2. any label containing `':'` is rejected - a URL or a `scheme://host` value
///      leaked into the label slot is the exact failure the TypeScript's comment
///      rules out, and it would also break the `label: id` log format;
///   3. whitespace anywhere is rejected - the vault labels are identifiers, and a
///      label with a space inside can forge a fake log boundary;
///   4. control characters are rejected, because they can split a log line;
///   5. a CREDENTIAL-SHAPED label is rejected: all characters in
///      `[A-Za-z0-9_-]`, at least 8 long, AND containing at least one digit, `-`, or
///      `_`. The digit/separator qualifier is load-bearing - without it the real
///      vault labels `MolabEndless` (twelve letters) and `STANDARD` (eight letters)
///      would be rejected, and A6 requires those to pass. Real credentials from the
///      vault's neighbours carry separators or digits (`sk-...`, `nvapi-...`,
///      dated org suffixes), so the qualifier still catches the shape that matters;
///   6. a label that [`crate::vault::classify`] does not call `valid` is rejected:
///      `NotAvailable`, `none`, `error`, `null`, `undefined` and the `*_PLACEHOLDER`
///      spellings are vault VALUE vocabulary, so seeing one in the label slot means a
///      slot's value leaked into its name. Every label the live artifact publishes for
///      the two relay slots classifies `valid`, so this costs nothing in deployment;
///   7. finally, a label that [`crate::error::redact_secrets`] would rewrite is
///      rejected, so the crate's own redaction oracle gets the last word rather than
///      disagreeing with this gate.
///
/// A length ceiling is deliberately NOT part of the rule: the TypeScript's own
/// longest label (`MarktonWillonLatest`, 19) sits well under any sensible cap, and a
/// cap would reject a legitimate long identifier without adding a security property.
#[must_use]
pub fn upstream_label_is_safe(label: &str) -> bool {
    if label.trim().is_empty() {
        return false;
    }

    if label
        .chars()
        .any(|character| character == ':' || character.is_control())
    {
        return false;
    }

    // A label with internal whitespace cannot be printed as one token.
    if label.chars().any(char::is_whitespace) {
        return false;
    }

    // A label that classifies as a VAULT VALUE rather than a name means a slot's value leaked
    // into its label field: `NotAvailable`, `none`, `error`, `null`, `undefined`, and the
    // `*_PLACEHOLDER` spellings are exactly the vocabulary `crate::vault` recognises. PORT
    // ADDITION (recorded, like the whole predicate): the TypeScript publishes `upstreamLabel`
    // verbatim with no vocabulary check (`local-strata.ts:13`), so this refusal exists only
    // here. It costs nothing in practice because every label the live artifact publishes for
    // the two relay slots classifies `valid` - the `clients` suite pins that against the
    // artifact's own names.
    if crate::vault::classify(label) != crate::vault::VaultValidity::Valid {
        return false;
    }

    if credential_shaped(label) {
        return false;
    }

    // The crate's own redaction must be a no-op on a printable label.
    crate::error::redact_secrets(label) == label
}

/// Rule 5 of [`upstream_label_is_safe`]: the identifier shape a secret is likely to
/// have.
fn credential_shaped(label: &str) -> bool {
    const MINIMUM_SECRET_LENGTH: usize = 8;

    if label.len() < MINIMUM_SECRET_LENGTH {
        return false;
    }

    let identifier_alphabet = label
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '-');

    if !identifier_alphabet {
        return false;
    }

    // A pure-word label is a NAME (`STANDARD`, `MolabEndless`, `DevilAngel`); a
    // secret almost never is, because it carries a prefix separator, a digit run, or
    // both.
    label
        .chars()
        .any(|character| character.is_ascii_digit() || character == '_' || character == '-')
}

/// JavaScript's `Boolean(value)` over a JSON value, which is what
/// `const stream = Boolean(body.stream)` applies.
///
/// The cases that are easy to get wrong are the ones this covers: an absent key is
/// falsy, `null` is falsy, `0`/`NaN`/`""` are falsy, and EVERY object and array -
/// including `[]` and `{}` - is truthy, as is any non-empty string including
/// `"false"`.
fn js_truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(number)) => number
            .as_f64()
            .is_some_and(|value| value != 0.0 && !value.is_nan()),
        Some(Value::String(text)) => !text.is_empty(),
        // `[]` and `{}` are truthy in JavaScript, and a JSON number-as-string like
        // `"0"` is truthy too: JS tests the string's length, not its value.
        Some(Value::Array(_) | Value::Object(_)) => true,
    }
}

/// A JSON type name for the "body must be an object" refusal - never the body itself,
/// which can hold a caller's prompt.
fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// The one clock read this layer performs for an elapsed span.
fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
