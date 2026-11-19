//! The served model set, COMPILED from `config/provider/**/*.json`.
//!
//! # `config/provider/` is the source of truth
//!
//! Every row lives in its own file under a per-provider folder
//! (`config/provider/vultr/vultr-glm5.json`,
//! `config/provider/makora/makora-glm5.json`, …). `build.rs` parses and validates
//! the whole tree recursively, then writes
//! `$OUT_DIR/model_rows.rs`, which this module includes as `CLIENTS`. There is no
//! hand-written row table here: adding, removing or repricing a model is a JSON
//! edit, and an invalid row is a compile error rather than a request-time failure.
//!
//! ## What a row carries - and what it must NOT carry
//!
//! Identity (`id`, `provider`, `model`), the vault REFERENCES (`vault.vendor`,
//! `vault.credential_slot`, `vault.endpoint_slot`), the request `behavior`,
//! pricing, visibility, and the request-shaping knobs (`reasoning_effort`,
//! `output_cap`, `usage_option`, `context_length`).
//!
//! **No credentials and no endpoint values.** The slots name entries in
//! `config/vault/generated/agentic-provider.json`; `src/vault.rs` resolves their
//! values at runtime. The build script rejects a slot that is not an
//! `*_api_keys` / `*_api_endpoints` identifier, so a pasted secret cannot pass.
//!
//! ## Ordering is automatic
//!
//! Every `*.json` under `config/provider/` (recursively) is scanned regardless of
//! its file name, and
//! `build.rs` sorts the rows by `id` before writing `CLIENTS`. `crate::registry`
//! preserves that order and never re-sorts it, so `/models` publishes the
//! alphabetical list; `/stats` applies its own id sort, which lands on the same
//! order. File names are cosmetic, and adding a file never renumbers another row.
//!
//! DEVIATION (deliberate): the TypeScript's `privateClients` is in registration
//! order. This table is alphabetical by id.
//!
//! ## The two rows this table does not carry, and why
//!
//! DEVIATION (deliberate, recorded). The TypeScript registers thirty clients; this
//! table carries twenty-nine:
//!
//!   1. `daytona/qwen3.8-27b` (`daytona/daytona-qwen3-8.ts`, index 6) is OMITTED. Its
//!      base URL does not exist until a GPU sandbox has booted - the TypeScript's own
//!      note in `registry.ts` records that "its worker cold-starts ONLY when a request
//!      is actually routed to it (construction is network-free)", and its endpoint
//!      comes from a sandbox handle rather than a vault entry
//!      (`daytona_api_endpoints/STANDARD` is a placeholder URL). A row built from the
//!      vault artifact today would therefore advertise a relayout that cannot answer a
//!      request, and `A7`'s catalog reconciliation is stated against the twenty-nine
//!      rows whose upstreams are configured. Porting it means porting the sandbox
//!      handle source, which is a separate substrate.
//!   2. The four `lightning/*` clients are omitted because the TypeScript does not
//!      register them either (`registry.ts`: "The Lightning offering is DISABLED ...
//!      every 'lightning/*' routing key resolves to nothing"). Their Chat-to-Responses
//!      translation adapter is likewise not ported here: it has no registered consumer
//!      today, and inventing a row for it would change the discoverable set.
//!
//! Both omissions are listed in `tests/registry.rs` as an explicit expected-diff against
//! the TypeScript, so the gap is asserted rather than remembered.
//!
//! The registry refuses a row - with a problem list, not a panic - if a vault
//! vendor or slot is missing, a pool is empty, a price is not a usable money
//! value, an id collides, or the method and the pools contradict each other. See
//! `crate::registry::Registry::try_new`.

use crate::client::{Behavior, ClientSpec, CredentialAdvance, RoutingVendor};
use crate::transport::KeylessHeaderPolicy;

impl ClientSpec {
    /// The row speaks the gateway-routed Chat Completions shape: every body gains the
    /// mandatory `vendor` field (`merge/merge-*.ts`).
    #[must_use]
    pub const fn routed(mut self, vendor: RoutingVendor) -> Self {
        self.behavior = Behavior::ChatRouted { vendor };

        self
    }

    /// The row rotates a CREDENTIAL pool on a 404 (`makora/*`, `nvidia/*`).
    #[must_use]
    pub const fn drained(mut self, advance: CredentialAdvance) -> Self {
        self.behavior = Behavior::ChatCredentialDrain { advance };

        self
    }

    /// The local-relay bundle, three facts that always travel together:
    ///   * the failover axis is the ENDPOINT pool, and each relay is named;
    ///   * there is no credential slot at all - "these servers require none"
    ///     (`local-strata.ts:238-241`);
    ///   * no `Authorization` header is ever sent, because the TypeScript pins
    ///     `defaultHeaders: { Authorization: null }` (`local-strata.ts:161`,
    ///     `local-basalt.ts:147`) while the SDK would otherwise send `Bearer none`.
    #[must_use]
    pub const fn keyless_relay(mut self) -> Self {
        self.behavior = Behavior::ChatKeylessRelay;
        self.credential_slot = None;
        self.keyless_header = KeylessHeaderPolicy::Suppress;

        self
    }

    /// Rates in dollars per one million tokens (`readonly inputCost` / `cacheCost` /
    /// `outputCost`).
    #[must_use]
    pub const fn priced(mut self, input: f64, cache_read: f64, output: f64) -> Self {
        self.pricing = crate::pricing::Pricing::per_million(input, cache_read, output);

        self
    }

    /// `enabled = false`: routable by id, absent from `/models` (`readonly enabled`).
    #[must_use]
    pub const fn hidden(mut self) -> Self {
        self.enabled = false;

        self
    }

    /// The `reasoning_effort` substituted when the caller supplied none, i.e. the
    /// `isInvalid(...) || === ''` arm of the family's ternary.
    #[must_use]
    pub const fn effort(mut self, default: &'static str) -> Self {
        self.default_reasoning_effort = Some(default);

        self
    }

    /// The same default, with an explicit `'none'` ALSO treated as "not provided" - the
    /// third clause the three GPT-family rows add (`merge-gpt-6-1-sol.ts:92`,
    /// `merge-gpt-6-luna.ts:90`, `token-router-sol.ts:74`).
    #[must_use]
    pub const fn effort_with_none(mut self, default: &'static str) -> Self {
        self.default_reasoning_effort = Some(default);
        self.coerce_none_effort = true;

        self
    }

    /// A `max_tokens` this row overwrites on every request (`local-strata.ts:216`,
    /// `local-basalt.ts:204`).
    #[must_use]
    pub const fn output_cap(mut self, max_tokens: u64) -> Self {
        self.forced_max_tokens = Some(max_tokens);

        self
    }

    /// The context length the catalog advertises for this row
    /// (`ClientSpec::context_length`). Config rows carry it explicitly so the
    /// advertised number is visible next to the rest of the model's data.
    #[must_use]
    pub const fn context_length(mut self, length: u32) -> Self {
        self.context_length = length;

        self
    }

    /// The row does not need `stream_options.include_usage`. Only the Responses
    /// passthrough can use this legitimately: a chat-native row without the opt-in
    /// never sees a `usage` chunk, which would freeze `outputTokenTotal` at 0
    /// (`nvidia-glm5-flash.ts:69-74`).
    #[must_use]
    pub const fn without_usage_option(mut self) -> Self {
        self.requires_usage_option = false;

        self
    }
}

// ---------------------------------------------------------------------------
// The compiled rows. `build.rs` renders this include from `config/provider/**/*.json`; its
// only export is the `CLIENTS` slice every consumer reads.
// ---------------------------------------------------------------------------
include!(concat!(env!("OUT_DIR"), "/model_rows.rs"));

/// Ids the TypeScript registers that this table does not, with the reason.
///
/// Asserted in `tests/registry.rs` so adding a row here without updating the parity note
/// is a test failure rather than a silent drift.
pub const OMITTED: &[(&str, &str)] = &[
    (
        "daytona/qwen3.8-27b",
        "its base URL exists only after a GPU-sandbox boot, so a vault-built row would \
         advertise an upstream that cannot answer a request",
    ),
    (
        "lightning/luna, lightning/sol, lightning/glm-5-3, lightning/mimo-2-6",
        "the Lightning offering is disabled in `registry.ts` as well, so omitting it IS \
         the parity behaviour",
    ),
];
