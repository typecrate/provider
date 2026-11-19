//! The served model set: twenty-nine rows, in the TypeScript's registration order.
//!
//! # Typecrate port - source lineage
//!
//! TypeScript origin: `packages/agentic/provider/src/registry.ts` (`privateClients` -
//! contents AND ordering) plus the fields and `create()` bodies of the thirty client
//! subclasses in `packages/agentic/provider/src/{makora,local,nvidia,merge,vultr,
//! token-router,open-router,sail}/*.ts`.
//!
//! ## Ordering is the contract, not a detail
//!
//! `registry.ts` repeats the same instruction above almost every entry: the client was
//! "appended ... to preserve all existing registry indices", because
//! `src/index.test.ts` asserts each position and the chat-completion route test keys on
//! `privateClients[0]`. The catalog and stats routes then depend on that order in
//! different ways: `/models` preserves it (`service-route.ts` filters without sorting)
//! while `/stats` deliberately re-sorts by id. `crate::registry` therefore builds this
//! slice in ARRAY ORDER and never sorts it - a sort here would silently break the
//! parity the TypeScript tests exist to hold.
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
//! ## How to add a row (the recipe)
//!
//! One row, five decisions, in this order:
//!   1. identity - `ClientSpec::plain(id, provider, model, vault_vendor,
//!      credential_slot, endpoint_slot)`. The three vault names are lookups into
//!      `config/vault/generated/agentic-provider.json` (vendor id, then slot ids), not
//!      display strings; `provider` is the display name the stats projection prints.
//!   2. method - exactly one of `.routed(vendor)` (a gateway that needs a `vendor`
//!      field), `.drained(advance)` (a credential pool that rotates on a 404),
//!      `.keyless_relay()` (a credential-less endpoint pool that rotates), or neither
//!      ([`crate::client::Behavior::ChatPlain`]).
//!   3. pricing - `.priced(input, cache_read, output)` per ONE MILLION tokens, or nothing
//!      for a row whose TypeScript class declares no rates (those render `$0.0000`).
//!   4. visibility - `.hidden()` for `enabled = false`, which keeps the row routable and
//!      removes it from `/models`.
//!   5. request shaping - `.effort(default)` / `.effort_with_none(default)` and
//!      `.output_cap(tokens)`; leave them alone for a row that forwards the caller's body.
//!
//! The registry then refuses the row - with a problem list, not a panic - if a vault
//! vendor or slot is missing, a pool is empty, a price is not a usable money value, an
//! id collides, or the method and the pools contradict each other. See
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

/// The output limit both local relays pin: `MAX_CONTEXT_LENGTH / 8`
/// (`local-strata.ts:49`, `local-basalt.ts:49`), i.e. one eighth of the advertised
/// context.
const LOCAL_OUTPUT_CAP: u64 = 32_768;

// ---------------------------------------------------------------------------
// The rows, in `registry.ts` order. The leading comment of each row names the
// TypeScript file it came from, which is what makes a diff reviewable.
// ---------------------------------------------------------------------------

// registry.ts index 0 - makora/makora-glm-5-3.ts. Effort is NOT injected: this family
// forwards whatever the caller sends, which is why it is also the row that proves the
// registry does not invent an effort.
const MAKORA_GLM_5_3: ClientSpec = ClientSpec::plain(
    "makora/glm-5.3",
    "Makora",
    "zai-org/GLM-5.3",
    "makora",
    "makora_api_keys",
    "makora_api_endpoints",
)
.drained(CredentialAdvance::SameThenNext)
.priced(0.72, 0.12, 2.38);

// index 1 - makora/makora-ds4-flash.ts (`effort = 'max'`).
const MAKORA_DS4_FLASH: ClientSpec = ClientSpec::plain(
    "makora/ds4-flash",
    "Makora",
    "deepseek-ai/DeepSeek-V4.1-Flash-1017",
    "makora",
    "makora_api_keys",
    "makora_api_endpoints",
)
.drained(CredentialAdvance::SameThenNext)
.priced(0.09, 0.02, 0.20)
.effort("max");

// index 2 - local/local-strata.ts. Holds the slot its Modal predecessor occupied
// (`registry.ts`), so its position is load-bearing for the catalog order. Hidden:
// `readonly enabled: boolean = false`.
const LOCAL_STRATA: ClientSpec = ClientSpec::relay(
    "local/strata",
    "Local",
    "local/strata",
    "strata",
    "local_strata_api_endpoints",
)
.priced(0.1, 0.1, 3.0)
.hidden()
.effort("xhigh")
.output_cap(LOCAL_OUTPUT_CAP);

// index 3 - nvidia/nvidia-glm5-flash.ts. NOTE the id/model mismatch, which is real and
// must not be tidied: the routing id is `nvidia/glm-5.3` while the wire model is
// `z-ai/glm-5.3-flash` (`nvidia-glm5-flash.ts:25,35`). Prices are the base-class zeros.
const NVIDIA_GLM_5_3: ClientSpec = ClientSpec::plain(
    "nvidia/glm-5.3",
    "Nvidia",
    "z-ai/glm-5.3-flash",
    "nvidia",
    "nvidia_api_keys",
    "nvidia_api_endpoints",
)
.drained(CredentialAdvance::NextOnEveryRotation)
.hidden();

// index 4 - nvidia/nvidia-deepseek-v4-flash.ts.
const NVIDIA_DEEPSEEK_V4_FLASH: ClientSpec = ClientSpec::plain(
    "nvidia/deepseek-v4-flash",
    "Nvidia",
    "deepseek-ai/deepseek-v4-flash-0731",
    "nvidia",
    "nvidia_api_keys",
    "nvidia_api_endpoints",
)
.drained(CredentialAdvance::NextOnEveryRotation)
.hidden();

// index 5 - nvidia/nvidia-kimi3.ts.
const NVIDIA_KIMI_K3: ClientSpec = ClientSpec::plain(
    "nvidia/kimi-k3",
    "Nvidia",
    "moonshotai/kimi-k3",
    "nvidia",
    "nvidia_api_keys",
    "nvidia_api_endpoints",
)
.drained(CredentialAdvance::NextOnEveryRotation)
.hidden();

// index 6 - daytona/qwen3.8-27b, OMITTED (see the module doc). The following rows keep
// their relative order, so every index below 6 is one lower than the TypeScript's.

// index 7 - token-router/token-router-glm-5-3.ts. Free tier model (`z-ai/glm-5.3-free`)
// at the reference rates; no effort injection.
const TOKEN_ROUTER_GLM_5_3: ClientSpec = ClientSpec::plain(
    "token-router/glm-5.3",
    "Token Router",
    "z-ai/glm-5.3-free",
    "token_router",
    "token_router_api_keys",
    "token_router_api_endpoints",
)
.priced(1.40, 0.26, 4.40);

// index 8 - makora/makora-glm5-flash.ts.
const MAKORA_GLM5_FLASH: ClientSpec = ClientSpec::plain(
    "makora/glm5-flash",
    "Makora",
    "zai-org/GLM-5.3-Flash",
    "makora",
    "makora_api_keys",
    "makora_api_endpoints",
)
.drained(CredentialAdvance::SameThenNext)
.priced(0.14, 0.02, 0.47)
.effort("max");

// index 9 - merge/merge-glm-5-3-flash.ts (the cheap Flash path through `pareto`).
const MERGE_GLM_5_3_FLASH: ClientSpec = ClientSpec::plain(
    "merge/glm-5.3-flash",
    "Merge",
    "zai/glm-5.3-flash",
    "merge",
    "merge_api_keys",
    "merge_api_endpoints",
)
.routed(RoutingVendor::Pareto)
.priced(0.15, 0.03, 0.5)
.hidden()
.effort("max");

// index 10 - merge/merge-kimi-k3.ts (`modal`).
const MERGE_KIMI_K3: ClientSpec = ClientSpec::plain(
    "merge/kimi-k3",
    "Merge",
    "moonshot/kimi-k3",
    "merge",
    "merge_api_keys",
    "merge_api_endpoints",
)
.routed(RoutingVendor::Modal)
.priced(3.0, 0.3, 15.0)
.hidden();

// index 11 - merge/merge-kimi-k2-6.ts. No price list in the TypeScript, so the
// base-class zeros stand.
const MERGE_KIMI_K2_6: ClientSpec = ClientSpec::plain(
    "merge/kimi-k2-6",
    "Merge",
    "moonshot/kimi-k2.6",
    "merge",
    "merge_api_keys",
    "merge_api_endpoints",
)
.routed(RoutingVendor::DeepInfra)
.hidden();

// index 12 - token-router/token-router-glm-5-3-flash.ts. Cross-gateway sibling of
// `open-router/glm-5.3-flash` below: same wire model, different rates.
const TOKEN_ROUTER_GLM_5_3_FLASH: ClientSpec = ClientSpec::plain(
    "token-router/glm-5.3-flash",
    "Token Router",
    "z-ai/glm-5.3-flash",
    "token_router",
    "token_router_api_keys",
    "token_router_api_endpoints",
)
.priced(0.075, 0.015, 0.25)
.effort("max");

// index 13 - open-router/open-router-glm-5-3-flash.ts. `enabled` stays TRUE; the row is
// hidden only by its `invalid` credential entry, which is the one credential-gated
// exclusion in the deployed set (`model-class.test.ts:288-296`).
const OPEN_ROUTER_GLM_5_3_FLASH: ClientSpec = ClientSpec::plain(
    "open-router/glm-5.3-flash",
    "Open Router",
    "z-ai/glm-5.3-flash",
    "open_router",
    "open_router_api_keys",
    "open_router_api_endpoints",
)
.priced(0.075, 0.015, 0.25)
.effort("max");

// index 14 - merge/merge-glm-5-3.ts (the full-price path through `telnyx`).
const MERGE_GLM_5_3: ClientSpec = ClientSpec::plain(
    "merge/glm-5.3",
    "Merge",
    "zai/glm-5.3",
    "merge",
    "merge_api_keys",
    "merge_api_endpoints",
)
.routed(RoutingVendor::Telnyx)
.priced(0.7, 0.13, 2.2)
.hidden()
.effort("max");

// index 15 - vultr/vultr-glm-5-3-flash.ts. Note the flat cache rate: Vultr prices the
// replayed prefix at the input rate on every model.
const VULTR_GLM_5_3_FLASH: ClientSpec = ClientSpec::plain(
    "vultr/glm-5.3-flash",
    "Vultr",
    "glm-5.3-flash",
    "vultr",
    "vultr_api_keys",
    "vultr_api_endpoints",
)
.priced(0.10, 0.10, 0.35)
.hidden()
.effort("max");

// index 16 - vultr/vultr-glm-5-3.ts.
const VULTR_GLM_5_3: ClientSpec = ClientSpec::plain(
    "vultr/glm-5.3",
    "Vultr",
    "glm-5.3",
    "vultr",
    "vultr_api_keys",
    "vultr_api_endpoints",
)
.priced(0.75, 0.75, 3.00)
.hidden()
.effort("max");

// index 17 - merge/merge-mimo-2-6.ts.
const MERGE_MIMO_2_6: ClientSpec = ClientSpec::plain(
    "merge/mimo-2-6",
    "Merge",
    "xiaomimimo/mimo-v2.6-pro",
    "merge",
    "merge_api_keys",
    "merge_api_endpoints",
)
.routed(RoutingVendor::DeepInfra)
.priced(0.43, 0.004, 0.87)
.hidden()
.effort("high");

// index 18 - vultr/vultr-mimo-2-6.ts.
const VULTR_MIMO_2_6: ClientSpec = ClientSpec::plain(
    "vultr/mimo-2-6",
    "Vultr",
    "mimo-v2.6-pro-rl",
    "vultr",
    "vultr_api_keys",
    "vultr_api_endpoints",
)
.priced(0.44, 0.44, 0.87)
.hidden()
.effort("high");

// index 19 - merge/merge-gpt-6-1-sol.ts. `'none'` is NOT an instruction for the openai
// vendor path, so it falls back to `medium` (`merge-gpt-6-1-sol.ts:92-93`).
const MERGE_GPT_6_1_SOL: ClientSpec = ClientSpec::plain(
    "merge/gpt-6-1-sol",
    "Merge",
    "openai/gpt-6.1-sol",
    "merge",
    "merge_api_keys",
    "merge_api_endpoints",
)
.routed(RoutingVendor::OpenAI)
.priced(2.0, 0.1, 10.0)
.hidden()
.effort_with_none("medium");

// index 20 - vultr/vultr-ds4-flash.ts.
const VULTR_DS4_FLASH: ClientSpec = ClientSpec::plain(
    "vultr/ds4-flash",
    "Vultr",
    "deepseek-ai/DeepSeek-V4.1-Flash-1017",
    "vultr",
    "vultr_api_keys",
    "vultr_api_endpoints",
)
.priced(0.15, 0.15, 0.60)
.hidden()
.effort("high");

// index 21 - merge/merge-ds4-flash.ts.
const MERGE_DS4_FLASH: ClientSpec = ClientSpec::plain(
    "merge/ds4-flash",
    "Merge",
    "deepseek/deepseek-v4.1-flash-1017",
    "merge",
    "merge_api_keys",
    "merge_api_endpoints",
)
.routed(RoutingVendor::DeepInfra)
.priced(0.2, 0.006, 0.6)
.hidden()
.effort("high");

// index 22 - local/local-basalt.ts. The second credential-less relay family, on its own
// endpoint listing; `enabled` keeps the base-class `true`, so this row IS in the catalog.
const LOCAL_BASALT: ClientSpec = ClientSpec::relay(
    "local/basalt",
    "Local",
    "local/basalt",
    "basalt",
    "local_basalt_api_endpoints",
)
.priced(0.10, 0.01, 0.60)
.effort("xhigh")
.output_cap(LOCAL_OUTPUT_CAP);

// index 23 - merge/merge-gpt-6-luna.ts. A DIFFERENT Luna generation from the disabled
// Lightning client, so this row adds a new upstream model to the served set.
const MERGE_GPT_6_LUNA: ClientSpec = ClientSpec::plain(
    "merge/gpt-6-luna",
    "Merge",
    "openai/gpt-6-luna",
    "merge",
    "merge_api_keys",
    "merge_api_endpoints",
)
.routed(RoutingVendor::OpenAI)
.priced(0.1, 0.01, 0.5)
.hidden()
.effort_with_none("medium");

// index 24 - token-router/token-router-sol.ts. Third gateway serving
// `openai/gpt-6.1-sol` (see index 19).
const TOKEN_ROUTER_SOL: ClientSpec = ClientSpec::plain(
    "token-router/sol",
    "Token Router",
    "openai/gpt-6.1-sol",
    "token_router",
    "token_router_api_keys",
    "token_router_api_endpoints",
)
.priced(2.0, 0.1, 10.0)
.effort_with_none("medium");

// index 25 - sail/sail-ds4-flash.ts. Sail carries a live credential, so all four Sail
// siblings are discoverable.
const SAIL_DS4_FLASH: ClientSpec = ClientSpec::plain(
    "sail/ds4-flash",
    "Sail",
    "deepseek-ai/DeepSeek-V4.1-Flash-1017",
    "sail",
    "sail_api_keys",
    "sail_api_endpoints",
)
.priced(0.09, 0.006, 0.30)
.effort("high");

// index 26 - sail/sail-glm5-flash.ts.
const SAIL_GLM5_FLASH: ClientSpec = ClientSpec::plain(
    "sail/glm5-flash",
    "Sail",
    "zai-org/GLM-5.3-Flash",
    "sail",
    "sail_api_keys",
    "sail_api_endpoints",
)
.priced(0.11, 0.02, 0.35)
.effort("max");

// index 27 - sail/sail-glm5.ts. The full-price sibling of the row above
// ($0.98/$0.18/$3.08 against $0.11/$0.02/$0.35).
const SAIL_GLM5: ClientSpec = ClientSpec::plain(
    "sail/glm5",
    "Sail",
    "zai-org/GLM-5.3",
    "sail",
    "sail_api_keys",
    "sail_api_endpoints",
)
.priced(0.98, 0.18, 3.08)
.effort("max");

// index 28 - sail/sail-kimi3.ts. A DIFFERENT upstream id from both `moonshot/kimi-k3`
// (index 10) and `moonshotai/kimi-k3` (index 5), so the three do not collapse into one.
const SAIL_KIMI3: ClientSpec = ClientSpec::plain(
    "sail/kimi3",
    "Sail",
    "moonshotai/Kimi-K3.1-Pro",
    "sail",
    "sail_api_keys",
    "sail_api_endpoints",
)
.priced(1.5, 0.2, 10.0)
.effort("max");

// index 29 - nvidia/nvidia-ds4-flash.ts. LAST in the TypeScript too (appended after
// SailKimi3 so no existing index moved), and a different wire id from index 4.
const NVIDIA_DS4_FLASH: ClientSpec = ClientSpec::plain(
    "nvidia/ds4-flash",
    "Nvidia",
    "deepseek-ai/deepseek-v4.1-flash",
    "nvidia",
    "nvidia_api_keys",
    "nvidia_api_endpoints",
)
.drained(CredentialAdvance::NextOnEveryRotation)
.hidden();

/// Every served model, in `registry.ts` order.
///
/// `A7` reconciliation against the TypeScript, stated as numbers:
///   * TypeScript: 30 registered, 12 discoverable, 18 not discoverable
///     (`model-class.test.ts:243-282`).
///   * This table: 29 registered, 11 discoverable against the live vault artifact, the
///     same 11 ids in the same order - the difference being the omitted
///     `daytona/qwen3.8-27b`, which is discoverable in the TypeScript
///     (`model-class.test.ts:310-316`).
///
/// Order is what `crate::registry` hands the catalog, so the `/models` list comes out
/// `makora/glm-5.3`, `makora/ds4-flash`, `token-router/glm-5.3`, `makora/glm5-flash`,
/// `token-router/glm-5.3-flash`, `local/basalt`, `token-router/sol`, `sail/ds4-flash`,
/// `sail/glm5-flash`, `sail/glm5`, `sail/kimi3` - the TypeScript's twelve with daytona
/// removed, still unsorted.
pub const CLIENTS: &[ClientSpec] = &[
    MAKORA_GLM_5_3,
    MAKORA_DS4_FLASH,
    LOCAL_STRATA,
    NVIDIA_GLM_5_3,
    NVIDIA_DEEPSEEK_V4_FLASH,
    NVIDIA_KIMI_K3,
    TOKEN_ROUTER_GLM_5_3,
    MAKORA_GLM5_FLASH,
    MERGE_GLM_5_3_FLASH,
    MERGE_KIMI_K3,
    MERGE_KIMI_K2_6,
    TOKEN_ROUTER_GLM_5_3_FLASH,
    OPEN_ROUTER_GLM_5_3_FLASH,
    MERGE_GLM_5_3,
    VULTR_GLM_5_3_FLASH,
    VULTR_GLM_5_3,
    MERGE_MIMO_2_6,
    VULTR_MIMO_2_6,
    MERGE_GPT_6_1_SOL,
    VULTR_DS4_FLASH,
    MERGE_DS4_FLASH,
    LOCAL_BASALT,
    MERGE_GPT_6_LUNA,
    TOKEN_ROUTER_SOL,
    SAIL_DS4_FLASH,
    SAIL_GLM5_FLASH,
    SAIL_GLM5,
    SAIL_KIMI3,
    NVIDIA_DS4_FLASH,
];

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
