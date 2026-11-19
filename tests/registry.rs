//! Domain-layer acceptance suite: the ordered registry and the row table (C3).
//!
//! Coverage of the acceptance list, by test letter:
//!   * A4 - `d` (the nvidia credential pool is the vault document in order, and rotation walks
//!     it), `e` (the makora `same-then-next` advance against a one-entry pool), `f` (the two
//!     relays rotate their NAMED endpoints in document order);
//!   * A5 - the drain half of `d`, `e` and `f`, plus `c`, which reconciles the discoverable
//!     subset with the artifact;
//!   * A6 - `i` (a hostile relay label blocks the build and is never echoed), with the real
//!     artifact labels in `f` and the allow-list itself pinned in `tests/clients.rs`;
//!   * A7 - `a` and `c` (registry order, the 11 discoverable ids, the 18 hidden ids, and the
//!     one model the table omits);
//!   * A8 - `b`, which projects every field of all 29 rows: that projection IS the row recipe
//!     the next task builds on;
//!   * A9 - `g`, `h`, `j`, `k`, `l`, `m`: the public shapes, and the fact that a refused build
//!     is a named problem list rather than a coercion.
//!
//! Credential VALUES never appear in this file: every assertion about a pool entry names its
//! LABEL, and the leak check in `i` proves the refusal message does not echo a value either.

use typecrate_provider::client::{Behavior, ClientSpec, CredentialAdvance, DEFAULT_CONTEXT_LENGTH};
use typecrate_provider::clients;
use typecrate_provider::pricing::Pricing;
use typecrate_provider::registry::Registry;
use typecrate_provider::transport::KeylessHeaderPolicy;
use typecrate_provider::vault::{self, VaultConfig};

/// The artifact the sibling TypeScript package generates, resolved the way a caller resolves
/// it - from the crate directory, not the process cwd. It is git-ignored, so the artifact
/// tests skip (loudly) rather than fabricate a vault when it is absent.
const ARTIFACT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../config/vault/generated/agentic-provider.json"
);

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Wraps a `vendors` array literal in the document envelope the loader requires (the schema
/// version, the consumer name, and the sentinel vocabulary the artifact itself declares). The
/// TEXT is exposed as well as the loaded value: the refusal tests need to call the loader
/// directly, to show which refusals happen there rather than in the registry.
fn envelope(vendors_json: &str) -> String {
    format!(
        r#"{{
            "schema_version": 1,
            "package": "@config/vault",
            "consumers": ["agentic-provider"],
            "sentinels": {{
                "keyless": ["none"],
                "invalid": ["NotAvailable", "error", "null", "undefined"],
                "placeholder": []
            }},
            "vendors": {vendors_json}
        }}"#
    )
}

/// A synthetic document is the only way this suite can express the counterfactuals the live
/// artifact does not contain - a missing vendor, a slot used as the wrong kind, a group entry -
/// and every one of them is refused by NAME, which is the point of the registry.
fn document(vendors_json: &str) -> VaultConfig {
    vault::load_from_str(&envelope(vendors_json)).unwrap_or_else(|error| {
        panic!(
            "the synthetic document must load; got {} problem(s): {}",
            error.problem_count(),
            error.problems().join(" | ")
        )
    })
}

/// The one-vendor `test` vault, with the two slots a client row needs, given verbatim.
fn vendor_array(key_slot: &str, endpoint_slot: &str) -> String {
    format!(r#"[{{ "id": "test", "slots": [{key_slot}, {endpoint_slot}] }}]"#)
}

/// A two-entry secret slot: the pool whose ORDER the rotation tests depend on.
const KEYS: &str = r#"{
    "id": "test_api_keys", "kind": "keys", "secret": true, "optional": false,
    "retired": false, "nesting": "flat",
    "entries": [
        { "label": "One", "value": "sk-one-one-one", "validity": "valid" },
        { "label": "Two", "value": "sk-two-two-two", "validity": "valid" }
    ],
    "default": "sk-one-one-one"
}"#;

/// A flat slot with NO entries. The loader refuses the shape outright (every live map has at
/// least one key), so `EMPTY_KEYS` exists to pin that refusal - which is why the registry's own
/// emptiness problems are a second line of defence rather than the reachable path.
const EMPTY_KEYS: &str = r#"{
    "id": "test_api_keys", "kind": "keys", "secret": true, "optional": false,
    "retired": false, "nesting": "flat", "entries": [], "default": null
}"#;

/// A two-entry public slot: the pool the two relays rotate.
const ENDPOINTS: &str = r#"{
    "id": "test_api_endpoints", "kind": "endpoints", "secret": false, "optional": true,
    "retired": false, "nesting": "flat",
    "entries": [
        { "label": "Alpha", "value": "http://127.0.0.1:1/v1", "validity": "valid" },
        { "label": "Beta", "value": "http://127.0.0.1:2/v1", "validity": "valid" }
    ],
    "default": "http://127.0.0.1:1/v1"
}"#;

/// One entry whose LABEL holds a credential: the misfiled-vault case `build_endpoints` refuses
/// to print (30 bytes, all in the allow-list's refusal class). It is also the pool's ONLY
/// entry, which is what makes the refusal come with a second problem: the row is left with
/// nothing to dial.
const CREDENTIAL_SHAPED_LABEL: &str = r#"{
    "id": "test_api_endpoints", "kind": "endpoints", "secret": false, "optional": true,
    "retired": false, "nesting": "flat",
    "entries": [
        { "label": "sk-proj-aaaaaaaaaaaaaaaaaaaaaa", "value": "http://127.0.0.1:1/v1", "validity": "valid" }
    ],
    "default": "http://127.0.0.1:1/v1"
}"#;

/// The same hostile label, but as the SECOND entry of a usable pool: the build refuses the row
/// and reports exactly one problem, naming the entry by its POSITION.
const PARTIALLY_HOSTILE_ENDPOINTS: &str = r#"{
    "id": "test_api_endpoints", "kind": "endpoints", "secret": false, "optional": true,
    "retired": false, "nesting": "flat",
    "entries": [
        { "label": "Alpha", "value": "http://127.0.0.1:1/v1", "validity": "valid" },
        { "label": "sk-proj-aaaaaaaaaaaaaaaaaaaaaa", "value": "http://127.0.0.1:2/v1", "validity": "valid" }
    ],
    "default": "http://127.0.0.1:1/v1"
}"#;

/// One NESTED keys slot: the only credential shape a client row cannot rotate.
const NESTED_KEYS: &str = r#"{
    "id": "test_api_keys", "kind": "keys", "secret": true, "optional": false,
    "retired": false, "nesting": "nested",
    "entries": [
        {
            "children": [
                { "label": "Leaf", "value": "sk-leaf-leaf-leaf", "validity": "valid" }
            ],
            "label": "Group",
            "validity": "valid",
            "value": null
        }
    ],
    "default": null
}"#;

/// Assembles the one-vendor document every refusal test starts from.
fn vault_doc(slot_json: &str) -> VaultConfig {
    document(&vendor_array(slot_json, ENDPOINTS))
}

/// Assembles a document whose endpoint slot is given separately (the relay-pool cases).
fn vault_doc_slots(key_slot: &str, endpoint_slot: &str) -> VaultConfig {
    document(&vendor_array(key_slot, endpoint_slot))
}

/// The probe keyed row: valid everywhere except where a test breaks it.
const PROBE: ClientSpec = ClientSpec::plain(
    "probe/keyed",
    "Probe",
    "probe-model",
    "test",
    "test_api_keys",
    "test_api_endpoints",
);

/// The probe relay row, for the cases only a relay can produce.
const PROBE_RELAY: ClientSpec = ClientSpec::relay(
    "probe/relay",
    "Probe",
    "probe-relay",
    "test",
    "test_api_endpoints",
);

/// Reads the live artifact, or SKIPS the named test when the checkout has none.
///
/// A skip is never silent here: the line names the test that asserts NOTHING, says so in words
/// (`NOT CHECKED`), names the artifact that is missing, and gives both the regeneration command and
/// the exact re-run. `cargo test` captures a PASSING test's stderr, so `--nocapture` is what
/// surfaces it — `knowledge.md` records that requirement next to the limitation.
///
/// A REJECTION of the vault's own output is NOT a skip: it stays a panic carrying every problem,
/// exactly like the artifact tests in `src/vault.rs`.
fn artifact(test: &str) -> Option<VaultConfig> {
    let raw = match std::fs::read_to_string(ARTIFACT) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!(
                "NOT CHECKED: `{test}` ran ZERO assertions - {ARTIFACT} is not generated in this \
                 checkout ({error}). Regenerate it with `yarn workspace @config/vault export -- \
                 --consumer agentic-provider`, then re-run `cargo test --offline --locked --test \
                 registry -- --nocapture` (without --nocapture cargo test hides this line)."
            );

            return None;
        }
        Err(error) => panic!("the artifact exists but could not be read: {error}"),
    };

    Some(vault::load_from_str(&raw).unwrap_or_else(|error| {
        panic!(
            "the live artifact must load cleanly; got {} problem(s): {}",
            error.problem_count(),
            error.problems().join(" | ")
        )
    }))
}

/// The vault entry labels of one artifact slot, in the order the LOADER holds them. This is a
/// second, independent source for the expected rotation order: the other is the hard-coded
/// list in each test, so a re-sort inside this crate cannot agree with itself.
fn artifact_labels(vault: &VaultConfig, vendor: &str, slot: &str) -> Vec<String> {
    vault
        .slot(vendor, slot)
        .unwrap_or_else(|| panic!("the artifact has no slot {vendor}/{slot}"))
        .entries()
        .iter()
        .map(|entry| entry.label().to_owned())
        .collect()
}

// ---------------------------------------------------------------------------
// A7 / A8: the table
// ---------------------------------------------------------------------------

/// The 29 routing ids, in table order - `registry.ts` order, entry for entry, with the single
/// omission (`daytona/qwen3.8-27b`). Order is a routing property: the catalog projection and
/// every fallback chain inherit it, so a re-sort here is a behaviour change.
#[test]
fn a_the_table_carries_twenty_nine_rows_in_the_typescript_registry_order() {
    let ids: Vec<&str> = clients::CLIENTS.iter().map(|row| row.id).collect();

    assert_eq!(
        ids,
        vec![
            "makora/glm-5.3",
            "makora/ds4-flash",
            "local/strata",
            "nvidia/glm-5.3",
            "nvidia/deepseek-v4-flash",
            "nvidia/kimi-k3",
            "token-router/glm-5.3",
            "makora/glm5-flash",
            "merge/glm-5.3-flash",
            "merge/kimi-k3",
            "merge/kimi-k2-6",
            "token-router/glm-5.3-flash",
            "open-router/glm-5.3-flash",
            "merge/glm-5.3",
            "vultr/glm-5.3-flash",
            "vultr/glm-5.3",
            "merge/mimo-2-6",
            "vultr/mimo-2-6",
            "merge/gpt-6-1-sol",
            "vultr/ds4-flash",
            "merge/ds4-flash",
            "local/basalt",
            "merge/gpt-6-luna",
            "token-router/sol",
            "sail/ds4-flash",
            "sail/glm5-flash",
            "sail/glm5",
            "sail/kimi3",
            "nvidia/ds4-flash",
        ],
        "twenty-nine rows, and the TypeScript's thirty minus daytona"
    );

    assert_eq!(
        clients::OMITTED,
        &[
            (
                "daytona/qwen3.8-27b",
                "its base URL exists only after a GPU-sandbox boot, so a vault-built row would \
                 advertise an upstream that cannot answer a request"
            ),
            (
                "lightning/luna, lightning/sol, lightning/glm-5-3, lightning/mimo-2-6",
                "the Lightning offering is disabled in `registry.ts` as well, so omitting it IS \
                 the parity behaviour"
            ),
        ][..],
        "the omission list is the parity record: adding a row without updating it is a drift"
    );
}

/// A8: the whole configuration surface of the table, as data. One row per model, every field a
/// declarative one - nothing here is per-vendor code, which is what the next task builds on.
#[test]
fn b_every_row_is_a_declarative_row_and_this_is_the_whole_table() {
    /// The projection tuple: identity, the two slot names, the failover shape spelled the way
    /// the vault spells its kinds, the money, and the three body-rewrite switches.
    type Row = (
        &'static str,         // routing id
        &'static str,         // display provider
        &'static str,         // upstream wire model
        &'static str,         // vault vendor
        Option<&'static str>, // credential slot
        &'static str,         // endpoint slot
        &'static str,         // behavior: plain | routed:<vendor> | relay | drain:<advance>
        (f64, f64, f64),      // input, cache-read, output per million
        bool,                 // enabled (the catalog switch)
        Option<&'static str>, // default reasoning effort
        bool,                 // whether an explicit 'none' falls back to that default
    );

    fn projection(row: &ClientSpec) -> Row {
        (
            row.id,
            row.provider,
            row.model,
            row.vault_vendor,
            row.credential_slot,
            row.endpoint_slot,
            match row.behavior {
                Behavior::ChatPlain => "plain",
                Behavior::ChatRouted { vendor } => match vendor {
                    typecrate_provider::client::RoutingVendor::Telnyx => "routed:telnyx",
                    typecrate_provider::client::RoutingVendor::Pareto => "routed:pareto",
                    typecrate_provider::client::RoutingVendor::Modal => "routed:modal",
                    typecrate_provider::client::RoutingVendor::DeepInfra => "routed:deepinfra",
                    typecrate_provider::client::RoutingVendor::OpenAI => "routed:openai",
                },
                Behavior::ChatKeylessRelay => "relay",
                Behavior::ChatCredentialDrain { advance } => match advance {
                    CredentialAdvance::SameThenNext => "drain:same-then-next",
                    CredentialAdvance::NextOnEveryRotation => "drain:next-on-every-rotation",
                },
                Behavior::ResponsesPassthrough => "responses",
            },
            (
                row.pricing.input,
                row.pricing.cache_read,
                row.pricing.output,
            ),
            row.enabled,
            row.default_reasoning_effort,
            row.coerce_none_effort,
        )
    }

    assert_eq!(
        clients::CLIENTS
            .iter()
            .map(projection)
            .collect::<Vec<Row>>(),
        vec![
            row(
                "makora/glm-5.3",
                "Makora",
                "zai-org/GLM-5.3",
                "makora",
                Some("makora_api_keys"),
                "makora_api_endpoints",
                "drain:same-then-next",
                (0.72, 0.12, 2.38),
                true,
                None,
                false
            ),
            row(
                "makora/ds4-flash",
                "Makora",
                "deepseek-ai/DeepSeek-V4.1-Flash-1017",
                "makora",
                Some("makora_api_keys"),
                "makora_api_endpoints",
                "drain:same-then-next",
                (0.09, 0.02, 0.20),
                true,
                Some("max"),
                false
            ),
            row(
                "local/strata",
                "Local",
                "local/strata",
                "strata",
                None,
                "local_strata_api_endpoints",
                "relay",
                (0.1, 0.1, 3.0),
                false,
                Some("xhigh"),
                false
            ),
            row(
                "nvidia/glm-5.3",
                "Nvidia",
                "z-ai/glm-5.3-flash",
                "nvidia",
                Some("nvidia_api_keys"),
                "nvidia_api_endpoints",
                "drain:next-on-every-rotation",
                (0.0, 0.0, 0.0),
                false,
                None,
                false
            ),
            row(
                "nvidia/deepseek-v4-flash",
                "Nvidia",
                "deepseek-ai/deepseek-v4-flash-0731",
                "nvidia",
                Some("nvidia_api_keys"),
                "nvidia_api_endpoints",
                "drain:next-on-every-rotation",
                (0.0, 0.0, 0.0),
                false,
                None,
                false
            ),
            row(
                "nvidia/kimi-k3",
                "Nvidia",
                "moonshotai/kimi-k3",
                "nvidia",
                Some("nvidia_api_keys"),
                "nvidia_api_endpoints",
                "drain:next-on-every-rotation",
                (0.0, 0.0, 0.0),
                false,
                None,
                false
            ),
            row(
                "token-router/glm-5.3",
                "Token Router",
                "z-ai/glm-5.3-free",
                "token_router",
                Some("token_router_api_keys"),
                "token_router_api_endpoints",
                "plain",
                (1.40, 0.26, 4.40),
                true,
                None,
                false
            ),
            row(
                "makora/glm5-flash",
                "Makora",
                "zai-org/GLM-5.3-Flash",
                "makora",
                Some("makora_api_keys"),
                "makora_api_endpoints",
                "drain:same-then-next",
                (0.14, 0.02, 0.47),
                true,
                Some("max"),
                false
            ),
            row(
                "merge/glm-5.3-flash",
                "Merge",
                "zai/glm-5.3-flash",
                "merge",
                Some("merge_api_keys"),
                "merge_api_endpoints",
                "routed:pareto",
                (0.15, 0.03, 0.5),
                false,
                Some("max"),
                false
            ),
            row(
                "merge/kimi-k3",
                "Merge",
                "moonshot/kimi-k3",
                "merge",
                Some("merge_api_keys"),
                "merge_api_endpoints",
                "routed:modal",
                (3.0, 0.3, 15.0),
                false,
                None,
                false
            ),
            row(
                "merge/kimi-k2-6",
                "Merge",
                "moonshot/kimi-k2.6",
                "merge",
                Some("merge_api_keys"),
                "merge_api_endpoints",
                "routed:deepinfra",
                (0.0, 0.0, 0.0),
                false,
                None,
                false
            ),
            row(
                "token-router/glm-5.3-flash",
                "Token Router",
                "z-ai/glm-5.3-flash",
                "token_router",
                Some("token_router_api_keys"),
                "token_router_api_endpoints",
                "plain",
                (0.075, 0.015, 0.25),
                true,
                Some("max"),
                false
            ),
            row(
                "open-router/glm-5.3-flash",
                "Open Router",
                "z-ai/glm-5.3-flash",
                "open_router",
                Some("open_router_api_keys"),
                "open_router_api_endpoints",
                "plain",
                (0.075, 0.015, 0.25),
                true,
                Some("max"),
                false
            ),
            row(
                "merge/glm-5.3",
                "Merge",
                "zai/glm-5.3",
                "merge",
                Some("merge_api_keys"),
                "merge_api_endpoints",
                "routed:telnyx",
                (0.7, 0.13, 2.2),
                false,
                Some("max"),
                false
            ),
            row(
                "vultr/glm-5.3-flash",
                "Vultr",
                "glm-5.3-flash",
                "vultr",
                Some("vultr_api_keys"),
                "vultr_api_endpoints",
                "plain",
                (0.10, 0.10, 0.35),
                false,
                Some("max"),
                false
            ),
            row(
                "vultr/glm-5.3",
                "Vultr",
                "glm-5.3",
                "vultr",
                Some("vultr_api_keys"),
                "vultr_api_endpoints",
                "plain",
                (0.75, 0.75, 3.00),
                false,
                Some("max"),
                false
            ),
            row(
                "merge/mimo-2-6",
                "Merge",
                "xiaomimimo/mimo-v2.6-pro",
                "merge",
                Some("merge_api_keys"),
                "merge_api_endpoints",
                "routed:deepinfra",
                (0.43, 0.004, 0.87),
                false,
                Some("high"),
                false
            ),
            row(
                "vultr/mimo-2-6",
                "Vultr",
                "mimo-v2.6-pro-rl",
                "vultr",
                Some("vultr_api_keys"),
                "vultr_api_endpoints",
                "plain",
                (0.44, 0.44, 0.87),
                false,
                Some("high"),
                false
            ),
            row(
                "merge/gpt-6-1-sol",
                "Merge",
                "openai/gpt-6.1-sol",
                "merge",
                Some("merge_api_keys"),
                "merge_api_endpoints",
                "routed:openai",
                (2.0, 0.1, 10.0),
                false,
                Some("medium"),
                true
            ),
            row(
                "vultr/ds4-flash",
                "Vultr",
                "deepseek-ai/DeepSeek-V4.1-Flash-1017",
                "vultr",
                Some("vultr_api_keys"),
                "vultr_api_endpoints",
                "plain",
                (0.15, 0.15, 0.60),
                false,
                Some("high"),
                false
            ),
            row(
                "merge/ds4-flash",
                "Merge",
                "deepseek/deepseek-v4.1-flash-1017",
                "merge",
                Some("merge_api_keys"),
                "merge_api_endpoints",
                "routed:deepinfra",
                (0.2, 0.006, 0.6),
                false,
                Some("high"),
                false
            ),
            row(
                "local/basalt",
                "Local",
                "local/basalt",
                "basalt",
                None,
                "local_basalt_api_endpoints",
                "relay",
                (0.10, 0.01, 0.60),
                true,
                Some("xhigh"),
                false
            ),
            row(
                "merge/gpt-6-luna",
                "Merge",
                "openai/gpt-6-luna",
                "merge",
                Some("merge_api_keys"),
                "merge_api_endpoints",
                "routed:openai",
                (0.1, 0.01, 0.5),
                false,
                Some("medium"),
                true
            ),
            row(
                "token-router/sol",
                "Token Router",
                "openai/gpt-6.1-sol",
                "token_router",
                Some("token_router_api_keys"),
                "token_router_api_endpoints",
                "plain",
                (2.0, 0.1, 10.0),
                true,
                Some("medium"),
                true
            ),
            row(
                "sail/ds4-flash",
                "Sail",
                "deepseek-ai/DeepSeek-V4.1-Flash-1017",
                "sail",
                Some("sail_api_keys"),
                "sail_api_endpoints",
                "plain",
                (0.09, 0.006, 0.30),
                true,
                Some("high"),
                false
            ),
            row(
                "sail/glm5-flash",
                "Sail",
                "zai-org/GLM-5.3-Flash",
                "sail",
                Some("sail_api_keys"),
                "sail_api_endpoints",
                "plain",
                (0.11, 0.02, 0.35),
                true,
                Some("max"),
                false
            ),
            row(
                "sail/glm5",
                "Sail",
                "zai-org/GLM-5.3",
                "sail",
                Some("sail_api_keys"),
                "sail_api_endpoints",
                "plain",
                (0.98, 0.18, 3.08),
                true,
                Some("max"),
                false
            ),
            row(
                "sail/kimi3",
                "Sail",
                "moonshotai/Kimi-K3.1-Pro",
                "sail",
                Some("sail_api_keys"),
                "sail_api_endpoints",
                "plain",
                (1.5, 0.2, 10.0),
                true,
                Some("max"),
                false
            ),
            row(
                "nvidia/ds4-flash",
                "Nvidia",
                "deepseek-ai/deepseek-v4.1-flash",
                "nvidia",
                Some("nvidia_api_keys"),
                "nvidia_api_endpoints",
                "drain:next-on-every-rotation",
                (0.0, 0.0, 0.0),
                false,
                None,
                false
            ),
        ],
        "a row changed here is a routing change, and this table is the review surface for it"
    );

    /// The projection helper, spelled as a call so the table above stays readable.
    /// Eleven positional fields is the shape of a row, not a design problem: the call sites
    /// below are a table, and their order is the order of `Row`.
    #[expect(
        clippy::too_many_arguments,
        reason = "one positional field per row property"
    )]
    const fn row(
        id: &'static str,
        provider: &'static str,
        model: &'static str,
        vault_vendor: &'static str,
        credential_slot: Option<&'static str>,
        endpoint_slot: &'static str,
        behavior: &'static str,
        pricing: (f64, f64, f64),
        enabled: bool,
        effort: Option<&'static str>,
        coerce_none: bool,
    ) -> Row {
        (
            id,
            provider,
            model,
            vault_vendor,
            credential_slot,
            endpoint_slot,
            behavior,
            pricing,
            enabled,
            effort,
            coerce_none,
        )
    }

    // The fields the projection above deliberately leaves out are all DERIVED properties of the
    // row shape, and each is pinned so no row can drift away from the family it belongs to.
    for row in clients::CLIENTS {
        // Every live row speaks chat completions and asks for usage on a stream; the Responses
        // passthrough variant has no live row, so the flag is uniformly true here.
        assert!(row.requires_usage_option, "{}", row.id);
        assert_eq!(row.context_length, DEFAULT_CONTEXT_LENGTH, "{}", row.id);

        // The keyless-header policy follows the row shape exactly: only a row with no
        // credential slot may suppress the header.
        assert_eq!(
            row.keyless_header,
            if row.credential_slot.is_none() {
                KeylessHeaderPolicy::Suppress
            } else {
                KeylessHeaderPolicy::SendNone
            },
            "{}: the header policy must be derived from the credential slot",
            row.id
        );

        // Only the two local relays clamp the caller's output cap, to one eighth of the
        // context (262144 / 8 = 32768, `local-strata.ts:216`).
        assert_eq!(
            row.forced_max_tokens,
            if row.credential_slot.is_none() {
                Some(32_768)
            } else {
                None
            },
            "{}",
            row.id
        );

        // A price is either a real one or the zero that means "unknown", never a negative or a
        // NaN; the registry re-checks this, and a row that fails it here would be refused there.
        assert!(row.pricing.is_sane(), "{}", row.id);
        assert_eq!(row.pricing.cache_write, 0.0, "{}", row.id);
    }

    // The cache-write rate is never set by a row: the sibling package has no such rate, and
    // `Pricing::per_million` keeps it at zero by construction.
    assert_eq!(
        clients::CLIENTS
            .iter()
            .filter(|row| row.pricing != Pricing::free())
            .count(),
        24,
        "five rows carry no price at all (the nvidia family and merge/kimi-k2-6, whose rates the \
         TypeScript leaves at zero), and 24 carry the documented ones"
    );
}

// ---------------------------------------------------------------------------
// A7: the artifact, the order, and the catalog split
// ---------------------------------------------------------------------------

/// Builds the whole table against the real vault: this is the only proof that the rows were
/// written against the vault that exists, and it is also the A7 reconciliation.
#[test]
fn c_the_live_artifact_builds_every_row_and_the_discoverable_subset_matches_the_baseline() {
    let Some(vault) = artifact(
        "c_the_live_artifact_builds_every_row_and_the_discoverable_subset_matches_the_baseline",
    ) else {
        return;
    };
    let registry = Registry::try_new(&vault).unwrap_or_else(|error| panic!("{error}"));

    assert_eq!(registry.clients().len(), 29);
    assert_eq!(
        registry
            .clients()
            .iter()
            .map(|client| client.id())
            .collect::<Vec<_>>(),
        clients::CLIENTS
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        "the vault supplies pools and must not touch the order"
    );

    // The catalog's projection: the TypeScript's twelve discoverable models minus daytona, in
    // the same unsorted order (`model-class.test.ts:243-282`).
    assert_eq!(
        registry.discoverable_ids(),
        vec![
            "makora/glm-5.3",
            "makora/ds4-flash",
            "token-router/glm-5.3",
            "makora/glm5-flash",
            "token-router/glm-5.3-flash",
            "local/basalt",
            "token-router/sol",
            "sail/ds4-flash",
            "sail/glm5-flash",
            "sail/glm5",
            "sail/kimi3",
        ],
        "A7: eleven discoverable rows, in registry order"
    );

    // The other half of the same fact: eighteen rows are not in the catalog, and their order is
    // the registry's too (the projection filters, it never sorts).
    assert_eq!(
        registry
            .clients()
            .iter()
            .filter(|client| !client.is_discoverable())
            .map(|client| client.id())
            .collect::<Vec<_>>(),
        vec![
            "local/strata",
            "nvidia/glm-5.3",
            "nvidia/deepseek-v4-flash",
            "nvidia/kimi-k3",
            "merge/glm-5.3-flash",
            "merge/kimi-k3",
            "merge/kimi-k2-6",
            "open-router/glm-5.3-flash",
            "merge/glm-5.3",
            "vultr/glm-5.3-flash",
            "vultr/glm-5.3",
            "merge/mimo-2-6",
            "vultr/mimo-2-6",
            "merge/gpt-6-1-sol",
            "vultr/ds4-flash",
            "merge/ds4-flash",
            "merge/gpt-6-luna",
            "nvidia/ds4-flash",
        ],
        "A7: eighteen hidden rows"
    );

    // The reason for each hidden row, from the artifact itself. Seventeen rows carry
    // `enabled: false` (the table's own `.hidden()`), and exactly one row is hidden while it is
    // still enabled: open-router, whose only vault entry classifies `invalid`. That row is the
    // live proof of the TypeScript's `model-class.test.ts:288-296` - routable by id, absent
    // from the catalog - and the reason the credential pool is NOT filtered by usability.
    let hidden_but_enabled: Vec<&str> = registry
        .clients()
        .iter()
        .filter(|client| !client.is_discoverable() && client.enabled())
        .map(|client| client.id())
        .collect();
    assert_eq!(
        hidden_but_enabled,
        vec!["open-router/glm-5.3-flash"],
        "only the invalid-credential row is hidden by the vault rather than by the table"
    );
    assert_eq!(
        registry
            .find("open-router/glm-5.3-flash")
            .expect("the row is routable")
            .credential_label()
            .expect("the pool has one entry"),
        "Default",
        "its pool entry is the artifact's invalid placeholder, named but never valued"
    );

    // Every other row is discoverable exactly when the table enables it: the pools the artifact
    // supplies are live, so `enabled` is the only switch left - and it is a catalog switch, not
    // a routing one (test `g` proves the routing half).
    for row in clients::CLIENTS {
        if row.id == "open-router/glm-5.3-flash" {
            continue;
        }

        let client = registry.find(row.id).expect("every row is registered");

        assert_eq!(client.is_discoverable(), row.enabled, "{}", row.id);
    }
}

/// A row the table hides still resolves by id: `enabled` is a CATALOG switch, never a routing
/// switch (`private-chat-completion.ts` looks the id up with no filter at all).
#[test]
fn g_a_hidden_row_still_routes_by_id_and_an_unknown_id_resolves_to_none() {
    let Some(vault) =
        artifact("g_a_hidden_row_still_routes_by_id_and_an_unknown_id_resolves_to_none")
    else {
        return;
    };
    let registry = Registry::try_new(&vault).unwrap_or_else(|error| panic!("{error}"));

    for id in [
        "nvidia/glm-5.3",
        "local/strata",
        "open-router/glm-5.3-flash",
        "nvidia/ds4-flash",
    ] {
        let client = registry.find(id).expect("a hidden row is still routable");

        assert!(
            !client.is_discoverable(),
            "{id} must stay out of the catalog"
        );
        assert!(
            !registry.discoverable_ids().contains(&id),
            "{id} must not be advertised"
        );
    }

    // Exact match only: the TypeScript's lookup is `find(client => client.id === model)`, so a
    // prefix, a case difference, or a trailing space is a miss, not a fuzzy hit.
    assert!(registry.find("nvidia/glm-5").is_none());
    assert!(registry.find("NVIDIA/glm-5.3").is_none());
    assert!(registry.find("nvidia/glm-5.3 ").is_none());
    assert!(registry.find("").is_none());
}

// ---------------------------------------------------------------------------
// A4 / A5: rotation order, and the drain that follows it
// ---------------------------------------------------------------------------

/// The nvidia pool: six entries, document order, rotated one per failover.
///
/// This is A4 (the order is proven against the artifact, and the artifact's order is pinned by
/// the literal list as well) and A5 (the row leaves the catalog once the pool has drained).
#[test]
fn d_the_nvidia_credential_pool_is_the_document_order_and_rotation_walks_it_once() {
    let Some(vault) =
        artifact("d_the_nvidia_credential_pool_is_the_document_order_and_rotation_walks_it_once")
    else {
        return;
    };
    let registry = Registry::try_new(&vault).unwrap_or_else(|error| panic!("{error}"));

    let client = registry
        .find("nvidia/glm-5.3")
        .expect("the row is registered");

    // The pool as the loader holds it, and as the artifact files it: both are the expectation,
    // and the hard-coded list is what catches a re-sort.
    let declared = artifact_labels(&vault, "nvidia", "nvidia_api_keys");
    assert_eq!(
        declared,
        vec![
            "DevilAngel",
            "Erosire",
            "Devonshire",
            "DrakeOneAU",
            "DarwinWin",
            "MinhSpam"
        ],
        "the artifact files six nvidia keys in this order"
    );

    // Rotation walk: the entry in force, then one per `switch_base_url()`. Six labels, each
    // reached exactly once, in the document order.
    let mut observed = vec![
        client
            .credential_label()
            .expect("the pool has a head on build"),
    ];
    let mut rotations = 0_u64;
    while client.switch_base_url() {
        rotations += 1;
        observed.push(
            client
                .credential_label()
                .expect("a rotation always selects an entry"),
        );
    }

    assert_eq!(
        observed, declared,
        "rotation order is vault document order, and no entry is skipped or repeated"
    );
    assert_eq!(rotations, 5, "n entries give n - 1 rotations");
    assert_eq!(client.credential_label().as_deref(), Some("MinhSpam"));

    // A5: the exhausted pool takes the row out of the catalog - `apiKey = ''` in the
    // TypeScript, the sticky `drained` flag here.
    assert!(
        !client.is_discoverable(),
        "a drained row is not routable to the catalog"
    );
    assert!(
        !client.switch_base_url(),
        "the drain is sticky: there is no next key"
    );
    assert!(!registry.discoverable_ids().contains(&"nvidia/glm-5.3"));

    // DEVIATION (deliberate, recorded): the TypeScript clears `apiKey`, so its next request
    // would send `Bearer ` with an empty value; this port keeps the LAST credential selected,
    // because the transport's exhaustion fallback is documented behaviour (`advance` pins the
    // cursor to the last entry) and the row is unreachable through the catalog anyway.
    assert_eq!(client.credential_label().as_deref(), Some("MinhSpam"));

    // One shared instance per row: a clone rotates the SAME cursor, which is why
    // `registry.ts` exports a module-level array.
    let twin = registry
        .find("nvidia/ds4-flash")
        .expect("the row is registered");
    twin.reset_rotation();
    let clone = twin.clone();
    assert_eq!(twin.credential_label(), clone.credential_label());
    assert!(clone.switch_base_url());
    assert_eq!(
        twin.credential_label().as_deref(),
        Some("Erosire"),
        "the clone moved the shared cursor"
    );
}

/// The makora pool: one entry, and the `same-then-next` advance the family's `getNewKey`
/// implements (it returns the entry it shifted OFF). The observable consequence on a one-entry
/// pool is one wasted retry, then the drain.
#[test]
fn e_a_one_entry_pool_repeats_its_head_once_and_then_drains() {
    let Some(vault) = artifact("e_a_one_entry_pool_repeats_its_head_once_and_then_drains") else {
        return;
    };
    let registry = Registry::try_new(&vault).unwrap_or_else(|error| panic!("{error}"));
    let client = registry
        .find("makora/glm-5.3")
        .expect("the row is registered");

    let declared = artifact_labels(&vault, "makora", "makora_api_keys");
    assert_eq!(declared, vec!["Draco"], "the artifact files one makora key");
    assert_eq!(client.credential_label().as_deref(), Some("Draco"));

    // Rotation #0 hands back the SAME credential (`makora-glm-5-3.ts:6-11`), so the first
    // failover is a retry on the key that just failed.
    assert!(client.switch_base_url(), "the first rotation succeeds");
    assert_eq!(
        client.credential_label().as_deref(),
        Some("Draco"),
        "the same-then-next advance re-sends the entry the cursor already pointed at"
    );
    assert!(
        client.is_discoverable(),
        "the row is still routable: the pool has not run out yet"
    );

    // Rotation #1 has nothing left to advance to, and that is the drain.
    assert!(
        !client.switch_base_url(),
        "the second rotation finds no credential"
    );
    assert!(
        !client.is_discoverable(),
        "A5: the drain flips the catalog flag"
    );
    assert!(!client.switch_base_url(), "and it stays flipped");

    // Each row owns its OWN pool AND its own rotation ordinal: the sibling makora row was
    // untouched by that drain, so it starts its own `same-then-next` sequence - one retry on the
    // shared credential, then its own drain.
    let sibling = registry
        .find("makora/glm5-flash")
        .expect("the row is registered");

    assert!(
        sibling.is_discoverable(),
        "the drain of one row does not drain the rows that share its vault slot"
    );
    assert_eq!(sibling.credential_label().as_deref(), Some("Draco"));
    assert!(
        sibling.switch_base_url(),
        "the sibling is at its own rotation #0, so this is the retry"
    );
    assert!(sibling.is_discoverable());
    assert!(
        !sibling.switch_base_url(),
        "the sibling's own rotation #1 has nothing to advance to"
    );
    assert!(!sibling.is_discoverable(), "and it drains independently");
}

/// The relay axis: a credential-less relay rotates its NAMED endpoints, publishes each name as
/// `upstreamLabel`, and its discoverability does NOT depend on the pool (there is no key, so
/// `hasValidApiKey` is vacuously true - `model-class.test.ts:224,301,308`).
#[test]
fn f_a_relay_rotates_its_named_endpoints_in_document_order() {
    let Some(vault) = artifact("f_a_relay_rotates_its_named_endpoints_in_document_order") else {
        return;
    };
    let registry = Registry::try_new(&vault).unwrap_or_else(|error| panic!("{error}"));

    for (id, vendor, slot, labels) in [
        (
            "local/strata",
            "strata",
            "local_strata_api_endpoints",
            vec![
                "Carl",
                "Candy",
                "Markton",
                "James",
                "Parma",
                "DevonNotebook",
                "Nqqtran",
                "FranSoullin",
            ],
        ),
        (
            "local/basalt",
            "basalt",
            "local_basalt_api_endpoints",
            vec![
                "MolabEndless",
                "MarktonWillonLatest",
                "FamialPhotoFast",
                "DracoDravenFast",
                "DrakeJohnsonFast",
                "CornethOneThreeFast",
                "LandarMcTyreFast",
            ],
        ),
    ] {
        let client = registry.find(id).expect("the row is registered");
        let declared = artifact_labels(&vault, vendor, slot);
        let labels: Vec<String> = labels.iter().map(|label| (*label).to_owned()).collect();

        assert_eq!(
            declared, labels,
            "the artifact files this pool in this order"
        );
        assert_eq!(
            client.upstream_label().as_deref(),
            Some(labels[0].as_str()),
            "{id}: the head of the pool is in force on build"
        );
        assert_eq!(
            client.base_url().as_deref(),
            Some(
                vault.slot(vendor, slot).expect("the slot exists").entries()[0]
                    .value_str()
                    .expect("an endpoint entry has a URL")
            ),
            "{id}: the head URL is in force on build"
        );

        // Walk the whole pool, collecting the published label at each step.
        let mut observed = vec![labels[0].clone()];
        let mut rotations = 0_u64;
        while client.switch_base_url() {
            rotations += 1;
            observed.push(
                client
                    .upstream_label()
                    .expect("a relay row always publishes a label"),
            );
        }

        assert_eq!(
            observed, labels,
            "{id}: the relay names every endpoint it dialed, in document order"
        );
        assert_eq!(
            rotations,
            labels.len() as u64 - 1,
            "{id}: n entries, n - 1 rotations"
        );
        assert!(!client.switch_base_url(), "{id}: the pool is exhausted");

        // The endpoint drain leaves discoverability alone. `local/basalt` is still in the
        // catalog and `local/strata` still is not - the table decides that, and it is the same
        // answer the TypeScript gives, because `isDiscoverable` reads only `enabled` and
        // `hasValidApiKey`.
        assert_eq!(client.is_discoverable(), client.enabled(), "{id}");
    }

    // Every non-relay row has no label to publish: the vault labels its non-local endpoint
    // entries `STANDARD`, and publishing that would rewrite every request log line in the
    // deployment from `model: <id>` to `STANDARD: <id>`.
    for client in registry.clients() {
        if client.id() != "local/strata" && client.id() != "local/basalt" {
            assert!(client.upstream_label().is_none(), "{}", client.id());
            assert!(
                client.base_url().is_some(),
                "{}: a keyed row still dials its slot's default URL",
                client.id()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// A6 / A9: refusals, at the registry boundary
// ---------------------------------------------------------------------------

/// A relay label that looks like a credential or a URL is refused before anything is built, and
/// the refusal message refuses to echo it: printing the offending string is the leak this rule
/// exists to prevent.
#[test]
fn i_a_hostile_relay_label_is_refused_without_being_printed() {
    const HOSTILE: &str = "sk-proj-aaaaaaaaaaaaaaaaaaaaaa";
    assert_eq!(
        HOSTILE.len(),
        30,
        "the refusal message quotes the byte length"
    );

    // The hostile label SECOND in an otherwise usable pool: exactly one problem, the entry named
    // by its POSITION, and the offending string never printed.
    let vault = vault_doc_slots(KEYS, PARTIALLY_HOSTILE_ENDPOINTS);
    let error = Registry::try_from_rows(&[PROBE_RELAY], &vault).expect_err("the build refuses");

    assert_eq!(
        error.problems(),
        &[
            "probe/relay: endpoint entry #1 of slot 'test_api_endpoints' has a relay name this \
             layer refuses to print (30 bytes, credential- or URL-shaped)"
        ],
        "the entry is named by POSITION in the pool, not by its content"
    );
    assert!(!error.to_string().contains(HOSTILE), "{error}");

    // The hostile entry as the pool's ONLY entry: the refusal now also empties the pool, and both
    // problems arrive in one pass - which is the collect-then-refuse contract, visible.
    let vault = vault_doc_slots(KEYS, CREDENTIAL_SHAPED_LABEL);
    let error = Registry::try_from_rows(&[PROBE_RELAY], &vault).expect_err("the build refuses");

    assert_eq!(
        error.problems(),
        &[
            "probe/relay: endpoint entry #0 of slot 'test_api_endpoints' has a relay name this \
             layer refuses to print (30 bytes, credential- or URL-shaped)",
            "probe/relay: the endpoint pool is empty, so there is nothing to dial"
        ],
        "a relay the vault left with nothing to dial is named as such, beside the cause"
    );

    // The same document is fine for a KEYED row: those rows drop the label, so nothing unsafe
    // ever reaches a log line. That asymmetry is a log-line property, not a data-quality one.
    let registry = Registry::try_from_rows(&[PROBE], &vault)
        .unwrap_or_else(|error| panic!("a keyed row must accept the document: {error}"));
    let client = registry.find("probe/keyed").expect("the row is registered");

    assert!(client.upstream_label().is_none());
    assert_eq!(client.base_url().as_deref(), Some("http://127.0.0.1:1/v1"));
    assert_eq!(client.credential_label().as_deref(), Some("One"));

    // An endpoint entry whose VALUE is a sentinel is still dialed: the TypeScript's failover list
    // is `Object.keys(ENDPOINTS)` with no usability filter, and this layer keeps that behaviour
    // for endpoints exactly as test `c` keeps it for credentials.
    let vault = document(&vendor_array(
        KEYS,
        r#"{
            "id": "test_api_endpoints", "kind": "endpoints", "secret": false, "optional": true,
            "retired": false, "nesting": "flat",
            "entries": [{ "label": "Alpha", "value": "NotAvailable", "validity": "invalid" }],
            "default": "NotAvailable"
        }"#,
    ));
    let registry = Registry::try_from_rows(&[PROBE_RELAY], &vault)
        .unwrap_or_else(|error| panic!("a printable label is enough: {error}"));

    assert_eq!(
        registry
            .find("probe/relay")
            .expect("the row is registered")
            .upstream_label()
            .as_deref(),
        Some("Alpha"),
        "the label is printable, so the relay publishes it"
    );
    assert_eq!(
        registry
            .find("probe/relay")
            .expect("the row is registered")
            .base_url()
            .as_deref(),
        Some("NotAvailable/v1"),
        "the entry is selected unfiltered, whatever its value, and `Endpoint::new` rebases the \
         path-less value to the `/v1` form it dials (`normalize_base_url`)"
    );
}

/// A nested (group) credential entry cannot be rotated by a client row: its value is `null`, so
/// the SDK would be handed an object as a key. The port refuses it instead.
#[test]
fn j_a_nested_credential_group_is_refused_naming_the_group() {
    let vault = vault_doc(NESTED_KEYS);
    let error = Registry::try_from_rows(&[PROBE], &vault).expect_err("the build refuses");

    assert_eq!(
        error.problems(),
        &[
            "probe/keyed: credential slot 'test_api_keys' contains the nested group 'Group'; no \
             client row rotates a group, so the row must name a flat slot"
        ]
    );
}

/// Every shape the table does not contain is refused with the reason the row can act on, and
/// nothing is coerced into a guess.
#[test]
fn k_a_miswired_row_is_refused_by_name_never_repaired() {
    let vault = vault_doc(KEYS);

    // -- the vault lookups -------------------------------------------------------------------
    let mut row = PROBE;
    row.vault_vendor = "nowhere";
    let error = Registry::try_from_rows(&[row], &vault).expect_err("the vendor lookup fails");
    assert_eq!(
        error.problems(),
        &[
            "probe/keyed: the vault document has no vendor 'nowhere' (a registry build never \
             falls back to a default, so a vault rename must be mirrored in the row)"
        ],
        "the vendor miss is reported once, and the fallback it refuses is named"
    );

    let mut row = PROBE;
    row.endpoint_slot = "absent_api_endpoints";
    let error =
        Registry::try_from_rows(&[row], &vault).expect_err("the endpoint slot lookup fails");
    assert_eq!(
        error.problems(),
        &["probe/keyed: the vault vendor 'test' has no endpoint slot 'absent_api_endpoints'"]
    );

    let mut row = PROBE;
    row.credential_slot = Some("absent_api_keys");
    let error =
        Registry::try_from_rows(&[row], &vault).expect_err("the credential slot lookup fails");
    assert_eq!(
        error.problems(),
        &["probe/keyed: the vault vendor 'test' has no credential slot 'absent_api_keys'"]
    );

    // -- the slot kinds ----------------------------------------------------------------------
    // A public slot used as a credential would put base URLs in `Authorization`.
    let mut row = PROBE;
    row.credential_slot = Some("test_api_endpoints");
    let error = Registry::try_from_rows(&[row], &vault).expect_err("the kind check fires");
    assert_eq!(
        error.problems(),
        &[
            "probe/keyed: 'test_api_endpoints' is a public slot in the vault vendor 'test', but \
             the row uses it as its credential slot"
        ]
    );

    // A secret slot used as the endpoint pool is the mirror image of the same mistake.
    let mut row = PROBE;
    row.endpoint_slot = "test_api_keys";
    let error = Registry::try_from_rows(&[row], &vault).expect_err("the kind check fires");
    assert_eq!(
        error.problems(),
        &[
            "probe/keyed: 'test_api_keys' is a secret slot in the vault vendor 'test', but the \
             row uses it as its endpoint slot"
        ]
    );

    // -- the pools those lookups produce ------------------------------------------------------
    // Both emptiness problems this registry carries are a SECOND line of defence, and the split
    // is worth pinning: the loader refuses a slot with no entries before any row can reach the
    // registry with one, so the reachable path to "the endpoint pool is empty" is the label
    // refusal in test `i` (every entry refused leaves the pool empty), while an empty CREDENTIAL
    // pool can only come from a caller that bypassed the loader. Test `i` pins the first; this
    // pins the boundary that keeps the second unreachable.
    let error = vault::load_from_str(&envelope(&vendor_array(EMPTY_KEYS, ENDPOINTS)))
        .expect_err("a vault slot with no entries is refused by the loader");
    assert_eq!(
        error.problems(),
        &[
            "vendor `test` slot `test_api_keys` has no entries: a slot with no entries has \
             neither a rotation pool nor a default, and every live map has at least one key"
        ],
        "the two layers divide the same shape: the loader refuses the document, the registry \
         would refuse the row"
    );

    // -- the row's own contradictions ---------------------------------------------------------
    let mut row = PROBE_RELAY;
    row.credential_slot = Some("test_api_keys");
    let error = Registry::try_from_rows(&[row], &vault_doc(KEYS))
        .expect_err("a relay with a credential is refused");
    assert_eq!(
        error.problems(),
        &[
            "probe/relay: a relay row declares a credential slot; these upstreams have no key to \
             rotate, so the failover axis is the endpoint pool"
        ]
    );

    // A draining row with NO credential axis drains nothing, and could never flip off the
    // catalog - the one thing that shape exists for.
    let mut row = PROBE;
    row.credential_slot = None;
    row.behavior = Behavior::ChatCredentialDrain {
        advance: CredentialAdvance::SameThenNext,
    };
    let error = Registry::try_from_rows(&[row], &vault_doc(KEYS))
        .expect_err("a drain row with no credential axis is refused");
    assert_eq!(
        error.problems(),
        &[
            "probe/keyed: a credential-draining row has no credential slot, so it drains nothing \
             and could never flip off the catalog"
        ]
    );

    let mut row = PROBE;
    row.behavior = Behavior::ResponsesPassthrough;
    let error = Registry::try_from_rows(&[row], &vault_doc(KEYS))
        .expect_err("the passthrough contradiction fires");
    assert_eq!(
        error.problems(),
        &[
            "probe/keyed: the Responses passthrough injects no body fields, so requiring the \
             usage option is a contradiction"
        ]
    );

    let mut row = PROBE;
    row.behavior = Behavior::ResponsesPassthrough;
    row.requires_usage_option = false;
    let error = Registry::try_from_rows(&[row], &vault_doc(KEYS)).err();
    assert!(
        error.is_none(),
        "the passthrough is a legal row once it stops asking for a field it never injects"
    );

    // -- money, and the identity of a row ------------------------------------------------------
    let mut row = PROBE;
    row.pricing = Pricing {
        input: f64::NAN,
        cache_read: 0.0,
        cache_write: 0.0,
        output: -1.0,
    };
    let error = Registry::try_from_rows(&[row], &vault_doc(KEYS))
        .expect_err("an unusable price is refused");
    assert_eq!(
        error.problems(),
        &[
            "probe/keyed: a price is not a usable money value (every rate must be finite and \
             non-negative)"
        ],
        "a NaN and a negative are reported as one problem for the row, not two"
    );

    let mut row = PROBE;
    row.id = "";
    let error =
        Registry::try_from_rows(&[row], &vault_doc(KEYS)).expect_err("an id-less row is refused");
    assert_eq!(error.problems(), &[": the row has no routing id"]);
}

/// The problem list is the payload: every refused row is named exactly once, with its id, in
/// row order - and a duplicate id is reported once (attributed to the later row) instead of as
/// a mirrored pair.
#[test]
fn l_problems_are_reported_as_one_attributed_list_per_row() {
    let vault = vault_doc(KEYS);

    // Two rows, two different causes, both in one pass.
    let mut later = PROBE;
    later.id = "probe/later";
    later.vault_vendor = "nowhere";
    let mut broken = PROBE;
    broken.id = "probe/broken";
    broken.pricing = Pricing::per_million(0.1, f64::INFINITY, 0.2);

    let error =
        Registry::try_from_rows(&[broken, later], &vault).expect_err("both rows are refused");
    assert_eq!(
        error.problems(),
        &[
            "probe/broken: a price is not a usable money value (every rate must be finite and \
             non-negative)",
            "probe/later: the vault document has no vendor 'nowhere' (a registry build never \
             falls back to a default, so a vault rename must be mirrored in the row)"
        ],
        "the list is in row order, so a reader fixes the table top to bottom"
    );
    assert_eq!(
        error.to_string(),
        "2 registry problem(s):\n  - probe/broken: a price is not a usable money value (every \
         rate must be finite and non-negative)\n  - probe/later: the vault document has no \
         vendor 'nowhere' (a registry build never falls back to a default, so a vault rename \
         must be mirrored in the row)",
        "the count leads, so a truncated log line still says how much it dropped"
    );

    // A collision is a property of the PAIR: one problem, attributed to the later row.
    let error =
        Registry::try_from_rows(&[PROBE, PROBE], &vault).expect_err("the collision is refused");
    assert_eq!(
        error.problems(),
        &[
            "probe/keyed: duplicate id: two rows claim the same routing key, and the router \
             resolves by id"
        ]
    );

    // A build that succeeds has no problems to print, and the empty table is a legal one: the
    // failure mode of the TypeScript (an `undefined` vault export resolved at import time) has
    // no counterpart here.
    let empty = Registry::try_from_rows(&[], &vault_doc(KEYS))
        .unwrap_or_else(|error| panic!("an empty table is a valid table: {error}"));
    assert!(empty.clients().is_empty());
    assert!(empty.discoverable_ids().is_empty());
    assert!(empty.find("probe/keyed").is_none());
}

// ---------------------------------------------------------------------------
// A9: the process-wide instance
// ---------------------------------------------------------------------------

/// The process registry: installed once, never rebuilt, and never derived from an ambient file
/// read. The counters it aggregates are the fleet roll-up the stats route projects.
#[test]
fn m_the_process_registry_installs_once_and_aggregates_every_row_ledger() {
    let Some(vault) =
        artifact("m_the_process_registry_installs_once_and_aggregates_every_row_ledger")
    else {
        return;
    };

    let installed = Registry::initialize_process(&vault).unwrap_or_else(|error| panic!("{error}"));
    let again = Registry::process().expect("the install took effect");
    assert!(std::ptr::eq(installed, again), "one instance per process");

    // A SECOND document cannot replace it: rebuilding would reset the rotation state the whole
    // design shares, which is why the first successful call wins. The document below holds a
    // single synthetic vendor, so if a rebuild DID happen this call would fail on the missing
    // `nvidia` vendor instead of returning the same instance.
    let conflicting = vault_doc(KEYS);
    let kept = Registry::initialize_process(&conflicting)
        .unwrap_or_else(|error| panic!("a second install returns the first: {error}"));
    assert!(std::ptr::eq(installed, kept), "no rebuild, no reset");
    assert_eq!(installed.clients().len(), 29);
    assert_eq!(installed.discoverable_ids().len(), 11);

    // The roll-up: `aggregate()` is the field-by-field sum of the row ledgers, and cost is
    // deliberately NOT part of it (each row prices its own tokens at its own rates).
    let aggregate = installed.aggregate();
    let mut fold = typecrate_provider::accounting::Totals::default();
    for client in installed.clients() {
        fold.add(&client.counters());
    }

    assert_eq!(aggregate, fold);
    assert_eq!(aggregate.counters.request_total, 0);
    assert_eq!(aggregate.counters.attempt_total, 0);
    assert_eq!(aggregate.counters.consecutive_failures, 0);

    // Nothing in this crate reached for the filesystem or the environment: the only way to
    // obtain a registry is to hand it the document.
    assert!(
        std::env::var_os("NOOBS_TYPECRATE_PROVIDER_VAULT").is_none(),
        "the registry has no environment input to begin with"
    );
}
