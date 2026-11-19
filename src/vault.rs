//! Vault artifact loader: the credential/endpoint substrate, read as DATA.
//!
//! TypeScript origin: the artifact contract in `config/vault/src/vault/**` -
//! `types.ts` (every shape), `sentinels.ts:vaultEntryValidity` (the classifier ported
//! below), `slots.ts:vaultSlotSecret`/`vaultSlotOptional` (the kind-derived flags
//! re-validated here), `slots.ts:vaultNestedEntries` (the group/leaf shape and its
//! structural group validity) and `snapshot.ts` (the emitted key order and
//! `default = entries[0].value`) - plus `config/vault/scripts/export-vault.ts`, the
//! writer of `config/vault/generated/<scope>.json`. The normative Rust-side rules are
//! `config/vault/knowledge.md`, section "Rust consumption contract"; this module is the
//! implementation of that section and is meant to be readable without any TypeScript.
//!
//! WHAT REPLACED WHAT. `@agentic/provider` gets its credentials through named
//! `import { NVIDIA_API_KEYS } from '@config/vault'` specifiers: the values are baked
//! into the JavaScript bundle at build time, so a TS build that ships without them ships
//! nothing. A Rust build has no such specifier. It reads ONE document that
//! `yarn workspace @config/vault export -- --consumer agentic-provider` wrote, and the
//! document is the whole interface: 11 vendors / 20 slots / 43 entries in the
//! `agentic-provider` scope. That is why the loader, not a code generator, is the first
//! module of this crate - every later module (transport, clients, routes) reads pools
//! through it.
//!
//! LOADER SHAPE - injected, never ambient (`config/vault/knowledge.md` "Loader shape",
//! following `packages/typecrate/harness/src/config.rs:13-19` and
//! `src/environment.rs`):
//!   * Nothing here reads an environment variable or a default path. [`load_from_str`]
//!     takes the document, [`load_from_path`] takes the caller's path; a `build.rs` or a
//!     bin that wants `config/vault/generated/agentic-provider.json` resolves that path
//!     itself and hands it in. The vault has no ambient input, and neither does this.
//!   * VALIDATE, NEVER COERCE. A bad artifact is refused, not patched: no missing field
//!     is filled with a plausible default, no wrong-typed field is stringified, no
//!     disagreeing pair is resolved silently. The only place the contract's
//!     "entries[0] wins" tie-break is expressed is [`VaultSlot::head`], and even there
//!     the disagreement is reported as a problem, so a load that succeeds never hides one.
//!   * ALL problems come back at once ([`VaultError::problems`]). `deny_unknown_fields`
//!     short-circuits by itself (a document cannot be walked past a field it cannot
//!     name), but every rule after parsing is collected, because the operator debugging a
//!     rejected artifact wants every defect, not the first one.
//!
//! CREDENTIALS NEVER LEAK. Every value the artifact carries is held in [`Secret`], whose
//! `Debug`/`Display` are hand-written to say `<redacted>`, so no `{:?}` of any struct in
//! this module can print a key into a log line - including the `serde` diagnostics, whose
//! value echoes go through [`redact_parse_message`]. Endpoint values are public config
//! (`slots.ts:vaultSlotSecret` says only `keys` slots are secret), so the same values are
//! reachable un-redacted through [`VaultSlot::public_values`] on a non-secret slot, and
//! through the explicitly named [`Secret::expose`] everywhere else. `cargo test` proves
//! both directions (`a_credential_value_can_never_be_printed`).
//!
//! ORDER IS LOAD-BEARING. Pools are `Vec`, never a map: `entries[0]` is the live default
//! and the head of the rotation queue (`knowledge.md` "Ordering is load-bearing"; the TS
//! consumers rotate with `Object.keys` + `shift()` in
//! `packages/agentic/provider/src/nvidia/nvidia-glm5-flash.ts:9-18`). Deserialising into
//! `HashMap`/`BTreeMap` would silently change which credential every consumer actually
//! uses, so nothing in this module sorts, and `nvidia_credential_pool_keeps_the_documents_own_order`
//! asserts the loaded order against the document's own array order.

use std::collections::HashSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// The only schema version this crate can read (`snapshot.ts:VAULT_SCHEMA_VERSION`,
/// emitted as `schema_version`). An artifact with a different one is refused rather than
/// guessed at: `deny_unknown_fields` is the field-level version gate and this constant is
/// the value-level one (`knowledge.md:203-207`).
pub const SCHEMA_VERSION: u32 = 1;

/// The `package` field the artifact must carry. It is verified before any value is
/// trusted, because the document is the credential source: a file that claims to come
/// from somewhere else is not this vault (`knowledge.md:167`).
pub const PACKAGE_NAME: &str = "@config/vault";

/// The suffix that DEFINES the placeholder class (`sentinels.ts:VAULT_PLACEHOLDER_SUFFIX`,
/// `:57-61`). It is a suffix rule rather than a closed set on purpose: a new placeholder
/// appears by editing a vendor module and must classify without editing a Rust release.
pub const PLACEHOLDER_SUFFIX: &str = "_PLACEHOLDER";

/// The built-in keyless floor (`sentinels.ts:VAULT_KEYLESS_SENTINELS` = `['none']`, whose
/// authority is `model-class.ts:32-35`: `none` means "this endpoint needs no credential"
/// and the model STAYS discoverable - it is not a failure).
///
/// The classifier reads its vocabulary from the artifact's `sentinels` block and UNIONS
/// these built-in floors with it, so a document that narrowed the list can never silently
/// demote a valid keyless marker. Nothing here ever NARROWS the artifact's vocabulary.
pub const KEYLESS_SENTINELS: [&str; 1] = ["none"];

/// The built-in invalid floor: verbatim `sentinels.ts:VAULT_INVALID_SENTINELS`, which is
/// itself a copy of `model-class.ts:39-44` (`INVALID_API_KEY_VALUES`): `NotAvailable` is
/// the Open Router stand-in, `error` is the OpenAI SDK's `apiKey ?? 'error'` substitution,
/// and `null`/`undefined` are stringified missing env vars.
pub const INVALID_SENTINELS: [&str; 4] = ["NotAvailable", "error", "null", "undefined"];

/// A quoted run in a `serde` diagnostic is redacted once it is at least this long.
///
/// WHY THIS EXISTS. `serde` puts the offending value in its message
/// (`invalid type: string "sk-live-...", expected u32`), and that message becomes part of
/// [`VaultError`], which is exactly the kind of thing an operator logs. Field and variant
/// names are in backticks and stay (they are never secrets), while a quoted payload this
/// long is replaced by `"<redacted:len>"`. Real credentials in this vault are far longer
/// than 9 characters, so the threshold trades nothing for the guarantee, and the rule is
/// visible in `a_credential_value_can_never_be_printed`.
const REDACT_MIN_LEN: usize = 9;

/// The one nesting level the contract emits below a `nesting: "nested"` group
/// (`slots.ts:vaultNestedEntries` maps `Object.entries(source)` once and its
/// `children` are leaves). Deeper nesting is a schema change, so it is refused.
const MAX_NESTING_DEPTH: usize = 1;

// ---------------------------------------------------------------------------
// Secret: the value newtype that makes redaction structural
// ---------------------------------------------------------------------------

/// A vault value, redacted in every formatted output unless the caller asks.
///
/// This is the type behind the leak rule: entries and defaults are `Secret`, so `Debug`
/// on [`VaultConfig`] (derived) cannot reach a credential - it reaches this type's
/// hand-written `Debug`, which prints a fixed marker. `Serialize`/`Deserialize` are
/// transparent, so the artifact bytes are unchanged in either direction.
///
/// EDGE CASES handled deliberately:
///   * `Display` is redacted too, because `println!("{value}")` is the easier way to leak.
///   * The alternate (`{:#}`) form is also redacted: the marker is written with
///     `write_str`, which ignores fill/alternate flags entirely.
///   * `PartialEq`/`Eq`/`Hash`/`Ord` compare the real value: comparing two credentials is
///     not a leak (the caller already holds both), and `default == entries[0].value` needs it.
///   * No `Default` on purpose: an empty `Secret` would be an unset credential that looks
///     set, and "unset" is already a validity class (`VaultValidity::Empty`).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// Wraps a value the caller already holds (a header builder, a test, a fixture).
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The un-redacted value - the name is deliberately loud, because this is the one
    /// place a credential leaves the redacted world (the `Authorization` header builder).
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Takes ownership of the value for a caller that has to move it into a request.
    #[must_use]
    pub fn into_exposed(self) -> String {
        self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Fixed text, so no flag of the formatter can widen it into the value.
        formatter.write_str("Secret(\"<redacted>\")")
    }
}

impl std::fmt::Display for Secret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

// ---------------------------------------------------------------------------
// The five validity classes and the classifier
// ---------------------------------------------------------------------------

/// How usable one vault value is (`types.ts:VaultValidity`, the table in
/// `knowledge.md` "The 'no key' vocabularies").
///
/// The five classes must stay distinguishable, because their CONSEQUENCES differ:
/// `keyless` is a working configuration (no credential needed), `placeholder` is a vendor
/// stand-in the TS services happily build clients with while the provider must not treat
/// it as a credential, `invalid` is a known-bad token, and `empty` is "unset" - reported
/// apart from `invalid` so an operator can tell a missing value from a poisoned one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VaultValidity {
    /// A real credential or endpoint: usable. The `Default` is the optimistic one, which
    /// is only ever observable on a value this crate constructed by hand.
    #[default]
    Valid,
    /// The `none` marker: **valid**, the endpoint needs no credential. Not an error.
    Keyless,
    /// A `*_PLACEHOLDER` stand-in: unusable as a credential, and a class of its own.
    Placeholder,
    /// `NotAvailable` / `error` / `null` / `undefined` (or a value the artifact's own
    /// vocabulary matches): outright unusable.
    Invalid,
    /// Empty or whitespace-only: unset.
    Empty,
}

impl VaultValidity {
    /// The literal the artifact uses for this class (`snake_case`, matching
    /// `types.ts:VaultValidity`).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Keyless => "keyless",
            Self::Placeholder => "placeholder",
            Self::Invalid => "invalid",
            Self::Empty => "empty",
        }
    }

    /// Whether a model client may actually use this value.
    ///
    /// `Valid` and `Keyless` only. A `keyless` entry participates in rotation because a
    /// request that needs no credential is a request that works
    /// (`model-class.ts:32-35`, call sites `src/local/local-strata.ts:149`,
    /// `src/local/local-basalt.ts:144`, `src/daytona/daytona-qwen3-8.ts:451`).
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Valid | Self::Keyless)
    }
}

/// `types.ts:VaultSlotKind`. The four kinds the registry declares today.
///
/// An unknown kind fails DESERIALISATION (serde rejects the unknown variant), which is the
/// right failure: a fifth kind is a schema-level change and this crate is the one that
/// predates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VaultSlotKind {
    /// A credential pool. The only kind whose values are secrets (`slots.ts:vaultSlotSecret`).
    #[default]
    Keys,
    /// An endpoint pool: public config, and caller-overridable per call
    /// (`slots.ts:vaultSlotOptional`).
    Endpoints,
    /// A username pool (Docker Hub image namespaces): public, and required, so not optional.
    Usernames,
    /// The nested GPU-service registry (`comfy_cloud_service_endpoints`): public but
    /// authoritative, and not caller-overridable.
    ServiceEndpoints,
}

impl VaultSlotKind {
    /// The literal the artifact uses for this kind.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Keys => "keys",
            Self::Endpoints => "endpoints",
            Self::Usernames => "usernames",
            Self::ServiceEndpoints => "service_endpoints",
        }
    }
}

/// `types.ts:VaultNesting`: whether a slot's entries are leaves (`flat`) or a group list
/// whose real endpoints sit in `children` (`nested`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VaultNesting {
    /// Every entry is a `{ label, value, validity }` leaf. All 20 slots of the
    /// `agentic-provider` scope are flat.
    #[default]
    Flat,
    /// Group entries carry `value: null` plus `children`. Only `--consumer all` emits one
    /// (the `modal` vendor's `comfy_cloud_service_endpoints`), which is why the structs
    /// below model `value`/`children`/`default` as `Option`: a struct reused ACROSS SCOPES
    /// must load both documents (`knowledge.md` F1, "the cross-scope Options").
    Nested,
}

// ---------------------------------------------------------------------------
// The classifier vocabulary
// ---------------------------------------------------------------------------

/// The `sentinels` block: the three literal vocabularies the document ships with it.
///
/// These are classifier VOCABULARY, not credentials, so they are plain `String`s and may
/// be printed. They are deliberately scope-INDEPENDENT in the artifact
/// (`knowledge.md` "CAVEAT" under Scope guarantee 2): the `agentic-provider` document
/// carries the `exa`/`zylora` placeholder literals even though that scope never receives
/// those vendors, because a scope-filtered vocabulary would change meaning per scope.
///
/// Note the `placeholder` list is the OBSERVED literals, while the classifier rule is the
/// `*_PLACEHOLDER` SUFFIX (`sentinels.ts:57-61`); this crate honours both, as a union.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default, deny_unknown_fields)]
pub struct VaultSentinels {
    keyless: Vec<String>,
    invalid: Vec<String>,
    placeholder: Vec<String>,
}

impl VaultSentinels {
    /// Builds a vocabulary directly (fixtures, and any caller that classifies without a
    /// document). The artifact's own block is the normal source:
    /// [`VaultConfig::sentinels`].
    #[must_use]
    pub fn new(keyless: Vec<String>, invalid: Vec<String>, placeholder: Vec<String>) -> Self {
        Self {
            keyless,
            invalid,
            placeholder,
        }
    }

    /// The literals that mean "no credential needed" (valid).
    #[must_use]
    pub fn keyless(&self) -> &[String] {
        &self.keyless
    }

    /// The literals that mean "outright unusable".
    #[must_use]
    pub fn invalid(&self) -> &[String] {
        &self.invalid
    }

    /// The literals the vault observed as placeholders when it built this document.
    #[must_use]
    pub fn placeholder(&self) -> &[String] {
        &self.placeholder
    }

    /// Classify one value against THIS vocabulary, unioned with the built-in floors.
    ///
    /// Check order is load-bearing and ports `sentinels.ts:73-98` exactly:
    ///   1. trimmed empty -> `empty`
    ///   2. trimmed in the keyless set -> `keyless`   (BEFORE invalid, so a future
    ///      sentinel collision cannot demote a valid keyless marker)
    ///   3. trimmed in the invalid set -> `invalid`
    ///   4. trimmed ends with `_PLACEHOLDER`, or is in the observed placeholder list ->
    ///      `placeholder`
    ///   5. otherwise -> `valid`
    ///
    /// Trimming is for CLASSIFICATION only; the emitted value stays the original string,
    /// so a rotating consumer never sees a silently rewritten credential
    /// (`sentinels.ts:85-87`).
    ///
    /// The TS classifier's first branch ("a non-string is `invalid`", `sentinels.ts:74`,
    /// needed there because `nvidia.ts:3` is annotated `any`) is unrepresentable here:
    /// `value` is typed, so a non-string is refused at parse time instead.
    ///
    /// DIVERGENCE worth knowing: Rust's `str::trim` uses `char::is_whitespace` and does
    /// not strip `U+FEFF`, which JavaScript's `String.prototype.trim` does. A value whose
    /// only whitespace is a BOM would classify `valid` here and `empty` there; the
    /// cross-check in [`validate`] turns that into a refusal, never a silent difference.
    #[must_use]
    pub fn classify(&self, value: &str) -> VaultValidity {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return VaultValidity::Empty;
        }
        if matches_any(trimmed, &self.keyless) || KEYLESS_SENTINELS.contains(&trimmed) {
            return VaultValidity::Keyless;
        }
        if matches_any(trimmed, &self.invalid) || INVALID_SENTINELS.contains(&trimmed) {
            return VaultValidity::Invalid;
        }
        if trimmed.ends_with(PLACEHOLDER_SUFFIX) || matches_any(trimmed, &self.placeholder) {
            return VaultValidity::Placeholder;
        }
        VaultValidity::Valid
    }
}

/// Classify against the built-in floor vocabulary, with no document in hand.
///
/// Convenience for callers that pre-screen a string (a request override, a fixture);
/// the artifact-driven path is [`VaultSentinels::classify`], which also honours literals a
/// newer vault added.
#[must_use]
pub fn classify(value: &str) -> VaultValidity {
    VaultSentinels::new(
        KEYLESS_SENTINELS
            .iter()
            .map(|literal| (*literal).to_owned())
            .collect(),
        INVALID_SENTINELS
            .iter()
            .map(|literal| (*literal).to_owned())
            .collect(),
        Vec::new(),
    )
    .classify(value)
}

/// Exact (whitespace-TRIMMED) membership. Case-sensitive, like `Array.includes` in
/// `sentinels.ts:94-95`: `Error` is NOT `error`.
fn matches_any(trimmed: &str, literals: &[String]) -> bool {
    literals.iter().any(|literal| literal.trim() == trimmed)
}

// ---------------------------------------------------------------------------
// The document
// ---------------------------------------------------------------------------

/// One pool entry: `{ label, value, validity }`, plus `children` on a group entry.
///
/// `label` is the source map key (account / relay / GPU-server name) and is user-visible
/// upstream configuration: `src/local/local-strata.ts:13` and `src/local/local-basalt.ts:13`
/// publish it as `upstreamLabel`. It is kept verbatim.
///
/// `value` is `Option` because a `nesting: "nested"` GROUP entry carries `value: null` and
/// puts its data in `children`; `children` is `Option` because a flat entry never carries
/// the key at all. A struct reused across scopes needs both (`knowledge.md` F1).
/// `skip_serializing_if` on `children` keeps a re-serialised flat entry byte-identical to
/// what `export-vault.ts` wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default, deny_unknown_fields)]
pub struct VaultEntry {
    label: String,
    value: Option<Secret>,
    validity: VaultValidity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    children: Option<Vec<VaultEntry>>,
}

impl VaultEntry {
    /// The pool key, verbatim (`upstreamLabel` upstream).
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The class this entry was emitted with (cross-checked against the document's own
    /// sentinel vocabulary at load time, so it cannot disagree with either side).
    #[must_use]
    pub fn validity(&self) -> VaultValidity {
        self.validity
    }

    /// Shortcut for `entry.validity().is_usable()`.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.validity.is_usable()
    }

    /// A `value: null` entry is a group: descend into [`Self::children`].
    #[must_use]
    pub fn is_group(&self) -> bool {
        self.value.is_none()
    }

    /// The value as a redacted handle - safe to store, move and `{:?}`.
    #[must_use]
    pub fn value(&self) -> Option<&Secret> {
        self.value.as_ref()
    }

    /// The value as a plain `&str`.
    ///
    /// PRINT THIS ONLY WHEN THE OWNING SLOT REPORTS `is_secret() == false`. It exists for
    /// endpoint/usernames/service_endpoints pools, whose values are public configuration;
    /// a `keys` slot's value is a credential and should go through [`Secret::expose`] at
    /// the point of use, where the intent is visible.
    #[must_use]
    pub fn value_str(&self) -> Option<&str> {
        self.value.as_ref().map(Secret::expose)
    }

    /// The leaves of a group entry; `None` on every flat entry.
    #[must_use]
    pub fn children(&self) -> Option<&[VaultEntry]> {
        self.children.as_deref()
    }
}

/// One slot: `{ id, kind, secret, optional, retired, nesting, entries, default }`.
///
/// Field order matches the emitted key order (`knowledge.md` "Emitted key order"), so a
/// re-serialised document keeps its shape. `entries` is a `Vec`: order is the pool order,
/// and `entries[0]` is both the live default and the rotation head.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default, deny_unknown_fields)]
pub struct VaultSlot {
    id: String,
    kind: VaultSlotKind,
    secret: bool,
    optional: bool,
    retired: bool,
    nesting: VaultNesting,
    entries: Vec<VaultEntry>,
    /// `Option<Secret>`: `null` only on the nested slot (its source map exports no
    /// `*_DEFAULT`), and `Secret` rather than the contract's `String` so the value cannot
    /// leak through `{:?}` on a credential slot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    r#default: Option<Secret>,
}

impl VaultSlot {
    /// The slot id: the source export name, lower-cased (`NVIDIA_API_KEYS` ->
    /// `nvidia_api_keys`).
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn kind(&self) -> VaultSlotKind {
        self.kind
    }

    /// `true` exactly for `kind: "keys"`: the values are credentials and need a runtime
    /// handoff. `false` means public configuration - NOT "unimportant"
    /// (`comfy_cloud_service_endpoints` is public but authoritative).
    #[must_use]
    pub fn is_secret(&self) -> bool {
        self.secret
    }

    /// `true` exactly for `kind: "endpoints"`: a caller may bring its own value per call.
    #[must_use]
    pub fn is_optional(&self) -> bool {
        self.optional
    }

    /// `true` when no declared scope reads this slot today. A crate may skip a retired
    /// slot, but must not delete the variant: `retired` is derived from the scope table and
    /// flips when a consumer is added.
    #[must_use]
    pub fn is_retired(&self) -> bool {
        self.retired
    }

    #[must_use]
    pub fn nesting(&self) -> VaultNesting {
        self.nesting
    }

    /// The pool, in document order. Never sorted, never re-keyed.
    #[must_use]
    pub fn entries(&self) -> &[VaultEntry] {
        &self.entries
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The labels in pool order (what an endpoint failover iterates:
    /// `src/local/local-strata.ts:13`).
    ///
    /// NOTE on the missing `#[must_use]` across every `impl Iterator` getter in this module
    /// (`labels`, `usable`, `slots_of_kind`, `*_slots`, `slots`): the `Iterator` trait is
    /// already `#[must_use]`, so adding the attribute trips `clippy::double_must_use`
    /// (warning-level, and `lint`/`lint:strict` are both clean). Value getters keep the
    /// attribute; iterators do not.
    pub fn labels(&self) -> impl Iterator<Item = &'_ str> + '_ {
        self.entries.iter().map(VaultEntry::label)
    }

    /// `entries[0]`: the rotation head and the AUTHORITATIVE default.
    ///
    /// The contract emits `default == entries[0].value` and the loader refuses a
    /// disagreement; if the two ever differ, `entries[0]` is what every TS consumer
    /// effectively used (`Object.values(MAP)[0]`), so this - not
    /// [`Self::default_value`] - is the value a client should start with.
    #[must_use]
    pub fn head(&self) -> Option<&VaultEntry> {
        self.entries.first()
    }

    /// The entries a client may actually use, in source order: this IS the rotation queue
    /// (`nvidia-glm5-flash.ts:9-18` `Object.keys` -> `shift()`).
    ///
    /// A slot can legitimately yield nothing - the live `open_router_api_keys` pool has a
    /// single entry, `Default: 'NotAvailable'`, which classifies `invalid`. The loader
    /// therefore reports usability, it does not enforce it: refusing that artifact would
    /// reject the vault's own truth.
    pub fn usable(&self) -> impl Iterator<Item = &'_ VaultEntry> + '_ {
        self.entries.iter().filter(|entry| entry.is_usable())
    }

    /// The first usable entry, i.e. where rotation starts when the head is unusable.
    #[must_use]
    pub fn usable_head(&self) -> Option<&VaultEntry> {
        self.usable().next()
    }

    /// The declared `default` field as a redacted handle.
    #[must_use]
    pub fn default_value(&self) -> Option<&Secret> {
        self.r#default.as_ref()
    }

    /// The declared `default` as a plain `&str` for a non-secret (endpoint) slot: the base
    /// URL a client starts with. `None` when the slot IS secret, so a credential cannot be
    /// printed by accident; use [`Self::default_value`] + [`Secret::expose`] for that.
    #[must_use]
    pub fn default_public(&self) -> Option<&str> {
        if self.secret {
            return None;
        }
        self.r#default.as_ref().map(Secret::expose)
    }

    /// Every entry value of a NON-secret slot, in pool order, as plain `&str` - the
    /// endpoint failover list (`src/local/local-basalt.ts:13`).
    ///
    /// `None` when the slot IS secret: the values exist, they are just credentials, and
    /// making the return type `Option` is what stops `{:?}`-free code from printing a key
    /// while still keeping the flag honest. Group entries contribute nothing here; read
    /// [`VaultEntry::children`] for those.
    #[must_use]
    pub fn public_values(&self) -> Option<Vec<&str>> {
        if self.secret {
            return None;
        }
        Some(
            self.entries
                .iter()
                .filter_map(VaultEntry::value_str)
                .collect(),
        )
    }
}

/// One vendor: `{ id, slots }`, in registry order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default, deny_unknown_fields)]
pub struct VaultVendor {
    id: String,
    slots: Vec<VaultSlot>,
}

impl VaultVendor {
    /// The vendor id: the module file name, `snake_case` (`open-router.ts` -> `open_router`).
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Every slot of this vendor, in document order.
    #[must_use]
    pub fn slots(&self) -> &[VaultSlot] {
        &self.slots
    }

    /// A slot by id (`nvidia_api_keys`).
    #[must_use]
    pub fn slot(&self, id: &str) -> Option<&VaultSlot> {
        self.slots.iter().find(|slot| slot.id == id)
    }

    /// The slots of one kind, in document order.
    pub fn slots_of_kind(&self, kind: VaultSlotKind) -> impl Iterator<Item = &'_ VaultSlot> + '_ {
        self.slots.iter().filter(move |slot| slot.kind == kind)
    }

    /// The credential pools (`kind: "keys"`) of this vendor, in document order. Plural on
    /// purpose: `opencode` has two (`opencode_api_keys` and `opencode_go_api_keys`).
    pub fn credential_slots(&self) -> impl Iterator<Item = &'_ VaultSlot> + '_ {
        self.slots_of_kind(VaultSlotKind::Keys)
    }

    /// The endpoint pools (`kind: "endpoints"`) of this vendor, in document order.
    pub fn endpoint_slots(&self) -> impl Iterator<Item = &'_ VaultSlot> + '_ {
        self.slots_of_kind(VaultSlotKind::Endpoints)
    }

    /// The vendor's first credential pool in source order: the one a per-vendor client
    /// uses. Every vendor of the `agentic-provider` scope has exactly one.
    #[must_use]
    pub fn credential_slot(&self) -> Option<&VaultSlot> {
        self.credential_slots().next()
    }

    /// The vendor's first endpoint pool in source order: the base URL a per-vendor client
    /// uses.
    #[must_use]
    pub fn endpoint_slot(&self) -> Option<&VaultSlot> {
        self.endpoint_slots().next()
    }
}

/// The whole artifact, validated (the TS `VaultSnapshot`, renamed because Rust already
/// uses "config" for what a consumer loads).
///
/// Every struct in this module carries `#[serde(rename_all = "snake_case", default,
/// deny_unknown_fields)]` AND `#[derive(Default)]` together: container-level `default`
/// fills an omitted field from `Self::default()`, so WITHOUT the derive the crate does not
/// compile (`knowledge.md` F1: 24 x E0277 measured).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default, deny_unknown_fields)]
pub struct VaultConfig {
    schema_version: u32,
    package: String,
    consumers: Vec<String>,
    sentinels: VaultSentinels,
    vendors: Vec<VaultVendor>,
}

impl VaultConfig {
    #[must_use]
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    #[must_use]
    pub fn package(&self) -> &str {
        &self.package
    }

    /// The scopes whose vendors are present in this document (`knowledge.md` Scope
    /// guarantee 2 - a consumer never receives a vendor, or a secret VALUE, it did not ask
    /// for).
    #[must_use]
    pub fn consumers(&self) -> &[String] {
        &self.consumers
    }

    /// The classifier vocabulary, which the loader also cross-checks every entry against.
    #[must_use]
    pub fn sentinels(&self) -> &VaultSentinels {
        &self.sentinels
    }

    /// Every vendor, in registry order, with every pool in document order.
    #[must_use]
    pub fn vendors(&self) -> &[VaultVendor] {
        &self.vendors
    }

    #[must_use]
    pub fn vendor_count(&self) -> usize {
        self.vendors.len()
    }

    /// Total slots across all vendors (20 for the `agentic-provider` artifact).
    #[must_use]
    pub fn slot_count(&self) -> usize {
        self.vendors.iter().map(|vendor| vendor.slots.len()).sum()
    }

    /// Total entries in the document, group entries and their children both counted
    /// (43 leaves for the `agentic-provider` artifact).
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.vendors
            .iter()
            .flat_map(|vendor| vendor.slots.iter())
            .map(|slot| count_entries(&slot.entries))
            .sum()
    }

    /// A vendor by id (`nvidia`).
    #[must_use]
    pub fn vendor(&self, id: &str) -> Option<&VaultVendor> {
        self.vendors.iter().find(|vendor| vendor.id == id)
    }

    /// A slot by vendor id + slot id (`nvidia`, `nvidia_api_keys`).
    #[must_use]
    pub fn slot(&self, vendor_id: &str, slot_id: &str) -> Option<&VaultSlot> {
        self.vendor(vendor_id)
            .and_then(|vendor| vendor.slot(slot_id))
    }

    /// Every slot, as `(vendor id, slot)`, in document order.
    pub fn slots(&self) -> impl Iterator<Item = (&'_ str, &'_ VaultSlot)> + '_ {
        self.vendors.iter().flat_map(|vendor| {
            vendor
                .slots
                .iter()
                .map(move |slot| (vendor.id.as_str(), slot))
        })
    }

    /// Every credential pool, as `(vendor id, slot)`, in document order - the surface a
    /// `/stats`-style view or a key-rotation report reads.
    pub fn credential_slots(&self) -> impl Iterator<Item = (&'_ str, &'_ VaultSlot)> + '_ {
        self.slots()
            .filter(|(_, slot)| slot.kind == VaultSlotKind::Keys)
    }

    /// Every endpoint pool, as `(vendor id, slot)`, in document order.
    pub fn endpoint_slots(&self) -> impl Iterator<Item = (&'_ str, &'_ VaultSlot)> + '_ {
        self.slots()
            .filter(|(_, slot)| slot.kind == VaultSlotKind::Endpoints)
    }

    /// How many slots carry credentials (`secret: true`): 9 in the `agentic-provider`
    /// artifact, of the 15 that exist across all scopes.
    #[must_use]
    pub fn secret_slot_count(&self) -> usize {
        self.slots().filter(|(_, slot)| slot.is_secret()).count()
    }
}

/// Counts entries recursively (groups and their leaves both).
fn count_entries(entries: &[VaultEntry]) -> usize {
    entries
        .iter()
        .map(|entry| 1 + entry.children.as_deref().map_or(0, count_entries))
        .sum()
}

// ---------------------------------------------------------------------------
// The one refusal type
// ---------------------------------------------------------------------------

/// Which stage refused, so a caller can tell "there is no artifact" from "the artifact is
/// wrong" - exactly the `missing` vs `drift` split `config/vault/scripts/export-vault.ts
/// --check` reports, and what a future `build.rs` needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VaultFailure {
    /// The path could not be read (absent on a fresh clone: `generated/` is git-ignored).
    Io,
    /// The bytes are not a document this crate can deserialise (syntax, or a type/field
    /// mismatch rejected by serde).
    Json,
    /// The document parsed and validation refused it; every problem is listed.
    Validator,
}

impl VaultFailure {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Io => "unreadable",
            Self::Json => "unparsable",
            Self::Validator => "rejected",
        }
    }
}

/// Every reason a vault artifact was refused, in one value.
///
/// There is no partial success: a caller either gets a [`VaultConfig`] it can rotate
/// through, or this, listing everything wrong with the document. The `problems` strings
/// name scopes, vendor ids, slot ids, labels and classes - never a value
/// ([`redact_parse_message`] is what keeps even the serde diagnostics clean).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "vault artifact {}: {} problem(s): {}",
    self.failure.as_str(),
    self.problems.len(),
    self.problems.join("; ")
)]
pub struct VaultError {
    failure: VaultFailure,
    problems: Vec<String>,
}

impl VaultError {
    /// A refusal carrying every problem found (the validator's shape).
    #[must_use]
    pub fn new(failure: VaultFailure, problems: Vec<String>) -> Self {
        Self { failure, problems }
    }

    /// A refusal with exactly one problem (the I/O and serde shapes).
    #[must_use]
    pub fn problem(failure: VaultFailure, problem: impl Into<String>) -> Self {
        Self {
            failure,
            problems: vec![problem.into()],
        }
    }

    #[must_use]
    pub fn failure(&self) -> VaultFailure {
        self.failure
    }

    /// Every problem, in the order the document was walked (document-level rules first,
    /// then vendors, slots and entries in source order).
    #[must_use]
    pub fn problems(&self) -> &[String] {
        &self.problems
    }

    #[must_use]
    pub fn problem_count(&self) -> usize {
        self.problems.len()
    }
}

// ---------------------------------------------------------------------------
// The loader
// ---------------------------------------------------------------------------

/// Parse and validate a vault document from memory.
///
/// The document is the caller's: an env var captured through `export -- --stdout`
/// (`config/vault/knowledge.md` "How a Rust process actually loads it"), a file already
/// read, or a test fixture. No path, no environment, no default.
///
/// Pipeline, in order: serde (`deny_unknown_fields` - the version gate), then
/// [`validate`] (every rule, collected), then accept. A refusal returns
/// [`VaultError`] with every problem; a success is a document whose pools may be rotated.
///
/// ```
/// use typecrate_provider::vault::{VaultFailure, load_from_str};
///
/// let refused = load_from_str("{}").expect_err("an empty document has no vendors");
/// assert_eq!(refused.failure(), VaultFailure::Validator);
/// ```
pub fn load_from_str(json: &str) -> Result<VaultConfig, VaultError> {
    let document: VaultConfig = serde_json::from_str(json)
        .map_err(|error| VaultError::problem(VaultFailure::Json, parse_problem(&error)))?;

    let problems = validate(&document);
    if !problems.is_empty() {
        return Err(VaultError::new(VaultFailure::Validator, problems));
    }

    // Counts only - never a value - so the log line a successful load may produce is safe.
    tracing::trace!(
        vendors = document.vendor_count(),
        slots = document.slot_count(),
        entries = document.entry_count(),
        "vault artifact accepted"
    );
    Ok(document)
}

/// Read a vault document from a caller-supplied path and hand it to [`load_from_str`].
///
/// The path is deliberately NOT defaulted: the vault artifact lives at
/// `config/vault/generated/<scope>.json`, is git-ignored, and does not exist on a fresh
/// clone (`config/vault/.gitignore:8`), so the caller decides what absence means (a
/// `build.rs` refuses the build, a server may start with no vendor). The I/O problem names
/// the path, which is the actionable half.
pub fn load_from_path(path: impl AsRef<Path>) -> Result<VaultConfig, VaultError> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|error| {
        VaultError::problem(
            VaultFailure::Io,
            format!(
                "could not read the vault artifact at {}: {error}",
                path.display()
            ),
        )
    })?;
    load_from_str(&text)
}

/// The one problem a serde failure produces, with its value echoes redacted.
fn parse_problem(error: &serde_json::Error) -> String {
    format!(
        "the artifact is not a vault document this crate can parse: {}",
        redact_parse_message(&error.to_string())
    )
}

/// Replaces long double-quoted runs in a serde diagnostic with `"<redacted:len>"`.
///
/// serde writes the offending scalar as a `{:?}` string (`invalid type: string
/// "sk-live-..."`, escapes included) and puts field/variant names in backticks, so scanning
/// for balanced `"` runs (honouring `\` escapes) hits value echoes only. An unterminated
/// run at the end of a message is treated as a run.
fn redact_parse_message(message: &str) -> String {
    let mut output = String::with_capacity(message.len());
    let mut characters = message.chars().peekable();

    while let Some(character) = characters.next() {
        if character != '"' {
            output.push(character);
            continue;
        }

        // Collect the run between the quotes, keeping escape pairs verbatim so a short
        // message survives unchanged.
        let mut run = String::new();
        while let Some(inner) = characters.next() {
            if inner == '\\' {
                run.push(inner);
                if let Some(escaped) = characters.next() {
                    run.push(escaped);
                }
                continue;
            }
            if inner == '"' {
                break;
            }
            run.push(inner);
        }

        if run.chars().count() >= REDACT_MIN_LEN {
            output.push_str("\"<redacted:");
            output.push_str(&run.chars().count().to_string());
            output.push_str(">\"");
        } else {
            output.push('"');
            output.push_str(&run);
            output.push('"');
        }
    }

    output
}

// ---------------------------------------------------------------------------
// Validation: validate, never coerce, and report everything at once
// ---------------------------------------------------------------------------

/// Every rule the loader enforces, as a list of human-readable problems. Empty means the
/// document is accepted.
///
/// WHY THESE RULES AND NOT A SCHEMA LIBRARY: the repository hand-rolls its guards (root
/// `AGENTS.md`, and `runtime/endpoint/provider/private/stats/stats-buffer.ts:97-123`), and
/// every rule below is a fact the emitting builder already guarantees
/// (`slots.ts`/`snapshot.ts`). So each problem names a document that did not come out of
/// `config/vault`, i.e. a hand edit, a truncated write, or a crate that predates the
/// artifact. Nothing here is a stylistic preference.
///
/// WHAT IS DELIBERATELY *NOT* A PROBLEM:
///   * an all-invalid pool (`open_router_api_keys` has exactly that in the live artifact) -
///     usability is reported by [`VaultSlot::usable`], never enforced at load time;
///   * a `vendors` list that is empty (`--public-only` over a scope of credential-only
///     vendors legitimately produces one);
///   * a `retired` slot (it is derived from the scope table and may flip);
///   * overlapping sentinel vocabularies: the classifier's precedence resolves them, and
///     `sentinels.ts:79-81` anticipates exactly that collision.
fn validate(document: &VaultConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();

    // -- document identity: verify before trusting (`knowledge.md:167`). ---------------
    if document.schema_version != SCHEMA_VERSION {
        problems.push(format!(
            "schema_version {} is not understood by this crate (it implements schema_version \
             {SCHEMA_VERSION}): this crate predates the artifact, and the artifact carries no \
             other version channel",
            document.schema_version
        ));
    }
    if document.package != PACKAGE_NAME {
        problems.push(format!(
            "package {:?} is not {PACKAGE_NAME:?}: the document was not emitted by \
             config/vault's export script, so its values are not this vault's",
            document.package
        ));
    }

    // -- consumers. -------------------------------------------------------------------
    if document.consumers.is_empty() {
        problems.push(
            "consumers is empty: every artifact is emitted for at least one scope, because \
             export-vault.ts requires --consumer with no default (least privilege is the \
             artifact's whole point)"
                .to_owned(),
        );
    }
    validate_unique_names("consumers", &document.consumers, &mut problems);

    // -- the classifier vocabulary the entries are cross-checked against. -------------
    if document.sentinels.keyless().is_empty() {
        problems.push(
            "sentinels.keyless is empty: the vocabulary always carries `none`, and an empty \
             list would silently demote every keyless endpoint to invalid"
                .to_owned(),
        );
    }
    if document.sentinels.invalid().is_empty() {
        problems.push(
            "sentinels.invalid is empty: the vocabulary always carries \
             NotAvailable|error|null|undefined (model-class.ts:39-44)"
                .to_owned(),
        );
    }
    validate_unique_names(
        "sentinels.keyless",
        document.sentinels.keyless(),
        &mut problems,
    );
    validate_unique_names(
        "sentinels.invalid",
        document.sentinels.invalid(),
        &mut problems,
    );
    // `sentinels.placeholder` may legitimately be empty (a vault with no placeholders); its
    // entries still may not be blank or duplicated.
    validate_unique_names(
        "sentinels.placeholder",
        document.sentinels.placeholder(),
        &mut problems,
    );

    // -- vendors, slots, entries, in source order. -----------------------------------
    let mut vendor_ids: HashSet<&str> = HashSet::new();
    for (index, vendor) in document.vendors.iter().enumerate() {
        if vendor.id.trim().is_empty() {
            problems.push(format!("vendors[{index}] has an empty id"));
        }
        if !vendor_ids.insert(vendor.id.as_str()) {
            problems.push(format!(
                "vendor `{}` appears more than once: a scope contains each of its vendors \
                 exactly once",
                vendor.id
            ));
        }
        if vendor.slots.is_empty() {
            problems.push(format!(
                "vendor `{}` carries no slots: the export script drops a vendor left with \
                 zero slots, so an empty list is not something it emits",
                vendor.id
            ));
        }

        let mut slot_ids: HashSet<&str> = HashSet::new();
        for (slot_index, slot) in vendor.slots.iter().enumerate() {
            validate_slot(
                &mut problems,
                &vendor.id,
                slot_index,
                slot,
                &document.sentinels,
                &mut slot_ids,
            );
        }
    }

    problems
}

/// Shared rule for `consumers` and each sentinel list: no blank entry, no duplicates.
/// Order is NOT required to be sorted here - the builder re-sorts scope lists into
/// declaration order, but that is its own concern, not a load-bearing invariant.
fn validate_unique_names(field: &str, names: &[String], problems: &mut Vec<String>) {
    let mut seen: HashSet<&str> = HashSet::new();
    for (index, name) in names.iter().enumerate() {
        if name.trim().is_empty() {
            problems.push(format!("{field}[{index}] is an empty name"));
        }
        if !seen.insert(name.as_str()) {
            problems.push(format!("{field} lists `{name}` more than once"));
        }
    }
}

/// One slot: identity, the two kind-derived flags, the entry shapes, and `default`.
// `'a` is spelled out (and shared by `slot` and `seen_slot_ids`) because `&mut HashSet<&str>`
// is INVARIANT over its element type: with two anonymous lifetimes rustc cannot prove that the
// slot id being inserted outlives the set it is inserted into ("argument requires that `'1` must
// outlive `'2`", with the note "requirement occurs because of a mutable reference to
// `HashSet<&str>`"). A shared named lifetime is the whole fix.
#[allow(clippy::too_many_arguments)]
fn validate_slot<'a>(
    problems: &mut Vec<String>,
    vendor_id: &str,
    slot_index: usize,
    slot: &'a VaultSlot,
    sentinels: &VaultSentinels,
    seen_slot_ids: &mut HashSet<&'a str>,
) {
    // Name the slot by id when it has one, by position when it does not - every message
    // below has to be actionable without printing a value.
    let name = if slot.id.trim().is_empty() {
        format!("vendor `{vendor_id}` slots[{slot_index}]")
    } else {
        format!("vendor `{vendor_id}` slot `{}`", slot.id)
    };

    if slot.id.trim().is_empty() {
        problems.push(format!("{name} has an empty id"));
    }
    if !seen_slot_ids.insert(slot.id.as_str()) {
        problems.push(format!(
            "{name} repeats a slot id within the same vendor: a vendor's slots are the \
             distinct export maps of that module"
        ));
    }

    // Both flags are DERIVED by the vault from `kind`, so a disagreement is a hand edit
    // (`slots.ts:vaultSlotSecret`, `slots.ts:vaultSlotOptional`).
    let secret_expected = slot.kind == VaultSlotKind::Keys;
    if slot.secret != secret_expected {
        problems.push(format!(
            "{name}: secret={} but kind is `{}`; config/vault derives the flag from kind \
             (slots.ts:vaultSlotSecret: only `keys` slots hold credentials)",
            slot.secret,
            slot.kind.as_str()
        ));
    }
    let optional_expected = slot.kind == VaultSlotKind::Endpoints;
    if slot.optional != optional_expected {
        problems.push(format!(
            "{name}: optional={} but kind is `{}`; config/vault derives the flag from kind \
             (slots.ts:vaultSlotOptional: only `endpoints` slots may be overridden per call)",
            slot.optional,
            slot.kind.as_str()
        ));
    }

    if slot.entries.is_empty() {
        problems.push(format!(
            "{name} has no entries: a slot with no entries has neither a rotation pool nor a \
             default, and every live map has at least one key"
        ));
    }

    validate_entries(problems, &name, &slot.entries, slot, sentinels, 1);
    validate_default(problems, &name, slot);
}

/// Walks one sibling list (recursing exactly once into a nested slot's groups).
fn validate_entries(
    problems: &mut Vec<String>,
    slot_name: &str,
    entries: &[VaultEntry],
    slot: &VaultSlot,
    sentinels: &VaultSentinels,
    depth: usize,
) {
    // Labels are unique among SIBLINGS: they are the keys of a JS object literal
    // (`Object.entries` cannot repeat a key), so a repeat means the document was edited.
    let mut labels: HashSet<&str> = HashSet::new();

    for (index, entry) in entries.iter().enumerate() {
        let path = format!("{slot_name} entries[{index}]");

        if entry.label.trim().is_empty() {
            problems.push(format!(
                "{path} has an empty label: the label is the pool key, and local-strata.ts:13 \
                 and local-basalt.ts:13 publish it upstream as `upstreamLabel`"
            ));
        }
        if !labels.insert(entry.label.as_str()) {
            problems.push(format!(
                "{path} repeats the label `{}` within the same sibling list: source map keys \
                 are unique by construction",
                entry.label
            ));
        }

        if slot.nesting == VaultNesting::Flat {
            // A flat slot's every entry is a leaf: a value, and no children.
            if entry.value.is_none() {
                problems.push(format!(
                    "{path} (`{}`) has `value: null` but the slot declares nesting `flat`: \
                     only the nested `*_service_endpoints` slot emits group entries",
                    entry.label
                ));
            }
            if entry.children.is_some() {
                problems.push(format!(
                    "{path} (`{}`) carries `children` but the slot declares nesting `flat`",
                    entry.label
                ));
            }
        } else {
            match (entry.value.as_ref(), entry.children.as_deref()) {
                // A group: `value: null` plus children.
                (None, Some(children)) => {
                    if depth > MAX_NESTING_DEPTH {
                        problems.push(format!(
                            "{path} (`{}`) nests children below the single group level the \
                             contract emits (slots.ts:vaultNestedEntries maps one level and \
                             its children are leaves)",
                            entry.label
                        ));
                    } else {
                        validate_entries(
                            problems,
                            &format!("{path} (`{}`) children", entry.label),
                            children,
                            slot,
                            sentinels,
                            depth + 1,
                        );
                    }
                    // A group's validity is STRUCTURAL, not value-derived
                    // (`slots.ts:vaultNestedEntries`: non-empty group -> 'valid', empty
                    // group -> 'empty').
                    let expected = if children.is_empty() {
                        VaultValidity::Empty
                    } else {
                        VaultValidity::Valid
                    };
                    if entry.validity != expected {
                        problems.push(format!(
                            "{path} (`{}`) declares validity `{}` but a group's validity is \
                             structural: non-empty children is `{}`, an empty group is `{}`",
                            entry.label,
                            entry.validity.as_str(),
                            VaultValidity::Valid.as_str(),
                            VaultValidity::Empty.as_str()
                        ));
                    }
                }
                (None, None) => problems.push(format!(
                    "{path} (`{}`) has `value: null` and no `children`, so it carries no data \
                     at all: `null` means 'descend'",
                    entry.label
                )),
                // A LEAF of a nested slot (every `children[]` entry the contract emits):
                // value present, no children. Nothing structural to report - the value's
                // class is cross-checked by `validate_validity` below.
                (Some(_), None) => {}
                // A leaf of a nested slot must not carry children of its own.
                (Some(_), Some(_)) => problems.push(format!(
                    "{path} (`{}`) carries `children` AND a value: in a nested slot a group is \
                     `null` + children and a leaf is a value with no children",
                    entry.label
                )),
            }
        }

        validate_validity(problems, &path, entry, sentinels);
    }
}

/// Cross-checks a LEAF's declared `validity` against the document's own vocabulary.
///
/// This is the rule that makes `sentinels` load-bearing instead of decorative: a document
/// whose labels lie about its values is refused. The message names the slot, the label and
/// BOTH classes - never the value, which may be a credential.
fn validate_validity(
    problems: &mut Vec<String>,
    path: &str,
    entry: &VaultEntry,
    sentinels: &VaultSentinels,
) {
    let Some(value) = entry.value.as_ref() else {
        // A group has no value to classify; its structural rule lives in
        // `validate_entries`.
        return;
    };

    let expected = sentinels.classify(value.expose());
    if entry.validity != expected {
        problems.push(format!(
            "{path} (`{}`) declares validity `{}` but the document's own sentinel vocabulary \
             classifies it `{}`",
            entry.label,
            entry.validity.as_str(),
            expected.as_str()
        ));
    }
}

/// `default` must be present exactly on a flat slot, and must equal `entries[0].value`.
///
/// `snapshot.ts:117` emits `default: entries.length === 0 ? null : entries[0].value`, and
/// `modal.ts:2` (the only nested slot) exports no `*_DEFAULT` at all, which is the whole
/// reason the field is `Option`. When the two disagree the contract says `entries[0]` wins
/// ([`VaultSlot::head`]) - but the disagreement is still a problem, because a document
/// where they differ did not come from `buildVaultSnapshot`, and a loader that quietly
/// picked one would hide the fact.
fn validate_default(problems: &mut Vec<String>, name: &str, slot: &VaultSlot) {
    match (
        slot.nesting,
        slot.r#default.is_some(),
        slot.entries.is_empty(),
    ) {
        (VaultNesting::Flat, false, false) => problems.push(format!(
            "{name} has `default: null`, but a flat slot always emits `default = \
             entries[0].value` (only the nested `*_service_endpoints` slot exports no \
             `*_DEFAULT`)"
        )),
        (VaultNesting::Nested, true, _) => problems.push(format!(
            "{name} has a non-null `default`, but a nested slot's `entries[0].value` is null, \
             so the export emits `default: null`"
        )),
        _ => {}
    }

    if slot.nesting != VaultNesting::Flat {
        return;
    }
    let (Some(default), Some(head)) = (slot.r#default.as_ref(), slot.entries.first()) else {
        return;
    };
    match head.value.as_ref() {
        Some(head_value) if default != head_value => problems.push(format!(
            "{name}: default disagrees with entries[0].value; the vault emits them equal, and \
             entries[0] is the authority (`head()`), so this document was not emitted by \
             buildVaultSnapshot"
        )),
        // `entries[0]` is a group entry: already reported by the flat/nesting rules.
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use serde_json::json;

    /// The artifact this checkout exports, resolved the way a caller would resolve it: from
    /// the crate directory, not from the process cwd (`export-vault.ts` resolves its own
    /// output directory the same way - `scripts/export-vault.ts:100-144`).
    ///
    /// It is GIT-IGNORED (`config/vault/.gitignore:8`), so a fresh clone has no such file;
    /// the tests that need it skip with the reason in their output, exactly like
    /// `config/vault/src/vault/artifact.test.ts`, and never substitute a fabricated value.
    const ARTIFACT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../config/vault/generated/agentic-provider.json"
    );

    /// A complete, minimal, hand-written document: both slot kinds, all five validity
    /// classes, the `agentic-provider` vocabulary verbatim, and a `retired: true` slot.
    ///
    /// It is the input for every counterfactual below, and its credential
    /// (`sk-head-head-head`) is what the leak tests look for in formatted output.
    const FIXTURE: &str = r#"{
  "schema_version": 1,
  "package": "@config/vault",
  "consumers": ["agentic-provider"],
  "sentinels": {
    "keyless": ["none"],
    "invalid": ["NotAvailable", "error", "null", "undefined"],
    "placeholder": ["EXA_API_KEY_PLACEHOLDER", "ZYLORA_API_KEY_PLACEHOLDER"]
  },
  "vendors": [
    {
      "id": "example",
      "slots": [
        {
          "id": "example_api_keys",
          "kind": "keys",
          "secret": true,
          "optional": false,
          "retired": false,
          "nesting": "flat",
          "entries": [
            { "label": "Head", "value": "sk-head-head-head", "validity": "valid" },
            { "label": "Keyless", "value": "none", "validity": "keyless" },
            { "label": "Placeholder", "value": "EXAMPLE_API_KEY_PLACEHOLDER", "validity": "placeholder" },
            { "label": "Broken", "value": "NotAvailable", "validity": "invalid" },
            { "label": "Unset", "value": "   ", "validity": "empty" },
            { "label": "Tail", "value": "sk-tail-tail-tail", "validity": "valid" }
          ],
          "default": "sk-head-head-head"
        },
        {
          "id": "example_api_endpoints",
          "kind": "endpoints",
          "secret": false,
          "optional": true,
          "retired": true,
          "nesting": "flat",
          "entries": [
            { "label": "Primary", "value": "https://primary.example/v1", "validity": "valid" },
            { "label": "Second", "value": "https://second.example/v1", "validity": "valid" }
          ],
          "default": "https://primary.example/v1"
        }
      ]
    }
  ]
}"#;

    /// The same document with ONE credential-bearing nested slot, the shape only
    /// `--consumer all` emits (`modal` / `comfy_cloud_service_endpoints`). Group entry keys
    /// are in the order `export-vault.ts` writes them (`children, label, validity, value`).
    const NESTED_FIXTURE: &str = r#"{
  "schema_version": 1,
  "package": "@config/vault",
  "consumers": ["all"],
  "sentinels": {
    "keyless": ["none"],
    "invalid": ["NotAvailable", "error", "null", "undefined"],
    "placeholder": ["EXA_API_KEY_PLACEHOLDER", "ZYLORA_API_KEY_PLACEHOLDER"]
  },
  "vendors": [
    {
      "id": "modal",
      "slots": [
        {
          "id": "comfy_cloud_service_endpoints",
          "kind": "service_endpoints",
          "secret": false,
          "optional": false,
          "retired": false,
          "nesting": "nested",
          "entries": [
            {
              "children": [
                { "label": "SpawnerA", "value": "https://a.example/v1", "validity": "valid" },
                { "label": "Keyless", "value": "none", "validity": "keyless" }
              ],
              "label": "A100",
              "validity": "valid",
              "value": null
            }
          ],
          "default": null
        }
      ]
    }
  ]
}"#;

    /// Reads and loads the live artifact.
    ///
    /// `None` only when the file is absent (a fresh clone). A REJECTION is a panic carrying
    /// every problem, because the loader refusing the vault's own output is a bug in this
    /// crate, and the problem list is the diagnosis.
    fn live_artifact() -> Option<(String, VaultConfig)> {
        let raw = match std::fs::read_to_string(ARTIFACT) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                eprintln!(
                    "SKIP: {ARTIFACT} does not exist ({error}); regenerate it with \
                     `yarn workspace @config/vault export -- --consumer agentic-provider`"
                );
                return None;
            }
            Err(error) => panic!("the artifact exists but could not be read: {error}"),
        };

        match load_from_str(&raw) {
            Ok(config) => Some((raw, config)),
            Err(error) => panic!(
                "the live artifact must load cleanly; got {} problem(s): {}",
                error.problem_count(),
                error.problems().join(" | ")
            ),
        }
    }

    /// The raw `label`s of one slot, read straight out of the document (not through the
    /// loader), in the order the document's own array holds them. This is the expectation
    /// the ordering test compares against, so it comes from the artifact, not from a
    /// hard-coded copy that could drift with it.
    fn raw_labels(document: &Value, vendor_id: &str, slot_id: &str) -> Vec<String> {
        raw_slot(document, vendor_id, slot_id)["entries"]
            .as_array()
            .expect("entries is an array")
            .iter()
            .map(|entry| {
                entry["label"]
                    .as_str()
                    .expect("label is a string")
                    .to_owned()
            })
            .collect()
    }

    /// The raw JSON object of one slot, found by id rather than by index.
    fn raw_slot<'value>(document: &'value Value, vendor_id: &str, slot_id: &str) -> &'value Value {
        document["vendors"]
            .as_array()
            .expect("vendors is an array")
            .iter()
            .find(|vendor| vendor["id"] == vendor_id)
            .expect("the vendor is in the artifact")["slots"]
            .as_array()
            .expect("slots is an array")
            .iter()
            .find(|slot| slot["id"] == slot_id)
            .expect("the slot is in the artifact")
    }

    /// Recursively collects every entry's validity (groups and children both).
    fn collect_validity(entries: &[VaultEntry], out: &mut Vec<VaultValidity>) {
        for entry in entries {
            out.push(entry.validity());
            if let Some(children) = entry.children() {
                collect_validity(children, out);
            }
        }
    }

    /// The class tally of a whole document, as a fixed-order list so a failure names the
    /// class that moved.
    fn validity_tally(config: &VaultConfig) -> Vec<(&'static str, usize)> {
        let mut classes: Vec<VaultValidity> = Vec::new();
        for (_, slot) in config.slots() {
            collect_validity(slot.entries(), &mut classes);
        }
        [
            VaultValidity::Valid,
            VaultValidity::Keyless,
            VaultValidity::Placeholder,
            VaultValidity::Invalid,
            VaultValidity::Empty,
        ]
        .into_iter()
        .map(|class| {
            (
                class.as_str(),
                classes.iter().filter(|c| **c == class).count(),
            )
        })
        .collect()
    }

    // -- the live artifact ---------------------------------------------------------

    /// The contract's own numbers for this scope: 11 vendors / 20 slots / 43 leaves / 9
    /// secret slots, registry order, and the emitted key order this crate's structs depend
    /// on. Values are never asserted (they are credentials); counts, ids and classes are.
    #[test]
    fn live_artifact_matches_the_documented_shape() {
        let Some((raw, config)) = live_artifact() else {
            return;
        };

        assert_eq!(config.schema_version(), 1);
        assert_eq!(config.package(), "@config/vault");
        assert_eq!(config.consumers(), ["agentic-provider".to_owned()]);

        // `knowledge.md` "Scope guarantee 2": exactly the 11 vendors of the scope, in
        // registry order.
        let vendor_ids: Vec<&str> = config.vendors().iter().map(VaultVendor::id).collect();
        assert_eq!(
            vendor_ids,
            [
                "basalt",
                "daytona",
                "lightning",
                "makora",
                "merge",
                "nvidia",
                "open_router",
                "sail",
                "strata",
                "token_router",
                "vultr"
            ]
        );

        assert_eq!(config.vendor_count(), 11);
        assert_eq!(config.slot_count(), 20);
        assert_eq!(config.entry_count(), 43);
        assert_eq!(config.secret_slot_count(), 9);
        assert_eq!(config.credential_slots().count(), 9);
        assert_eq!(config.endpoint_slots().count(), 11);

        // The classifier vocabulary is global, not scope-filtered: the two placeholder
        // literals belong to vendors this scope never receives (`knowledge.md` CAVEAT).
        assert_eq!(config.sentinels().keyless(), ["none".to_owned()]);
        assert_eq!(
            config.sentinels().invalid(),
            [
                "NotAvailable".to_owned(),
                "error".to_owned(),
                "null".to_owned(),
                "undefined".to_owned()
            ]
        );
        assert_eq!(
            config.sentinels().placeholder(),
            [
                "EXA_API_KEY_PLACEHOLDER".to_owned(),
                "ZYLORA_API_KEY_PLACEHOLDER".to_owned()
            ]
        );

        // `knowledge.md` F4, measured over `agentic-provider` on this checkout: 42 valid,
        // 1 invalid, no placeholders, no keyless, no empty. This is worktree-dependent by
        // design (the uncommitted `basalt.ts` `MolabEndless` entry is one of the 43): a
        // failure here means the artifact on disk was regenerated from different vault text,
        // which is the drift signal `config/vault/src/vault/artifact.test.ts` also guards.
        assert_eq!(
            validity_tally(&config),
            vec![
                ("valid", 42),
                ("keyless", 0),
                ("placeholder", 0),
                ("invalid", 1),
                ("empty", 0)
            ]
        );

        // ... and the one invalid leaf is the Open Router stand-in
        // (`open-router.ts:3 Default: 'NotAvailable'`).
        let open_router = config
            .slot("open_router", "open_router_api_keys")
            .expect("the slot is in the artifact");
        assert_eq!(open_router.len(), 1);
        assert_eq!(open_router.head().expect("one entry").label(), "Default");
        assert_eq!(
            open_router.head().expect("one entry").validity(),
            VaultValidity::Invalid
        );

        // The `agentic-provider` document contains no `null` at all (knowledge.md F1), which
        // is why the flat rules below are allowed to be strict.
        assert!(!raw.contains(": null"));
        assert!(!raw.contains("\"children\""));
    }

    /// Ordering is load-bearing, so the loader must hand back the document's own array
    /// order. The expectation is read out of the artifact's raw bytes, not typed by hand.
    #[test]
    fn nvidia_credential_pool_keeps_the_documents_own_order() {
        let Some((raw, config)) = live_artifact() else {
            return;
        };
        let document: Value = serde_json::from_str(&raw).expect("the artifact is valid JSON");

        let expected = raw_labels(&document, "nvidia", "nvidia_api_keys");
        let slot = config
            .slot("nvidia", "nvidia_api_keys")
            .expect("the slot is in the artifact");
        let loaded: Vec<String> = slot.labels().map(str::to_owned).collect();

        assert_eq!(loaded.len(), 6);
        assert_eq!(loaded, expected);

        // The guard CAN bite: this pool is not in any order a `HashMap`/`BTreeMap` would
        // produce, so a map-based deserialisation would fail the assertion above.
        let mut sorted = loaded.clone();
        sorted.sort();
        assert_ne!(loaded, sorted);

        // `entries[0]` is the rotation head and the live default.
        assert_eq!(
            slot.head().expect("the pool is non-empty").label(),
            expected[0]
        );
        assert_eq!(slot.entries()[5].label(), expected[5]);
    }

    /// `default == entries[0].value` for every slot of the real artifact, and the accessor
    /// pair agrees with it (the loader refuses a disagreement, so this also proves the rule
    /// holds for all 20 slots rather than for a chosen one).
    #[test]
    fn default_is_the_pool_head_in_every_slot() {
        let Some((_, config)) = live_artifact() else {
            return;
        };

        let mut checked = 0;
        for (vendor_id, slot) in config.slots() {
            assert_eq!(slot.nesting(), VaultNesting::Flat, "{vendor_id} is flat");
            let head = slot.head().expect("every live slot has entries");
            assert_eq!(
                slot.default_value(),
                head.value(),
                "the declared default equals entries[0].value"
            );
            checked += 1;
        }
        assert_eq!(checked, 20);
    }

    // -- the classifier ------------------------------------------------------------

    /// `'none'` is a VALID keyless credential, never a failure
    /// (`model-class.ts:32-35`), and trimming does not demote it.
    #[test]
    fn none_is_a_valid_keyless_credential() {
        assert_eq!(classify("none"), VaultValidity::Keyless);
        assert_eq!(classify("  none  "), VaultValidity::Keyless);
        assert!(VaultValidity::Keyless.is_usable());

        let config = load_from_str(FIXTURE).expect("the fixture is a valid artifact");
        let keys = config
            .slot("example", "example_api_keys")
            .expect("the fixture has the slot");
        let keyless = keys
            .entries()
            .iter()
            .find(|entry| entry.label() == "Keyless")
            .expect("the fixture has a keyless entry");
        assert_eq!(keyless.validity(), VaultValidity::Keyless);
        assert!(keyless.is_usable());
        assert_eq!(keyless.value_str(), Some("none"));
    }

    /// The four invalid literals (padded and cased variants included) and every empty
    /// spelling are unusable; `empty` stays distinct from `invalid`.
    #[test]
    fn invalid_literals_and_empty_values_are_unusable() {
        let cases: Vec<(&str, VaultValidity)> = vec![
            ("NotAvailable", VaultValidity::Invalid),
            ("error", VaultValidity::Invalid),
            ("null", VaultValidity::Invalid),
            ("undefined", VaultValidity::Invalid),
            ("  error  ", VaultValidity::Invalid),
            // Case-sensitive, like `Array.includes` in sentinels.ts:94.
            ("Error", VaultValidity::Valid),
            ("", VaultValidity::Empty),
            ("   ", VaultValidity::Empty),
            ("\t\n ", VaultValidity::Empty),
            ("https://primary.example/v1", VaultValidity::Valid),
            ("sk-live-token", VaultValidity::Valid),
        ];

        let actual: Vec<(&str, &'static str)> = cases
            .iter()
            .map(|(value, _)| (*value, classify(value).as_str()))
            .collect();
        let expected: Vec<(&str, &'static str)> = cases
            .iter()
            .map(|(value, class)| (*value, class.as_str()))
            .collect();
        assert_eq!(actual, expected);

        assert!(!VaultValidity::Invalid.is_usable());
        assert!(!VaultValidity::Empty.is_usable());
    }

    /// A placeholder is its OWN class: never merged into `invalid`, never usable as a
    /// credential, and recognised by the `_PLACEHOLDER` suffix rule as well as by the
    /// literals the artifact lists.
    #[test]
    fn placeholders_are_their_own_class() {
        let config = load_from_str(FIXTURE).expect("the fixture is a valid artifact");
        let sentinels = config.sentinels();

        assert_eq!(
            sentinels.classify("EXA_API_KEY_PLACEHOLDER"),
            VaultValidity::Placeholder
        );
        assert_eq!(
            sentinels.classify("ZYLORA_API_KEY_PLACEHOLDER"),
            VaultValidity::Placeholder
        );
        // A literal the vault has never seen still classifies, because the rule is the
        // suffix (sentinels.ts:57-61).
        assert_eq!(
            sentinels.classify("BRAND_NEW_API_KEY_PLACEHOLDER"),
            VaultValidity::Placeholder
        );
        assert_eq!(
            sentinels.classify("NotAvailable"),
            VaultValidity::Invalid,
            "the Open Router stand-in is invalid, NOT a placeholder (knowledge.md:115-117)"
        );
        assert_ne!(VaultValidity::Placeholder, VaultValidity::Invalid);
        assert!(!VaultValidity::Placeholder.is_usable());

        let keys = config
            .slot("example", "example_api_keys")
            .expect("the fixture has the slot");
        let placeholder = keys
            .entries()
            .iter()
            .find(|entry| entry.label() == "Placeholder")
            .expect("the fixture has a placeholder entry");
        assert_eq!(placeholder.validity(), VaultValidity::Placeholder);
    }

    /// The rotation queue: usable entries only, in source order, and a slot whose every
    /// entry is unusable legitimately yields nothing.
    #[test]
    fn usable_credentials_are_offered_in_source_order() {
        let config = load_from_str(FIXTURE).expect("the fixture is a valid artifact");
        let keys = config
            .slot("example", "example_api_keys")
            .expect("the fixture has the slot");

        let queue: Vec<&str> = keys.usable().map(VaultEntry::label).collect();
        assert_eq!(queue, ["Head", "Keyless", "Tail"]);
        assert_eq!(
            keys.usable_head().expect("queue is non-empty").label(),
            "Head"
        );

        let endpoints = config
            .slot("example", "example_api_endpoints")
            .expect("the fixture has the slot");
        assert_eq!(
            endpoints.public_values(),
            Some(vec![
                "https://primary.example/v1",
                "https://second.example/v1"
            ])
        );
        assert_eq!(
            endpoints.default_public(),
            Some("https://primary.example/v1")
        );
        // A credential slot refuses the plain-`&str` view: the flag is what gates printing.
        assert_eq!(keys.public_values(), None);
        assert_eq!(keys.default_public(), None);
        assert_eq!(
            keys.default_value(),
            keys.head().and_then(VaultEntry::value)
        );

        // The live Open Router pool is legitimately empty of usable credentials.
        if let Some((_, live)) = live_artifact() {
            let open_router = live
                .slot("open_router", "open_router_api_keys")
                .expect("the slot is in the artifact");
            assert_eq!(open_router.usable().count(), 0);
            assert_eq!(open_router.usable_head(), None);
        }
    }

    // -- the nested shape (the cross-scope `Option`s) ------------------------------

    /// The `all`-scope shape: `value: null` + `children` on the group, `default: null`, and
    /// a structural group validity. `agentic-provider.json` has no `null` at all, so this is
    /// the document that a struct WITHOUT the `Option`s would fail to load
    /// (`knowledge.md` F1).
    #[test]
    fn the_nested_slot_shape_loads() {
        let config = load_from_str(NESTED_FIXTURE).expect("the nested fixture is valid");
        assert_eq!(config.entry_count(), 3);

        let slot = config
            .slot("modal", "comfy_cloud_service_endpoints")
            .expect("the nested fixture has the slot");
        assert_eq!(slot.nesting(), VaultNesting::Nested);
        assert!(!slot.is_secret());
        assert!(!slot.is_optional());
        assert_eq!(slot.default_value(), None);
        assert_eq!(slot.default_public(), None);

        let group = slot.head().expect("one group entry");
        assert!(group.is_group());
        assert_eq!(group.value(), None);
        let children = group.children().expect("the group has children");
        assert_eq!(children.len(), 2);
        let child_labels: Vec<&str> = children.iter().map(VaultEntry::label).collect();
        assert_eq!(child_labels, ["SpawnerA", "Keyless"]);
        let child_values: Vec<Option<&str>> = children.iter().map(VaultEntry::value_str).collect();
        assert_eq!(child_values, [Some("https://a.example/v1"), Some("none")]);
        // A group contributes no value of its own to the slot-level view.
        assert_eq!(slot.public_values(), Some(vec![]));
    }

    /// Two nested defects are refused rather than tolerated: an empty group that claims to
    /// be `valid`, and a nested slot that carries a non-null `default`. The mutations go
    /// through `serde_json::Value` so they never have to quote a value.
    #[test]
    fn nested_shape_defects_are_refused() {
        // An empty group is legal only when it says `empty` (slots.ts:vaultNestedEntries).
        let empty_group_but_valid = {
            let mut document: Value =
                serde_json::from_str(NESTED_FIXTURE).expect("the nested fixture is valid JSON");
            document["vendors"][0]["slots"][0]["entries"][0]["children"] = json!([]);
            serde_json::to_string(&document).expect("re-serialisable")
        };
        let refused =
            load_from_str(&empty_group_but_valid).expect_err("an empty group cannot claim `valid`");
        assert_eq!(refused.failure(), VaultFailure::Validator);
        assert_eq!(refused.problem_count(), 1);
        assert_eq!(
            refused.problems()[0],
            "vendor `modal` slot `comfy_cloud_service_endpoints` entries[0] (`A100`) declares \
             validity `valid` but a group's validity is structural: non-empty children is \
             `valid`, an empty group is `empty`"
        );

        // The SAME document with the group's own validity set to `empty` is what the builder
        // emits for an empty group, so it must load and offer nothing.
        let empty_group_but_empty = {
            let mut document: Value = serde_json::from_str(&empty_group_but_valid)
                .expect("the previous step serialised cleanly");
            let group = &mut document["vendors"][0]["slots"][0]["entries"][0];
            group["validity"] = json!(VaultValidity::Empty.as_str());
            serde_json::to_string(&document).expect("re-serialisable")
        };
        let config = load_from_str(&empty_group_but_empty).expect("an empty group is legal");
        let group = config
            .slot("modal", "comfy_cloud_service_endpoints")
            .and_then(VaultSlot::head)
            .expect("the group is still there");
        assert_eq!(
            group.children().expect("the children key is present").len(),
            0
        );
        assert_eq!(config.entry_count(), 1);

        // A nested slot's `entries[0].value` is null, so the export emits `default: null`.
        let nested_with_default = {
            let mut document: Value =
                serde_json::from_str(NESTED_FIXTURE).expect("the nested fixture is valid JSON");
            document["vendors"][0]["slots"][0]["default"] = json!("https://not.emitted/v1");
            serde_json::to_string(&document).expect("re-serialisable")
        };
        let refused = load_from_str(&nested_with_default)
            .expect_err("a nested slot has no *_DEFAULT to emit");
        assert_eq!(refused.problem_count(), 1);
        assert_eq!(
            refused.problems()[0],
            "vendor `modal` slot `comfy_cloud_service_endpoints` has a non-null `default`, but \
             a nested slot's `entries[0].value` is null, so the export emits `default: null`"
        );
    }

    // -- redaction -----------------------------------------------------------------

    /// The leak proof: NO credential value of the live artifact can be printed through
    /// `Debug`/`Display` of anything in this module, and the parse-error path redacts its
    /// value echoes too.
    #[test]
    fn a_credential_value_can_never_be_printed() {
        // The wrapper's own formats are fixed text in every mode.
        let secret = Secret::new("sk-live-abcdefghijklmnop");
        assert_eq!(format!("{secret:?}"), "Secret(\"<redacted>\")");
        assert_eq!(format!("{secret:#?}"), "Secret(\"<redacted>\")");
        assert_eq!(format!("{secret}"), "<redacted>");
        assert_eq!(secret.expose(), "sk-live-abcdefghijklmnop");

        // A rejection whose document holds credentials names the defect, not the value.
        let wrong_package = FIXTURE.replace("\"@config/vault\"", "\"@config/other\"");
        let refused = load_from_str(&wrong_package).expect_err("the package must match");
        let rendered = format!("{refused} / {refused:?}");
        assert!(!rendered.contains("sk-head-head-head"), "{rendered}");
        assert!(!rendered.contains("sk-tail-tail-tail"), "{rendered}");
        assert_eq!(refused.problem_count(), 1);

        // A serde diagnostic echoes the offending scalar, so the message goes through the
        // redactor: a 21-character string sitting where a u32 belongs.
        let wrong_version_type =
            load_from_str("{\n  \"schema_version\": \"sk-super-secret-token\",\n  \"package\": \"@config/vault\"\n}")
                .expect_err("schema_version is a number");
        let rendered = format!("{wrong_version_type}");
        assert!(!rendered.contains("sk-super-secret-token"), "{rendered}");
        assert!(rendered.contains("\"<redacted:21>\""), "{rendered}");

        let Some((_, config)) = live_artifact() else {
            return;
        };

        // Every credential value the live artifact carries: its 19 credential-pool entries
        // (the 43 leaves split into 19 `keys` entries and 24 endpoint entries) plus the 9
        // defaults of the 9 secret slots. Counts only - nothing here prints them.
        let secrets: Vec<String> = config
            .credential_slots()
            .flat_map(|(_, slot)| {
                slot.entries()
                    .iter()
                    .filter_map(VaultEntry::value)
                    .chain(slot.default_value())
            })
            .map(Secret::expose)
            .map(str::to_owned)
            .collect();
        assert_eq!(secrets.len(), 19 + 9);

        // ONE legitimate coincidence has to be excluded first, and pinning it is part of the
        // test: the Open Router credential IS the literal `NotAvailable`
        // (`open-router.ts:3`), and that literal is public classifier vocabulary the document
        // prints in `sentinels.invalid` by contract (it is not a secret - the vault ships it
        // to every scope, even scopes that never receive open_router). Every OTHER credential
        // must be absent from every formatted form.
        let vocabulary: Vec<&str> = config
            .sentinels()
            .keyless()
            .iter()
            .chain(config.sentinels().invalid().iter())
            .chain(config.sentinels().placeholder().iter())
            .map(String::as_str)
            .collect();
        let (coincidences, credentials): (Vec<String>, Vec<String>) = secrets
            .into_iter()
            .partition(|secret| vocabulary.iter().any(|literal| *literal == secret.trim()));
        // The head entry value and the `default` of that same slot, and nothing else.
        assert_eq!(coincidences, vec!["NotAvailable".to_owned(); 2]);
        assert_eq!(credentials.len(), 26);

        // Nothing in any formatted form of any struct in this module contains any of them.
        let forms = vec![
            format!("{config:?}"),
            format!("{config:#?}"),
            format!("{:?}", config.vendors()),
            format!("{:#?}", config.vendors()),
        ];
        for form in &forms {
            for credential in &credentials {
                assert!(
                    !form.contains(credential.as_str()),
                    "a credential leaked into {form}"
                );
            }
        }

        // The redaction did not erase the useful half: ids and labels survive, and every
        // value position is the marker - all 43 entry values (endpoints included, because
        // redaction is by TYPE and the type cannot tell a slot apart) plus the 20 defaults.
        let alternate = format!("{config:#?}");
        assert!(alternate.contains("nvidia_api_keys"));
        assert_eq!(alternate.matches("<redacted>").count(), 43 + 20);
    }

    // -- the version gate and the problem list -------------------------------------

    /// `deny_unknown_fields` is the version gate (`knowledge.md:203-207`): a field this
    /// crate cannot name is refused at the root AND inside an entry, and the message keeps
    /// the field name (back-quoted, so the redactor leaves it alone) while listing the
    /// expected fields.
    #[test]
    fn deny_unknown_fields_rejects_an_injected_key() {
        let injected_root = FIXTURE.replace(
            "{\n  \"schema_version\": 1,",
            "{\n  \"bogus\": true,\n  \"schema_version\": 1,",
        );
        assert_ne!(injected_root, FIXTURE, "the replacement was applied");
        let refused = load_from_str(&injected_root).expect_err("an unknown field is a refusal");
        assert_eq!(refused.failure(), VaultFailure::Json);
        assert_eq!(refused.problem_count(), 1);
        assert_eq!(
            refused.problems()[0],
            "the artifact is not a vault document this crate can parse: unknown field `bogus`, \
             expected one of `schema_version`, `package`, `consumers`, `sentinels`, `vendors` \
             at line 2 column 9"
        );

        // The same gate inside an entry. The expected list is the struct's declaration
        // order (`label, value, validity, children`), which also proves `children` is part
        // of the struct even for a FLAT slot - the cross-scope shape of `knowledge.md` F1.
        let injected_entry = FIXTURE.replace(
            "{ \"label\": \"Head\", \"value\": \"sk-head-head-head\",",
            "{ \"bogus\": 1, \"label\": \"Head\", \"value\": \"sk-head-head-head\",",
        );
        assert_ne!(injected_entry, FIXTURE, "the replacement was applied");
        let refused =
            load_from_str(&injected_entry).expect_err("an unknown entry field is a refusal");
        assert_eq!(refused.problem_count(), 1);
        assert_eq!(
            refused.problems()[0],
            "the artifact is not a vault document this crate can parse: unknown field `bogus`, \
             expected one of `label`, `value`, `validity`, `children` at line 22 column 21"
        );
    }

    /// Never a partial success, never a silent default, and never only the first problem:
    /// one document carrying five independent defects reports all five, in document order.
    #[test]
    fn every_problem_comes_back_at_once() {
        let defective = r#"{
  "schema_version": 2,
  "package": "@config/other",
  "consumers": ["agentic-provider"],
  "sentinels": {
    "keyless": ["none"],
    "invalid": ["NotAvailable", "error", "null", "undefined"],
    "placeholder": []
  },
  "vendors": [
    {
      "id": "example",
      "slots": [
        {
          "id": "example_api_keys",
          "kind": "keys",
          "secret": false,
          "optional": false,
          "retired": false,
          "nesting": "flat",
          "entries": [
            { "label": "Head", "value": "sk-head-head-head", "validity": "valid" },
            { "label": "Broken", "value": "NotAvailable", "validity": "valid" }
          ],
          "default": "sk-other-other-other"
        }
      ]
    }
  ]
}"#;

        let refused = load_from_str(defective).expect_err("five defects, five problems");
        assert_eq!(refused.failure(), VaultFailure::Validator);
        assert_eq!(refused.problem_count(), 5);
        assert_eq!(
            refused.problems(),
            [
                "schema_version 2 is not understood by this crate (it implements \
                 schema_version 1): this crate predates the artifact, and the artifact carries \
                 no other version channel",
                "package \"@config/other\" is not \"@config/vault\": the document was not \
                 emitted by config/vault's export script, so its values are not this vault's",
                "vendor `example` slot `example_api_keys`: secret=false but kind is `keys`; \
                 config/vault derives the flag from kind (slots.ts:vaultSlotSecret: only `keys` \
                 slots hold credentials)",
                "vendor `example` slot `example_api_keys` entries[1] (`Broken`) declares \
                 validity `valid` but the document's own sentinel vocabulary classifies it \
                 `invalid`",
                "vendor `example` slot `example_api_keys`: default disagrees with \
                 entries[0].value; the vault emits them equal, and entries[0] is the authority \
                 (`head()`), so this document was not emitted by buildVaultSnapshot"
            ]
        );
        // The Display is the one line an operator logs: every problem, still no value.
        let rendered = refused.to_string();
        assert!(rendered.starts_with("vault artifact rejected: 5 problem(s): "));
        assert!(!rendered.contains("sk-head-head-head"), "{rendered}");
        assert!(!rendered.contains("sk-other-other-other"), "{rendered}");
    }

    /// `default` must equal `entries[0].value` even in the live document; mutating only the
    /// `default` of the real nvidia pool produces exactly that one problem.
    #[test]
    fn default_disagreeing_with_the_pool_head_is_refused() {
        let Some((raw, _)) = live_artifact() else {
            return;
        };
        let mut document: Value = serde_json::from_str(&raw).expect("the artifact is valid JSON");

        let head = raw_slot(&document, "nvidia", "nvidia_api_keys")["entries"][0]["value"]
            .as_str()
            .expect("entries[0] carries a value")
            .to_owned();

        let vendors = document["vendors"].as_array_mut().expect("array");
        let nvidia = vendors
            .iter_mut()
            .find(|vendor| vendor["id"] == "nvidia")
            .expect("nvidia is in the artifact");
        let slots = nvidia["slots"].as_array_mut().expect("array");
        let slot = slots
            .iter_mut()
            .find(|slot| slot["id"] == "nvidia_api_keys")
            .expect("the pool is in the artifact");
        slot["default"] = json!("NOT-THE-HEAD");

        let refused = load_from_str(&serde_json::to_string(&document).expect("re-serialisable"))
            .expect_err("a disagreeing default is a refusal");
        assert_eq!(refused.problem_count(), 1);
        assert_eq!(
            refused.problems()[0],
            "vendor `nvidia` slot `nvidia_api_keys`: default disagrees with entries[0].value; \
             the vault emits them equal, and entries[0] is the authority (`head()`), so this \
             document was not emitted by buildVaultSnapshot"
        );
        // The head is the authority, and the mutation above replaced it only in `default`.
        assert!(!refused.to_string().contains(&head));
    }

    /// A caller-provided path that does not exist is an I/O failure naming the path - the
    /// `missing` verdict a future `build.rs` has to distinguish from a rejected document
    /// (`export-vault.ts --check`).
    #[test]
    fn a_missing_artifact_is_an_io_problem_that_names_the_path() {
        let missing = std::path::Path::new(ARTIFACT).with_file_name("no-such-scope.json");
        let refused = load_from_path(&missing).expect_err("the file does not exist");

        assert_eq!(refused.failure(), VaultFailure::Io);
        assert_eq!(refused.problem_count(), 1);
        assert!(
            refused.problems()[0].starts_with(&format!(
                "could not read the vault artifact at {}: ",
                missing.display()
            )),
            "{}",
            refused.problems()[0]
        );

        // The same rule through the str entry point: an empty document is refused by the
        // validator (never accepted with silent defaults), even though `default` filled
        // every field.
        let refused = load_from_str("{}").expect_err("an empty document is not a vault");
        assert_eq!(refused.failure(), VaultFailure::Validator);
        assert_eq!(
            refused.problems(),
            [
                "schema_version 0 is not understood by this crate (it implements \
                 schema_version 1): this crate predates the artifact, and the artifact carries \
                 no other version channel",
                "package \"\" is not \"@config/vault\": the document was not emitted by \
                 config/vault's export script, so its values are not this vault's",
                "consumers is empty: every artifact is emitted for at least one scope, because \
                 export-vault.ts requires --consumer with no default (least privilege is the \
                 artifact's whole point)",
                "sentinels.keyless is empty: the vocabulary always carries `none`, and an empty \
                 list would silently demote every keyless endpoint to invalid",
                "sentinels.invalid is empty: the vocabulary always carries \
                 NotAvailable|error|null|undefined (model-class.ts:39-44)"
            ]
        );
    }

    /// The fixture is a valid artifact end to end - the baseline every counterfactual above
    /// mutates, asserted once so a broken fixture cannot make a defect test pass vacuously.
    #[test]
    fn the_fixture_loads_and_exposes_both_pools() {
        let config = load_from_str(FIXTURE).expect("the fixture is a valid artifact");
        assert_eq!(config.vendor_count(), 1);
        assert_eq!(config.slot_count(), 2);
        assert_eq!(config.entry_count(), 8);
        assert_eq!(config.secret_slot_count(), 1);

        let vendor = config.vendor("example").expect("one vendor");
        let credential_ids: Vec<&str> = vendor.credential_slots().map(VaultSlot::id).collect();
        let endpoint_ids: Vec<&str> = vendor.endpoint_slots().map(VaultSlot::id).collect();
        assert_eq!(credential_ids, ["example_api_keys"]);
        assert_eq!(endpoint_ids, ["example_api_endpoints"]);

        let keys = vendor.credential_slot().expect("one credential pool");
        assert!(keys.is_secret());
        assert!(!keys.is_optional());
        assert!(!keys.is_retired());

        let endpoints = vendor.endpoint_slot().expect("one endpoint pool");
        assert!(!endpoints.is_secret());
        assert!(endpoints.is_optional());
        assert!(endpoints.is_retired(), "retired is not a load-time problem");

        assert_eq!(
            validity_tally(&config),
            vec![
                ("valid", 4),
                ("keyless", 1),
                ("placeholder", 1),
                ("invalid", 1),
                ("empty", 1)
            ]
        );
        // Every value in the document is a `Secret` - the 2 endpoint values included, since
        // the type cannot tell a slot apart: 8 entry values + 2 defaults.
        assert_eq!(format!("{config:?}").matches("<redacted>").count(), 10);
    }
}
