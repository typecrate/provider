//! The process-wide model registry: rows plus vault, reconciled in array order.
//!
//! # Typecrate port - source lineage
//!
//! TypeScript origin: `packages/agentic/provider/src/registry.ts` (`privateClients`) and
//! the two consumers that define what the registry is FOR:
//! `runtime/endpoint/provider/private/chat-completion/private-chat-completion.ts`
//! (lookup by id) and
//! `runtime/endpoint/provider/private/models/service-route.ts` (the catalog's filtered,
//! order-preserving projection).
//!
//! ## Three properties this type exists to hold
//!
//!   1. **ORDER.** The list is built in `crate::clients` order and never re-sorted.
//!      `registry.ts` says why, entry by entry: every client was appended "to preserve all
//!      existing registry indices", and the TypeScript's tests assert positions
//!      (`src/index.test.ts`, and the chat-completion test's `privateClients[0]`). The
//!      catalog inherits that order (`service-route.ts` filters without sorting), while
//!      `/stats` re-sorts by id on its own projection (`private-stats.ts:285`) - so order
//!      must be preserved HERE and must not be "helpfully" normalised.
//!
//!   2. **ONE SHARED INSTANCE PER ROW.** Each row yields exactly one [`ModelClient`], and
//!      cloning that client is identity-preserving, so rotation state and counters are
//!      process-wide - which is the reason `registry.ts` exports a shared array instead of
//!      constructing clients per request.
//!
//!   3. **DISCOVERABILITY IS A QUERY, NEVER A REBUILD.** `model-class.ts:388` defines
//!      `isDiscoverable` as `enabled && hasValidApiKey`, and `index.ts:34-37` re-reads the
//!      same flag on every call. A drained credential must therefore stay in the list with
//!      the flag false (the "not routable" case), never be removed.
//!
//!   4. **NO AMBIENT INPUT.** The vault document is INJECTED ([`VaultConfig`]). A registry
//!      build never reads the filesystem or the environment, so a suite can build one from
//!      an in-memory document while a binary decides where the bytes come from. That is the
//!      counterpart of the TypeScript's named `import { NVIDIA_API_KEYS } from
//!      '@config/vault'` specifiers, which resolve at build time and are not re-derivable
//!      at runtime.
//!
//! ## Refusal, not coercion
//!
//! [`Registry::try_from_rows`] collects EVERY problem it can find and returns them
//! together, rather than panicking on the first or papering over one. A row whose vault
//! vendor is absent, whose pool is empty, whose price is not a usable money value, whose id
//! collides, or whose declared failover method contradicts its pools is named exactly once,
//! with the row id. `registry.ts` cannot offer this: TypeScript constructors run at import
//! time, and a missing vault export surfaces as an unresolved `undefined` far from its
//! cause.
//!
//! DEVIATION (deliberate, recorded): a bad row is a refusal with a problem list, where the
//! TypeScript would proceed with `apiKey: undefined` and a model that fails at request time.

use std::sync::OnceLock;

use crate::accounting::Totals;
use crate::client::{Behavior, ClientSpec, ModelClient, upstream_label_is_safe};
use crate::transport::{Credential, Endpoint, Target, Transport};
use crate::vault::{VaultConfig, VaultEntry, VaultSlot};

/// Every reason a set of rows could not become a registry.
///
/// The problem list is the payload: a caller printing this error sees all of them, which is
/// what makes a vault rename or a row typo a one-pass fix instead of a loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryError {
    /// Human-readable problems, one per rejected row (and one per duplicate id), in row
    /// order.
    problems: Vec<String>,
}

impl RegistryError {
    /// The individual problems, in discovery order.
    #[must_use]
    pub fn problems(&self) -> &[String] {
        &self.problems
    }

    /// One problem, attributed to a row. Every check reports through here so no line in a
    /// refusal is anonymous.
    fn problem(id: &str, detail: impl AsRef<str>) -> String {
        format!("{id}: {}", detail.as_ref())
    }
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The count first, so a log line that truncates the list still says how much was
        // truncated.
        write!(
            formatter,
            "{} registry problem(s):\n  - {}",
            self.problems.len(),
            self.problems.join("\n  - ")
        )
    }
}

impl std::error::Error for RegistryError {}

/// The served model set: one shared client per row, in row order.
#[derive(Debug, Clone)]
pub struct Registry {
    clients: Vec<ModelClient>,
}

/// One row plus the slot names it resolved to, so a failure message can name them without
/// repeating the lookup.
struct ResolvedSlots<'vault> {
    endpoints: Option<&'vault VaultSlot>,
    credentials: Option<&'vault VaultSlot>,
}

impl Registry {
    /// Build every row in [`crate::clients`] against an injected vault document.
    ///
    /// # Errors
    ///
    /// A [`RegistryError`] listing every rejected row.
    pub fn try_new(vault: &VaultConfig) -> Result<Self, RegistryError> {
        Self::try_from_rows(crate::clients::CLIENTS, vault)
    }

    /// Build an explicit set of rows - the seam for a suite that wants a two-row registry
    /// with synthetic ids instead of the whole deployed table.
    ///
    /// # Errors
    ///
    /// See [`Self::try_new`].
    pub fn try_from_rows(rows: &[ClientSpec], vault: &VaultConfig) -> Result<Self, RegistryError> {
        let mut problems: Vec<String> = Vec::new();
        let mut clients: Vec<ModelClient> = Vec::with_capacity(rows.len());

        // Duplicate ids are checked over the WHOLE table first, because the collision is a
        // property of the PAIR: reporting it once, attributed to the later row, is enough
        // to fix it, and it keeps the list free of mirrored entries.
        for (index, row) in rows.iter().enumerate() {
            if rows[..index].iter().any(|earlier| earlier.id == row.id) {
                problems.push(RegistryError::problem(
                    row.id,
                    "duplicate id: two rows claim the same routing key, and the router \
                     resolves by id",
                ));
            }
        }

        for row in rows {
            let mut row_problems: Vec<String> = Vec::new();

            // The slot name a failure should name, per kind.
            if let Some(client) = build(row, vault, &mut row_problems) {
                clients.push(client);
            }

            // `build` may return a client AND non-fatal notes? No: it returns `None` exactly
            // when it has appended problems, so this drains either nothing or the whole row's
            // reasons - a row is never dropped silently.
            problems.extend(
                row_problems
                    .drain(..)
                    .map(|detail| RegistryError::problem(row.id, detail)),
            );
        }

        if problems.is_empty() {
            return Ok(Self { clients });
        }

        Err(RegistryError { problems })
    }

    /// Adopt clients that already carry their transports - the seam for a suite that points
    /// rows at an in-process listener instead of at the vault.
    ///
    /// No validation runs here: the caller built the transports, so the pools are by
    /// construction what the caller intended. A suite that wants validation writes a vault
    /// document and calls [`Self::try_from_rows`].
    #[must_use]
    pub fn from_clients(clients: Vec<ModelClient>) -> Self {
        Self { clients }
    }

    /// Every client, in row order. NOT sorted - see the module doc's first property.
    #[must_use]
    pub fn clients(&self) -> &[ModelClient] {
        &self.clients
    }

    /// Resolve a routing key, exactly as the chat and responses routes do
    /// (`private-chat-completion.ts`: `privateClients.find(client => client.id === model)`).
    ///
    /// The filter is EXACTLY the id: a hidden row (disabled, or credential-gated) still
    /// resolves here, which is what makes `enabled = false` a catalog switch and not a
    /// routing switch.
    #[must_use]
    pub fn find(&self, id: &str) -> Option<&ModelClient> {
        self.clients.iter().find(|client| client.id() == id)
    }

    /// The catalog's rows: discoverable clients in registration order
    /// (`service-route.ts`: `privateClients.filter(c => c.isDiscoverable).map(...)`, with no
    /// sort anywhere in the projection).
    #[must_use]
    pub fn discoverable(&self) -> Vec<&ModelClient> {
        self.clients
            .iter()
            .filter(|client| client.is_discoverable())
            .collect()
    }

    /// The ids the catalog advertises, in order - the `data` array of
    /// `GET /providers/private/v1/models`.
    #[must_use]
    pub fn discoverable_ids(&self) -> Vec<&'static str> {
        self.discoverable()
            .iter()
            .map(|client| client.id())
            .collect()
    }

    /// The fleet roll-up of every row's counters: the `totals` half of the stats projection
    /// (`private-stats.ts:286-300`).
    ///
    /// Cost is NOT included, because each row prices its own tokens at its own rates
    /// (`private-stats.ts:229-231`); a caller that wants fleet dollars sums
    /// [`ModelClient::total_cost`] across the rows it is projecting.
    #[must_use]
    pub fn aggregate(&self) -> Totals {
        let mut totals = Totals::default();

        for client in &self.clients {
            totals.add(&client.counters());
        }

        totals
    }
}

// ---------------------------------------------------------------------------
// The process-wide singleton
// ---------------------------------------------------------------------------

/// The one registry a process serves from, built once.
static PROCESS_REGISTRY: OnceLock<Registry> = OnceLock::new();

impl Registry {
    /// Install the process registry, building it from the injected document.
    ///
    /// FIRST CALL WINS: a second call returns the instance that was installed first rather
    /// than rebuilding it, because rebuilding would reset the rotation state that the whole
    /// design shares (the TypeScript freezes its client array at module scope for the same
    /// reason). The asymmetry is deliberate: a FAILED first call installs nothing, so a
    /// caller may retry with a corrected document.
    ///
    /// # Errors
    ///
    /// See [`Self::try_new`]. A failure leaves the process registry unset.
    pub fn initialize_process(vault: &VaultConfig) -> Result<&'static Registry, RegistryError> {
        if let Some(installed) = PROCESS_REGISTRY.get() {
            return Ok(installed);
        }

        let registry = Self::try_new(vault)?;

        // `get_or_init` closes the race between two initializing threads: the loser's
        // freshly built registry is dropped and every caller sees one instance.
        Ok(PROCESS_REGISTRY.get_or_init(|| registry))
    }

    /// The installed process registry, `None` before [`Self::initialize_process`] succeeded.
    ///
    /// A getter, not an initialiser: nothing in this crate can obtain a vault document by
    /// itself, so a caller that forgot to initialise gets `None` instead of an implicit file
    /// read.
    #[must_use]
    pub fn process() -> Option<&'static Registry> {
        PROCESS_REGISTRY.get()
    }
}

// ---------------------------------------------------------------------------
// Row -> client
// ---------------------------------------------------------------------------

/// Turn one row into one shared client, appending every problem it finds to `problems`.
///
/// `problems` is that ROW'S OWN list: [`Registry::try_from_rows`] passes a fresh vector per
/// row and attributes the drained entries to the row id, which is what lets the emptiness
/// checks below stay local to one row instead of reacting to an earlier row's failure.
///
/// The check order is the order a reader fixes things in: the row's own invariants (no input
/// needed), then the vault lookups, then the pools those lookups produce, then the money.
fn build(row: &ClientSpec, vault: &VaultConfig, problems: &mut Vec<String>) -> Option<ModelClient> {
    check_row_shape(row, problems);

    let Some(vendor) = vault.vendor(row.vault_vendor) else {
        problems.push(format!(
            "the vault document has no vendor '{}' (a registry build never falls back to a \
             default, so a vault rename must be mirrored in the row)",
            row.vault_vendor
        ));

        return None;
    };

    let slots = ResolvedSlots {
        endpoints: resolve_slot(
            vendor.slot(row.endpoint_slot),
            row.vault_vendor,
            row.endpoint_slot,
            "endpoint",
            problems,
        )
        .filter(|slot| slot_kind_agrees(slot, row, "endpoint", false, problems)),
        credentials: match row.credential_slot {
            Some(name) => resolve_slot(
                vendor.slot(name),
                row.vault_vendor,
                name,
                "credential",
                problems,
            )
            .filter(|slot| slot_kind_agrees(slot, row, "credential", true, problems)),
            None => None,
        },
    };

    let endpoints = slots
        .endpoints
        .and_then(|slot| build_endpoints(row, slot, problems));

    let credentials = match row.credential_slot {
        // A relay row has no credential axis at all: an EMPTY pool is the faithful
        // representation, because the transport then selects no credential and the
        // keyless-header policy decides what (if anything) goes in `Authorization`.
        None => Some(Vec::new()),
        Some(_) => slots
            .credentials
            .and_then(|slot| build_credentials(row, slot, problems)),
    };

    let (Some(endpoints), Some(credentials)) = (endpoints, credentials) else {
        return None;
    };

    if endpoints.is_empty() {
        problems.push("the endpoint pool is empty, so there is nothing to dial".to_owned());
    }

    if row.credential_slot.is_some() && credentials.is_empty() {
        // The reason the emptiness is a refusal rather than a state: the TypeScript builds the
        // client anyway and hands the SDK `apiKey: undefined`, so the failure surfaces as a 401
        // at request time instead of as a problem at startup.
        problems.push(
            "the credential pool is empty while the row declares a credential (the TypeScript \
             would still construct the client, with `apiKey: undefined`)"
                .to_owned(),
        );
    }

    if !problems.is_empty() {
        return None;
    }

    // The target: the two pools plus the keyless-header policy the row chose.
    let target = Target::new(endpoints, credentials)
        .keyless(row.credential_slot.is_none())
        .keyless_header(row.keyless_header);

    Some(ModelClient::new(*row, Transport::new(target)))
}

/// The row-level invariants that need no vault at all.
fn check_row_shape(row: &ClientSpec, problems: &mut Vec<String>) {
    // One writer for the literal notes, so each site below stays a `&'static str` instead of
    // repeating a conversion.
    let mut note = |detail: &'static str| problems.push(detail.to_owned());

    if row.id.is_empty() {
        note("the row has no routing id");
    }

    if !row.pricing.is_sane() {
        note(
            "a price is not a usable money value (every rate must be finite and \
             non-negative)",
        );
    }

    // The failover method and the pools must agree. The TypeScript cannot get this wrong,
    // because a subclass declares the override and the constructor's pools in one place; a DATA
    // row can, and the failure mode is a 404 that rotates an axis with no pool behind it.
    if matches!(row.behavior, Behavior::ChatKeylessRelay) && row.credential_slot.is_some() {
        note(
            "a relay row declares a credential slot; these upstreams have no key to rotate, \
             so the failover axis is the endpoint pool",
        );
    }

    if matches!(row.behavior, Behavior::ChatCredentialDrain { .. }) && row.credential_slot.is_none()
    {
        note(
            "a credential-draining row has no credential slot, so it drains nothing and \
             could never flip off the catalog",
        );
    }

    if matches!(row.behavior, Behavior::ResponsesPassthrough) && row.requires_usage_option {
        note(
            "the Responses passthrough injects no body fields, so requiring the usage option \
             is a contradiction",
        );
    }
}

/// One named slot, or a problem naming the vendor and the slot.
fn resolve_slot<'vault>(
    slot: Option<&'vault VaultSlot>,
    vendor: &str,
    name: &str,
    kind: &str,
    problems: &mut Vec<String>,
) -> Option<&'vault VaultSlot> {
    slot.or_else(|| {
        problems.push(format!(
            "the vault vendor '{vendor}' has no {kind} slot '{name}'"
        ));

        None
    })
}

/// Whether a resolved slot is the KIND the row asked for.
///
/// The vault distinguishes its slot kinds on purpose, and the artifact already records the
/// answer as the slot's `secret` flag: every `*_api_keys` slot is secret, every endpoint slot
/// is public. Pointing a row's credential axis at an endpoint slot would put base URLs in the
/// `Authorization` header and keep the credential out of the request, with no error anywhere -
/// which is exactly the class of silent misplumbing this layer exists to refuse.
fn slot_kind_agrees(
    slot: &VaultSlot,
    row: &ClientSpec,
    kind: &str,
    secret_expected: bool,
    problems: &mut Vec<String>,
) -> bool {
    if slot.is_secret() == secret_expected {
        return true;
    }

    problems.push(format!(
        "'{}' is a {} slot in the vault vendor '{}', but the row uses it as its {kind} slot",
        slot.id(),
        if slot.is_secret() { "secret" } else { "public" },
        row.vault_vendor
    ));

    false
}

/// The endpoint pool: EVERY entry of the slot, in document order, for a relay row; the
/// slot's default value alone for every other row.
///
/// WHY ALL ENTRIES for a relay: the TypeScript's failover list is
/// `Object.keys(LOCAL_STRATA_API_ENDPOINTS)` (`local-strata.ts:13`) - every entry the vault
/// declares, in the order it declares them, with no usability filter. WHY ONLY THE DEFAULT
/// for the others: those clients construct with `<PROVIDER>_API_ENDPOINTS_DEFAULT` and their
/// `switchBaseUrl()` returns false, so the remaining entries are never selectable.
///
/// The LABEL rule is the other half of the asymmetry, and it is a log-line property: a relay
/// row prints its endpoint NAME in `upstreamLabel`, so its pool carries [`Endpoint::labeled`]
/// with the vault entry's label attached. Every other row keeps NO label, because the TypeScript
/// sets `upstreamLabel` only in the relay families. Publishing the vault's `STANDARD` /
/// `Standard` / `Default` entry name on the other rows would rewrite every request log line from
/// `model: <id>` to `STANDARD: <id>`, which is a visible change to a deployed log.
fn build_endpoints(
    row: &ClientSpec,
    slot: &VaultSlot,
    problems: &mut Vec<String>,
) -> Option<Vec<Endpoint>> {
    if row.behavior.publishes_relay_label() {
        let mut pool = Vec::with_capacity(slot.entries().len());

        for (index, entry) in slot.entries().iter().enumerate() {
            let label = entry.label();

            // The label is validated BEFORE anything is built from the entry, and the refusal
            // message deliberately does NOT echo it. This allow-list exists for the case where a
            // label slot was filled with something credential- or URL-shaped, and a refusal that
            // printed the offending string would leak exactly what it is complaining about.
            if !upstream_label_is_safe(label) {
                problems.push(format!(
                    "endpoint entry #{index} of slot '{}' has a relay name this layer refuses \
                     to print ({} bytes, credential- or URL-shaped)",
                    slot.id(),
                    label.len()
                ));

                continue;
            }

            // Safe to print from here on: the label passed the allow-list, so by construction it
            // is neither a credential nor a URL.
            let Some(base_url) = entry.value_str() else {
                problems.push(format!(
                    "endpoint entry '{label}' of slot '{}' carries no usable URL",
                    slot.id()
                ));

                continue;
            };

            pool.push(Endpoint::labeled(base_url, label));
        }

        return Some(pool);
    }

    // Non-relay rows: the declared default, with the first entry's value as the fallback for
    // a hand-written document that declares entries but no `default` field. (Every GENERATED
    // artifact declares `default`; suites build documents by hand.)
    let Some(url) = slot
        .default_public()
        .or_else(|| slot.entries().iter().find_map(VaultEntry::value_str))
        .filter(|url| !url.trim().is_empty())
    else {
        problems.push(format!(
            "endpoint slot '{}' has no default value, and the row selects no failover pool",
            slot.id()
        ));

        return None;
    };

    Some(vec![Endpoint::new(url)])
}

/// The credential pool: every entry in document order, unfiltered.
///
/// THE FILTER QUESTION, answered because it changes deployed behaviour. The vault classifies
/// each entry and [`VaultSlot::usable`] exists; this port deliberately uses
/// [`VaultSlot::entries`], because the TypeScript's pool is `Object.keys(MAKORA_API_KEYS)` -
/// every entry, valid or not. `open_router` is the live proof: its single entry classifies
/// `invalid`, yet the deployed client still constructs with that placeholder value, still
/// sends `Authorization: Bearer NotAvailable`, and stays routable by id - it is only removed
/// from the catalog (`model-class.test.ts:288-296`). Filtering the pool here would have made
/// that row UNROUTABLE, which is a behaviour change dressed up as validation.
///
/// A nested (group) entry is refused instead: no client row rotates a group, and the
/// TypeScript's `Object.keys` over a nested map would hand the SDK a credential whose value is
/// an object - a bug, not a behaviour to port.
fn build_credentials(
    row: &ClientSpec,
    slot: &VaultSlot,
    problems: &mut Vec<String>,
) -> Option<Vec<Credential>> {
    let mut pool = Vec::with_capacity(slot.entries().len());

    for entry in slot.entries() {
        if entry.is_group() {
            problems.push(format!(
                "credential slot '{}' contains the nested group '{}'; no client row rotates a \
                 group, so the row must name a flat slot",
                slot.id(),
                entry.label()
            ));

            return None;
        }

        // `row` stays in the signature for the symmetry of the two builders: a future
        // per-row credential rule (a provider-specific sentinel set, say) has its row here
        // without another refactor.
        let _ = row;

        pool.push(Credential::from_entry(entry));
    }

    Some(pool)
}
