//! Build script: compiles the served model table from `config/provider/**/*.json`.
//!
//! `config/provider/` is the SOURCE OF TRUTH for the provider's model rows, laid
//! out one folder per provider (`config/provider/nvidia/`, `config/provider/vultr/`,
//! …). The scan is RECURSIVE: every `*.json` anywhere under it is one row, whatever
//! its file name and however deeply nested. This script parses and validates the
//! whole tree, then writes `$OUT_DIR/model_rows.rs`, which `src/clients.rs` includes.
//! A malformed row is therefore a compile error, not a request-time surprise.
//!
//! * **No credentials live here.** A row names a vault VENDOR and its credential/
//!   endpoint SLOT IDs (`config/vault/generated/agentic-provider.json` supplies the
//!   values at runtime, through `src/vault.rs`). The slot-name checks below reject
//!   anything that is not an `*_api_keys` / `*_api_endpoints` identifier.
//! * **Order is automatic.** Every `*.json` in the tree is read regardless of its
//!   name or depth, and the rows are sorted by `id` before rendering. There is no
//!   `order` field and no manifest to renumber; the build fails only on a
//!   duplicate `id` (or a malformed row).
//! * **Everything is validated at once.** Problems are collected and printed
//!   together, in the same spirit as `Registry::try_new`.
//! * `cargo::rerun-if-changed=config/provider` watches the tree, so ADDING a new
//!   row (in a new subdirectory too) re-runs this script; the tree contains only
//!   checked-in config, never build output.

use std::collections::HashSet;
use std::env;
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde::Deserialize;

/// The catalog context length every row advertises unless it says otherwise
/// (`src/client.rs::DEFAULT_CONTEXT_LENGTH`).
const DEFAULT_CONTEXT_LENGTH: u32 = 262_144;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelConfig {
    id: String,
    provider: String,
    model: String,
    vault: VaultRefs,
    behavior: Behavior,
    #[serde(default)]
    routing_vendor: Option<RoutingVendor>,
    #[serde(default)]
    credential_advance: Option<CredentialAdvance>,
    #[serde(default)]
    pricing: Option<Pricing>,
    #[serde(default = "enabled_by_default")]
    enabled: bool,
    #[serde(default)]
    reasoning_effort: Option<String>,
    #[serde(default)]
    coerce_none_effort: bool,
    #[serde(default)]
    output_cap: Option<u64>,
    #[serde(default = "usage_option_by_default")]
    usage_option: bool,
    #[serde(default = "default_context_length")]
    context_length: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VaultRefs {
    vendor: String,
    #[serde(default)]
    credential_slot: Option<String>,
    endpoint_slot: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pricing {
    input: f64,
    cache_read: f64,
    output: f64,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[allow(clippy::enum_variant_names)]
enum Behavior {
    #[serde(rename = "chat-plain")]
    ChatPlain,
    #[serde(rename = "chat-routed")]
    ChatRouted,
    #[serde(rename = "chat-credential-drain")]
    ChatCredentialDrain,
    #[serde(rename = "chat-keyless-relay")]
    ChatKeylessRelay,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RoutingVendor {
    #[serde(rename = "telnyx")]
    Telnyx,
    #[serde(rename = "pareto")]
    Pareto,
    #[serde(rename = "modal")]
    Modal,
    #[serde(rename = "deepinfra")]
    DeepInfra,
    #[serde(rename = "openai")]
    OpenAI,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum CredentialAdvance {
    #[serde(rename = "next-on-every-rotation")]
    NextOnEveryRotation,
    #[serde(rename = "same-then-next")]
    SameThenNext,
}

fn enabled_by_default() -> bool {
    true
}

fn usage_option_by_default() -> bool {
    true
}

fn default_context_length() -> u32 {
    DEFAULT_CONTEXT_LENGTH
}

fn main() -> ExitCode {
    let package_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let config_dir = package_dir.join("config").join("provider");

    // Watch the tree, not just today's files: a NEW row (or a new subdirectory)
    // must rebuild too.
    println!("cargo::rerun-if-changed=config/provider");

    match compile(&config_dir) {
        Ok(generated) => {
            let out_dir = PathBuf::from(
                env::var("OUT_DIR").expect("OUT_DIR is always set for a build script"),
            );
            let out_path = out_dir.join("model_rows.rs");

            // Only rewrite on change, so an unchanged table never forces a recompile.
            if fs::read_to_string(&out_path).ok().as_deref() != Some(generated.as_str()) {
                fs::write(&out_path, generated).expect("write generated model_rows.rs");
            }

            ExitCode::SUCCESS
        }
        Err(problems) => {
            for problem in problems {
                eprintln!("[typecrate-provider/config] {problem}");
            }

            ExitCode::FAILURE
        }
    }
}

/// Reads, validates, and renders the tree rooted at `config/provider`. Every
/// problem found is returned; nothing is written unless the whole set is clean.
fn compile(config_dir: &Path) -> Result<String, Vec<String>> {
    let mut problems = Vec::new();
    let mut rows: Vec<(String, ModelConfig)> = Vec::new();

    let mut paths = Vec::new();
    collect_json_files(config_dir, &mut paths, &mut problems);
    paths.sort();

    for path in paths {
        let relative = path.strip_prefix(config_dir).unwrap_or(&path);
        let label = format!("config/provider/{}", relative.display());

        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) => {
                problems.push(format!("{label}: cannot read: {error}"));
                continue;
            }
        };

        match serde_json::from_str::<ModelConfig>(&text) {
            Ok(config) => rows.push((label, config)),
            Err(error) => problems.push(format!("{label}: invalid JSON: {error}")),
        }
    }

    if rows.is_empty() && problems.is_empty() {
        problems.push(
            "config/provider: no *.json model files - it is the source of truth and may not be empty"
                .to_owned(),
        );
    }

    let mut ids: HashSet<&str> = HashSet::new();

    for (file_name, config) in &rows {
        validate(file_name, config, &mut ids, &mut problems);
    }

    if !problems.is_empty() {
        return Err(problems);
    }

    // File names are free-form and carry no meaning: the table order is the
    // alphabetical order of the routing ids, computed here so a new file can
    // never collide with or renumber an existing row.
    rows.sort_by(|left, right| left.1.id.cmp(&right.1.id));

    Ok(render(&rows))
}

/// Collects every `*.json` under `dir`, recursively. Symlinks are skipped
/// (`symlink_metadata` reports them as neither a file nor a directory), so a link
/// cycle can never make the build loop.
fn collect_json_files(dir: &Path, files: &mut Vec<PathBuf>, problems: &mut Vec<String>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            problems.push(format!(
                "{}: cannot read a directory: {error}",
                dir.display()
            ));
            return;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                problems.push(format!(
                    "{}: cannot read a directory entry: {error}",
                    dir.display()
                ));
                continue;
            }
        };

        let path = entry.path();

        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() => collect_json_files(&path, files, problems),
            Ok(metadata)
                if metadata.is_file()
                    && path.extension().and_then(OsStr::to_str) == Some("json") =>
            {
                files.push(path);
            }
            Ok(_) => {}
            Err(error) => problems.push(format!(
                "{}: cannot read file metadata: {error}",
                path.display()
            )),
        }
    }
}

fn validate<'a>(
    file_name: &'a str,
    config: &'a ModelConfig,
    ids: &mut HashSet<&'a str>,
    problems: &mut Vec<String>,
) {
    let fail = |problems: &mut Vec<String>, message: String| {
        problems.push(format!("{file_name}: {message}"))
    };

    if !ids.insert(config.id.as_str()) {
        fail(problems, format!("duplicate id {:?}", config.id));
    }

    if config.id.trim().is_empty() || !config.id.contains('/') {
        fail(
            problems,
            format!("id {:?} must be `<provider>/<model>`", config.id),
        );
    }
    if config.id.chars().any(char::is_whitespace) {
        fail(
            problems,
            format!("id {:?} must not contain whitespace", config.id),
        );
    }
    if config.provider.trim().is_empty() {
        fail(problems, "provider must not be empty".to_owned());
    }
    if config.model.trim().is_empty() {
        fail(problems, "model must not be empty".to_owned());
    }

    if !is_identifier(&config.vault.vendor) {
        fail(
            problems,
            format!(
                "vault.vendor {:?} must be a lower-case vendor id (a-z, 0-9, _)",
                config.vault.vendor
            ),
        );
    }

    if !is_identifier(&config.vault.endpoint_slot)
        || !config.vault.endpoint_slot.ends_with("_api_endpoints")
    {
        fail(
            problems,
            format!(
                "vault.endpoint_slot {:?} must name a vault slot ending in `_api_endpoints` - \
                 credentials and endpoint VALUES live in @config/vault, never in this file",
                config.vault.endpoint_slot
            ),
        );
    }

    match config.behavior {
        Behavior::ChatKeylessRelay => {
            if config.vault.credential_slot.is_some() {
                fail(
                    problems,
                    "behavior `chat-keyless-relay` requires vault.credential_slot: null".to_owned(),
                );
            }
            if config.routing_vendor.is_some() || config.credential_advance.is_some() {
                fail(
                    problems,
                    "behavior `chat-keyless-relay` does not take routing_vendor or credential_advance".to_owned(),
                );
            }
        }
        Behavior::ChatRouted => {
            if config.routing_vendor.is_none() {
                fail(
                    problems,
                    "behavior `chat-routed` requires routing_vendor".to_owned(),
                );
            }
            if config.credential_advance.is_some() {
                fail(
                    problems,
                    "behavior `chat-routed` does not take credential_advance".to_owned(),
                );
            }
            if config.vault.credential_slot.is_none() {
                fail(
                    problems,
                    "behavior `chat-routed` requires vault.credential_slot".to_owned(),
                );
            }
        }
        Behavior::ChatCredentialDrain => {
            if config.credential_advance.is_none() {
                fail(
                    problems,
                    "behavior `chat-credential-drain` requires credential_advance".to_owned(),
                );
            }
            if config.routing_vendor.is_some() {
                fail(
                    problems,
                    "behavior `chat-credential-drain` does not take routing_vendor".to_owned(),
                );
            }
            if config.vault.credential_slot.is_none() {
                fail(
                    problems,
                    "behavior `chat-credential-drain` requires vault.credential_slot".to_owned(),
                );
            }
        }
        Behavior::ChatPlain => {
            if config.routing_vendor.is_some() || config.credential_advance.is_some() {
                fail(
                    problems,
                    "behavior `chat-plain` does not take routing_vendor or credential_advance"
                        .to_owned(),
                );
            }
            if config.vault.credential_slot.is_none() {
                fail(
                    problems,
                    "behavior `chat-plain` requires vault.credential_slot".to_owned(),
                );
            }
        }
    }

    if let Some(slot) = &config.vault.credential_slot
        && (!is_identifier(slot) || !slot.ends_with("_api_keys"))
    {
        fail(
            problems,
            format!(
                "vault.credential_slot {slot:?} must name a vault slot ending in `_api_keys` - \
                 the credential VALUE lives in @config/vault, never in this file"
            ),
        );
    }

    if let Some(pricing) = &config.pricing {
        for (name, value) in [
            ("input", pricing.input),
            ("cache_read", pricing.cache_read),
            ("output", pricing.output),
        ] {
            if !value.is_finite() || value < 0.0 {
                fail(
                    problems,
                    format!("pricing.{name} must be a finite, non-negative USD rate (got {value})"),
                );
            }
        }
    }

    if let Some(effort) = &config.reasoning_effort {
        if effort.trim().is_empty() {
            fail(
                problems,
                "reasoning_effort must not be empty when present".to_owned(),
            );
        }
    } else if config.coerce_none_effort {
        fail(
            problems,
            "coerce_none_effort requires reasoning_effort".to_owned(),
        );
    }

    if config.output_cap == Some(0) {
        fail(
            problems,
            "output_cap must be greater than zero when present".to_owned(),
        );
    }

    if config.context_length == 0 {
        fail(
            problems,
            "context_length must be greater than zero".to_owned(),
        );
    }
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Renders the generated Rust module. The strings are emitted as string literals
/// with `Debug`, which is valid Rust source and escapes everything that needs it.
fn render(rows: &[(String, ModelConfig)]) -> String {
    let mut out = String::new();

    out.push_str(
        "// @generated by build.rs from config/provider/**/*.json. Do not edit by hand;\n",
    );
    out.push_str("// add or change a provider by editing its file in `config/provider/`.\n");
    out.push_str("pub const CLIENTS: &[ClientSpec] = &[\n");

    for (file_name, config) in rows {
        let _ = writeln!(out, "    // {file_name}");

        match config.behavior {
            Behavior::ChatKeylessRelay => {
                let _ = writeln!(
                    out,
                    "    ClientSpec::relay(\n        {},\n        {},\n        {},\n        {},\n        {},\n    )",
                    literal(&config.id),
                    literal(&config.provider),
                    literal(&config.model),
                    literal(&config.vault.vendor),
                    literal(&config.vault.endpoint_slot),
                );
            }
            _ => {
                let credential_slot = config
                    .vault
                    .credential_slot
                    .as_deref()
                    .expect("validated: non-relay rows carry a credential slot");

                let _ = writeln!(
                    out,
                    "    ClientSpec::plain(\n        {},\n        {},\n        {},\n        {},\n        {},\n        {},\n    )",
                    literal(&config.id),
                    literal(&config.provider),
                    literal(&config.model),
                    literal(&config.vault.vendor),
                    literal(credential_slot),
                    literal(&config.vault.endpoint_slot),
                );
            }
        }

        match config.behavior {
            Behavior::ChatRouted => {
                let vendor = config
                    .routing_vendor
                    .expect("validated: routed rows carry a routing vendor");

                let _ = writeln!(out, "    .routed({})", routing_vendor_code(vendor));
            }
            Behavior::ChatCredentialDrain => {
                let advance = config
                    .credential_advance
                    .expect("validated: drain rows carry a credential advance");

                let _ = writeln!(out, "    .drained({})", credential_advance_code(advance));
            }
            Behavior::ChatPlain | Behavior::ChatKeylessRelay => {}
        }

        if let Some(pricing) = &config.pricing {
            let _ = writeln!(
                out,
                "    .priced({:?}, {:?}, {:?})",
                pricing.input, pricing.cache_read, pricing.output
            );
        }

        if !config.enabled {
            out.push_str("    .hidden()\n");
        }

        if let Some(effort) = &config.reasoning_effort {
            if config.coerce_none_effort {
                let _ = writeln!(out, "    .effort_with_none({})", literal(effort));
            } else {
                let _ = writeln!(out, "    .effort({})", literal(effort));
            }
        }

        if let Some(cap) = config.output_cap {
            let _ = writeln!(out, "    .output_cap({cap})");
        }

        if !config.usage_option {
            out.push_str("    .without_usage_option()\n");
        }

        let _ = writeln!(out, "    .context_length({}),", config.context_length);
    }

    out.push_str("];\n");

    out
}

fn routing_vendor_code(vendor: RoutingVendor) -> &'static str {
    match vendor {
        RoutingVendor::Telnyx => "RoutingVendor::Telnyx",
        RoutingVendor::Pareto => "RoutingVendor::Pareto",
        RoutingVendor::Modal => "RoutingVendor::Modal",
        RoutingVendor::DeepInfra => "RoutingVendor::DeepInfra",
        RoutingVendor::OpenAI => "RoutingVendor::OpenAI",
    }
}

fn credential_advance_code(advance: CredentialAdvance) -> &'static str {
    match advance {
        CredentialAdvance::NextOnEveryRotation => "CredentialAdvance::NextOnEveryRotation",
        CredentialAdvance::SameThenNext => "CredentialAdvance::SameThenNext",
    }
}

fn literal(value: &str) -> String {
    format!("{value:?}")
}
