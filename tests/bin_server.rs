//! Coverage for `src/bin/server.rs` — the standalone listener's PROCESS rules: flag/environment
//! precedence, the credential gate, the banner labels, the router it builds, and its log filter.
//!
//! # How a binary gets tested from `tests/`
//!
//! A `[[bin]]` target is not importable, so this file includes it with `#[path]` and asserts the
//! module directly. That is the sibling bin's strategy — `packages/typecrate/harness/src/bin/server.rs:527-539`
//! explains why the pure-function shape is mandatory in the first place: writing the process
//! environment is `unsafe` in edition 2024 and this crate forbids `unsafe` outright, so a test
//! feeds values to `resolve()`/`from_parts()` rather than calling `set_var`, and `serve`'s own
//! [`serve::vault_source_from_values`] exists for the same reason. The only difference here is that
//! the tests live under `tests/` instead of inline, which is why the items they reach are
//! `pub(crate)` (`src/bin/server.rs`, "Why a binary has `pub(crate)` items in it").
//!
//! `main()` itself is the one thing NOT asserted: it is three `process::exit` calls plus a runtime
//! build, and the exit codes are pinned by the live smoke test recorded in the task evidence
//! (`--help` → 0, bogus vault path → 1, SIGTERM → 0). What IS asserted here is the decision that
//! reaches each of them.
//!
//! WHY THE ROUTER IS SERVED OVER A REAL SOCKET: `router()` is where the mount contract lives —
//! `ANY` on the four paths, `GET` only on `/health`, the disabled body limit, the gateway layer. A
//! verb that takes 400 on a contract route and a verb that takes 405 on `/health` is exactly the
//! difference between "handler verdict" and "router verdict", and only a request can tell them
//! apart.
//! `tests/gateway.rs:19-24` gives the same reasoning for the layer it covers.

#![allow(clippy::too_many_lines)]

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::http::Method;
use serde_json::Value;
use tokio::net::TcpListener;
use tracing_core::Level;

use typecrate_provider::{
    registry::Registry,
    serve::{
        self, CHAT_COMPLETIONS_ROUTE, DEFAULT_VAULT_ARTIFACT, HANDLER_NOT_FOUND, JSON_CONTENT_TYPE,
        MISSING_ID_ERROR, MODELS_ROUTE, REGENERATION_COMMAND, RESPONSES_ROUTE, STATS_ROUTE,
        ServeState, VAULT_JSON_ENV,
    },
};

#[path = "../src/bin/server.rs"]
#[allow(
    dead_code,
    reason = "the file is the binary's `main`, which this test target never calls; the binary \
              target itself is compiled WITHOUT this allow, so real dead code in the product is \
              still caught by the `-D warnings` gate"
)]
mod server;

use server::{
    Config, DEFAULT_HOST, EXIT_OK, EXIT_RUNTIME, EXIT_USAGE, Env, Gate, HEALTH_PATH, HELP,
    HOST_REFUSAL, PORT_ENV, banner_api, banner_health, banner_url, from_parts, gate, parse_flags,
    parse_host, resolve, router, source_label,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// `&[&str]` -> `Vec<OsString>`, i.e. what `std::env::args_os` hands `main`.
fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(|value| OsString::from(*value)).collect()
}

/// Resolution with an EMPTY environment, i.e. what a plain `cargo run --bin` does.
fn config(values: &[&str]) -> Config {
    resolve(&args(values), &Env::default())
        .unwrap_or_else(|error| panic!("`{values:?}` resolves on an empty environment: {error}"))
}

/// Resolution with an explicit environment map — the `set_var`-free path (module doc).
fn config_with(values: &[&str], env: &Env) -> Config {
    resolve(&args(values), env).unwrap_or_else(|error| panic!("`{values:?}` resolves: {error}"))
}

/// A ready state with NO rows, so the catalog is the empty list and no vault artifact has to
/// exist: the same constructor `tests/routes.rs:327` uses.
fn ready_state() -> ServeState {
    ServeState::Ready(Registry::from_clients(Vec::new()))
}

/// The state of a deployment that never exported its vault — the exact input the credential gate
/// exists to catch. Addressed through an explicit path, never through the environment.
fn missing_vault_state() -> ServeState {
    serve::state_from_source(&serve::VaultSource::Path(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/no-such-vault.json"),
    ))
}

/// A hermetic client (`no_proxy`, as `tests/routes.rs:289-295` explains).
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("a proxy-free client")
}

/// One answer as it arrived on the wire.
struct Answer {
    status: reqwest::StatusCode,
    headers: reqwest::header::HeaderMap,
    body: String,
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|error| panic!("the body is not JSON: {error}\n{}", self.body))
    }
}

/// THE ROUTER UNDER TEST — `src/bin/server.rs`'s own `router()`, on a real loopback listener.
///
/// This is the same value the process serves; `axum::serve` is reached with `with_state` exactly as
/// `run()` does it.
async fn serve(state: ServeState) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback listener for the bin's router");
    let address = listener
        .local_addr()
        .expect("the bound address of a loopback listener")
        .to_string();

    let service = router().with_state(Arc::new(state));

    tokio::spawn(async move {
        axum::serve(listener, service)
            .await
            .expect("the bin's listener lives until the process ends");
    });

    address
}

async fn request(
    address: &str,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<String>,
) -> Answer {
    let mut request = http_client().request(method.clone(), format!("http://{address}{path}"));

    for (name, value) in headers {
        request = request.header(*name, *value);
    }

    if let Some(body) = body {
        request = request.body(body);
    }

    let response = request
        .send()
        .await
        .unwrap_or_else(|error| panic!("the {method} {path} request failed: {error}"));

    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .bytes()
        .await
        .expect("a body the server finished writing");

    Answer {
        status,
        headers,
        body: String::from_utf8(bytes.to_vec()).expect("every provider body is UTF-8"),
    }
}

// ---------------------------------------------------------------------------
// The bind address: flags, environment, defaults, in one precedence chain
// ---------------------------------------------------------------------------

#[test]
fn the_default_is_a_reachable_listener_on_the_declared_port() {
    // A cloud container has no client on loopback, so this host binds the wildcard by default —
    // the deliberate difference from `packages/typecrate/harness/src/bin/server.rs:40` (127.0.0.1).
    let resolved = config(&[]);

    assert_eq!(
        resolved.address,
        "0.0.0.0:8800".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(DEFAULT_HOST, "0.0.0.0");
    // The port default is the crate's own route-table fact, not a second literal.
    assert_eq!(resolved.address.port(), serve::PROVIDER_PORT);
    assert!(!resolved.allow_unconfigured);
    assert!(!resolved.help);
}

#[test]
fn the_environment_supplies_the_address_when_no_flag_names_one() {
    // `PORT` is the variable a container runner injects, so the deploy needs no flag at all.
    assert_eq!(
        config_with(
            &[],
            &Env {
                host: Some("127.0.0.1".to_owned()),
                port: Some("7001".to_owned()),
            },
        )
        .address,
        "127.0.0.1:7001".parse::<SocketAddr>().unwrap()
    );

    // One variable alone still composes with the other default.
    assert_eq!(
        config_with(
            &[],
            &Env {
                host: None,
                port: Some("0".to_owned()),
            },
        )
        .address,
        "0.0.0.0:0".parse::<SocketAddr>().unwrap()
    );

    // A blank value counts as UNSET — what `export PORT=` leaves behind — the same rule `serve`
    // applies to the two vault variables (`serve::vault_source_from_values`).
    assert_eq!(
        config_with(
            &[],
            &Env {
                host: Some("   ".to_owned()),
                port: Some(String::new()),
            },
        )
        .address,
        "0.0.0.0:8800".parse::<SocketAddr>().unwrap()
    );

    // Padding around a real value is trimmed, not rejected.
    assert_eq!(
        config_with(
            &[],
            &Env {
                host: None,
                port: Some(" 9009 ".to_owned()),
            },
        )
        .address,
        "0.0.0.0:9009".parse::<SocketAddr>().unwrap()
    );
}

#[test]
fn a_flag_beats_the_environment_it_would_otherwise_read() {
    let resolved = config_with(
        &["--host", "127.0.0.1", "--port", "4321"],
        &Env {
            host: Some("10.0.0.1".to_owned()),
            port: Some("7001".to_owned()),
        },
    );

    assert_eq!(
        resolved.address,
        "127.0.0.1:4321".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(PORT_ENV, "PORT");
}

#[test]
fn an_address_that_already_names_a_port_wins_over_the_port_flag() {
    // The more specific value is authoritative, so the two flags cannot disagree
    // (`packages/typecrate/harness/src/bin/server.rs:259-283`, the same rule).
    assert_eq!(
        config(&["--host", "0.0.0.0:1", "--port", "4321"])
            .address
            .to_string(),
        "0.0.0.0:1"
    );
    assert_eq!(
        config(&["--host", "[::1]", "--port", "4321"])
            .address
            .to_string(),
        "[::1]:4321"
    );
    assert_eq!(
        config(&["--host", "[::1]"]).address.to_string(),
        format!("[::1]:{}", serve::PROVIDER_PORT)
    );
}

#[test]
fn an_unusable_port_host_or_option_is_bad_usage() {
    // Each of these is exit 2 in `main`, i.e. "a restart will not fix this, an edit will".
    assert_eq!(
        resolve(&args(&["--port", "8800a"]), &Env::default()).unwrap_err(),
        "--port expects a number in 0..=65535, got `8800a`"
    );
    assert_eq!(
        resolve(&args(&["--port"]), &Env::default()).unwrap_err(),
        "--port needs a value"
    );
    assert_eq!(
        resolve(&args(&["--host"]), &Env::default()).unwrap_err(),
        "--host needs a value"
    );
    assert_eq!(
        resolve(&args(&["--nope"]), &Env::default()).unwrap_err(),
        "unknown option `--nope`"
    );

    // A `PORT` the runner set to something unparseable is refused, NOT silently replaced by 8800:
    // binding a different port than the probe dials is how a healthy-looking container becomes
    // unreachable.
    assert_eq!(
        resolve(
            &args(&[]),
            &Env {
                host: None,
                port: Some("99999".to_owned()),
            },
        )
        .unwrap_err(),
        "PORT expects a number in 0..=65535, got `99999`"
    );
    assert_eq!(
        resolve(
            &args(&[]),
            &Env {
                host: None,
                port: Some("auto".to_owned()),
            },
        )
        .unwrap_err(),
        "PORT expects a number in 0..=65535, got `auto`"
    );

    // A bare `::1` stays a refusal: unbracketed it is ambiguous with `host:port`, and this CLI
    // never guesses. A hostname is a refusal too — the bind address must be numeric, so the start
    // never depends on a nameserver.
    assert_eq!(
        parse_host("::1", 8800).unwrap_err(),
        format!("`::1` {HOST_REFUSAL}")
    );
    assert_eq!(
        parse_host("provider.internal", 8800).unwrap_err(),
        format!("`provider.internal` {HOST_REFUSAL}")
    );
    assert_eq!(
        parse_host("[::1]", 1).unwrap(),
        "[::1]:1".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(
        parse_host("0.0.0.0", 0).unwrap(),
        "0.0.0.0:0".parse::<SocketAddr>().unwrap()
    );
}

#[test]
fn help_short_circuits_resolution_but_not_an_unknown_option() {
    // Printing usage must not depend on an address parsing, and must not survive a typo in the
    // flag list either — the same two rules `packages/typecrate/harness/src/bin/server.rs:489-499`
    // pins for the sibling CLI.
    let resolved = config(&["--help", "--host", "not an address"]);
    assert!(resolved.help);
    assert_eq!(
        resolved.address,
        "0.0.0.0:8800".parse::<SocketAddr>().unwrap()
    );

    // A bad `PORT` cannot stop `--help` either, because resolution is skipped entirely.
    assert!(
        config_with(
            &["--help"],
            &Env {
                host: None,
                port: Some("nope".to_owned()),
            },
        )
        .help
    );

    assert_eq!(
        resolve(&args(&["--help", "--nope"]), &Env::default()).unwrap_err(),
        "unknown option `--nope`"
    );
}

#[test]
fn the_flags_parse_into_options_so_the_precedence_stays_one_chain() {
    // Every field is an `Option`, which is what makes `from_parts` a chain of `or_else` rather
    // than interleaved overwrites.
    assert_eq!(
        parse_flags(&args(&[
            "--host",
            "10.0.0.2",
            "--port",
            "1",
            "--allow-unconfigured"
        ]))
        .unwrap(),
        server::Flags {
            host: Some("10.0.0.2".to_owned()),
            port: Some(1),
            allow_unconfigured: true,
            help: false,
        }
    );

    // `-h` is HOST, never help — a CLI that reuses `-h` for two things cannot be scripted.
    assert!(!parse_flags(&args(&["-h", "::1"])).unwrap().help);
    assert!(parse_flags(&args(&["--help"])).unwrap().help);
    assert_eq!(
        from_parts(&server::Flags::default(), &Env::default())
            .unwrap()
            .address
            .port(),
        serve::PROVIDER_PORT
    );
}

// ---------------------------------------------------------------------------
// The credential gate: the one rule that decides whether this process binds at all
// ---------------------------------------------------------------------------

#[test]
fn a_refused_vault_refuses_the_bind_and_names_the_fix() {
    let state = missing_vault_state();
    assert!(
        !state.is_ready(),
        "a path that does not exist cannot be ready"
    );

    let Gate::Refuse(reason) = gate(&state, false) else {
        panic!("a missing artifact with no opt-out must refuse the bind");
    };

    // The two halves an operator needs, in the printable sentence `Unavailable` already carries:
    // the artifact that was tried, and the one command that regenerates it.
    assert!(
        reason.contains(REGENERATION_COMMAND),
        "the refusal does not name the regeneration command: {reason}"
    );
    assert!(
        reason.contains(&format!(
            "{}",
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/no-such-vault.json")
                .display()
        )),
        "the refusal does not name the artifact that was tried: {reason}"
    );

    // The opt-out must NOT be able to change what a ready state means, and a refusal keeps the
    // same text whichever way the flag goes — one message, one fix.
    let Gate::ProceedAnyway(same) = gate(&state, true) else {
        panic!("`--allow-unconfigured` keeps the library's lazy 503 instead of refusing");
    };
    assert_eq!(same, reason);

    assert_eq!(gate(&ready_state(), false), Gate::Ready);
    assert_eq!(gate(&ready_state(), true), Gate::Ready);

    // The exit code the refusal maps to, stated where the test can see it: 1, not 2 — the CLI was
    // used correctly and the deployment is what is broken.
    assert_eq!((EXIT_OK, EXIT_RUNTIME, EXIT_USAGE), (0, 1, 2));
}

#[test]
fn the_banner_names_the_vault_input_without_printing_it() {
    // The secret rule of this crate (`src/serve.rs`, "The secret rule this module is responsible
    // for"): the log and the banner never see credential bytes. An INLINE document IS the
    // credential set, so `--{VAULT_JSON_ENV}` deployments get the VARIABLE name, not the value -
    // `Debug` on that variant would print every key next to the port number.
    let document = r#"{"secrets":["sk-live-9f3a7c21be4d0000feed"]}"#;

    assert_eq!(
        source_label(&serve::VaultSource::Inline(document.to_owned())),
        VAULT_JSON_ENV
    );
    assert!(
        !source_label(&serve::VaultSource::Inline(document.to_owned())).contains("sk-live"),
        "an inline vault document leaked into the banner"
    );

    assert_eq!(
        source_label(&serve::VaultSource::Path(PathBuf::from(
            "./agentic-provider.json"
        ))),
        "./agentic-provider.json"
    );
    assert_eq!(
        source_label(&serve::VaultSource::DefaultArtifact),
        DEFAULT_VAULT_ARTIFACT
    );
}

#[test]
fn the_banner_reports_urls_a_client_can_open() {
    // `0.0.0.0` is a set of interfaces and no client can navigate to it, so a wildcard bind is
    // reported on loopback and an explicit bind is reported as itself.
    assert_eq!(
        banner_api("0.0.0.0:8800".parse().unwrap()),
        format!("http://127.0.0.1:8800{}", serve::MOUNT)
    );
    assert_eq!(
        banner_health("0.0.0.0:8800".parse().unwrap()),
        format!("http://127.0.0.1:8800{HEALTH_PATH}")
    );
    assert_eq!(
        banner_api("192.168.1.20:8800".parse().unwrap()),
        format!("http://192.168.1.20:8800{}", serve::MOUNT)
    );
    assert_eq!(
        banner_url(HEALTH_PATH, "[::1]:8800".parse().unwrap()),
        format!("http://[::1]:8800{HEALTH_PATH}")
    );
}

#[test]
fn help_documents_every_line_a_start_prints() {
    // `--help` is the offline documentation an operator reads from a container shell, so each rule
    // this file implements is asserted to be IN it rather than eyeballed.
    for documented in [
        "--host",
        "--port",
        "--allow-unconfigured",
        "--help",
        "PORT",
        "TYPECRATE_PROVIDER_HOST",
        VAULT_JSON_ENV,
        serve::VAULT_PATH_ENV,
        "RUST_LOG",
        CHAT_COMPLETIONS_ROUTE,
        RESPONSES_ROUTE,
        MODELS_ROUTE,
        STATS_ROUTE,
        HEALTH_PATH,
        "Access-Control-Allow-Private-Network",
        "204",
        "0 clean shutdown, 1 could not serve",
        "SIGTERM",
        "drains in-flight",
    ] {
        assert!(
            HELP.contains(documented),
            "`--help` does not document `{documented}`"
        );
    }
}

// ---------------------------------------------------------------------------
// The router: the mount the process actually serves
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_bin_serves_health_and_the_four_contract_routes() {
    let address = serve(ready_state()).await;

    // `/health` is the bin-only probe: 200, and the exact document `serve::health_document` builds.
    let health = request(&address, Method::GET, HEALTH_PATH, &[], None).await;
    assert_eq!(health.status.as_u16(), 200);
    assert_eq!(health.header("content-type"), Some(JSON_CONTENT_TYPE));
    assert_eq!(
        health.json(),
        serde_json::json!({
            "ok": true,
            "service": "typecrate-provider",
            "version": typecrate_provider::version(),
        })
    );

    // The catalog answers from the router-mounted handler, with the policy headers the gateway
    // layer adds (the layer's own behaviour is `tests/gateway.rs`).
    let models = request(&address, Method::GET, MODELS_ROUTE, &[], None).await;
    assert_eq!(models.status.as_u16(), 200);
    assert_eq!(
        models.json(),
        serde_json::json!({ "object": "list", "data": [] })
    );
    assert_eq!(
        models.header("access-control-allow-private-network"),
        Some("true")
    );

    // A preflight is answered before routing, on THIS router too.
    let preflight = request(
        &address,
        Method::OPTIONS,
        MODELS_ROUTE,
        &[("access-control-request-method", "GET")],
        None,
    )
    .await;
    assert_eq!(preflight.status.as_u16(), 204);

    // `GET /stats` is the document, and `POST /stats` without `?id=` is the reset's own 400 - both
    // proofs that the mount reaches the handler behind the two verbs the TypeScript declares.
    let stats = request(&address, Method::GET, STATS_ROUTE, &[], None).await;
    assert_eq!(stats.status.as_u16(), 200);
    let reset = request(&address, Method::POST, STATS_ROUTE, &[], None).await;
    assert_eq!(reset.status.as_u16(), 400);
    assert_eq!(
        reset.json(),
        serde_json::json!({ "error": MISSING_ID_ERROR })
    );
}

#[tokio::test]
async fn every_contract_route_is_any_so_the_handler_prints_the_verb_refusal() {
    // THE reason the mount is `any(...)` and not `get(...)`/`post(...)`: the deployed gateway
    // answers a verb its handler map does not declare with `400 {"error":"handler not found"}`, and
    // a router-level verb filter would pre-empt that with axum's body-less 405
    // (`endpoint/provider/provider-route.rs`, "Why every group is `ANY`").
    let address = serve(ready_state()).await;

    for path in [
        CHAT_COMPLETIONS_ROUTE,
        RESPONSES_ROUTE,
        MODELS_ROUTE,
        STATS_ROUTE,
    ] {
        let answer = request(&address, Method::PUT, path, &[], None).await;

        assert_eq!(answer.status.as_u16(), 400, "PUT {path}");
        assert_eq!(
            answer.json(),
            serde_json::json!({ "error": HANDLER_NOT_FOUND }),
            "PUT {path}"
        );
    }

    // `/health` is the deliberate contrast: it is the BIN's own route, not the contract's, so it
    // takes axum's router verdict instead of the gateway's handler verdict.
    let health = request(&address, Method::PUT, HEALTH_PATH, &[], None).await;
    assert_eq!(health.status.as_u16(), 405);

    // And a path this process does not mount is a 404 that still carries the policy, i.e. the
    // gateway layer really is the outermost one on this router too.
    let missing = request(
        &address,
        Method::GET,
        "/providers/private/v1/nope",
        &[],
        None,
    )
    .await;
    assert_eq!(missing.status.as_u16(), 404);
    assert_eq!(
        missing.header("access-control-allow-private-network"),
        Some("true")
    );
}

#[tokio::test]
async fn the_contract_routes_answer_503_while_health_still_answers_200() {
    // The whole point of splitting the two surfaces: with `--allow-unconfigured`, a listener can be
    // up (probe-visible) while the routes report the credential fault verbatim. `GET /health` is a
    // liveness claim; the 503 body is the readiness answer, and it names the fix.
    let address = serve(missing_vault_state()).await;

    let health = request(&address, Method::GET, HEALTH_PATH, &[], None).await;
    assert_eq!(health.status.as_u16(), 200);

    let models = request(&address, Method::GET, MODELS_ROUTE, &[], None).await;
    assert_eq!(models.status.as_u16(), 503);
    let reason = models.json()["error"]
        .as_str()
        .expect("`error` is a bare string")
        .to_owned();
    assert!(
        reason.contains(REGENERATION_COMMAND),
        "the 503 does not name the regeneration command: {reason}"
    );
}

#[tokio::test]
async fn a_body_over_one_mebibyte_is_read_in_full_and_reaches_the_model_lookup() {
    // There is no request-body cap: a body several times the old 1 MiB ceiling is read in full and
    // answered by the model lookup (404, the id owns no row) instead of the 500 an unreadable body
    // produces. `to_bytes(.., usize::MAX)` and the disabled `DefaultBodyLimit` are what make the
    // large request take the SAME verdict as the small one.
    let address = serve(ready_state()).await;

    let small = request(
        &address,
        Method::POST,
        CHAT_COMPLETIONS_ROUTE,
        &[("content-type", JSON_CONTENT_TYPE)],
        Some(r#"{"model":"nope/missing","input":"hi"}"#.to_owned()),
    )
    .await;
    assert_eq!(small.status.as_u16(), 404);
    assert_eq!(
        small.json(),
        serde_json::json!({ "error": "Model 'nope/missing' not found" })
    );

    let large = request(
        &address,
        Method::POST,
        CHAT_COMPLETIONS_ROUTE,
        &[("content-type", JSON_CONTENT_TYPE)],
        Some(format!(
            r#"{{"model":"nope/missing","input":"{}"}}"#,
            "u".repeat(2 * 1024 * 1024),
        )),
    )
    .await;
    assert_eq!(large.status.as_u16(), 404);
    assert_eq!(
        large.json(),
        serde_json::json!({ "error": "Model 'nope/missing' not found" })
    );
}

// ---------------------------------------------------------------------------
// The log surface: `RUST_LOG`, and what one event becomes
// ---------------------------------------------------------------------------

#[test]
fn a_bare_level_sets_the_global_filter_and_nothing_else() {
    use server::logging::Filter;

    let level = |text: &str| Filter::parse(text).global;
    assert_eq!(level("off"), tracing_core::LevelFilter::OFF);
    assert_eq!(level("none"), tracing_core::LevelFilter::OFF);
    assert_eq!(level("error"), tracing_core::LevelFilter::ERROR);
    assert_eq!(level("warn"), tracing_core::LevelFilter::WARN);
    assert_eq!(level("info"), tracing_core::LevelFilter::INFO);
    assert_eq!(level("debug"), tracing_core::LevelFilter::DEBUG);
    assert_eq!(level("trace"), tracing_core::LevelFilter::TRACE);
    // Case and padding are irrelevant.
    assert_eq!(level("  WARN  "), tracing_core::LevelFilter::WARN);
    assert_eq!(level("Error"), tracing_core::LevelFilter::ERROR);

    // Unset, empty items and a spec of only commas all stay at the default.
    assert_eq!(Filter::parse("").global, server::logging::DEFAULT_LEVEL);
    assert_eq!(Filter::parse("").scoped, []);
    assert_eq!(Filter::parse(", ,").global, server::logging::DEFAULT_LEVEL);
    // The last bare level in the spec wins.
    assert_eq!(
        Filter::parse("info,trace,debug").global,
        tracing_core::LevelFilter::DEBUG
    );
}

#[test]
fn a_target_prefixed_directive_scopes_its_level_and_a_bad_one_is_reported() {
    use server::logging::{Directive, Filter};

    let scoped = Filter::parse("info,typecrate_provider=trace");
    assert_eq!(scoped.global, tracing_core::LevelFilter::INFO);
    assert_eq!(
        scoped.scoped,
        [Directive {
            prefix: Some("typecrate_provider".to_owned()),
            level: tracing_core::LevelFilter::TRACE,
        }]
    );
    // An empty prefix is the global level, not a prefix that matches everything.
    assert_eq!(
        Filter::parse("=debug").global,
        tracing_core::LevelFilter::DEBUG
    );
    assert_eq!(Filter::parse("=debug").scoped, []);

    // A knob that is misconfigured says so and keeps serving: never a guess, never an abort.
    let misconfigured = Filter::parse("bogus,typecrate_provider=nope,debug");
    assert_eq!(misconfigured.global, tracing_core::LevelFilter::DEBUG);
    assert_eq!(misconfigured.scoped, []);
    assert_eq!(
        misconfigured.ignored,
        ["bogus".to_owned(), "typecrate_provider=nope".to_owned()]
    );
}

#[test]
fn the_filter_allows_by_target_then_by_level() {
    use server::logging::Filter;

    let info = Filter::at(tracing_core::LevelFilter::INFO);
    assert!(info.allows("typecrate_provider::serve", Level::INFO));
    assert!(info.allows("typecrate_provider::serve", Level::WARN));
    assert!(!info.allows("typecrate_provider::serve", Level::DEBUG));

    // The longest matching prefix wins, so one broad directive can be narrowed by a longer one.
    let layered = Filter::parse("trace,typecrate_provider=warn,typecrate_provider::serve=error");
    assert!(layered.allows("typecrate_services::server", Level::TRACE));
    assert!(layered.allows("typecrate_provider::client", Level::WARN));
    assert!(!layered.allows("typecrate_provider::client", Level::INFO));
    assert!(layered.allows("typecrate_provider::serve", Level::ERROR));
    assert!(!layered.allows("typecrate_provider::serve", Level::WARN));

    // `off` is above every level, so it enables nothing.
    assert!(!Filter::at(tracing_core::LevelFilter::OFF).allows("anything", Level::ERROR));
}

/// The rendering contract, over a real dispatch: one event is exactly one line, and an event with
/// named fields keeps them after the message.
///
/// One test only, and it holds the interest cache rebuild for its whole body: `tracing` caches a
/// callsite's interest per subscriber, so two captures in one process stomp on each other (the
/// reasoning `packages/typecrate/services/src/logging.rs:519-530` states). The probe callsites are
/// private to this function, so no other test in this file can share them.
#[test]
fn one_event_becomes_exactly_one_line() {
    use server::logging::{Filter, LogSubscriber};

    let sink = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = LogSubscriber::new(
        SinkGuard(Arc::clone(&sink)),
        Filter::at(tracing_core::LevelFilter::TRACE),
    );
    let _guard = tracing::subscriber::set_default(subscriber);
    tracing::callsite::rebuild_interest_cache();

    probe_line();

    let written = sink_bytes(&sink);
    assert_eq!(
        written,
        "INFO  typecrate_provider::serve: provider route refused: the vault is missing vendors=3\n"
    );
}

/// A sink that forwards to the shared `Vec` the test reads. `LogSubscriber::new` owns its sink, so
/// the handle has to be a `Clone`able wrapper - `Arc<Mutex<..>>` alone would not satisfy
/// `io::Write`.
struct SinkGuard(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SinkGuard {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let mut sink = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::io::Write::write(&mut *sink, buffer)
    }

    fn write_all(&mut self, buffer: &[u8]) -> std::io::Result<()> {
        let mut sink = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::io::Write::write_all(&mut *sink, buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut sink = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::io::Write::flush(&mut *sink)
    }
}

fn sink_bytes(sink: &Arc<Mutex<Vec<u8>>>) -> String {
    let written = sink
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();

    String::from_utf8(written).expect("the subscriber writes UTF-8")
}

#[inline(never)]
fn probe_line() {
    tracing::info!(
        target: "typecrate_provider::serve",
        vendors = 3,
        "provider route refused: the vault is missing"
    );
}
