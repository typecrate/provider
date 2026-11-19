//! `typecrate-provider-server` - the standalone `/providers/private/v1` listener.
//!
//! The native form of the TypeScript provider process
//! (`runtime/endpoint/provider/private/*`, which the underload gateway binds on
//! `LOCAL_AREA_NETWORK_PROVIDER_PORT` = 6660; the Rust surface publishes 8800 - the deviation
//! is recorded on [`serve::PROVIDER_PORT`]). The library owns the handlers, the vault gate and
//! the response shapes (`src/serve.rs`); this file is the thinnest possible process wrapper:
//! flags, the credential gate, one bind, the banner, the shutdown signal.
//!
//! # Why a bin at all, when the rlib already has a host
//!
//! `endpoint/provider/provider-route.rs` compiles the same four handlers into
//! `typecrate-services`, which owns the CORS layer, the body cap for its own extractor routes, the
//! access log and the multi-listener bind. That is the right host for a repo dev box. It is the
//! wrong host for a cloud container: a modal/daytona payload is ONE executable plus ONE stage directory, and
//! pulling `typecrate-services` in would drag its generated route table, its playground
//! builtins and its `/v1/harness` gateway along, and would mean a container that runs Node or a
//! second binary to serve a provider. So this bin links ONLY `typecrate_provider`, and
//! `cargo build --release --bin typecrate-provider-server` produces an artifact with no JS
//! runtime in it at all.
//!
//! WHY THE BIN DOES NOT DEPEND ON `typecrate_services`: the two hosts are alternatives, never
//! stacked - an embedder that runs the discovered route file never starts this executable.
//! Keeping the edge one-way (`bin -> lib`) is also what keeps a CLI parse out of the library and
//! out of the services binary. The same shape is
//! `packages/typecrate/harness/src/bin/server.rs`, which this file mirrors in structure (gate
//! before bind, read-back address in the banner, SIGTERM, exit codes, pure resolver).
//!
//! # The one thing this file adds to the contract: it refuses to bind on nothing
//!
//! The library's default behaviour is deliberately SOFT: while the vault artifact is missing or
//! refused, every route answers `503` with the regeneration command, because that library is
//! compiled into `typecrate-services`, which also serves `/v1/harness` and must boot with no
//! artifact on disk (`src/serve.rs`, "The deviation the wire cannot avoid").
//!
//! A cloud readiness probe cannot tell those two processes apart. Container runners probe by TCP
//! connect (modal's and daytona's anonymous-payload probes both do), so a soft-503 listener
//! PASSES its readiness check while serving nothing but 503s, and a bad credential becomes a
//! silently-degraded deployment instead of a failed one. So the standalone host inverts the
//! default: [`gate`] runs BEFORE the bind, and an unavailable state is exit 1 with the reason on
//! stderr. `--allow-unconfigured` restores the library's lazy 503 for the cases where booting
//! empty is what the operator asked for.
//!
//! # Exit codes
//!
//! The sibling convention (`packages/typecrate/services/src/bin/server.rs:251-254`): `0` clean
//! shutdown, `1` could not serve, `2` bad usage - a supervisor restarts the second and fixes the
//! first. `main` builds the tokio runtime by hand rather than with `#[tokio::main]`, and calls
//! [`std::process::exit`] with the code, because `[profile.release] panic = "abort"`
//! (`Cargo.toml`) turns a `main -> Result` into an abort status instead of the promised code.
//!
//! # Why a binary has `pub(crate)` items in it
//!
//! The rules this file encodes - flag/environment precedence, the credential gate, the banner
//! labels, the log filter - are all PURE functions, and `tests/bin_server.rs` includes this file
//! with `#[path]` so it can assert them directly. That is the sibling bin's strategy
//! (`packages/typecrate/harness/src/bin/server.rs:527-539` explains why: writing the process
//! environment is `unsafe` in edition 2024 and this crate forbids `unsafe`, so a test feeds values
//! to a pure `resolve()` instead of calling `set_var`), with the tests living under `tests/`
//! instead of inline. `pub(crate)` is what makes them reachable from that test target without
//! making them part of any published surface - this is a binary, so nothing here is importable in
//! the first place. Nothing else in the file is widened.
//!
//! # Logging
//!
//! [`logging`] at the bottom of this file. The provider logs through the `tracing` facade - the
//! per-completion token line (`src/serve.rs:617-629`), the `[failover]` lines
//! (`src/client.rs:865-879`), the `[retry] HTTP 429` line whose stated intent is "so a pause is
//! not invisible in the terminal" (`src/transport.rs:1415-1418`) and the vault-down `warn!`
//! (`src/serve.rs:553`, whose own promise is that "a refused route has to be explainable from
//! the log alone"). `tracing`'s default dispatch is `NoSubscriber`, which answers `enabled(..)`
//! with `false`, so an executable that installs nothing throws every one of those lines away.
//! `tracing-subscriber` is not in any typecrate lockfile and would break
//! `tests/transport.rs::the_lock_package_set_is_a_strict_subset_of_the_siblings`, so the
//! subscriber is hand-written over `tracing` + `tracing-core`, mirroring
//! `packages/typecrate/services/src/logging.rs`.

use std::ffi::OsString;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Router, middleware};
use tokio::net::TcpListener;
use typecrate_provider::serve;

/// The executable's own name: the prefix every operator-facing line carries, so a line is
/// attributable when a supervisor pipes two processes into one log.
pub(crate) const PROGRAM: &str = "typecrate-provider-server";

/// The default bind. `0.0.0.0`, NOT the harness bin's `127.0.0.1`: a container has no client on
/// loopback - the probe and the caller both arrive on an interface address - so the standalone
/// provider is reachable by default and a caller who wants loopback passes it explicitly.
pub(crate) const DEFAULT_HOST: &str = "0.0.0.0";

/// The container-runner port variable. Named here rather than in the library because the
/// library's [`serve::PROVIDER_PORT`] is a route-table fact (services reads it), while `PORT` is
/// a process fact only this host has.
pub(crate) const PORT_ENV: &str = "PORT";

/// The bind-address variable, the host-scoped counterpart of [`PORT_ENV`].
pub(crate) const HOST_ENV: &str = "TYPECRATE_PROVIDER_HOST";

/// The readiness path. BIN-ONLY on purpose: it is not one of [`serve`]'s four contract routes,
/// and adding a fifth path constant to the library would change what
/// `packages/typecrate/services` lists for this package.
pub(crate) const HEALTH_PATH: &str = "/health";

/// Exit code for a clean Ctrl+C/SIGINT/SIGTERM shutdown.
pub(crate) const EXIT_OK: i32 = 0;
/// Exit code for anything the process could not do: the credential gate, a refused bind, a
/// runtime that would not start.
pub(crate) const EXIT_RUNTIME: i32 = 1;
/// Exit code for bad usage - including a `PORT`/`TYPECRATE_PROVIDER_HOST` the runner set to
/// something unparseable, which is an operator-typed value like a bad flag, not a server fault.
pub(crate) const EXIT_USAGE: i32 = 2;

pub(crate) const HELP: &str = "\
typecrate-provider-server - the native /providers/private/v1 listener

USAGE:
    cargo run --release --bin typecrate-provider-server -- [OPTIONS]
    typecrate-provider-server [OPTIONS]

OPTIONS:
    -h, --host <addr>          Bind address: <ip>, <ip>:<port>, [<ipv6>] or
                               [<ipv6>]:<port>. Default 0.0.0.0 (or
                               TYPECRATE_PROVIDER_HOST) - reachable by default,
                               unlike the sibling bins, because a container has no
                               client on loopback. An address that already names a
                               port wins over --port.
        --port <port>          Port, 0..=65535. Default: PORT, then 8800. Pass 0 for
                               any free port; the banner prints the port actually
                               bound, never the one asked for.
        --allow-unconfigured   Bind even when the vault artifact is missing,
                               unreadable or refused. Default off, and off means this
                               process exits 1 before binding and prints the
                               regeneration command. With the flag every contract
                               route answers 503 instead - the behaviour of the
                               typecrate-services gateway, which also serves other
                               routes and therefore cannot afford to abort.
        --help                 Print this help and exit 0.

ENVIRONMENT:
    PORT                          The port, unless --port names one. This is the
                                  variable container runners inject (modal, daytona,
                                  and a plain image whose entrypoint reads it), so a
                                  deploy needs no flag at all.
    TYPECRATE_PROVIDER_HOST       The bind address, unless --host names one.
    TYPECRATE_PROVIDER_VAULT_JSON The vault document inline, highest priority.
    TYPECRATE_PROVIDER_VAULT_PATH A path to the artifact, second priority; relative
                                  to the process's working directory, which for a
                                  pushed payload is the stage directory.
    RUST_LOG                      Verbosity: off, none, error, warn, info, debug or
                                  trace, optionally comma-separated with
                                  <target-prefix>=<level> to scope one module (for
                                  instance typecrate_provider::serve=trace). An unset
                                  value runs at info, which is where the token line,
                                  the failover lines and the retry pause are visible.

WHERE IT ANSWERS:
    ANY  /providers/private/v1/chat/completions
    ANY  /providers/private/v1/responses
    ANY  /providers/private/v1/models
    ANY  /providers/private/v1/stats
    GET  /health                  {\"ok\":true,\"service\":...,\"version\":...}
                                  - this process only; the typecrate-services
                                  gateway does not mount it.

The four contract routes are mounted as ANY so the handler prints the deployed
gateway's own verb-miss refusal, 400 {\"error\":\"handler not found\"}, instead of
axum's body-less 405. Every response carries Access-Control-Allow-Origin,
-Allow-Methods and -Headers as * plus Access-Control-Allow-Private-Network: true,
and an OPTIONS preflight - one carrying Access-Control-Request-Method - answers 204
before routing.

Exit codes: 0 clean shutdown, 1 could not serve (the vault artifact was refused and
--allow-unconfigured was not passed, or the address was already in use), 2 bad
usage.

The process stops on SIGTERM, SIGINT and Ctrl+C - a container sends SIGTERM, and the
default stop signal of an anonymous payload is SIGTERM too - and drains in-flight
requests before exiting.";

/// The resolved configuration the process runs on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Config {
    /// The bind address, already resolved from flags, environment and defaults.
    pub(crate) address: SocketAddr,
    /// `--allow-unconfigured`: keep the library's lazy 503 instead of refusing to bind.
    pub(crate) allow_unconfigured: bool,
    pub(crate) help: bool,
}

/// The two ambient address inputs, read ONCE and handed to the pure resolver.
///
/// Reading them here rather than inside [`from_parts`] is what keeps the precedence rules
/// unit-testable: writing the process environment is `unsafe` in edition 2024 and this crate
/// forbids `unsafe` outright (`[lints.rust]`), so a test feeds values instead of calling
/// `set_var` - the same reason `src/serve.rs` splits [`serve::vault_source_from_env`] from
/// [`serve::vault_source_from_values`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Env {
    pub(crate) host: Option<String>,
    pub(crate) port: Option<String>,
}

impl Env {
    /// The live process environment.
    #[must_use]
    pub(crate) fn from_env() -> Self {
        Self {
            host: std::env::var(HOST_ENV).ok(),
            port: std::env::var(PORT_ENV).ok(),
        }
    }
}

/// Parsed command line, before the environment and the defaults are applied. Every field is an
/// `Option` so the precedence chain stays one obvious `or_else` per setting instead of
/// interleaved overwrites.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Flags {
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) allow_unconfigured: bool,
    pub(crate) help: bool,
}

fn main() {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();

    let config = match resolve(&args, &Env::from_env()) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{PROGRAM}: {message}\n\n{HELP}");
            std::process::exit(EXIT_USAGE);
        }
    };

    if config.help {
        println!("{HELP}");
        std::process::exit(EXIT_OK);
    }

    // The log surface goes up BEFORE anything else can emit, for two independent reasons: with no
    // subscriber installed `tracing`'s `NoSubscriber` drops every event the provider library
    // produces, and a subscriber installed after the first requests have arrived would miss them.
    logging::install_from_env();

    // THE CREDENTIAL GATE, before the bind (module doc: why the standalone host inverts the
    // library's soft default). `process_state` is the library's own once-per-process state, so
    // this call also WARMS it - the artifact is read here, once, and not on the request that
    // first happens to arrive after a probe passes.
    let state = serve::process_state();

    match gate(state, config.allow_unconfigured) {
        Gate::Ready => {}
        Gate::ProceedAnyway(reason) => {
            // Loud, because the flag's whole risk is that it is set and then forgotten: the
            // banner will say `vault: <name>` and every route will answer 503.
            tracing::warn!(
                target: "typecrate_provider::server",
                "starting unconfigured anyway (--allow-unconfigured): {reason}"
            );
        }
        Gate::Refuse(reason) => {
            // `reason` already carries `serve::REGENERATION_COMMAND` (every `Unavailable`
            // sentence ends with it), so the fix is printed without repeating it.
            eprintln!("{PROGRAM}: refusing to bind: {reason}");
            std::process::exit(EXIT_RUNTIME);
        }
    }

    // The runtime is built by hand: `panic = "abort"` in the release profile would turn a
    // `#[tokio::main] async fn main() -> Result` into an abort status rather than the promised
    // 1 / 2 / 0 (module doc).
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("{PROGRAM}: could not start the tokio runtime: {error}");
            std::process::exit(EXIT_RUNTIME);
        }
    };

    std::process::exit(runtime.block_on(run(config, state)));
}

/// Flags + environment -> the configuration. Pure, so the precedence rules are testable without
/// touching the process environment (see [`Env`]).
pub(crate) fn resolve(args: &[OsString], env: &Env) -> Result<Config, String> {
    from_parts(&parse_flags(args)?, env)
}

/// The precedence chain, spelled once per setting:
///
/// * host: `--host` > `TYPECRATE_PROVIDER_HOST` > [`DEFAULT_HOST`];
/// * port: an inline port inside the host address > `--port` > `PORT` > [`serve::PROVIDER_PORT`].
///
/// The CLI wins over the environment because a flag is the more specific statement, and an
/// address that already names a port wins over `--port` because it is the more specific address -
/// the same two rules `packages/typecrate/harness/src/bin/server.rs:259-283` states.
pub(crate) fn from_parts(flags: &Flags, env: &Env) -> Result<Config, String> {
    if flags.help {
        // `--help` short-circuits RESOLUTION, not just the bind: printing usage must not depend on
        // an address parsing or a port being in range, so `--help PORT=nope` still prints help.
        return Ok(Config {
            address: SocketAddr::from(([0, 0, 0, 0], serve::PROVIDER_PORT)),
            allow_unconfigured: false,
            help: true,
        });
    }

    let host = flags
        .host
        .as_deref()
        .or(configured(env.host.as_deref()))
        .unwrap_or(DEFAULT_HOST);

    // A non-numeric `PORT` is a refusal rather than a fall-back-to-8800: a runner that set 99999
    // means a specific port, and silently binding another one is how a healthy-looking container
    // ends up unreachable.
    let env_port = configured(env.port.as_deref())
        .map(|raw| {
            raw.parse::<u16>()
                .map_err(|_| format!("{PORT_ENV} expects a number in 0..=65535, got `{raw}`"))
        })
        .transpose()?;

    let port = flags.port.or(env_port).unwrap_or(serve::PROVIDER_PORT);

    let address = parse_host(host, port)?;

    Ok(Config {
        address,
        allow_unconfigured: flags.allow_unconfigured,
        help: false,
    })
}

/// One flag pass. `-h` is the HOST flag (the sibling bin's rule, restated), so the help short
/// form is deliberately absent: a CLI that reuses `-h` for two things cannot be scripted.
pub(crate) fn parse_flags(args: &[OsString]) -> Result<Flags, String> {
    let mut flags = Flags::default();
    let mut index = 0;

    while index < args.len() {
        let flag = args[index]
            .to_str()
            .ok_or_else(|| format!("argument {} is not valid text", index + 1))?;

        index += 1;

        match flag {
            "--help" => flags.help = true,
            "-h" | "--host" => flags.host = Some(take_text(args, &mut index, flag)?),
            "--port" => {
                let raw = take_text(args, &mut index, flag)?;
                flags.port =
                    Some(raw.parse::<u16>().map_err(|_| {
                        format!("--port expects a number in 0..=65535, got `{raw}`")
                    })?);
            }
            "--allow-unconfigured" => flags.allow_unconfigured = true,
            other => return Err(format!("unknown option `{other}`")),
        }
    }

    Ok(flags)
}

/// One text flag value, advancing `index`; the name only points the error at the right flag.
fn take_text(args: &[OsString], index: &mut usize, name: &str) -> Result<String, String> {
    let raw = args
        .get(*index)
        .ok_or_else(|| format!("{name} needs a value"))?;

    *index += 1;

    raw.to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("{name} value is not valid text"))
}

/// One environment or flag value, with an empty or whitespace-only value read as UNSET - the same
/// rule [`serve`] applies to the two vault variables, and what `set VAR=` on Windows and
/// `export VAR=` on POSIX both leave behind.
fn configured(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// The refusal every unusable host spells, naming the accepted forms.
pub(crate) const HOST_REFUSAL: &str =
    "--host is not a valid address (expected <ip>, <ip>:<port>, [<ipv6>], or [<ipv6>]:<port>)";

/// Accepts `ip`, `ip:port`, and the bracketed IPv6 spellings `[::1]` / `[::1]:8800`. An inline
/// port wins over `port`, so the two cannot disagree about which one is authoritative. A bare
/// `::1` stays a refusal: without the brackets the value is ambiguous with `host:port`, and this
/// CLI never guesses. The bind address has to be NUMERIC - `TcpListener::bind` takes a resolved
/// `SocketAddr`, and inventing a DNS step would make the start depend on a nameserver rather than
/// on what the operator typed.
pub(crate) fn parse_host(raw: &str, port: u16) -> Result<SocketAddr, String> {
    if let Ok(address) = raw.parse::<SocketAddr>() {
        return Ok(address);
    }

    if let Some(stripped) = raw
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        && let Ok(ip) = stripped.parse::<IpAddr>()
    {
        return Ok(SocketAddr::new(ip, port));
    }

    if raw.contains(':') {
        return Err(format!("`{raw}` {HOST_REFUSAL}"));
    }

    format!("{raw}:{port}")
        .parse()
        .map_err(|_| format!("`{raw}` {HOST_REFUSAL}"))
}

/// What the credential gate decided. Split out of `main` so the three outcomes - and the exact
/// strings a deploy sees - are assertable without a filesystem, an environment or a socket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    /// The artifact was accepted; bind and serve it.
    Ready,
    /// It was refused, and `--allow-unconfigured` asked for the library's lazy 503 anyway.
    ProceedAnyway(String),
    /// It was refused, and this process must not bind at all (exit 1).
    Refuse(String),
}

/// The state + the opt-out flag -> the decision.
#[must_use]
pub(crate) fn gate(state: &serve::ServeState, allow_unconfigured: bool) -> Gate {
    if state.is_ready() {
        return Gate::Ready;
    }

    let reason = state
        .unavailable()
        .map(serve::Unavailable::message)
        .unwrap_or("the provider serving state is unavailable")
        .to_owned();

    if allow_unconfigured {
        Gate::ProceedAnyway(reason)
    } else {
        Gate::Refuse(reason)
    }
}

/// What the banner prints for the vault input.
///
/// The rule this exists to enforce: NO credential bytes. `VaultSource::Inline` holds the whole
/// document, `Debug`-ing it would print every key next to the port number, and the crate's secret
/// rule (`src/serve.rs`, "The secret rule this module is responsible for") is that the only
/// credential-derived value the log ever sees is a relay name. So an inline document is named by
/// the variable that carried it, exactly as [`serve::VaultSource::artifact`] does inside a refusal.
#[must_use]
pub(crate) fn source_label(source: &serve::VaultSource) -> String {
    match source {
        serve::VaultSource::Inline(_) => serve::VAULT_JSON_ENV.to_owned(),
        serve::VaultSource::Path(path) => path.display().to_string(),
        serve::VaultSource::DefaultArtifact => serve::DEFAULT_VAULT_ARTIFACT.to_owned(),
    }
}

/// Binds, prints the banner, serves until the shutdown signal, and reports the exit code.
async fn run(config: Config, state: &'static serve::ServeState) -> i32 {
    let listener = match TcpListener::bind(config.address).await {
        Ok(listener) => listener,
        Err(error) if error.kind() == ErrorKind::AddrInUse => {
            eprintln!(
                "{PROGRAM}: cannot bind {} - address already in use. Nothing was killed; pass \
                 --host/--port or free the port first.",
                config.address
            );
            return EXIT_RUNTIME;
        }
        Err(error) => {
            eprintln!("{PROGRAM}: cannot bind {}: {error}", config.address);
            return EXIT_RUNTIME;
        }
    };

    // The bound address is READ BACK rather than assumed, because `--port 0` asks for any free
    // port and a runner that injected `PORT` needs to see which one it actually got.
    let local = match listener.local_addr() {
        Ok(local) => local,
        Err(error) => {
            eprintln!(
                "{PROGRAM}: bound {} but cannot read its address: {error}",
                config.address
            );
            return EXIT_RUNTIME;
        }
    };

    println!(
        "{} v{} listening on {local}",
        serve::SERVICE_NAME,
        typecrate_provider::version()
    );
    println!("  api:      {}", banner_api(local));
    println!("  health:   {}", banner_health(local));
    println!(
        "  vault:    {}",
        source_label(&serve::vault_source_from_env())
    );

    // One `Arc` per process, one refcount bump per request: the state is immutable for the life of
    // the process, so the handler needs shared ownership, not a clone of the whole registry.
    let shared = Arc::new((*state).clone());

    // `with_state` comes LAST of the state-bearing chain: it erases the state type from the router,
    // which is what turns `Router<Arc<ServeState>>` into the `Router<()>` that `axum::serve`
    // accepts (`packages/typecrate/services/src/server/mod.rs:501` is the same move).
    let outcome = axum::serve(listener, router().with_state(shared))
        .with_graceful_shutdown(shutdown_signal())
        .await;

    match outcome {
        Ok(()) => {
            println!("{PROGRAM}: shutdown complete");
            EXIT_OK
        }
        Err(error) => {
            eprintln!("{PROGRAM}: server error: {error}");
            EXIT_RUNTIME
        }
    }
}

/// The whole HTTP surface of this process.
///
/// Two orders are load-bearing:
///
///   * every contract path is `any(...)`, never `get(...)`/`post(...)`. The mount being `ANY` is
///     what lets the HANDLER print the deployed gateway's verb-miss answer,
///     `400 {"error":"handler not found"}`; a router-level verb filter would pre-empt it with
///     axum's body-less 405 and would also stop an `OPTIONS` from ever reaching a handler. Same
///     reasoning as `endpoint/provider/provider-route.rs` ("Why every group is `ANY`"), stated
///     there for the services router and here for this one;
///   * `.layer` only affects routes registered BEFORE it, so [`DefaultBodyLimit`] is added after
///     the routes and [`serve::gateway`] is added LAST, so the cross-origin set and the preflight
///     `204` cover every response this process can produce, including a fallback 404. Identical
///     layer order to `packages/typecrate/services/src/server/mod.rs:480-539`, which is the point:
///     the same request gets the same headers whichever host served it.
///
/// `/health` is the one path here that is not the contract's, and it is deliberately OUTSIDE
/// [`serve::MOUNT`]: a probe path must not appear in the route table services lists for this
/// package.
///
/// There is **no** request-body cap, matching the deployed gateway: [`DefaultBodyLimit::disable`]
/// turns off axum's 2 MiB default as well, so no layer can refuse a body the handlers would read.
pub(crate) fn router() -> Router<Arc<serve::ServeState>> {
    Router::new()
        .route(serve::CHAT_COMPLETIONS_ROUTE, any(chat_completions))
        .route(serve::RESPONSES_ROUTE, any(responses))
        .route(serve::MODELS_ROUTE, any(models))
        .route(serve::STATS_ROUTE, any(stats))
        .route(HEALTH_PATH, get(health))
        .layer(DefaultBodyLimit::disable())
        .layer(middleware::from_fn(serve::gateway))
}

/// `ANY /providers/private/v1/chat/completions` - see [`serve::chat_completions`].
async fn chat_completions(
    State(state): State<Arc<serve::ServeState>>,
    request: Request,
) -> Response {
    serve::chat_completions(&state, request).await
}

/// `ANY /providers/private/v1/responses` - see [`serve::responses`].
async fn responses(State(state): State<Arc<serve::ServeState>>, request: Request) -> Response {
    serve::responses(&state, request).await
}

/// `ANY /providers/private/v1/models` - see [`serve::models`].
async fn models(State(state): State<Arc<serve::ServeState>>, request: Request) -> Response {
    serve::models(&state, request).await
}

/// `ANY /providers/private/v1/stats`, both verbs - see [`serve::stats`].
async fn stats(State(state): State<Arc<serve::ServeState>>, request: Request) -> Response {
    serve::stats(&state, request).await
}

/// `GET /health` - `{ok, service, version}`, `200` unconditionally. See
/// [`serve::health_document`] for why readiness here is a liveness claim and the credential
/// verdict is the gate's job.
///
/// It answers 200 even in the `--allow-unconfigured` state, and that is the honest split: the
/// probe asks whether a process is up and speaking HTTP, and the routes are the surface that
/// reports an unaccepted vault.
async fn health() -> Response {
    // A `Value` built by `health_document` cannot fail to serialise; the fallback keeps that total
    // without an `expect` on a request path.
    let bytes = serde_json::to_vec(&serve::health_document()).unwrap_or_else(|_| b"null".to_vec());

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, serve::JSON_CONTENT_TYPE)],
        Body::from(bytes),
    )
        .into_response()
}

/// `http://<reachable host>:<port><path>`. `0.0.0.0` / `::` is a set of interfaces and no client
/// can navigate to it, so a wildcard bind is reported on loopback and an explicit bind is
/// reported as itself. `SocketAddr::Display` brackets an IPv6 host, which is what makes the IPv6
/// URL correct without a special case.
pub(crate) fn banner_url(path: &str, local: SocketAddr) -> String {
    let reported: IpAddr = match local.ip() {
        IpAddr::V4(v4) if v4.is_unspecified() => IpAddr::from([127, 0, 0, 1]),
        IpAddr::V6(v6) if v6.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        other => other,
    };

    format!("http://{}{path}", SocketAddr::new(reported, local.port()))
}

pub(crate) fn banner_api(local: SocketAddr) -> String {
    banner_url(serve::MOUNT, local)
}

pub(crate) fn banner_health(local: SocketAddr) -> String {
    banner_url(HEALTH_PATH, local)
}

/// Resolves when the process should stop: Ctrl+C / SIGINT, or SIGTERM - which is what a container
/// sends, and what the default stop signal of an anonymous payload is. A signal backend that is
/// unavailable logs why and then waits forever, so one missing signal source cannot stop the
/// server from starting. Mirrors `packages/typecrate/harness/src/bin/server.rs:382-414`.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("{PROGRAM}: Ctrl+C unavailable ({error})");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                eprintln!("{PROGRAM}: SIGTERM unavailable ({error})");
                std::future::pending::<()>().await;
            }
        }
    };

    // Windows has no SIGTERM to subscribe to; the console Ctrl+C branch above is the shutdown
    // path there, so this one never fires.
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }

    eprintln!("{PROGRAM}: shutdown signal received, draining");
}

// ---------------------------------------------------------------------------
// The log surface: a hand-written `tracing` subscriber over `tracing-core` + `RUST_LOG`
// ---------------------------------------------------------------------------

/// The process's log surface: one [`LogSubscriber`] on stdout plus the `RUST_LOG` level filter.
///
/// Why hand-written: `tracing-subscriber` is resolved in NO typecrate lockfile, and this
/// standalone workspace has no vendored registry, so a new package here would break both the
/// offline build and `tests/transport.rs::the_lock_package_set_is_a_strict_subset_of_the_siblings`.
/// `tracing` 0.1.44 and `tracing-core` 0.1.36 are already resolved, so the subscriber below is
/// written against those two only - the same answer
/// `packages/typecrate/services/src/logging.rs` gives, with one difference: services has a
/// per-request access-log target whose message is printed verbatim, and this process has no
/// access-log line, so the filter here is the plain two-rule version (a matching target-scoped
/// directive, otherwise the global ceiling).
///
/// What one event becomes, exactly (one `\n`-terminated line):
/// `<LEVEL padded to 5> <target>: <message>[ <field>=<value>...]`, e.g.
/// ` WARN typecrate_provider::serve: provider route refused: ...`. No timestamp, matching the
/// untimestamped `console.log` lines of the TS gateway, and it keeps every rendered line a
/// byte-exact string a test can pin.
///
/// A write error is IGNORED (a closed pipe, an EPIPE on stdout): `println!` panics on one, and
/// this profile builds `panic = "abort"`, so a logging failure would otherwise take down a process
/// that is otherwise serving fine.
// `pub(crate)` for the same reason the pure helpers above are: `tests/bin_server.rs` includes this
// file and asserts the filter grammar and the rendered line, and a private module is invisible to
// it. Nothing here is importable from outside this binary.
pub(crate) mod logging {
    use std::fmt;
    use std::io;
    use std::sync::{Arc, Mutex};

    use tracing::level_filters::LevelFilter;
    use tracing_core::field::{Field, Visit};
    use tracing_core::span::{self, Id};
    use tracing_core::subscriber::Subscriber;
    use tracing_core::{Event, Level, Metadata};

    /// The environment variable that controls verbosity - the one name this repo uses for the
    /// knob (`packages/typecrate/services/src/logging.rs:67`).
    pub(crate) const LEVEL_ENV: &str = "RUST_LOG";

    /// The level an unset (or empty) `RUST_LOG` runs at: `info`, i.e. the token line, the
    /// `[failover]` and `[retry]` lines, the vault-down `warn!` and every `error!` are ON, while
    /// the crate's `debug!` request-body diagnostics and its `trace!` vault line stay off.
    pub(crate) const DEFAULT_LEVEL: LevelFilter = LevelFilter::INFO;

    /// The number every span id answers with. There is no span store (module doc) and
    /// `span::Id` is a non-zero `u64`, so `0` is not available.
    const NO_SPAN_ID: u64 = 1;

    /// One `RUST_LOG` directive: a bare level (`debug`), or a target-scoped one
    /// (`typecrate_provider=trace`).
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(crate) struct Directive {
        /// The target prefix this directive narrows, `None` for the process-wide level.
        pub(crate) prefix: Option<String>,
        /// The maximum level the directive allows.
        pub(crate) level: LevelFilter,
    }

    /// The resolved level filter, immutable for the life of the process - which is what lets the
    /// `tracing` callsite registry cache interest for it.
    ///
    /// The three fields are `pub(crate)` rather than private-behind-getters on purpose: every one
    /// of them is READ inside this module (`allows` reads `scoped` and `global`,
    /// `install_from_env` reads `ignored`), so no getter is needed to reach them from a test, and
    /// a getter the binary itself never calls is a `dead_code` warning under the `-D warnings`
    /// gate.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(crate) struct Filter {
        /// The process-wide level every other event is measured against.
        pub(crate) global: LevelFilter,
        /// The target-scoped directives, in the order they were written.
        pub(crate) scoped: Vec<Directive>,
        /// Directive text this parser could not read, kept verbatim so the caller can say so out
        /// loud instead of silently logging at the default.
        pub(crate) ignored: Vec<String>,
    }

    impl Default for Filter {
        fn default() -> Self {
            Self::at(DEFAULT_LEVEL)
        }
    }

    impl Filter {
        /// A process-wide level with no target scoping.
        #[must_use]
        pub(crate) fn at(level: LevelFilter) -> Self {
            Self {
                global: level,
                scoped: Vec::new(),
                ignored: Vec::new(),
            }
        }

        /// Whether an event from `target` at `level` is rendered: a matching target-scoped
        /// directive wins, and the LONGEST matching prefix wins, so `typecrate_provider=info`
        /// still applies under `RUST_LOG=trace,typecrate_provider::serve=warn`. Otherwise the
        /// event must be at or below the process-wide level. `LevelFilter::OFF` is above every
        /// level, so it enables nothing.
        #[must_use]
        pub(crate) fn allows(&self, target: &str, level: Level) -> bool {
            match self
                .scoped
                .iter()
                .filter(|directive| {
                    directive
                        .prefix
                        .as_deref()
                        .is_some_and(|prefix| target.starts_with(prefix))
                })
                .max_by_key(|directive| directive.prefix.as_ref().map_or(0, |prefix| prefix.len()))
            {
                Some(directive) => level <= directive.level,
                None => level <= self.global,
            }
        }

        /// The minimal `RUST_LOG` grammar, spelled out in [`super::HELP`]'s `ENVIRONMENT:`
        /// section:
        ///
        /// * comma-separated items; surrounding whitespace and empty items (`info,,debug`) are
        ///   ignored, so a shell-built list with a stray comma still works;
        /// * `<level>` sets the process-wide level, and the last bare level in the spec wins;
        /// * `<target-prefix>=<level>` caps the events whose `target` STARTS WITH that prefix
        ///   (module paths, so `typecrate_provider` covers `typecrate_provider::serve`), and an
        ///   EMPTY prefix (`=debug`) is the process-wide level again;
        /// * levels are case-insensitive: `off`/`none`, `error`, `warn`, `info`, `debug`, `trace`;
        /// * an item that is not one of those shapes lands in `Filter::ignored` rather than
        ///   being guessed at or aborted on - a malformed knob must never stop the process, and
        ///   never panic under `panic = "abort"`.
        ///
        /// An unset or empty spec is [`DEFAULT_LEVEL`].
        #[must_use]
        pub(crate) fn parse(spec: &str) -> Self {
            let mut filter = Self::default();

            for item in spec.split(',') {
                let item = item.trim();
                if item.is_empty() {
                    continue;
                }

                // `=debug` (an empty prefix) is the global level again - the prefix part only
                // means "narrow this target" when there is a prefix.
                let (prefix, level_text) = match item.split_once('=') {
                    Some((prefix, level)) => (Some(prefix.trim()), level.trim()),
                    None => (None, item),
                };

                let Some(level) = parse_level(level_text) else {
                    filter.ignored.push(item.to_owned());
                    continue;
                };

                match prefix.filter(|prefix| !prefix.is_empty()) {
                    Some(prefix) => filter.scoped.push(Directive {
                        prefix: Some(prefix.to_owned()),
                        level,
                    }),
                    None => filter.global = level,
                }
            }

            filter
        }
    }

    /// One level word -> [`LevelFilter`], case-insensitively. `none` is accepted as a synonym for
    /// `off` because that is what the `env_logger`/`tracing-subscriber` documentation calls it.
    fn parse_level(text: &str) -> Option<LevelFilter> {
        match text.to_ascii_lowercase().as_str() {
            "off" | "none" => Some(LevelFilter::OFF),
            "error" => Some(LevelFilter::ERROR),
            "warn" => Some(LevelFilter::WARN),
            "info" => Some(LevelFilter::INFO),
            "debug" => Some(LevelFilter::DEBUG),
            "trace" => Some(LevelFilter::TRACE),
            _ => None,
        }
    }

    /// The terminal subscriber: one whole line per event, into anything that is an [`io::Write`].
    ///
    /// The sink is generic so the exact bytes of every line are assertable: `std::io::stdout()` in
    /// the executable, a `Vec<u8>` in a test.
    pub(crate) struct LogSubscriber<W> {
        sink: Arc<Mutex<W>>,
        filter: Filter,
    }

    // The `io::Write` bound is on the whole impl rather than only on `write_line`, because `render`
    // is a static on the same type and a test wants to reach both through one path - the same shape
    // `packages/typecrate/services/src/logging.rs:271` uses.
    impl<W: io::Write> LogSubscriber<W> {
        /// A subscriber over its own sink.
        #[must_use]
        pub(crate) fn new(sink: W, filter: Filter) -> Self {
            Self {
                sink: Arc::new(Mutex::new(sink)),
                filter,
            }
        }

        /// Renders one event as exactly one line (including its trailing `\n`).
        ///
        /// Split out from the `Subscriber` impl so the rendering rule - the whole contract of this
        /// module - is testable without a `Context`.
        #[must_use]
        pub(crate) fn render(event: &Event<'_>) -> String {
            let mut visitor = LineVisitor::default();
            event.record(&mut visitor);

            // `{:<5}` is the alignment: `INFO `/`WARN ` are five columns wide, so every target
            // starts in the same column.
            let mut line = format!(
                "{:<5} {}: ",
                event.metadata().level(),
                event.metadata().target()
            );

            // The message first, then any named field the event carried (`vendors=3`), space
            // separated. `message` is absent only for an event with named fields and no message,
            // in which case the fields follow the target directly.
            let mut first = true;
            for part in visitor
                .message
                .as_deref()
                .into_iter()
                .chain(visitor.fields.iter().map(String::as_str))
            {
                if !first {
                    line.push(' ');
                }
                first = false;
                line.push_str(part);
            }

            line.push('\n');
            line
        }

        /// One write of one whole line, errors discarded on purpose (module doc).
        fn write_line(&self, line: &str) {
            // A poisoned mutex still holds a perfectly usable sink, and a log line is not a
            // reasonable place to panic - hence `unwrap_or_else(into_inner)` rather than `expect`.
            //
            // The lock is held ACROSS the write: that is what makes one event one contiguous line,
            // since two tasks logging at the same moment would otherwise interleave half a line
            // each into the same sink.
            let mut sink = self
                .sink
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let _ = sink.write_all(line.as_bytes());
        }
    }

    impl<W: io::Write + Send + 'static> Subscriber for LogSubscriber<W> {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            self.filter.allows(metadata.target(), *metadata.level())
        }

        // Spans are answered and dropped - nothing in this process opens one (module doc). A
        // CONSTANT id with no store is therefore the honest answer; a future `#[instrument]` needs
        // a real store, not a patched constant.
        fn new_span(&self, _span: &span::Attributes<'_>) -> Id {
            Id::from_u64(NO_SPAN_ID)
        }

        fn record(&self, _span: &Id, _values: &span::Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            self.write_line(&Self::render(event));
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    /// Pulls the `message` field out of an event and renders every other field as `name=value`.
    ///
    /// Only `record_debug` and `record_str` are implemented because everything else in
    /// [`tracing_core::field`] forwards to `record_debug` by default, and because a `str`/`String`
    /// value must take the `record_str` path rather than `{:?}` - that is what keeps
    /// `label=name` from printing as `label="name"`.
    #[derive(Default)]
    struct LineVisitor {
        message: Option<String>,
        /// Rendered `name=value` pairs, in the order the event carried them.
        fields: Vec<String>,
    }

    impl LineVisitor {
        fn push(&mut self, name: &str, value: String) {
            if name == "message" {
                self.message = Some(value);
            } else {
                self.fields.push(format!("{name}={value}"));
            }
        }
    }

    impl Visit for LineVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.push(field.name(), value.to_owned());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.push(field.name(), format!("{value:?}"));
        }
    }

    /// Installs [`LogSubscriber`] on stdout for the whole process, at the level `RUST_LOG` asks
    /// for, and reports every directive it could not read on stderr.
    ///
    /// The `Err` half of [`install`] (a subscriber was already installed, only possible for an
    /// embedder) is reported and ignored: this is a binary, and a logging problem is not a reason
    /// to exit a process that can serve.
    pub(crate) fn install_from_env() {
        // `var_os` rather than `var`: `var` reports a value that is not valid UTF-8 as
        // `Err(NotUnicode)`, and `.ok()` would turn "the operator set an unreadable knob" into
        // "the knob is unset" - the default filter with NO notice on stderr, which is exactly the
        // silent change the contract in `Filter::parse` rules out.
        let raw = std::env::var_os(LEVEL_ENV);
        let spec = match raw.as_deref().map(std::ffi::OsStr::to_str) {
            Some(Some(spec)) => Some(spec),
            Some(None) => {
                eprintln!(
                    "{}: ignoring the unreadable {LEVEL_ENV} value - it is not valid text \
                     (expected a level, or target=level)",
                    super::PROGRAM
                );
                None
            }
            None => None,
        };

        match install(spec) {
            Ok(filter) => {
                for ignored in &filter.ignored {
                    eprintln!(
                        "{}: ignoring the unreadable {LEVEL_ENV} directive \"{ignored}\" \
                         (expected a level, or target=level)",
                        super::PROGRAM
                    );
                }
            }
            Err(message) => eprintln!("{}: {message}", super::PROGRAM),
        }
    }

    /// Installs the subscriber for one already-read spec. `None` is "unset" and means
    /// [`DEFAULT_LEVEL`]. Split from [`install_from_env`] so the process-environment read stays in
    /// one place and the install itself is a function of its argument.
    ///
    /// The live [`Filter`] comes back so the caller can report its `ignored` directives.
    pub(crate) fn install(spec: Option<&str>) -> Result<Filter, String> {
        let filter = Filter::parse(spec.unwrap_or(""));
        let subscriber = LogSubscriber::new(io::stdout(), filter.clone());

        match tracing::subscriber::set_global_default(subscriber) {
            Ok(()) => Ok(filter),
            Err(error) => Err(error.to_string()),
        }
    }
}
