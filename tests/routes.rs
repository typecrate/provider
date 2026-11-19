//! Behavioural coverage of the four provider routes: the HTTP surface `src/serve.rs` builds and the
//! discovered route file `endpoint/provider/provider-route.rs` mounts in the services router.
//!
//! WHY EVERY ASSERTION CITES A SOURCE: the TypeScript under `runtime/endpoint/provider/private/*` is
//! the deployed wire contract this port has to satisfy, and `src/serve.rs` is the Rust port of it. Every
//! expectation below is read out of one of those two BEFORE the code ran, and the places where this port
//! answers differently are named in `src/serve.rs`'s module doc and called out in the assertion that
//! depends on one.
//!
//! WHICH CODE EACH TEST ACTUALLY EXERCISES:
//!   * the PROVIDER side is `serve::chat_completions` / `serve::responses` / `serve::models` /
//!     `serve::stats`, mounted at the exact paths of
//!     `runtime/endpoint/provider/private/{chat-completion,responses,models,stats}/service-route.ts`;
//!   * the UPSTREAM side is a real `axum` listener in this file, so the provider's outbound reqwest leg —
//!     its `Authorization` header, its upstream path, its rewritten body — is a real socket too (the same
//!     real-socket discipline as `tests/transport.rs:158-188`).
//!
//! WHY THE PROVIDER SIDE IS ALSO A REAL LISTENER: `serve`'s handlers are plain
//! `(state, request) -> Response` functions, so calling one directly would exercise the handler while
//! proving nothing about the RESPONSE. The mount is the contract: `application/json` with no charset
//! (hono's `c.json()`), `text/event-stream` plus `Cache-Control: no-cache` and nothing else invented, and
//! a streamed body whose frames arrive in order with nothing after the terminal one.
//!
//! WHY `axum` AND NOT A HAND-WRITTEN PARSE LOOP: `axum` is already a dependency of this crate for the
//! discovered route file, and `hyper` never appears in this crate's `Cargo.lock`, so a second HTTP
//! implementation would add third-party surface this crate does not otherwise have.
//!
//! WHAT LIVES ELSEWHERE: upstream request shaping, the retry/rotation loop and the stream-failure
//! CLASSIFICATION are covered by `tests/clients.rs` and `tests/transport.rs`. This file covers what only
//! the route can decide: status codes, bodies, headers, frame sequencing and the readiness gate.

use std::{
    collections::VecDeque,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
    routing::any,
};
use futures_util::Stream;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use typecrate_provider::{
    client::{ClientSpec, ModelClient},
    error::UNKNOWN_STREAM_ERROR_MESSAGE,
    pricing::{Pricing, TOKENS_PER_DOLLAR},
    registry::Registry,
    serve::{
        CHAT_COMPLETIONS_ROUTE, DEFAULT_VAULT_ARTIFACT, HANDLER_NOT_FOUND, INTERNAL_ERROR,
        JSON_CONTENT_TYPE, MISSING_ID_ERROR, MODEL_CREATED_EPOCH, MODEL_OWNED_BY, MODELS_ROUTE,
        MOUNT, PROVIDER_PORT, REGENERATION_COMMAND, RESPONSES_ROUTE, STATS_ROUTE, ServeState,
        VaultSource, chat_completions, models, responses, state_from_source, stats,
    },
    // The sentinel frame the route appends, imported from the codec rather than restated, so the
    // assertion is against the byte string the product writes.
    sse::DONE_FRAME,
    transport::{Credential, Endpoint, Policy, Target, Transport, estimate_request_tokens},
    vault::Secret,
};

// The vault value this suite dials with. It appears in assertion VALUES only, never in a message this
// file prints, because a printed credential is the one leak the port forbids.
const SECRET: &str = "sk-test-credential-value";

// ---------------------------------------------------------------------------
// The scripted upstream
// ---------------------------------------------------------------------------

/// One scripted answer, as data rather than as code, so a test says what the upstream SENT and the shared
/// harness turns that into a real HTTP response.
enum Reply {
    /// An answer with a status of its own. The JSON is written verbatim, so key order and number
    /// formatting survive and the recorded body is exactly what the provider sent.
    Json(u16, Value),
    /// A `text/event-stream` answer whose body yields `frames` and then ENDS cleanly. `frames` are
    /// bytes-as-written, so a test can send a frame that is not valid JSON at all.
    Stream { frames: Vec<String> },
}

/// One request as the upstream received it, recorded in arrival order.
struct Served {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Value,
}

impl Served {
    /// The bearer token the provider sent, exactly as the `Authorization` header spelled it.
    fn bearer(&self) -> Option<&str> {
        self.headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
    }

    /// The prompt-token figure the estimator would charge for the body the upstream RECEIVED
    /// (`transport::estimate_request_tokens`: one unit per BMP character, four units per token).
    ///
    /// Deliberately NOT what the counters charge: the client prices the CALLER's body
    /// (`client.rs:697-706`, because `recordRequestTokens` receives `body` and not the pinned payload,
    /// per `model-class.ts:707`), so this figure is the same estimate over a payload that carries the
    /// rewrite's extra bytes. The two are asserted against each other where that distinction matters.
    fn received_estimate(&self) -> u64 {
        estimate_request_tokens(&self.body)
    }
}

/// The scripted upstream: answers from a queue, and remembers what it was asked.
struct Upstream {
    script: Mutex<VecDeque<Reply>>,
    seen: Mutex<Vec<Served>>,
}

/// Starts one scripted upstream and returns its `host:port` plus its ledger.
///
/// The listener task is deliberately left running for the process: every test binds its own ephemeral
/// port, so a leaked listener cannot disturb the next test. A script that runs out answers `418` with an
/// impossible-to-mistake body, which turns "the provider dialled more often than the test scripted" into
/// a visible verdict instead of a hang.
async fn upstream(script: Vec<Reply>) -> (String, Arc<Upstream>) {
    let served = Arc::new(Upstream {
        script: Mutex::new(script.into()),
        seen: Mutex::new(Vec::new()),
    });

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback listener for the scripted upstream");
    let address = listener
        .local_addr()
        .expect("the bound address of a loopback listener")
        .to_string();

    // `with_state` consumes the recorder, so the spawned task gets its own clone of the same ledger and
    // the caller keeps the handle it reads.
    let served_upstream = Arc::clone(&served);
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/{*path}", any(answer))
                .with_state(served_upstream),
        )
        .await
        .expect("the scripted upstream lives until the process ends");
    });

    (address, served)
}

/// Records the request, then answers with the next scripted reply.
async fn answer(State(up): State<Arc<Upstream>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let (method, uri, headers) = (parts.method, parts.uri, parts.headers);
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("the provider sends a whole body");
    // A body that is not JSON must not crash the recorder: the ledger keeps `Null` for it, and no test
    // here depends on a non-JSON upstream request.
    let recorded: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    let path = uri.path().to_owned();
    up.seen.lock().expect("a released ledger").push(Served {
        method,
        path,
        headers,
        body: recorded,
    });

    let reply = {
        let mut script = up.script.lock().expect("a released script");
        script.pop_front()
    };

    match reply {
        Some(Reply::Json(status, value)) => Response::builder()
            .status(StatusCode::from_u16(status).expect("a status a test can name explicitly"))
            // The provider's transport accepts a `json` media type with `charset=utf-8` and matches it
            // case-insensitively (`src/transport.rs`, the content-type gate), so this spelling exercises
            // the gate rather than bypassing it.
            .header("content-type", "application/json; charset=utf-8")
            .body(Body::from(value.to_string()))
            .expect("a static JSON answer"),
        Some(Reply::Stream { frames }) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(yields(frames)))
            .expect("a streaming answer"),
        // The script is the test's promise. Running out means the provider made more attempts than the
        // test modeled, which is a fact the test wants stated rather than papered over.
        None => Response::builder()
            .status(StatusCode::IM_A_TEAPOT)
            .header("content-type", "text/plain")
            .body(Body::from("the scripted upstream ran out of answers"))
            .expect("a static text answer"),
    }
}

/// A one-shot stream that yields `frames` verbatim and then ends cleanly.
///
/// Clean ending is the point: the provider must not invent an error for a body that simply stops
/// (`private-chat-completion.ts` prints its log line and emits the sentinel after any exhaustion).
fn yields(
    frames: Vec<String>,
) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, std::io::Error>> + Send>> {
    Box::pin(futures_util::stream::unfold(
        VecDeque::from(frames),
        |mut pending| async move {
            let frame = pending.pop_front()?;

            Some((Ok(frame.into_bytes()), pending))
        },
    ))
}

// ---------------------------------------------------------------------------
// The rows
// ---------------------------------------------------------------------------

/// What the upstream answers for a settled request. `completion_tokens` is the only token figure the
/// counters read verbatim (`accounting.rs`, `output_token_total`); the prompt side is ESTIMATED from the
/// request body and so does not depend on this block at all.
fn settled(model: &str, content: &str) -> Value {
    json!({
        "id": "chatcmpl-fixture",
        "object": "chat.completion",
        "model": model,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content },
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": 1000,
            "completion_tokens": 500,
            "total_tokens": 1500,
        },
    })
}

/// The row every test starts from: a Chat Completions row with a credential, so it is routable, and
/// with pricing chosen so every cost bracket of a fixture request is a distinct figure.
struct Row {
    id: &'static str,
    model: &'static str,
    provider: &'static str,
    credential: Credential,
    pricing: Pricing,
}

impl Row {
    /// A row with a real credential: listed by `/models` AND routable.
    fn visible(id: &'static str, model: &'static str) -> Self {
        Self {
            id,
            model,
            provider: "Test",
            credential: Credential::new(&Secret::new(SECRET)),
            pricing: Pricing {
                // Dollars per MILLION tokens. A port that priced the cached replay as fresh input, or
                // dropped it, moves every figure the stats test asserts.
                input: 1_000.0,
                cache_read: 100.0,
                cache_write: 0.0,
                output: 2_000.0,
            },
        }
    }

    /// A row whose only credential is a vault PLACEHOLDER: `is_discoverable` is false (the classifier's
    /// `*_PLACEHOLDER` rule, `src/vault.rs:360-362`) while `Registry::find` still resolves it. That
    /// asymmetry is the product's, not this fixture's invention.
    fn hidden(id: &'static str, model: &'static str) -> Self {
        Self {
            credential: Credential::new(&Secret::new("EXAMPLE_API_KEY_PLACEHOLDER")),
            ..Self::visible(id, model)
        }
    }
}

/// A hermetic HTTP client. `no_proxy` is load-bearing: `reqwest::Client::builder()` honours `HTTP_PROXY`
/// from the developer's environment, and a proxy would move a request off loopback.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("a proxy-free client")
}

/// Builds one row's client the way `src/registry.rs` builds a real row's: the two pools, then the keyless
/// flags derived from the row, then the transport. `tests/clients.rs:405-428` is the precedent, and a
/// test that assembled the target any other way would be testing a shape the registry never builds.
fn client_for(row: &Row, address: &str) -> ModelClient {
    let mut spec = ClientSpec::plain(
        row.id,
        row.provider,
        row.model,
        "test",
        "test_api_keys",
        "test_api_endpoints",
    );
    spec.pricing = row.pricing;

    let target = Target::new(
        vec![Endpoint::new(&format!("http://{address}"))],
        vec![row.credential.clone()],
    )
    .keyless(spec.credential_slot.is_none())
    .keyless_header(spec.keyless_header);

    ModelClient::new(
        spec,
        Transport::with_http_client(target, Policy::standard(), http_client()),
    )
}

/// A registry over `rows`, in the order given — the order `/models` publishes and `/stats` re-sorts.
fn registry(rows: &[Row], address: &str) -> Registry {
    Registry::from_clients(rows.iter().map(|row| client_for(row, address)).collect())
}

// ---------------------------------------------------------------------------
// The fixture: the four handlers, served
// ---------------------------------------------------------------------------

/// The four routes of one state, served over a real loopback listener.
///
/// The paths are the constants `serve` publishes, because THAT is the pairing the discovered route file
/// has to keep true: `endpoint/provider/provider-route.rs` mounts the same values in the services
/// router, and a test that typed its own path string would pin a mount the product does not have.
///
/// WHY THE ADAPTERS TAKE `State<ServeState>` HERE AND NOTHING IN THE PRODUCT: axum needs a state type to
/// build a `Router`, and the provider's state is `ServeState` itself. The services router mounts the same
/// handlers against the SERVICES state and reaches the provider's state through `serve::process_state()`,
/// which is why `endpoint/provider/provider-route.rs` takes no extractor. Either way the handler future
/// has to be `Send + Sync` — axum boxes the response body, and a boxed body is `Send + Sync` — which is
/// what `transport::SettlementHook`'s `+ Sync` bound exists to keep true. If that bound is ever dropped,
/// the error appears HERE as `Handler<_, _>` is not satisfied, and in services at
/// `src/server/metadata.rs`'s mount site.
async fn bind(state: &ServeState) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback listener for the provider routes");
    let address = listener
        .local_addr()
        .expect("the bound address of a loopback listener")
        .to_string();

    let served = state.clone();
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route(CHAT_COMPLETIONS_ROUTE, any(chat_route))
                .route(RESPONSES_ROUTE, any(relay_route))
                .route(MODELS_ROUTE, any(catalog_route))
                .route(STATS_ROUTE, any(metrics_route))
                .with_state(served),
        )
        .await
        .expect("the provider listener lives until the process ends");
    });

    address
}

/// The four adapters of the discovered route file (`endpoint/provider/provider-route.rs`), with an
/// explicit state instead of the process one. They are named `async fn`s rather than closures because
/// axum's `Handler` bound is written for named async functions of extractors, and `with_state` supplies
/// the state each one names.
async fn chat_route(State(state): State<ServeState>, request: Request) -> Response {
    chat_completions(&state, request).await
}

async fn relay_route(State(state): State<ServeState>, request: Request) -> Response {
    responses(&state, request).await
}

async fn catalog_route(State(state): State<ServeState>, request: Request) -> Response {
    models(&state, request).await
}

async fn metrics_route(State(state): State<ServeState>, request: Request) -> Response {
    stats(&state, request).await
}

/// One answer as it arrived on the wire, headers and bytes included.
struct Answer {
    status: StatusCode,
    headers: Vec<(String, String)>,
    bytes: Vec<u8>,
}

impl Answer {
    /// Captures a whole response. The body is drained before anything is asserted: the frame rules are
    /// about the FINAL bytes, and a streamed answer must still reach its end.
    async fn capture(response: reqwest::Response) -> Self {
        let status = response.status();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or("<bytes>").to_owned(),
                )
            })
            .collect();
        let bytes = response
            .bytes()
            .await
            .expect("the provider body is readable to the end")
            .to_vec();

        Self {
            status,
            headers,
            bytes,
        }
    }

    /// The body, decoded as UTF-8.
    fn text(&self) -> String {
        String::from_utf8(self.bytes.clone()).expect("every provider answer is UTF-8")
    }

    /// The body, parsed as the JSON the provider promises. A parse failure prints the bytes, because the
    /// likeliest failure is an answer that is not JSON at all.
    fn json(&self) -> Value {
        serde_json::from_slice(&self.bytes)
            .unwrap_or_else(|_| panic!("the answer is not JSON: {}", self.text()))
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// The whole provider-side fixture. The upstream's address is a build-time input only: the registry
/// points at it, and no assertion needs to name it (a frame's target sentence is pinned in
/// `tests/transport.rs`, where the transport's own target is observable).
struct Provider {
    address: String,
    served: Arc<Upstream>,
}

impl Provider {
    /// Serves `rows` against an upstream scripted with `script`.
    async fn start(rows: &[Row], script: Vec<Reply>) -> Self {
        let (upstream_address, served) = upstream(script).await;
        let address = bind(&ServeState::Ready(registry(rows, &upstream_address))).await;

        Self { address, served }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.address)
    }

    /// The JSON POST every relay test starts with.
    async fn post(&self, path: &str, body: Value) -> Answer {
        self.request(Method::POST, path, Some(body.to_string()))
            .await
    }

    /// A POST whose body is given as bytes-as-written, so a test can send something that is not JSON.
    async fn post_raw(&self, path: &str, body: &str) -> Answer {
        self.request(Method::POST, path, Some(body.to_owned()))
            .await
    }

    /// A POST carrying a query string — the only way to reach the stats reset verb.
    async fn post_query(&self, path: &str, query: &str, body: Value) -> Answer {
        self.request(
            Method::POST,
            &format!("{path}?{query}"),
            Some(body.to_string()),
        )
        .await
    }

    async fn get(&self, path: &str) -> Answer {
        self.request(Method::GET, path, None).await
    }

    async fn request(&self, method: Method, path: &str, body: Option<String>) -> Answer {
        let mut request = http_client()
            .request(method.clone(), self.url(path))
            .header("content-type", JSON_CONTENT_TYPE);
        if let Some(body) = body {
            request = request.body(body);
        }

        Answer::capture(
            request
                .send()
                .await
                .unwrap_or_else(|error| panic!("the {method} {path} request failed: {error}")),
        )
        .await
    }

    /// Everything the upstream was asked, in order.
    fn served(&self) -> Vec<Served> {
        self.served
            .seen
            .lock()
            .expect("a released ledger")
            .iter()
            .map(|request| Served {
                method: request.method.clone(),
                path: request.path.clone(),
                headers: request.headers.clone(),
                body: request.body.clone(),
            })
            .collect()
    }

    fn served_count(&self) -> usize {
        self.served.seen.lock().expect("a released ledger").len()
    }
}

/// The chat body every test starts from: one model, one message, an explicit cap.
fn chat_body(model: &str) -> Value {
    json!({
        "model": model,
        "messages": [{ "role": "user", "content": "hi" }],
        "max_tokens": 512,
    })
}

/// The Responses body every relay test starts with (`responses.ts` reads `model` and `input`).
fn responses_body(model: &str) -> Value {
    json!({ "model": model, "input": "hi" })
}

/// The same body with `stream: true`, which is how both relays are asked for a stream.
///
/// Written as an assignment rather than `json!({ "stream": true, ..body })` because the `json!` macro of
/// the serde_json this crate locks has no spread arm (`serde_json-1.0.154/src/macros.rs`), and an
/// assertion that only compiles against a newer serde_json is not a contract.
fn streaming(mut body: Value) -> Value {
    body["stream"] = json!(true);

    body
}

/// The prompt-token figure the counters charge for a request built by [`chat_body`].
///
/// `estimate_request_tokens` prices the CALLER's body, not the rewritten payload
/// (`client.rs:697-706`), and that body serializes to 83 characters with every one of them in the BMP:
/// `{"model":..,"messages":[..],"max_tokens":512}` is 83 units, so `div_ceil(83, 4)` is 21. The
/// figure is a literal here on
/// purpose: it is the number the stats rows and every cost below are derived from, and a reader can
/// count the characters of [`chat_body`] to check it.
const ESTIMATED_PROMPT_TOKENS: u64 = 21;

/// The same estimate over the payload the UPSTREAM received: 21 tokens priced over 101 characters,
/// because the rewrite swaps the 10-character routing slug for the 14-character wire id (+4 units) and
/// materialises `"stream":false` (+14 units), i.e. `div_ceil(101, 4)` = 26.
const RECEIVED_ESTIMATE: u64 = 26;

/// The dollars one fixture request costs, derived from the tokens the counters charge and the rates in
/// [`Row::visible`] using the SAME operations `Pricing::cost` performs (`src/pricing.rs:99-124`), down to
/// the per-million divisor the production formula names, so the assertion pins the formula instead of
/// restating an observation.
///
/// The four figures are the row's `inputCost` / `cacheCost` / `outputCost` / `totalCost` in that order.
/// A cold row has no replayed prefix, so the cache bracket is zero and `inputCost` is the whole-prompt
/// figure (`accounting.rs:305-321` is the baseline rule that makes that true).
#[allow(clippy::eq_op)] // the fixture's 500 output tokens at 2_000/M multiply to exactly
// `TOKENS_PER_DOLLAR`, which is what makes the output cost a round `1.0`; the formula stays spelled out
// rather than restated as a literal, so the rates and the token figures it derives from stay visible.
fn fixture_costs(tokens: u64) -> (f64, f64, f64, f64) {
    let prompt = tokens as f64;
    let input = (prompt * 1_000.0 + 0.0 * 100.0) / TOKENS_PER_DOLLAR;
    let cache = 0.0 * 100.0 / TOKENS_PER_DOLLAR;
    let output = 500.0 * 2_000.0 / TOKENS_PER_DOLLAR;

    (input - cache, cache, output, input + output)
}

/// The keys of a JSON object, in the order the document spells them.
fn keys_of(value: &Value) -> Vec<&str> {
    value
        .as_object()
        .expect("the answer is a JSON object")
        .keys()
        .map(String::as_str)
        .collect()
}

/// The path a Chat row's upstream request is posted to, for the endpoints THIS suite builds.
///
/// Two rules compose into it: the row's behaviour contributes `chat/completions` (`src/client.rs`,
/// `Behavior::path`), and the endpoint normalizer APPENDS `/v1` to a base URL that carries no version
/// segment (`src/transport.rs:262-285`, rule 4 — the convention 10 of the 11 vault vendors already
/// follow, and the one `daytona-qwen3-8.ts:500-502` spells by hand). The fixture endpoints are bare
/// `http://127.0.0.1:<port>` authorities, so the version segment is on the wire, and a test that typed
/// `/chat/completions` would be asserting a rule the transport does not have.
const CHAT_WIRE_PATH: &str = "/v1/chat/completions";

/// One `data:` frame as the upstream writes it, in serde_json's compact form, so the provider's
/// re-serialisation of it is byte-identical and a byte-exact assertion is meaningful.
fn frame(payload: &str) -> String {
    format!("data: {payload}\n\n")
}

// ---------------------------------------------------------------------------
// The mount, and the route file that declares it
// ---------------------------------------------------------------------------

#[test]
fn the_mount_is_the_one_the_route_file_declares() {
    // `endpoint/provider/provider-route.rs` writes these as LITERALS — the `service_routes!` grammar
    // binds `$port:literal`, so a Rust constant cannot be named there
    // (`packages/typecrate/services/src/server/metadata.rs`; the `}, port: 8010;` group in
    // `examples/endpoints/health/health-route.rs` is the precedent) — so this is the only place the two
    // spellings of the mount can meet.
    assert_eq!(PROVIDER_PORT, 8800);
    assert_eq!(MOUNT, "/providers/private/v1");
    assert_eq!(
        CHAT_COMPLETIONS_ROUTE,
        "/providers/private/v1/chat/completions"
    );
    assert_eq!(RESPONSES_ROUTE, "/providers/private/v1/responses");
    assert_eq!(MODELS_ROUTE, "/providers/private/v1/models");
    assert_eq!(STATS_ROUTE, "/providers/private/v1/stats");

    // The mount is the shared prefix of all four, exactly as `private/constant.ts:1` is in the TypeScript
    // (`route: \`${BASE_URL}/chat/completions\``).
    for route in [
        CHAT_COMPLETIONS_ROUTE,
        RESPONSES_ROUTE,
        MODELS_ROUTE,
        STATS_ROUTE,
    ] {
        assert!(
            route.starts_with(&format!("{MOUNT}/")),
            "{route} is not under {MOUNT}"
        );
    }

    // The mount itself depends on `ServeState` being a state axum can extract, which axum spells
    // `Clone + Send + Sync + 'static`. Naming the bound here turns a confusing `Handler<_, _>`
    // unsatisfied-trait error at the mount site into this test's name.
    fn assert_extractable<State: Clone + Send + Sync + 'static>() {}
    assert_extractable::<ServeState>();

    // The refusal sentences are the gateway's own words, not paraphrases
    // (`packages/underload/service/src/server/handler.ts`).
    assert_eq!(HANDLER_NOT_FOUND, "handler not found");
    assert_eq!(INTERNAL_ERROR, "Internal Server Error");
    assert_eq!(MISSING_ID_ERROR, "missing id query parameter");

    // The artifact this crate reads when nothing is configured is the PROVIDER crate's own view of the
    // vault document — which matters because the route file is compiled into the services binary, and a
    // path baked in there would describe the services package instead.
    assert!(
        DEFAULT_VAULT_ARTIFACT.ends_with("/config/vault/generated/agentic-provider.json"),
        "{DEFAULT_VAULT_ARTIFACT}"
    );
    assert_eq!(
        REGENERATION_COMMAND,
        "yarn workspace @config/vault export -- --consumer agentic-provider"
    );
}

#[tokio::test]
async fn a_verb_the_route_file_does_not_declare_gets_the_gateway_refusal() {
    // WHY THIS TEST EXISTS AT ALL: the TypeScript declares one handler PER verb
    // (`{ POST: privateChatCompletion }`, `{ GET: getModelsList }`,
    // `{ GET: privateStats, POST: privateStatsReset }`), and the gateway answers anything else with
    // `{ error: 'handler not found' }` at 400 — 400, not 405
    // (`packages/underload/service/src/server/handler.ts`). The Rust route file declares each path as
    // `any(...)`, so `serve` has to produce that refusal itself, and only a request with the wrong verb
    // can tell the two apart.
    //
    // Every request below carries a valid body and a READY state, so nothing else could produce the
    // verdict: the verb gate runs before the readiness gate, before the body, and before routing.
    let provider = Provider::start(&[Row::visible("test/alpha", "upstream/alpha")], vec![]).await;

    for (method, route) in [
        (Method::GET, CHAT_COMPLETIONS_ROUTE),
        (Method::GET, RESPONSES_ROUTE),
        (Method::PUT, CHAT_COMPLETIONS_ROUTE),
        (Method::POST, MODELS_ROUTE),
        (Method::DELETE, MODELS_ROUTE),
        (Method::PUT, STATS_ROUTE),
    ] {
        let answer = provider
            .request(method.clone(), route, Some("{}".to_owned()))
            .await;

        assert_eq!(
            answer.status,
            StatusCode::BAD_REQUEST,
            "{method} {route} is the gateway's 400, not axum's body-less 405"
        );
        assert_eq!(
            answer.json(),
            json!({ "error": HANDLER_NOT_FOUND }),
            "{method} {route} must carry the gateway's own body, not a paraphrase"
        );
        assert_eq!(
            answer.header("content-type"),
            Some(JSON_CONTENT_TYPE),
            "{method} {route}: the refusal is a JSON answer like every other answer"
        );
    }

    // The declared verbs still work, so the gate is a gate and not a wall: `/stats` answers BOTH verbs,
    // because the TypeScript's verb map declares both.
    assert_eq!(
        provider.get(STATS_ROUTE).await.status,
        StatusCode::OK,
        "GET {STATS_ROUTE} is the projection verb"
    );
    assert_eq!(
        provider
            .post_query(STATS_ROUTE, "id=test/alpha", json!({}))
            .await
            .status,
        StatusCode::OK,
        "POST {STATS_ROUTE} is the reset verb"
    );
    assert_eq!(
        provider.get(MODELS_ROUTE).await.status,
        StatusCode::OK,
        "GET {MODELS_ROUTE} is the catalog verb"
    );

    assert_eq!(
        provider.served_count(),
        0,
        "no verb-gate request may reach the upstream"
    );
}

#[tokio::test]
async fn the_advertised_any_verb_is_refused_before_readiness_on_every_route() {
    // WHY THIS IS A SEPARATE TEST: the published listing advertises these four mounts as `ANY`
    // (`endpoint/provider/provider-route.rs` declares `ANY` groups, and `route_metadata` lists the
    // literal token), and the services dispatch test replays the LISTING verbatim
    // (`packages/typecrate/services/tests/server_http.rs::every_listed_endpoint_is_the_method_the_socket_dispatches`).
    // So `ANY` itself, spelled as a request method, has to have a defined answer. It does: hyper accepts
    // it as an extension method, axum's `any()` delivers it, and the handler's verb gate is step (1),
    // BEFORE the readiness gate (`src/serve.rs`, `completion`/`catalog`/`metrics`) — so the answer is the
    // gateway's 400 on a machine that has no vault artifact at all, which is the only reason that test
    // can pin one body for all four rows regardless of what the developer's `config/vault/generated`
    // directory happens to contain.
    let any = Method::from_bytes(b"ANY").expect("ANY is a usable extension method token");
    let expected = json!({ "error": HANDLER_NOT_FOUND });

    // Ready half: the state has rows, so the refusal can only come from the verb gate.
    let provider = Provider::start(&[Row::visible("test/alpha", "upstream/alpha")], vec![]).await;

    // Unready half: the same requests against a state whose vault artifact does not exist. Same 400,
    // which is the ordering proof — a 503 here would mean readiness outranks the verb gate.
    let artifact = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/no-such-vault.json");
    let unavailable = bind(&state_from_source(&VaultSource::Path(artifact))).await;

    for route in [
        CHAT_COMPLETIONS_ROUTE,
        RESPONSES_ROUTE,
        MODELS_ROUTE,
        STATS_ROUTE,
    ] {
        for address in [&provider.address, &unavailable] {
            let answer = request_at(address, any.clone(), route, None).await;

            assert_eq!(
                answer.status,
                StatusCode::BAD_REQUEST,
                "ANY {route} against a state at {address}"
            );
            assert_eq!(
                answer.json(),
                expected,
                "ANY {route} against a state at {address} must print the gateway's refusal"
            );
            assert_eq!(
                answer.header("content-type"),
                Some(JSON_CONTENT_TYPE),
                "ANY {route} against a state at {address}"
            );
        }
    }

    assert_eq!(
        provider.served_count(),
        0,
        "the listed verb of the mount is not a verb of the handler, so nothing dials"
    );
}

/// One request to an already-bound address, for the fixtures that build their own state.
async fn request_at(address: &str, method: Method, path: &str, body: Option<String>) -> Answer {
    let mut request = http_client()
        .request(method.clone(), format!("http://{address}{path}"))
        .header("content-type", JSON_CONTENT_TYPE);
    if let Some(body) = body {
        request = request.body(body);
    }

    Answer::capture(
        request
            .send()
            .await
            .unwrap_or_else(|error| panic!("the {method} {path} request failed: {error}")),
    )
    .await
}

// ---------------------------------------------------------------------------
// Readiness: the 503 that precedes every other verdict
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_missing_vault_artifact_refuses_every_route_with_a_503_naming_the_fix() {
    // This is NOT the "no model" answer. A deployment that has not exported its vault must be able to
    // tell a deployment fault from a model fault, so the status (503: a 5xx that is distinguishable both
    // from the model 404 and from the gateway's 500), the message (which names the artifact AND the
    // command that regenerates it) and the absence of a panic are all part of the contract.
    //
    // The artifact is addressed through `VaultSource::Path` rather than through the environment:
    // `std::env::set_var` is `unsafe` in edition 2024, and an explicit path source is exactly what
    // `vault_source_from_env` returns when `TYPECRATE_PROVIDER_VAULT_PATH` names a file.
    let artifact = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/no-such-vault.json");
    let state = state_from_source(&VaultSource::Path(artifact.clone()));

    assert!(
        !state.is_ready(),
        "a path that does not exist cannot be a registry"
    );
    let reason = state
        .unavailable()
        .expect("an unbuilt surface carries its reason");
    assert!(
        reason
            .message()
            .starts_with("the private provider vault artifact is unavailable:"),
        "{}",
        reason.message()
    );
    assert!(
        reason.message().contains(&artifact.display().to_string()),
        "the refusal names the artifact it tried to read"
    );
    assert!(
        reason.message().contains(REGENERATION_COMMAND),
        "the refusal names the command that fixes it"
    );

    let address = bind(&state).await;

    // Every route, with the verb its route file declares, and with a body that would otherwise be VALID:
    // readiness is decided before the body is read, so a bad body cannot turn this into a 500.
    for (method, path, body) in [
        (
            Method::POST,
            CHAT_COMPLETIONS_ROUTE,
            Some(chat_body("test/alpha")),
        ),
        (
            Method::POST,
            RESPONSES_ROUTE,
            Some(responses_body("test/alpha")),
        ),
        (Method::GET, MODELS_ROUTE, None),
        (Method::GET, STATS_ROUTE, None),
    ] {
        let mut request = http_client()
            .request(method.clone(), format!("http://{address}{path}"))
            .header("content-type", JSON_CONTENT_TYPE);
        if let Some(body) = body {
            request = request.body(body.to_string());
        }

        let answer = Answer::capture(request.send().await.expect("the route answers")).await;

        assert_eq!(
            answer.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{method} {path}"
        );
        let document = answer.json();
        assert_eq!(
            keys_of(&document),
            vec!["error"],
            "{method} {path}: one `error` key, shaped like every other refusal"
        );

        let message = document["error"]
            .as_str()
            .expect("the error is a string")
            .to_owned();
        assert_eq!(message, reason.message(), "{method} {path}");
        assert!(
            message.contains(&artifact.display().to_string())
                && message.contains(REGENERATION_COMMAND),
            "the answer carries the same fix list the state carries: {message}"
        );
    }

    // The process-wide state, built from the ambient environment, resolves to one of the two states and
    // never panics: this code is linked into the services binary, which also serves its own routes, so a
    // missing git-ignored artifact must not stop that binary from starting.
    match typecrate_provider::serve::process_state() {
        ServeState::Ready(registry) => {
            assert!(
                !registry.clients().is_empty(),
                "a ready process surface has rows to serve"
            );
        }
        ServeState::Unavailable(reason) => {
            assert!(
                reason.message().contains(REGENERATION_COMMAND),
                "{}",
                reason.message()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// POST /chat/completions — settled
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_settled_completion_is_answered_verbatim_and_the_upstream_body_proves_the_wire() {
    let document = settled("upstream/alpha", "hello");
    let provider = Provider::start(
        &[Row::visible("test/alpha", "upstream/alpha")],
        vec![Reply::Json(200, document.clone())],
    )
    .await;

    let answer = provider
        .post(CHAT_COMPLETIONS_ROUTE, chat_body("test/alpha"))
        .await;

    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(
        answer.header("content-type"),
        Some(JSON_CONTENT_TYPE),
        "hono's `c.json()` writes exactly this, with no charset"
    );

    // The completion document is the UPSTREAM's, verbatim: the settled arm of `serve` re-serialises the
    // parsed value and never re-shapes it, so the caller sees what the model sent.
    assert_eq!(answer.json(), document);

    let served = provider.served();
    let [request] = served.as_slice() else {
        panic!("exactly one upstream request");
    };
    assert_eq!(request.method, Method::POST);
    assert_eq!(
        request.path, CHAT_WIRE_PATH,
        "the upstream path is the row's behaviour under the version segment the endpoint normalizer          appends (`src/transport.rs:262-285` rule 4), not the mount"
    );
    assert_eq!(
        request.bearer(),
        Some(format!("Bearer {SECRET}").as_str()),
        "the row's credential travels as the SDK's bearer header"
    );

    let sent = &request.body;
    assert_eq!(
        sent["model"], "upstream/alpha",
        "the wire id replaces the routing id (`ModelClient::rewrite`)"
    );
    assert_eq!(sent["messages"], chat_body("test/alpha")["messages"]);
    assert_eq!(
        sent["max_tokens"], 512,
        "the caller's cap survives the rewrite"
    );
    assert_eq!(
        sent["stream"], false,
        "`stream` is always present, defaulted to false (`private-chat-completion.ts`)"
    );
    assert!(
        !sent
            .get("stream_options")
            .is_some_and(|value| !value.is_null()),
        "`stream_options` is a stream-only injection (`client.rs:976-989`)"
    );

    // The counters belong to the row the request resolved to.
    let metrics = provider.get(STATS_ROUTE).await.json();
    assert_eq!(metrics["models"][0]["requests"], 1);
    assert_eq!(metrics["models"][0]["outputTokens"], json!(500));
    assert_eq!(
        metrics["models"][0]["inputTokens"],
        json!(ESTIMATED_PROMPT_TOKENS),
        "the prompt side is the estimator's figure for the CALLER's body (`client.rs:697-706`)"
    );
    assert_eq!(
        request.received_estimate(),
        json!(RECEIVED_ESTIMATE),
        "the payload the upstream got is a different, larger figure: the rewrite is what makes the two          diverge, and the row still reports the caller's"
    );
}

#[tokio::test]
async fn a_model_the_registry_does_not_have_is_a_404_naming_the_model_and_never_dials() {
    // Both relays throw `Model '${body.model}' not found`, and an uncaught throw lands on the gateway's
    // `{ error: <message> }` — `error` a BARE STRING, not an object. The id is the one AS TYPED: an
    // unknown slug must not fall back to a default row.
    let provider = Provider::start(&[Row::visible("test/alpha", "upstream/alpha")], vec![]).await;

    for (route, body) in [
        (CHAT_COMPLETIONS_ROUTE, chat_body("test/missing")),
        (RESPONSES_ROUTE, responses_body("test/missing")),
    ] {
        let answer = provider.post(route, body).await;

        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{route}");
        assert_eq!(
            answer.json(),
            json!({ "error": "Model 'test/missing' not found" }),
            "{route} must name the model exactly as the request spelled it, as a bare string"
        );
        assert_eq!(
            answer.header("content-type"),
            Some(JSON_CONTENT_TYPE),
            "{route}"
        );
    }

    // The label follows JS template interpolation for every shape the TypeScript can reach
    // (`serve::model_label`): an absent key prints `undefined`, `null` prints `null`, a number or bool
    // prints its `String()` form, and an object prints `[object Object]`.
    for (body, label) in [
        (json!({ "messages": [] }), "undefined"),
        (json!({ "model": Value::Null }), "null"),
        (json!({ "model": 7 }), "7"),
        (json!({ "model": true }), "true"),
        (json!({ "model": { "id": "x" } }), "[object Object]"),
        (json!({ "model": ["a", "b"] }), "a,b"),
    ] {
        let answer = provider.post(CHAT_COMPLETIONS_ROUTE, body).await;

        assert_eq!(answer.status, StatusCode::NOT_FOUND);
        assert_eq!(
            answer.json(),
            json!({ "error": format!("Model '{label}' not found") }),
            "JS interpolation, spelled the way the deployed message spells it"
        );
    }

    assert_eq!(
        provider.served_count(),
        0,
        "an unknown model is refused before any upstream request"
    );
}

#[tokio::test]
async fn a_body_that_is_not_a_json_object_resolves_no_model_and_is_a_404() {
    // The deployed gateway never lets a bad body become an error, but the shapes in the loop below
    // reach the SAME 404 by two different paths, and `src/serve.rs:783-795` is the version this
    // reproduces:
    //   * `""` and `"not json"` do not parse at all, and ONLY a parse failure is folded into `{}` —
    //     `start.ts:815` is `try { body = await context.req.json(); } catch { body = {}; }`;
    //   * `[1,2]` / `7` / `"text"` parse fine and the gateway keeps them INTACT, so the 404 is not
    //     produced by folding at all: it is the route's own destructure, and
    //     `const { model } = [1,2]` / `= 7` / `= "text"` yields `undefined` WITHOUT throwing
    //     (`private-chat-completion.ts:65-66`), so the lookup misses and the relay answers
    //     `404 Model 'undefined' not found` (`private-chat-completion.ts:78-83`);
    //   * `null` is the one shape that is NOT folded and does NOT reach that lookup: destructuring it
    //     throws, unwinds out of the handler, and the gateway catch answers
    //     `500 {"error":"Internal Server Error"}` (`start.ts:908-911`) — pinned by the separate
    //     `a_null_body_is_the_one_shape_the_gateway_does_not_fold` below, which is why `null` is
    //     absent from this list.
    // Either 404 path lands on `model === undefined`, so `src/serve.rs` may fold the body into the
    // empty document and still produce the deployed verdict: no 500, and no invented 400.
    let provider = Provider::start(&[Row::visible("test/alpha", "upstream/alpha")], vec![]).await;

    for body in ["", "not json", "[1,2]", "7", "\"text\""] {
        let answer = provider.post_raw(CHAT_COMPLETIONS_ROUTE, body).await;

        assert_eq!(
            answer.status,
            StatusCode::NOT_FOUND,
            "the body {body:?} is the empty object, whose model no row owns"
        );
        assert_eq!(
            answer.json(),
            json!({ "error": "Model 'undefined' not found" }),
            "the route's own 404 text, with the model name rendered from `undefined` \
             (`private-chat-completion.ts:78-83`)"
        );
    }

    // The Responses surface shares the handler (`src/serve.rs`, `responses` -> `completion`), so it
    // folds the same way and answers with the same 404 rather than the gateway's 500.
    let responses = provider.post_raw(RESPONSES_ROUTE, "not json").await;

    assert_eq!(responses.status, StatusCode::NOT_FOUND);
    assert_eq!(
        responses.json(),
        json!({ "error": "Model 'undefined' not found" }),
        "the same fold on the same document, one route over"
    );

    // The 404 for a body fault coexists with the 404 for a routing fault, and the order is visible: an
    // object body naming an unknown model is a 404 too, for the reason its own name gives.
    let missing = provider
        .post(CHAT_COMPLETIONS_ROUTE, json!({ "model": "test/missing" }))
        .await;
    assert_eq!(
        missing.status,
        StatusCode::NOT_FOUND,
        "a parseable object with an unknown model is a 404"
    );
    assert_eq!(
        missing.json(),
        json!({ "error": "Model 'test/missing' not found" })
    );

    assert_eq!(
        provider.served_count(),
        0,
        "a malformed body never reaches the upstream"
    );
}

#[tokio::test]
async fn a_null_body_is_the_one_shape_the_gateway_does_not_fold() {
    // `null` is the exception, and it is a JavaScript exception: `const { model } = null` THROWS
    // (`TypeError: Cannot destructure property 'model'`) inside the handler body
    // (`private-chat-completion.ts:66`), and a throw out of a handler is the gateway's
    // `500 {"error":"Internal Server Error"}` (`packages/underload/service/src/server/start.ts:908-911`).
    // So the constant 500 is NOT the malformed-body answer (that is the 404 above) — it is the
    // unparseable-BY-DESTRUCTURE answer, and `src/serve.rs` keeps it for `Value::Null` only.
    let provider = Provider::start(&[Row::visible("test/alpha", "upstream/alpha")], vec![]).await;

    let answer = provider.post_raw(CHAT_COMPLETIONS_ROUTE, "null").await;

    assert_eq!(
        answer.status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "`null` throws out of the destructure, which is the gateway's constant"
    );
    assert_eq!(answer.json(), json!({ "error": INTERNAL_ERROR }));

    assert_eq!(
        provider.served_count(),
        0,
        "a `null` body never reaches the upstream either"
    );
}

#[tokio::test]
async fn an_upstream_failure_before_the_head_is_the_gateways_500_and_leaks_nothing() {
    // A pre-stream upstream failure is a THROW in the TypeScript (`private-chat-completion.ts` has no
    // catch around the settled call), so the answer is the gateway's constant — not the upstream's status,
    // and not the upstream's body. Pinning both directions matters: a caller must be able to tell "the
    // provider failed" from "the model refused", and a vault value must never ride out in a message.
    let refusal = json!({
        "error": { "message": "Invalid API key", "code": "invalid_request_error" },
    });
    let provider = Provider::start(
        &[Row::visible("test/alpha", "upstream/alpha")],
        vec![Reply::Json(401, refusal.clone())],
    )
    .await;

    let answer = provider
        .post(CHAT_COMPLETIONS_ROUTE, chat_body("test/alpha"))
        .await;

    assert_eq!(
        answer.status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "the upstream's 401 does not travel: the provider's own throw is the answer"
    );
    assert_eq!(answer.json(), json!({ "error": INTERNAL_ERROR }));

    let body = answer.text();
    for leak in [
        "Invalid API key",
        "invalid_request_error",
        SECRET,
        "upstream/alpha",
    ] {
        assert!(
            !body.contains(leak),
            "the response body must not carry {leak:?}: {body}"
        );
    }

    // One attempt: a 401 is not the 404 that drives the failover loop, so nothing rotates. The scripted
    // upstream would answer 418 if the provider dialled again, so this count is the observable half of the
    // retry rule as seen from the route.
    assert_eq!(
        provider.served_count(),
        1,
        "an auth failure is not a rotation or retry trigger"
    );
}

// ---------------------------------------------------------------------------
// POST /chat/completions — streamed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_streamed_completion_copies_every_frame_and_terminates_with_the_sentinel() {
    // The frame rule is the whole reason `serve::Relay` exists:
    //   * each upstream chunk is re-serialised into `data: …\n\n` (`private-chat-completion.ts` stringifies
    //     the chunk it parsed) and nothing else is added;
    //   * CLEAN exhaustion writes the request log line and then exactly one `data: [DONE]\n\n`
    //     (`private-chat-completion.ts`, the tail of the loop, and `serve.rs`'s `Poll::Ready(None)` arm);
    //   * and the sentinel is the last thing on the wire.
    const FIRST: &str =
        r#"{"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"he"}}]}"#;
    const SECOND: &str =
        r#"{"id":"c2","object":"chat.completion.chunk","choices":[{"delta":{"content":"llo"}}]}"#;
    const THIRD: &str = r#"{"id":"c3","object":"chat.completion.chunk","choices":[],"usage":{"completion_tokens":4}}"#;

    let provider = Provider::start(
        &[Row::visible("test/alpha", "upstream/alpha")],
        vec![Reply::Stream {
            frames: vec![frame(FIRST), frame(SECOND), frame(THIRD)],
        }],
    )
    .await;

    let answer = provider
        .post(CHAT_COMPLETIONS_ROUTE, streaming(chat_body("test/alpha")))
        .await;

    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(
        answer.header("content-type"),
        Some("text/event-stream"),
        "the media type hono's `streamSSE` sets"
    );
    assert_eq!(
        answer.header("cache-control"),
        Some("no-cache"),
        "the one header `streamSSE` sets besides the media type"
    );

    // Byte-exact, in order, with the sentinel last and nothing after it.
    let expected = [
        frame(FIRST),
        frame(SECOND),
        frame(THIRD),
        DONE_FRAME.to_owned(),
    ]
    .concat();
    assert_eq!(
        answer.text(),
        expected,
        "the frames are copied, never re-framed"
    );
    assert_eq!(answer.text().matches("[DONE]").count(), 1, "one sentinel");
    assert!(
        answer.text().ends_with("\n\n"),
        "the last frame is terminated"
    );

    // The upstream was dialled AS a stream.
    let served = provider.served();
    let [request] = served.as_slice() else {
        panic!("exactly one upstream request");
    };
    assert_eq!(request.path, CHAT_WIRE_PATH);
    assert_eq!(request.body["stream"], true);
    assert_eq!(
        request.body["stream_options"],
        json!({ "include_usage": true }),
        "a chat-native row asks for usage on the trailing chunk (`requires_usage_option`)"
    );

    // A settled stream settles the counters too, which is what makes the sentinel and the numbers agree.
    let metrics = provider.get(STATS_ROUTE).await.json();
    assert_eq!(metrics["models"][0]["requests"], 1);
    assert_eq!(
        metrics["models"][0]["outputTokens"],
        json!(4),
        "the trailing usage chunk's figure is the reported output (`accounting.rs`, `record_stream`)"
    );
}

#[tokio::test]
async fn an_upstream_that_stops_early_still_gets_the_sentinel_and_no_error() {
    // The provider never invents a failure for a body that simply ENDS without saying `[DONE]`:
    // `private-chat-completion.ts` emits the sentinel after any clean exhaustion, and the truncation fact
    // `transport` exposes is one this route deliberately ignores. The caller sees the chunk plus the
    // sentinel, and nothing else.
    const PARTIAL: &str = r#"{"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"partial"}}]}"#;

    let provider = Provider::start(
        &[Row::visible("test/alpha", "upstream/alpha")],
        vec![Reply::Stream {
            frames: vec![frame(PARTIAL)],
        }],
    )
    .await;

    let answer = provider
        .post(CHAT_COMPLETIONS_ROUTE, streaming(chat_body("test/alpha")))
        .await;

    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(answer.text(), format!("{}{}", frame(PARTIAL), DONE_FRAME));
    assert_eq!(answer.text().matches("[DONE]").count(), 1);
}

#[tokio::test]
async fn a_mid_stream_error_frame_is_one_frame_and_no_sentinel_follows_it() {
    // An `{"error": …}` frame on an otherwise-200 stream is the upstream's failure, and the answer the
    // caller gets is the CLASSIFIED frame; then the stream closes normally, with no `[DONE]` after it
    // (`start.ts`'s supervisor writes the frame and returns, and `serve.rs`'s `Err` arm sets `finished`, so
    // the sentinel can never follow).
    //
    // The classification itself is pinned in `tests/transport.rs:1470-1522`: an error object with no HTTP
    // status is the `unknown` class, whose message is the frozen legacy constant. What THIS test pins is
    // that the route writes exactly that frame, byte for byte, with `param` present, `status` and `errno`
    // absent, and nothing after it.
    const CHUNK: &str =
        r#"{"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"he"}}]}"#;
    const ERROR: &str = r#"{"error":{"message":"Rate limit exceeded","type":"rate_limit_error","code":"rate_limit_exceeded"}}"#;

    let provider = Provider::start(
        &[Row::visible("test/alpha", "upstream/alpha")],
        vec![Reply::Stream {
            frames: vec![
                frame(CHUNK),
                frame(ERROR),
                // A frame AFTER the error: the upstream body continued, so this one must never reach the
                // caller.
                frame(r#"{"id":"never","object":"chat.completion.chunk","choices":[]}"#),
            ],
        }],
    )
    .await;

    let answer = provider
        .post(CHAT_COMPLETIONS_ROUTE, streaming(chat_body("test/alpha")))
        .await;

    assert_eq!(
        answer.status,
        StatusCode::OK,
        "the head was already committed before the failure: {}",
        answer.text()
    );
    assert_eq!(
        answer.header("content-type"),
        Some("text/event-stream"),
        "a failed stream is still an SSE answer, because the head cannot be undone"
    );

    let expected = format!(
        "{}data: {}\n\n",
        frame(CHUNK),
        json!({
            "error": {
                "message": UNKNOWN_STREAM_ERROR_MESSAGE,
                "type": "stream_error",
                "code": "unknown",
                // `param` is present even though nothing produced one: the wire shape keeps every key the
                // TypeScript's frame has, and omits only the two that are conditional.
                "param": Value::Null,
            }
        })
    );
    assert_eq!(
        answer.text(),
        expected,
        "key ORDER is part of the frame contract"
    );

    let body = answer.text();
    assert!(
        !body.contains("[DONE]"),
        "the failure frame IS the termination: {body}"
    );
    assert!(
        !body.contains("never"),
        "the frame after the failure never reached the wire: {body}"
    );
    assert!(
        !body.contains("Rate limit exceeded"),
        "the frame carries the classified message, not the upstream's object: {body}"
    );
    assert_eq!(
        provider.served_count(),
        1,
        "a failure after the head is not retryable: the stream is already committed"
    );
}

#[tokio::test]
async fn a_mid_stream_error_frame_with_a_status_is_still_an_unknown_frame() {
    // The other in-band shape: an error object that DOES write a `status`. It changes nothing, because
    // the gateway cannot see that number. The SDK hands the payload to
    // `new APIError(undefined, data.error, undefined, headers)` (`node_modules/openai/core/streaming.js:50`),
    // and `APIError` assigns the status it is passed (`node_modules/openai/core/error.js:12`), so
    // `err.status` is `undefined`; `readUpstreamStatus` (`start.ts:506-517`) reads only
    // `error.status`, so `upstreamStatusCode` is `undefined`, the status arm of `classifyStreamError`
    // (`start.ts:600-601`) does not fire, and `start.ts:644` leaves the `status` key out of the frame
    // literal entirely (`start.ts:695-702`). What reaches a browser is therefore the frozen `unknown`
    // frame, byte for byte — the frame a caller string-matches to decide whether to retry.
    const CHUNK: &str =
        r#"{"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"he"}}]}"#;
    const ERROR: &str = r#"{"error":{"message":"Too many requests","status":429}}"#;

    let provider = Provider::start(
        &[Row::visible("test/alpha", "upstream/alpha")],
        vec![Reply::Stream {
            frames: vec![frame(CHUNK), frame(ERROR)],
        }],
    )
    .await;

    let answer = provider
        .post(CHAT_COMPLETIONS_ROUTE, streaming(chat_body("test/alpha")))
        .await;

    let body = answer.text();
    let frames: Vec<&str> = body.trim_end_matches('\n').split("\n\n").collect();

    assert_eq!(
        frames.len(),
        2,
        "one chunk frame and one failure frame: {body}"
    );
    assert_eq!(frames[0], format!("data: {CHUNK}"));

    let failure: Value = serde_json::from_str(
        frames[1]
            .strip_prefix("data: ")
            .expect("an SSE data line")
            .trim_end_matches('\r'),
    )
    .expect("the terminal frame is JSON");

    assert_eq!(
        keys_of(&failure["error"]),
        vec!["message", "type", "code", "param"],
        "`status` is ABSENT for an in-band frame — it is the `upstream_status` class that stamps it, \
         and an in-band `status` never reaches that class (`start.ts:644`, `:695-702`)"
    );
    assert_eq!(failure["error"]["type"], "stream_error");
    assert_eq!(
        failure["error"]["code"], "unknown",
        "the code the frozen message belongs to (`start.ts:581-620` falls through to `unknown`)"
    );
    assert_eq!(failure["error"]["param"], Value::Null);
    assert_eq!(
        failure["error"]["message"], UNKNOWN_STREAM_ERROR_MESSAGE,
        "the frozen constant, NOT the upstream's own text — the frame's `message` is a wire value \
         consumers string-match (`start.ts:656`, `:686-690`)"
    );

    // The whole frame, byte for byte, so no `status` and no upstream text can sneak back in.
    assert_eq!(
        frames[1],
        "data: {\"error\":{\"message\":\"Upstream stream terminated\",\"type\":\"stream_error\",\"code\":\"unknown\",\"param\":null}}"
    );

    assert!(
        !body.contains("429"),
        "the status the upstream wrote stays off the wire entirely: {body}"
    );
    assert!(
        !body.contains("Too many requests"),
        "the upstream's own text stays off the wire too: {body}"
    );
    assert!(
        !body.contains("[DONE]"),
        "no sentinel after a failure frame: {body}"
    );
}

// ---------------------------------------------------------------------------
// POST /responses
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_responses_route_relays_the_caller_body_to_the_row_s_configured_behaviour() {
    // This port has no Chat↔Responses translation layer yet (a deferred deviation, `src/serve.rs`), so the
    // route relays to the row's configured behaviour — `/chat/completions` for every registered row today
    // (`client.rs`, `Behavior::path`) — while the caller keeps the Responses body it sent. What is pinned
    // is the PATH and the passthrough of the body, which is what the deviation actually promises: a port
    // that silently rewrote the body into the Chat shape would fail here.
    let provider = Provider::start(
        &[Row::visible("test/alpha", "upstream/alpha")],
        vec![Reply::Json(200, json!({ "id": "resp-1", "output": [] }))],
    )
    .await;

    let answer = provider
        .post(RESPONSES_ROUTE, responses_body("test/alpha"))
        .await;

    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(
        answer.json(),
        json!({ "id": "resp-1", "output": [] }),
        "the upstream document is handed back untouched"
    );

    let served = provider.served();
    let [request] = served.as_slice() else {
        panic!("exactly one upstream request");
    };
    assert_eq!(request.method, Method::POST);
    assert_eq!(
        request.path, CHAT_WIRE_PATH,
        "the relay path is the row's behaviour, not the route's name"
    );
    assert_eq!(
        request.body["input"], "hi",
        "the caller's Responses body is relayed"
    );
    assert_eq!(
        request.body["model"], "upstream/alpha",
        "the wire id is still pinned"
    );
    assert_eq!(request.body["stream"], false);
    assert_eq!(
        request.bearer(),
        Some(format!("Bearer {SECRET}").as_str()),
        "the relay uses the row's credential exactly as the chat route does"
    );
}

#[tokio::test]
async fn a_streamed_responses_answer_terminates_the_same_way_as_a_chat_answer() {
    // The terminal rule is identical for both relays, and that identity is the contract: one `Relay` type
    // serves both, and only the folder name in a log line differs (`CHAT_COMPLETIONS_ENDPOINT` versus
    // `RESPONSES_ENDPOINT`, neither of which is on the wire here).
    const DELTA: &str = r#"{"type":"response.output_text.delta","delta":"hi"}"#;

    let provider = Provider::start(
        &[Row::visible("test/alpha", "upstream/alpha")],
        vec![Reply::Stream {
            frames: vec![frame(DELTA)],
        }],
    )
    .await;

    let answer = provider
        .post(RESPONSES_ROUTE, streaming(responses_body("test/alpha")))
        .await;

    assert_eq!(
        answer.header("content-type"),
        Some("text/event-stream"),
        "the media type hono's `streamSSE` writes"
    );
    assert_eq!(answer.header("cache-control"), Some("no-cache"));
    assert_eq!(
        answer.text(),
        format!("{}{}", frame(DELTA), DONE_FRAME),
        "the upstream frame, then the provider's own sentinel"
    );
}

// ---------------------------------------------------------------------------
// GET /models
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_model_list_is_the_openai_envelope_and_hides_placeholder_rows() {
    // The projection is `models/service-route.ts`: `{ object: 'list', data: [...] }` in that key order,
    // `data` = the registry in ROW order filtered by `isDiscoverable` with no sort anywhere, and each entry
    // the OpenAI model object with the frozen `created` and `owned_by` literals. `enabled` is deliberately
    // not a field: the entry shape stays the OpenAI one, which is why discoverability is a filter and not
    // a column.
    let provider = Provider::start(
        &[
            Row::visible("test/alpha", "upstream/alpha"),
            Row::hidden("test/beta", "upstream/beta"),
            Row::visible("test/gamma", "upstream/gamma"),
        ],
        vec![Reply::Json(200, settled("upstream/beta", "hidden"))],
    )
    .await;

    let answer = provider.get(MODELS_ROUTE).await;

    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(answer.header("content-type"), Some(JSON_CONTENT_TYPE));

    let document = answer.json();
    assert_eq!(
        keys_of(&document),
        vec!["object", "data"],
        "the two keys of the catalog envelope, in that order"
    );
    assert_eq!(document["object"], "list");

    let data = document["data"].as_array().expect("a data array");
    assert_eq!(
        data.iter()
            .map(|entry| entry["id"].as_str().expect("an id"))
            .collect::<Vec<_>>(),
        vec!["test/alpha", "test/gamma"],
        "the placeholder row is filtered out, and filtering never reorders the survivors"
    );

    // One entry, field for field and in the TypeScript's key order.
    assert_eq!(
        keys_of(&data[0]),
        vec!["id", "object", "created", "owned_by", "context_length"]
    );
    assert_eq!(
        data[0],
        json!({
            "id": "test/alpha",
            "object": "model",
            "created": MODEL_CREATED_EPOCH,
            "owned_by": MODEL_OWNED_BY,
            "context_length": 262_144,
        }),
        "the frozen literals of `models/service-route.ts`, and the row's own context length"
    );
    assert_eq!(MODEL_CREATED_EPOCH, 1_677_610_602);
    assert_eq!(MODEL_OWNED_BY, "localhost");

    // The asymmetry the registry is built on: a placeholder row is hidden from the CATALOGUE but still
    // resolves for a request (`Registry::find` applies no discoverability filter). Conflating the two
    // would 404 a model the deployment can still call, so the assertion has to be a successful round trip.
    let hidden = provider
        .post(CHAT_COMPLETIONS_ROUTE, chat_body("test/beta"))
        .await;
    assert_eq!(
        hidden.json(),
        settled("upstream/beta", "hidden"),
        "a hidden row is routable, even though `/models` never names it"
    );

    // The catalogue is a read, not a dial: none of the list requests touched the upstream.
    assert_eq!(
        provider.served_count(),
        1,
        "only the hidden row's successful round trip dialled"
    );
}

// ---------------------------------------------------------------------------
// GET /stats
// ---------------------------------------------------------------------------

/// The row keys of `private-stats.ts`'s `ModelStats`, in declaration order, followed by the two counters
/// this port appends AFTER that block so the TypeScript's row shape stays readable inside the JSON
/// (`serve::stats_row`).
const STATS_ROW_KEYS: [&str; 22] = [
    "id",
    "model",
    "provider",
    "inputTokens",
    "cacheTokens",
    "outputTokens",
    "requests",
    "inputCost",
    "cacheCost",
    "outputCost",
    "totalCost",
    "inputPrice",
    "outputPrice",
    "cachePrice",
    "milliseconds",
    "ttftMilliseconds",
    "generationMilliseconds",
    "measuredPrefillTokens",
    "measuredDecodeTokens",
    "measuredDecodeMilliseconds",
    "enabled",
    "discoverable",
];

/// The totals keys of `private-stats.ts`'s `StatsTotals`, in order, with the two port counters appended
/// (`serve::stats_totals`).
const STATS_TOTALS_KEYS: [&str; 13] = [
    "inputTokens",
    "cacheTokens",
    "outputTokens",
    "requests",
    "totalCost",
    "milliseconds",
    "ttftMilliseconds",
    "generationMilliseconds",
    "measuredPrefillTokens",
    "measuredDecodeTokens",
    "measuredDecodeMilliseconds",
    "attemptTotal",
    "consecutiveFailures",
];

#[tokio::test]
async fn the_stats_document_is_json_only_and_reports_the_rows_that_served() {
    // The document is `{ models: [...], totals: {...} }` (`private-stats.ts`: rows sorted by id with a
    // code-unit compare, then the fleet roll-up). This port serves the JSON ONLY — the TypeScript's HTML
    // dashboard is a separate deferred deviation, recorded in `src/serve.rs` — so "the answer starts with
    // `{`" is itself an assertion.
    let provider = Provider::start(
        &[
            Row::visible("test/alpha", "upstream/alpha"),
            Row::hidden("test/beta", "upstream/beta"),
        ],
        vec![
            Reply::Json(200, settled("upstream/alpha", "one")),
            Reply::Json(200, settled("upstream/beta", "two")),
        ],
    )
    .await;

    for id in ["test/beta", "test/alpha"] {
        let answer = provider.post(CHAT_COMPLETIONS_ROUTE, chat_body(id)).await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    }

    let answer = provider.get(STATS_ROUTE).await;
    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(answer.header("content-type"), Some(JSON_CONTENT_TYPE));
    assert_eq!(
        answer.text().trim_start().chars().next(),
        Some('{'),
        "the stats answer is JSON, never the HTML page"
    );

    let document = answer.json();
    assert_eq!(
        keys_of(&document),
        vec!["models", "totals"],
        "the two keys of the stats document, in that order"
    );

    let rows = document["models"].as_array().expect("a models array");
    assert_eq!(
        rows.iter()
            .map(|row| row["id"].as_str().expect("an id"))
            .collect::<Vec<_>>(),
        vec!["test/alpha", "test/beta"],
        "rows are sorted by id — the order `collectStats` produces, which is NOT the registry order \
         `/models` keeps. The hidden row is listed here because the stats surface has no discoverability \
         filter: a deployment has to see the cost of the model it is paying for."
    );

    // The two chat requests above dialled in the order the test sent them (beta, then alpha), and the
    // prompt side of each row's counters is the estimator's figure for the body THAT row sent — which is
    // what the token and cost assertions below are derived from.
    let served = provider.served();
    let alpha_tokens = ESTIMATED_PROMPT_TOKENS;
    let beta_tokens = ESTIMATED_PROMPT_TOKENS;
    assert_eq!(
        served[0].received_estimate(),
        RECEIVED_ESTIMATE,
        "both rows were asked with the same caller body, so both received the same rewritten payload"
    );

    let row = &rows[0];
    let (input_cost, cache_cost, output_cost, total_cost) = fixture_costs(alpha_tokens);
    assert_eq!(
        keys_of(row),
        STATS_ROW_KEYS.to_vec(),
        "the ModelStats field order, verbatim, with the two port counters appended last"
    );
    assert_eq!(
        row["id"], "test/alpha",
        "the row is keyed by the ROUTING id, not the wire id"
    );
    assert_eq!(row["model"], "upstream/alpha");
    assert_eq!(row["provider"], "Test");
    assert_eq!(row["requests"], json!(1));
    assert_eq!(
        row["inputTokens"],
        json!(alpha_tokens),
        "the prompt side is the estimator's figure for what the row sent"
    );
    assert_eq!(
        row["cacheTokens"],
        json!(0),
        "a first request has no replayed prefix to charge as cache (`accounting.rs:305-321`)"
    );
    assert_eq!(
        row["outputTokens"],
        json!(500),
        "the output figure is the upstream usage, added verbatim"
    );
    assert_eq!(row["inputCost"], json!(input_cost));
    assert_eq!(row["cacheCost"], json!(cache_cost));
    assert_eq!(row["outputCost"], json!(output_cost));
    assert_eq!(row["totalCost"], json!(total_cost));
    assert_eq!(row["inputPrice"], 1_000.0);
    assert_eq!(row["outputPrice"], 2_000.0);
    assert_eq!(row["cachePrice"], 100.0);
    assert_eq!(
        [
            "ttftMilliseconds",
            "generationMilliseconds",
            "measuredPrefillTokens",
            "measuredDecodeTokens",
            "measuredDecodeMilliseconds",
        ]
        .iter()
        .map(|key| row[key].as_u64().expect("a number"))
        .collect::<Vec<_>>(),
        vec![0; 5],
        "a settled response records no phase split at all (`accounting.rs`)"
    );
    assert_eq!(row["enabled"], true);
    assert_eq!(row["discoverable"], true);
    assert_eq!(
        rows[1]["discoverable"], false,
        "the placeholder row says so"
    );
    assert_eq!(
        rows[1]["requests"],
        json!(1),
        "each row reports only its own traffic"
    );

    // `milliseconds` is wall-clock, so it is pinned as a NUMBER rather than as a value: the answer must
    // carry the field, and a request that metered zero milliseconds would be a metering bug.
    assert!(rows[0]["milliseconds"].is_number());

    let totals = &document["totals"];
    assert_eq!(
        keys_of(totals),
        STATS_TOTALS_KEYS.to_vec(),
        "the StatsTotals field order, with the port counters appended last"
    );
    assert_eq!(
        totals["inputTokens"],
        json!(alpha_tokens + beta_tokens),
        "the fleet roll-up is the sum of the rows"
    );
    assert_eq!(totals["cacheTokens"], json!(0));
    assert_eq!(totals["outputTokens"], json!(1_000));
    assert_eq!(totals["requests"], json!(2));
    assert_eq!(
        totals["attemptTotal"],
        json!(2),
        "one attempt per request issue on a success path (port addition, `accounting.rs`)"
    );
    assert_eq!(totals["consecutiveFailures"], json!(0));
    assert_eq!(
        totals["totalCost"],
        json!(total_cost + fixture_costs(beta_tokens).3),
        "the fleet figure is the sum of each row's OWN brackets, because each row prices its own tokens \
         at its own rates (`private-stats.ts`)"
    );
}

#[tokio::test]
async fn the_stats_reset_verb_clears_one_row_and_refuses_the_ids_it_does_not_own() {
    // POST is the reset verb, keyed by `?id=`, in the three verdicts `privateStatsReset` uses
    // (`private-stats.ts`): no `id`, or an empty one, is 400 `missing id query parameter`; an id no row
    // owns is 404 `unknown model id: <id>`; a row zeroes its counters and answers `{"reset":"<id>"}`. What
    // is NOT cleared is the catalogue — the dashboard has to keep showing an idle model.
    let provider = Provider::start(
        &[
            Row::visible("test/alpha", "upstream/alpha"),
            Row::visible("test/beta", "upstream/beta"),
        ],
        vec![
            Reply::Json(200, settled("upstream/alpha", "one")),
            Reply::Json(200, settled("upstream/beta", "two")),
            Reply::Json(200, settled("upstream/alpha", "three")),
        ],
    )
    .await;

    for id in ["test/alpha", "test/beta"] {
        let answer = provider.post(CHAT_COMPLETIONS_ROUTE, chat_body(id)).await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    }

    // The refusals first, against the same counters, so a passing assertion below cannot be an artifact of
    // an earlier reset.
    for query in ["", "id="] {
        let answer = provider.post_query(STATS_ROUTE, query, json!({})).await;

        assert_eq!(
            answer.status,
            StatusCode::BAD_REQUEST,
            "the verdict for {query:?}"
        );
        assert_eq!(answer.json(), json!({ "error": MISSING_ID_ERROR }));
    }

    let unknown = provider
        .post_query(STATS_ROUTE, "id=test/nope", json!({}))
        .await;
    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    assert_eq!(
        unknown.json(),
        json!({ "error": "unknown model id: test/nope" }),
        "the 404 names the id the caller meant"
    );

    let before = provider.get(STATS_ROUTE).await.json();
    assert_eq!(before["models"][0]["requests"], json!(1));
    assert_eq!(before["models"][1]["requests"], json!(1));

    // `serve::query_value` decodes the query token the way Hono's `req.query()` does, so a percent-encoded
    // slug resets the row the caller actually named.
    let answer = provider
        .post_query(STATS_ROUTE, "id=test%2Falpha", json!({}))
        .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    assert_eq!(answer.json(), json!({ "reset": "test/alpha" }));

    let after = provider.get(STATS_ROUTE).await.json();
    let rows = after["models"].as_array().expect("a models array");
    assert_eq!(rows.len(), 2, "reset clears counters, never the catalogue");
    assert_eq!(rows[0]["id"], "test/alpha");
    assert_eq!(
        [
            "requests",
            "inputTokens",
            "cacheTokens",
            "outputTokens",
            "inputCost",
            "cacheCost",
            "outputCost",
            "totalCost",
        ]
        .iter()
        .map(|key| rows[0][key].as_f64().expect("a number"))
        .collect::<Vec<_>>(),
        vec![0.0; 8],
        "the reset row is zeroed in every counter, token figure and dollar figure"
    );
    assert_eq!(rows[1]["requests"], json!(1), "the other row is untouched");
    assert_eq!(
        after["totals"]["requests"],
        json!(1),
        "the roll-up follows the rows rather than remembering the reset traffic"
    );

    // Counters restart from the reset instead of continuing from 2, and the row is still routable.
    let answer = provider
        .post(CHAT_COMPLETIONS_ROUTE, chat_body("test/alpha"))
        .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());

    let after = provider.get(STATS_ROUTE).await.json();
    assert_eq!(
        after["models"][0]["requests"],
        json!(1),
        "a reset row counts from zero again"
    );
    assert_eq!(after["models"][0]["outputTokens"], json!(500));
}
