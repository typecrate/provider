//! Behavioural tests for the declarative model client (`src/client.rs`) and the row
//! vocabulary it serves (`src/clients.rs`).
//!
//! Every upstream here is a REAL in-process listener on `127.0.0.1:0` (the scripted-provider
//! pattern of `tests/transport.rs:206-245`, copied rather than shared because integration tests
//! are separate binaries and a shared harness crate is not allowed by this lockfile), so these
//! tests pin what actually goes on the wire: header spelling, header rotation order, the rewritten
//! body, and the exact moment the counters fold. Nothing leaves loopback, no port is fixed, and
//! the only waiting is the pause a test configures itself.
//!
//! The specification each test names is the TypeScript original:
//!   * `packages/agentic/provider/src/model-class.ts` - the payload rewrite
//!     (`withModel:899-901` plus each family's `create()` body), the request accounting
//!     (`accountUpstreamCall:700-734`, `recordRequestTokens:770-786`), the failover loop
//!     (`:555-654`, whose 404 handler IS `switchBaseUrl:933-935`), `trackStream:831-879`,
//!     `isDiscoverable:388` and `hasValidApiKey:378`.
//!   * `packages/agentic/provider/src/local/local-strata.ts` / `local-basalt.ts` - the
//!     credential-less relay, its endpoint rotation and its `upstreamLabel`.
//!   * `packages/agentic/provider/src/nvidia/nvidia-glm5-flash.ts` and
//!     `makora/makora-glm-5-3.ts` - the two DIFFERENT credential-drain shapes, which is why
//!     `CredentialAdvance` is an enum rather than one rotation function.
//!
//! WHY THE UPSTREAM IS AXUM (default features, and a normal dependency of the crate since
//! `src/serve.rs` became an axum handler): `tests/transport.rs` proves the scripted SSE answer needs
//! no `sse` feature; this suite reuses that shape to observe the client layer above it.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    body::Body,
    extract::{Request, State},
    http::{StatusCode, header},
    response::Response,
    routing::any,
};
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use typecrate_provider::{
    accounting::Counters,
    client::{
        Behavior, ClientSpec, CredentialAdvance, ModelClient, RoutingVendor, upstream_label_is_safe,
    },
    error::StreamFailure,
    pricing::Pricing,
    sse::{DONE_FRAME, data_frame},
    transport::{
        Credential, Endpoint, KeylessHeaderPolicy, Policy, Target, Transport, TransportFailure,
        UpstreamStream, estimate_request_tokens,
    },
    vault::{Secret, load_from_str},
};

/// A credential long enough to trip the redactor (`src/vault.rs:102`) and `sk-`-shaped, so
/// "no credential value in any diagnostic" is a real test and not a tautology.
const SECRET: &str = "sk-live-9f3a7c21be4d0000feed";

/// `local-strata.ts:216` / `local-basalt.ts:204`: `MAX_CONTEXT_LENGTH / 8`, the cap those two
/// rows OVERWRITE a caller's `max_tokens` with. `clients.rs` keeps the constant private, so the
/// literal is restated here and pinned by `a_capped_row_overwrites_the_callers_max_tokens`.
const OUTPUT_CAP: u64 = 32_768;

// ---------------------------------------------------------------------------
// The scripted upstream
// ---------------------------------------------------------------------------

/// One scripted answer, consumed by exactly one attempt.
enum Reply {
    /// `status` plus a JSON document (a settled response).
    Json(StatusCode, Value),
    /// `status` plus a non-2xx JSON error document, which is how a 404 from the failover tests is
    /// scripted: the client must propagate THAT verdict, not invent one.
    Error(StatusCode, Value),
    /// A 200 SSE body: one frame per poll. `gap` parks a waker in front of every frame, which is
    /// what makes "the consumer went away mid-stream" and "the stream completed" distinguishable.
    Sse {
        frames: Vec<Vec<u8>>,
        gap: Option<Duration>,
    },
}

impl Reply {
    /// An unspaced SSE answer from pre-encoded frames.
    fn sse(frames: Vec<Vec<u8>>) -> Self {
        Self::Sse { frames, gap: None }
    }

    /// SSE frames built from chunk values, terminated by `[DONE]` (the shape every chat-native
    /// upstream actually sends).
    fn chunks(frames: Vec<Value>) -> Self {
        let mut encoded: Vec<Vec<u8>> = frames
            .into_iter()
            .map(|frame| data_frame(&frame).into_bytes())
            .collect();
        encoded.push(DONE_FRAME.as_bytes().to_vec());

        Self::sse(encoded)
    }

    /// The same, one `gap` in front of every frame.
    fn paced_chunks(frames: Vec<Value>, gap: Duration) -> Self {
        let mut reply = Self::chunks(frames);

        if let Self::Sse { gap: slot, .. } = &mut reply {
            *slot = Some(gap);
        }

        reply
    }
}

/// What one attempt looked like on the wire, as the upstream saw it.
#[derive(Clone, Debug)]
struct Captured {
    path: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Captured {
    /// The value of one header, or `None` when the header was absent. Absence is a first-class
    /// assertion here: the credential-less relays must send NO `Authorization` at all.
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// The request body, parsed.
    fn sent(&self) -> Value {
        serde_json::from_str(&self.body).expect("the transport always sends valid JSON")
    }
}

/// The scripted upstream: a per-attempt script queue plus the capture ledger.
#[derive(Default)]
struct Upstream {
    script: Mutex<VecDeque<Reply>>,
    seen: Mutex<Vec<Captured>>,
}

impl Upstream {
    /// The requests this upstream answered, in order.
    fn seen(&self) -> Vec<Captured> {
        self.seen.lock().expect("ledger lock").clone()
    }

    /// How many attempts reached the upstream.
    fn attempts(&self) -> usize {
        self.seen.lock().expect("ledger lock").len()
    }

    /// The `authorization` header of every attempt, in order. Absent becomes `"<none>"` so a
    /// rotation sequence is one exact `Vec<String>` comparison.
    fn authorizations(&self) -> Vec<String> {
        self.seen()
            .into_iter()
            .map(|attempt| {
                attempt
                    .header("authorization")
                    .unwrap_or("<none>")
                    .to_owned()
            })
            .collect()
    }

    /// The request bodies of every attempt, parsed, in order.
    fn sent(&self) -> Vec<Value> {
        self.seen().into_iter().map(|a| a.sent()).collect()
    }
}

/// Starts one scripted upstream and returns its `host:port` plus its ledger handle.
async fn provider(script: Vec<Reply>) -> (String, Arc<Upstream>) {
    let upstream = Arc::new(Upstream {
        script: Mutex::new(script.into()),
        seen: Mutex::new(Vec::new()),
    });

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback listener");
    let address = listener
        .local_addr()
        .expect("the bound address of a loopback listener")
        .to_string();

    let served = Arc::clone(&upstream);

    tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/{*path}", any(handle))
                .with_state(served),
        )
        .await
        .expect("the scripted upstream lives until the process ends");
    });

    (address, upstream)
}

/// Answers the next scripted reply, after recording what arrived.
async fn handle(State(upstream): State<Arc<Upstream>>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                value.to_str().unwrap_or("<binary>").to_owned(),
            )
        })
        .collect();
    let body = axum::body::to_bytes(request.into_body(), 4 * 1024 * 1024)
        .await
        .expect("the request body is readable");

    upstream.seen.lock().expect("ledger lock").push(Captured {
        path,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    });

    // Taken and released before any await, so no lock is ever held across a poll.
    match upstream.script.lock().expect("script lock").pop_front() {
        Some(Reply::Json(status, document)) | Some(Reply::Error(status, document)) => {
            response(status, "application/json", Body::from(document.to_string()))
        }
        Some(Reply::Sse { frames, gap }) => response(
            StatusCode::OK,
            "text/event-stream",
            Body::from_stream(ScriptedSse {
                frames,
                index: 0,
                gap,
                pacer: None,
            }),
        ),
        // The script ran out: an answer impossible to mistake for a real one, so a failing test
        // names its own bug instead of hiding behind a client verdict.
        None => response(
            StatusCode::IM_A_TEAPOT,
            "text/plain",
            Body::from("script exhausted: the client made one attempt too many"),
        ),
    }
}

/// A response built from DEFAULT axum features only - no `sse`, no `json`.
fn response(status: StatusCode, content_type: &str, body: Body) -> Response {
    axum::http::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-cache")
        .body(body)
        .expect("a status, two headers and a body always build a response")
}

/// The SSE body: one scripted frame per poll, then `None`.
struct ScriptedSse {
    frames: Vec<Vec<u8>>,
    index: usize,
    gap: Option<Duration>,
    /// The delay in front of the NEXT frame; a completed sleep is dropped and re-armed, so spacing
    /// is per frame rather than once.
    pacer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Stream for ScriptedSse {
    type Item = Result<Vec<u8>, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index >= self.frames.len() {
            return Poll::Ready(None);
        }

        if let Some(delay) = self.gap {
            let armed = self
                .pacer
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(delay)));

            if Future::poll(armed.as_mut(), context).is_pending() {
                return Poll::Pending;
            }

            self.pacer = None;
        }

        let index = self.index;
        let frame = std::mem::take(&mut self.frames[index]);
        self.index += 1;

        Poll::Ready(Some(Ok(frame)))
    }
}

// ---------------------------------------------------------------------------
// Test-side construction
// ---------------------------------------------------------------------------

/// A hermetic HTTP client. `no_proxy` is load-bearing: `reqwest::Client::builder()` honours
/// `HTTP_PROXY` from the developer's environment, and a proxy would move every request off
/// loopback.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("a proxy-free client")
}

/// The row every behavioural test starts from: a Chat Completions row with a credential pool, no
/// request shaping, and the vault names a real row would use. Field-by-field defaults come from
/// `ClientSpec::plain`, so a test that wants a variant says so at the call site.
fn row(id: &'static str) -> ClientSpec {
    ClientSpec::plain(
        id,
        "Test",
        "glm-5.3",
        "test",
        "test_api_keys",
        "test_api_endpoints",
    )
}

/// A pool of usable credentials in the given order - the stand-in for a vault credential slot,
/// where ORDER is the whole point.
fn keyed(values: &[&str]) -> Vec<Credential> {
    values
        .iter()
        .map(|value| Credential::new(&Secret::new(*value)))
        .collect()
}

/// A one-vendor vault document whose credential pool is `entries` and whose slot `default` is
/// `default`, shaped exactly like the real artifact (`src/vault.rs:1410`'s fixture is the
/// template). The loader's validator requires `default == entries[0].value`
/// (`src/vault.rs:1373`), so a test that wants a particular head must pass the matching default.
/// This is how the tests get credentials with VAULT VALIDITIES, which `Credential::new` alone
/// cannot express.
fn vault_doc(entries: &str, default: &str) -> String {
    format!(
        r#"{{
  "schema_version": 1,
  "package": "@config/vault",
  "consumers": ["agentic-provider"],
  "sentinels": {{
    "keyless": ["none"],
    "invalid": ["NotAvailable", "error", "null", "undefined"],
    "placeholder": ["EXAMPLE_API_KEY_PLACEHOLDER"]
  }},
  "vendors": [{{
    "id": "test",
    "slots": [
      {{
        "id": "test_api_keys",
        "kind": "keys",
        "secret": true,
        "optional": false,
        "retired": false,
        "nesting": "flat",
        "entries": {entries},
        "default": "{default}"
      }}
    ]
  }}]
}}"#
    )
}

/// The credential pool of a synthetic document, in document order, labels and validities intact.
fn pool_from(doc: &str) -> Vec<Credential> {
    let vault = load_from_str(doc).expect("the synthetic document parses");
    let vendor = vault
        .vendors()
        .iter()
        .find(|vendor| vendor.id() == "test")
        .expect("the synthetic vendor");
    let slot = vendor
        .credential_slot()
        .expect("the synthetic credential slot");

    slot.entries().iter().map(Credential::from_entry).collect()
}

/// Builds the client under test.
///
/// The `Target` is assembled exactly the way `src/registry.rs:355-358` assembles a real row's:
/// the two pools, then `.keyless(...)` derived from "this row has no credential slot", then the
/// row's own keyless-header policy. A test that assembled the target any other way would be
/// testing a shape the registry never builds.
fn client_on(
    spec: ClientSpec,
    endpoints: Vec<Endpoint>,
    credentials: Vec<Credential>,
) -> ModelClient {
    client_on_with(spec, endpoints, credentials, Policy::standard())
}

/// The same, with a caller-chosen retry policy (the 429 test needs a pause it can afford to wait
/// through). The address lives in `endpoints`, which is where the registry puts it too.
fn client_on_with(
    spec: ClientSpec,
    endpoints: Vec<Endpoint>,
    credentials: Vec<Credential>,
    policy: Policy,
) -> ModelClient {
    let target = Target::new(endpoints, credentials)
        .keyless(spec.credential_slot.is_none())
        .keyless_header(spec.keyless_header);

    ModelClient::new(
        spec,
        Transport::with_http_client(target, policy, http_client()),
    )
}

/// The one-endpoint case, which is every row except the relays: the label is DROPPED here,
/// because `registry.rs` publishes endpoint labels only for a `ChatKeylessRelay` row.
fn single_endpoint(address: &str) -> Vec<Endpoint> {
    vec![Endpoint::new(&format!("http://{address}"))]
}

/// A chat row over one scripted upstream with `credentials` as its pool.
fn client_with_pool(spec: ClientSpec, address: &str, credentials: Vec<Credential>) -> ModelClient {
    client_on(spec, single_endpoint(address), credentials)
}

/// The request body every test starts from. `stream` is added per test, so the rewrite decisions
/// that depend on it stay visible at the call site.
fn body(model: &str) -> Value {
    json!({
        "model": model,
        "messages": [{ "role": "user", "content": "hello" }],
        "temperature": 0.5
    })
}

/// A minimal settled-200 document, with the usage the accounting tests read back.
fn completion(output_tokens: u64) -> Value {
    json!({
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "hi" } }],
        "usage": { "prompt_tokens": 11, "completion_tokens": output_tokens, "total_tokens": 11 + output_tokens }
    })
}

/// A streaming chunk with generated content (the shape `has_generated_payload` looks for).
fn content_chunk(text: &str) -> Value {
    json!({
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "choices": [{ "index": 0, "delta": { "content": text } }]
    })
}

/// The terminal chunk carrying usage, which is what `stream_options.include_usage` buys
/// (`model-class.ts:846-853` reads exactly this shape).
fn usage_chunk(output_tokens: u64) -> Value {
    json!({
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "choices": [],
        "usage": { "prompt_tokens": 11, "completion_tokens": output_tokens, "total_tokens": 11 + output_tokens }
    })
}

/// Drains a chunk stream, keeping a terminal failure instead of unwrapping it away.
async fn collect(stream: &mut UpstreamStream) -> (Vec<Value>, Option<StreamFailure>) {
    let mut chunks = Vec::new();

    while let Some(item) = stream.next().await {
        match item {
            Ok(chunk) => chunks.push(chunk),
            Err(failure) => return (chunks, Some(failure)),
        }
    }

    (chunks, None)
}

/// A settled success against `client`, unwrapped: the tests that care about accounting do not want
/// to repeat the unwrap everywhere.
async fn ok_json(client: &ModelClient, payload: &Value) -> Value {
    let answer = client
        .create(payload)
        .await
        .expect("the scripted upstream answers 200");

    answer
        .response()
        .cloned()
        .expect("a non-streaming body settles as a document")
}

/// Asserts the counters the FAILURES of a test must never disturb, so each test states only the
/// figures it actually pins. `Counters` is `PartialEq`-free by design (it is a snapshot struct of
/// scalars), so this helper is a list of exact `==` checks rather than one `toEqual`.
#[must_use]
fn counters_of(client: &ModelClient) -> Counters {
    client.counters()
}

/// The exactly-once follow-up: after a stream has settled, no LATER poll may fold anything again.
/// Yielding then re-reading the counters is how that is observed - a second settlement would put
/// the figures back.
async fn counters_after_settlement(client: &ModelClient) -> Counters {
    client.reset_counters();

    // Two yields plus one tick of the runtime: enough for any queued wake-up of the same stream to
    // run, and the reason the result below must still be all-zero.
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    counters_of(client)
}

/// The all-zero ledger, spelled once.
fn zeroed() -> Counters {
    Counters {
        input_token_total: 0,
        cache_token_total: 0,
        output_token_total: 0,
        request_total: 0,
        millisecond_total: 0,
        ttft_millisecond_total: 0,
        generation_millisecond_total: 0,
        measured_prefill_token_total: 0,
        measured_decode_token_total: 0,
        measured_decode_millisecond_total: 0,
        attempt_total: 0,
        consecutive_failures: 0,
    }
}

// ---------------------------------------------------------------------------
// The payload rewrite
// ---------------------------------------------------------------------------

/// `withModel` (`model-class.ts:899-901`): the row's model wins over the caller's, and everything
/// else survives untouched.
#[tokio::test]
async fn the_model_field_is_always_the_rows() {
    let (address, upstream) = provider(vec![Reply::Json(StatusCode::OK, completion(4))]).await;
    let client = client_with_pool(row("test/glm-5.3"), &address, keyed(&["k1"]));

    let payload = body("caller-model");
    let answer = ok_json(&client, &payload).await;

    assert_eq!(
        answer["id"], "chatcmpl-1",
        "the upstream document is returned"
    );
    assert_eq!(
        upstream.sent(),
        vec![json!({
            "model": "glm-5.3",
            "messages": [{ "role": "user", "content": "hello" }],
            "temperature": 0.5,
            "stream": false
        })],
        "the caller's model is replaced and nothing else changes"
    );
    assert_eq!(
        upstream.seen()[0].path,
        "/v1/chat/completions",
        "a scheme-less base URL is rebased to /v1 and posted to chat/completions"
    );
    assert_eq!(
        upstream.seen()[0].header("authorization"),
        Some("Bearer k1"),
        "the pool head is the credential in force"
    );
}

/// `stream_options` is the client's job and appears ONLY on a streaming request from a row that
/// needs it - and it is ABSENT, never `null`, for a row that does not
/// (`nvidia-glm5-flash.ts:79` and the `requires_usage_option:false` proof in the module doc).
#[tokio::test]
async fn stream_options_reaches_only_a_streaming_request_that_needs_it() {
    let (address, upstream) = provider(vec![
        Reply::chunks(vec![content_chunk("a"), usage_chunk(3)]),
        Reply::chunks(vec![content_chunk("b"), usage_chunk(3)]),
        Reply::Json(StatusCode::OK, completion(3)),
    ])
    .await;

    let plain = client_with_pool(row("test/plain"), &address, keyed(&["k1"]));
    let quiet = client_with_pool(
        row("test/quiet").without_usage_option(),
        &address,
        keyed(&["k1"]),
    );

    // 1. streaming + needs it => injected.
    let streamed = json!({ "model": "x", "stream": true });
    assert!(
        plain
            .create(&streamed)
            .await
            .expect("the head is 200")
            .is_stream(),
        "a streaming request returns a stream"
    );

    // 2. streaming + does not need it => the key is absent from the wire body.
    assert!(
        quiet
            .create(&streamed)
            .await
            .expect("the head is 200")
            .is_stream()
    );

    // 3. non-streaming => absent either way, on the row that needs it.
    assert!(
        ok_json(&plain, &json!({ "model": "x", "stream": false }))
            .await
            .is_object()
    );

    let sent = upstream.sent();

    assert_eq!(
        sent[0],
        json!({
            "model": "glm-5.3",
            "stream": true,
            "stream_options": { "include_usage": true }
        }),
        "the injected pair is exactly this, and the caller's model is still replaced"
    );
    assert_eq!(
        sent[1],
        json!({ "model": "glm-5.3", "stream": true }),
        "a row without the usage option sends NO stream_options key at all"
    );
    assert_eq!(
        sent[2],
        json!({ "model": "glm-5.3", "stream": false }),
        "a non-streaming request never carries stream_options"
    );
    assert_eq!(sent[1].get("stream_options"), None, "absent, not null");
}

/// `merge-glm-5-3-flash.ts:87` and `merge-gpt-6-1-sol.ts:101` inject `vendor` AFTER the spread, so a
/// caller cannot bypass the gateway's routing contract by sending its own. Both spellings the row can
/// emit are pinned by name here, so a renamed `RoutingVendor::as_str` arm (`client.rs:203-211`) is a
/// test failure rather than a wrong routing header.
#[tokio::test]
async fn a_routed_row_overwrites_the_callers_vendor() {
    let (address, upstream) = provider(vec![
        Reply::Json(StatusCode::OK, completion(1)),
        Reply::Json(StatusCode::OK, completion(1)),
    ])
    .await;
    let spec = row("merge/glm-5.3-flash").routed(RoutingVendor::Pareto);
    let client = client_with_pool(spec, &address, keyed(&["k1"]));

    ok_json(
        &client,
        &json!({ "model": "x", "vendor": "telnyx", "stream": false }),
    )
    .await;

    // The `openai` spelling, i.e. the `merge-gpt-6-1-sol.ts:101` / `merge-gpt-6-luna.ts:99` rows:
    // `client.rs:975` writes `document['vendor'] = vendor.as_str()`, and `client.rs:209` maps the
    // `OpenAI` variant to exactly `"openai"`. Same caller-supplied override, same replacement.
    let sol = row("merge/gpt-6.1-sol").routed(RoutingVendor::OpenAI);
    let sol_client = client_with_pool(sol, &address, keyed(&["k1"]));

    ok_json(
        &sol_client,
        &json!({ "model": "x", "vendor": "telnyx", "stream": false }),
    )
    .await;

    assert_eq!(
        upstream.sent(),
        vec![
            json!({
                "model": "glm-5.3",
                "vendor": "pareto",
                "stream": false
            }),
            json!({
                "model": "glm-5.3",
                "vendor": "openai",
                "stream": false
            }),
        ],
        "each row's vendor replaces the caller's, and both spellings go out exactly as TypeScript writes them"
    );
    assert_eq!(RoutingVendor::Pareto.as_str(), "pareto");
    assert_eq!(RoutingVendor::OpenAI.as_str(), "openai");
}

/// Every reasoning family defaults the effort when it is absent, `null`, or `''` - and forwards
/// anything else, INCLUDING `'none'`, which for these providers is a real instruction.
#[tokio::test]
async fn a_default_effort_replaces_absent_null_and_empty_but_not_none() {
    let (address, upstream) = provider(
        (0..5)
            .map(|_| Reply::Json(StatusCode::OK, completion(1)))
            .collect(),
    )
    .await;

    let spec = row("test/glm-5.3").effort("medium");
    let client = client_with_pool(spec, &address, keyed(&["k1"]));

    let cases: Vec<(&str, Value)> = vec![
        ("absent", json!({ "model": "x" })),
        ("null", json!({ "model": "x", "reasoning_effort": null })),
        ("empty", json!({ "model": "x", "reasoning_effort": "" })),
        ("none", json!({ "model": "x", "reasoning_effort": "none" })),
        (
            "minimal",
            json!({ "model": "x", "reasoning_effort": "minimal" }),
        ),
    ];

    for (_, payload) in &cases {
        ok_json(&client, payload).await;
    }

    let sent = upstream.sent();

    assert_eq!(
        sent,
        vec![
            json!({ "model": "glm-5.3", "reasoning_effort": "medium", "stream": false }),
            json!({ "model": "glm-5.3", "reasoning_effort": "medium", "stream": false }),
            json!({ "model": "glm-5.3", "reasoning_effort": "medium", "stream": false }),
            json!({ "model": "glm-5.3", "reasoning_effort": "none", "stream": false }),
            json!({ "model": "glm-5.3", "reasoning_effort": "minimal", "stream": false }),
        ],
        "the three 'not provided' spellings take the default; every explicit value survives"
    );
}

/// The three GPT-family rows are the ONLY ones that treat `'none'` as "not provided"
/// (`merge-gpt-6-1-sol.ts:92`, `merge-gpt-6-luna.ts:90`, `token-router-sol.ts:74`).
#[tokio::test]
async fn a_row_that_coerces_none_sends_its_default_instead() {
    let (address, upstream) = provider(vec![
        Reply::Json(StatusCode::OK, completion(1)),
        Reply::Json(StatusCode::OK, completion(1)),
    ])
    .await;

    let spec = row("merge/gpt-6-luna").effort_with_none("medium");
    let client = client_with_pool(spec, &address, keyed(&["k1"]));

    ok_json(
        &client,
        &json!({ "model": "x", "reasoning_effort": "none" }),
    )
    .await;
    ok_json(
        &client,
        &json!({ "model": "x", "reasoning_effort": "high" }),
    )
    .await;

    assert_eq!(
        upstream.sent(),
        vec![
            json!({ "model": "glm-5.3", "reasoning_effort": "medium", "stream": false }),
            json!({ "model": "glm-5.3", "reasoning_effort": "high", "stream": false }),
        ],
        "'none' becomes the default, and any other effort still survives"
    );
    assert!(!row("test/glm-5.3").effort("medium").coerce_none_effort);
    assert!(
        spec.coerce_none_effort,
        "the two spellings differ in exactly this flag"
    );
}

/// A row with no default injects nothing at all: `...body` decides, which means a caller-supplied
/// `null` survives and reaches upstream as JSON null (the TypeScript has no effort ternary on
/// these families).
#[tokio::test]
async fn a_row_without_an_effort_default_injects_nothing() {
    let (address, upstream) = provider(
        (0..3)
            .map(|_| Reply::Json(StatusCode::OK, completion(1)))
            .collect(),
    )
    .await;

    let client = client_with_pool(row("test/glm-5.3"), &address, keyed(&["k1"]));

    for payload in [
        json!({ "model": "x" }),
        json!({ "model": "x", "reasoning_effort": null }),
        json!({ "model": "x", "reasoning_effort": "none" }),
    ] {
        ok_json(&client, &payload).await;
    }

    assert_eq!(
        upstream.sent(),
        vec![
            json!({ "model": "glm-5.3", "stream": false }),
            json!({ "model": "glm-5.3", "reasoning_effort": null, "stream": false }),
            json!({ "model": "glm-5.3", "reasoning_effort": "none", "stream": false }),
        ],
        "no default means no injection - and no deletion either"
    );
}

/// `local-strata.ts:216` overwrites the caller's cap with one eighth of the context window, while
/// every other row leaves the caller's value alone.
#[tokio::test]
async fn a_capped_row_overwrites_the_callers_max_tokens() {
    let (address, upstream) = provider(
        (0..2)
            .map(|_| Reply::Json(StatusCode::OK, completion(1)))
            .collect(),
    )
    .await;

    let capped = client_with_pool(
        row("local/strata").output_cap(OUTPUT_CAP),
        &address,
        vec![Credential::keyless()],
    );
    let plain = client_with_pool(row("test/glm-5.3"), &address, keyed(&["k1"]));

    ok_json(
        &capped,
        &json!({ "model": "x", "max_tokens": 1_024, "stream": false }),
    )
    .await;
    ok_json(
        &plain,
        &json!({ "model": "x", "max_tokens": 1_024, "stream": false }),
    )
    .await;

    assert_eq!(
        upstream.sent(),
        vec![
            json!({ "model": "glm-5.3", "max_tokens": OUTPUT_CAP, "stream": false }),
            json!({ "model": "glm-5.3", "max_tokens": 1_024, "stream": false }),
        ],
        "the relay's constant replaces the caller's cap; an uncapped row keeps it"
    );
}

/// `const stream = Boolean(body.stream)` - the flag becomes a real JSON boolean, which is what
/// makes the injected `stream_options` and the transport's streaming decision agree by
/// construction. The JavaScript truth table includes `[]` (truthy) and `"0"` (truthy: JS tests
/// length, not value).
#[tokio::test]
async fn the_stream_flag_is_normalised_to_a_json_boolean() {
    let (address, upstream) = provider(vec![
        Reply::chunks(vec![content_chunk("a")]),
        Reply::chunks(vec![content_chunk("b")]),
        Reply::chunks(vec![content_chunk("c")]),
        Reply::Json(StatusCode::OK, completion(1)),
        Reply::Json(StatusCode::OK, completion(2)),
        Reply::Json(StatusCode::OK, completion(3)),
    ])
    .await;

    let client = client_with_pool(row("test/glm-5.3"), &address, keyed(&["k1"]));

    // (label, the caller's `stream` spelling, JavaScript's verdict on it)
    let cases: Vec<(&str, Value, bool)> = vec![
        ("number one", json!(1), true),
        ("non-empty string", json!("x"), true),
        ("empty array", json!([]), true),
        ("number zero", json!(0), false),
        ("empty string", json!(""), false),
        ("null", json!(null), false),
    ];

    for (label, flag, truthy) in cases {
        let payload = json!({ "model": "x", "stream": flag });
        let answer = client
            .create(&payload)
            .await
            .unwrap_or_else(|failure| panic!("{label} must reach the upstream head: {failure:?}"));

        // The payload's boolean and the transport's decision must match - that is the whole point
        // of normalising before sending.
        if truthy {
            assert!(answer.is_stream(), "{label} is truthy, so the body streams");
        } else {
            assert!(
                answer.is_response(),
                "{label} is falsy, so the body settles"
            );
        }
    }

    assert_eq!(
        upstream
            .sent()
            .into_iter()
            .map(|sent| sent["stream"].clone())
            .collect::<Vec<Value>>(),
        vec![
            json!(true),
            json!(true),
            json!(true),
            json!(false),
            json!(false),
            json!(false)
        ],
        "every spelling becomes a JSON boolean, in the same truth order the transport used"
    );
}

/// `accountUpstreamCall:707` passes the CALLER's body to `recordRequestTokens`, not the pinned
/// payload, so the prompt estimate describes what the caller sent - while the bytes on the wire
/// are the rewritten ones. The estimate is `serialized length / 4` in UTF-16 units
/// (`model-class.ts:82-85`), which is why the two figures below cannot coincide.
#[tokio::test]
async fn the_prompt_estimate_describes_the_callers_body_not_the_payload() {
    let (address, upstream) = provider(vec![Reply::Json(StatusCode::OK, completion(0))]).await;

    // The row's model is 7 characters, the caller's is 20, so the two serializations differ by
    // exactly 13 characters and their estimates differ by 3 tokens.
    let spec = ClientSpec::plain(
        "test/short",
        "Test",
        "glm-5.3",
        "test",
        "test_api_keys",
        "test_api_endpoints",
    );
    let client = client_with_pool(spec, &address, keyed(&["k1"]));

    let payload = json!({
        "model": "0123456789abcdefghij",
        "messages": [{ "role": "user", "content": "hello" }],
        "temperature": 0.5,
        // Pinned into the CALLER's body so the only difference between the two serializations is
        // the model swap: the rewrite's normalised `stream` cannot move the arithmetic.
        "stream": false
    });
    let wire = serde_json::to_string(&payload).expect("a test body always serializes");

    assert_eq!(
        wire.len(),
        112,
        "the pinned serialization length, so the estimate below is an exact figure rather than a self-referential one"
    );
    assert_eq!(
        estimate_request_tokens(&payload),
        28,
        "112 UTF-16 code units / 4, exactly"
    );

    ok_json(&client, &payload).await;

    let counters = counters_of(&client);
    let sent = upstream.sent()[0].clone();

    assert_eq!(
        serde_json::to_string(&sent).expect("serializes").len(),
        99,
        "the wire body is 13 characters shorter: the row's 7-character model replaced the caller's 20-character one"
    );
    assert_eq!(
        estimate_request_tokens(&sent),
        25,
        "the rewritten payload would have estimated 3 tokens lower - and 99/4 shows the CEILING: \
         model-class.ts:82-85 rounds up, so a partial 4-character group still costs a token"
    );
    assert_eq!(
        counters.input_token_total, 28,
        "the counter holds the CALLER's estimate, exactly once"
    );
    assert_eq!(
        counters.cache_token_total, 0,
        "the cache split is not part of this path"
    );
    assert_eq!(
        counters.measured_prefill_token_total, 0,
        "a settled response records no phase pair"
    );
}

// ---------------------------------------------------------------------------
// Credentials, headers, and the two drain shapes
// ---------------------------------------------------------------------------

/// The credential-less relays pin `defaultHeaders: { Authorization: null }`
/// (`local-strata.ts:161`, `local-basalt.ts:147`), so they send NO `Authorization` header - while
/// every OTHER keyless selection sends `Bearer none`, which is what `openai@6.42.0` really sends
/// for `apiKey: 'none'`.
#[tokio::test]
async fn a_keyless_relay_sends_no_header_while_a_keyless_pool_sends_bearer_none() {
    let relay_spec = row("local/strata").keyless_relay();
    let pool_spec = row("test/glm-5.3");

    let (relay_address, relay_upstream) =
        provider(vec![Reply::Json(StatusCode::OK, completion(1))]).await;
    let (pool_address, pool_upstream) =
        provider(vec![Reply::Json(StatusCode::OK, completion(1))]).await;

    let relay = client_on(
        relay_spec,
        vec![Endpoint::labeled(
            &format!("http://{relay_address}"),
            "Carl",
        )],
        vec![],
    );
    let pooled = client_with_pool(pool_spec, &pool_address, vec![Credential::keyless()]);

    assert!(
        ok_json(&relay, &json!({ "model": "x", "stream": false }))
            .await
            .is_object()
    );
    assert!(
        ok_json(&pooled, &json!({ "model": "x", "stream": false }))
            .await
            .is_object()
    );

    assert_eq!(
        relay_upstream.seen()[0].header("authorization"),
        None,
        "the relay sends no Authorization at all"
    );
    assert_eq!(
        pool_upstream.seen()[0].header("authorization"),
        Some("Bearer none"),
        "a keyless pool entry still sends the SDK's `Bearer none`"
    );
    assert_eq!(
        relay_spec.keyless_header,
        KeylessHeaderPolicy::Suppress,
        "the relay row is the Suppress case by declaration"
    );
    assert_eq!(
        pool_spec.keyless_header,
        KeylessHeaderPolicy::SendNone,
        "and every other row is SendNone"
    );
}

/// A relay row publishes the endpoint's vault NAME as `upstreamLabel`; no other row does, because
/// `private-chat-completion.ts` would then print `STANDARD: <id>` instead of `model: <id>` for
/// every non-local provider.
#[tokio::test]
async fn only_a_relay_row_publishes_its_endpoint_label() {
    let (relay_address, _) = provider(vec![Reply::Json(StatusCode::OK, completion(1))]).await;
    let (plain_address, plain_upstream) =
        provider(vec![Reply::Json(StatusCode::OK, completion(1))]).await;

    let relay = client_on(
        row("local/strata").keyless_relay(),
        vec![Endpoint::labeled(
            &format!("http://{relay_address}"),
            "Carl",
        )],
        vec![],
    );

    // The non-relay row's pool is built with `Endpoint::new` (label DROPPED) by `registry.rs`, so
    // even a label-carrying URL must not surface here.
    let plain = client_on(
        row("test/glm-5.3"),
        vec![Endpoint::labeled(
            &format!("http://{plain_address}"),
            "STANDARD",
        )],
        keyed(&["k1"]),
    );

    assert_eq!(relay.upstream_label().as_deref(), Some("Carl"));
    assert_eq!(
        plain.upstream_label(),
        None,
        "a non-relay row never publishes an endpoint label"
    );
    assert_eq!(
        plain.base_url(),
        Some(format!("http://{plain_address}/v1")),
        "and its base URL is the rebased endpoint, which is what the route reports as `client.baseURL`"
    );

    ok_json(&plain, &json!({ "model": "x", "stream": false })).await;
    assert_eq!(
        plain_upstream.seen()[0].path,
        "/v1/chat/completions",
        "the labeled URL still dials the same path"
    );
}

/// The Nvidia drain shape: `getNewKey()` SHIFTS the queue and returns the NEW head, so every
/// rotation is a different credential, and the pool runs out after `len` rotations
/// (`nvidia-glm5-flash.ts:80-85`).
#[tokio::test]
async fn a_draining_pool_sends_every_credential_once() {
    let not_found = |id: &str| {
        Reply::Error(
            StatusCode::NOT_FOUND,
            json!({ "error": { "message": format!("no model {id}") } }),
        )
    };

    let (address, upstream) = provider(vec![not_found("a"), not_found("b"), not_found("c")]).await;

    let spec = row("nvidia/glm-5.3").drained(CredentialAdvance::NextOnEveryRotation);
    let client = client_with_pool(spec, &address, keyed(&["k1", "k2", "k3"]));

    assert!(client.is_discoverable(), "a healthy row is discoverable");

    let failure = client
        .create(&json!({ "model": "x", "stream": false }))
        .await
        .expect_err("every relay in the pool answers 404");

    assert_eq!(
        upstream.authorizations(),
        vec!["Bearer k1", "Bearer k2", "Bearer k3"],
        "rotation order is pool order, and no credential repeats"
    );
    assert_eq!(
        upstream.attempts(),
        3,
        "the pool length bounds the attempts"
    );
    assert_eq!(
        failure.status(),
        Some(StatusCode::NOT_FOUND),
        "the raw 404 propagates - the route turns it into a 503"
    );

    let counters = counters_of(&client);

    assert_eq!(
        counters.request_total, 1,
        "one logical request, however many rotations"
    );
    assert_eq!(counters.attempt_total, 3, "one attempt per issue");
    assert_eq!(counters.consecutive_failures, 1);
    assert!(
        !client.is_discoverable(),
        "a drained pool hides the row (A5)"
    );
}

/// The Makora drain shape is the OTHER one: `getNewKey()` returns the entry it shifted OFF, so the
/// first rotation re-sends the credential the cursor already holds, and only the second moves on
/// (`makora-glm-5-3.ts:80-88`). A one-key pool therefore wastes exactly one retry before draining.
#[tokio::test]
async fn a_makora_pool_re_sends_its_head_before_it_advances() {
    let not_found = || {
        Reply::Error(
            StatusCode::NOT_FOUND,
            json!({ "error": { "message": "no model" } }),
        )
    };

    // Two credentials: k1, k1, k2.
    let (address, upstream) = provider(vec![not_found(), not_found(), not_found()]).await;
    let spec = row("makora/glm-5.3").drained(CredentialAdvance::SameThenNext);
    let two = client_with_pool(spec, &address, keyed(&["k1", "k2"]));

    two.create(&json!({ "model": "x", "stream": false }))
        .await
        .expect_err("the pool runs out");

    assert_eq!(
        upstream.authorizations(),
        vec!["Bearer k1", "Bearer k1", "Bearer k2"],
        "the one-step lag the family's getNewKey shape produces"
    );
    assert_eq!(upstream.attempts(), 3);

    // One credential (the live Makora pool has exactly one valid entry): k1, k1, then drain.
    let (small_address, small_upstream) = provider(vec![not_found(), not_found()]).await;
    let one = client_with_pool(spec, &small_address, keyed(&["solo"]));

    one.create(&json!({ "model": "x", "stream": false }))
        .await
        .expect_err("one rotation, then the drain");

    assert_eq!(
        small_upstream.authorizations(),
        vec!["Bearer solo", "Bearer solo"],
        "a one-key pool retries once on the SAME key and then drains"
    );
    assert_eq!(small_upstream.attempts(), 2);
    assert!(!one.is_discoverable());
}

/// After the drain, the row is HIDDEN but still ROUTABLE, and - the recorded deviation from the
/// TypeScript - it keeps sending its LAST credential instead of dropping the header.
#[tokio::test]
async fn a_drained_row_stays_routable_and_keeps_its_last_credential() {
    let (address, upstream) = provider(vec![
        Reply::Error(
            StatusCode::NOT_FOUND,
            json!({ "error": { "message": "no model" } }),
        ),
        Reply::Json(StatusCode::OK, completion(6)),
    ])
    .await;

    let spec = row("nvidia/deepseek-v4-flash").drained(CredentialAdvance::NextOnEveryRotation);
    let client = client_with_pool(spec, &address, keyed(&["only"]));

    client
        .create(&json!({ "model": "x", "stream": false }))
        .await
        .expect_err("the pool is one entry long, so the first 404 drains it");

    assert!(!client.is_discoverable(), "drained => hidden");
    assert!(client.enabled(), "but `enabled` itself is untouched (A5)");

    // The row still serves traffic, and the exhaustion fallback in `src/transport.rs` keeps the
    // last credential in force.
    ok_json(&client, &json!({ "model": "x", "stream": false })).await;

    assert_eq!(
        upstream.authorizations(),
        vec!["Bearer only", "Bearer only"],
        "DEVIATION (recorded): the TypeScript sends no header after `apiKey = ''`; this port re-sends the last credential"
    );

    let counters = counters_of(&client);

    assert_eq!(counters.request_total, 2, "both logical requests counted");
    assert_eq!(counters.attempt_total, 2);
    assert_eq!(
        counters.output_token_total, 6,
        "the surviving request folded its usage"
    );
    assert_eq!(
        counters.consecutive_failures, 0,
        "the success cleared the failure streak"
    );
}

/// A `ChatPlain` row has no failover axis at all: one attempt, the pool head, and the 404
/// propagates (`override switchBaseUrl() { return false }` in every merge/vultr/sail/token-router
/// file).
#[tokio::test]
async fn a_plain_row_never_rotates() {
    let (address, upstream) = provider(vec![Reply::Error(
        StatusCode::NOT_FOUND,
        json!({ "error": { "message": "no model" } }),
    )])
    .await;

    let client = client_with_pool(row("merge/glm-5.3"), &address, keyed(&["k1", "k2"]));

    let failure = client
        .create(&json!({ "model": "x", "stream": false }))
        .await
        .expect_err("404");

    assert!(!client.switch_base_url(), "a plain row refuses to rotate");
    assert_eq!(
        upstream.attempts(),
        1,
        "the transport made exactly one attempt"
    );
    assert_eq!(
        upstream.authorizations(),
        vec!["Bearer k1"],
        "and it used the pool head"
    );
    assert_eq!(failure.status(), Some(StatusCode::NOT_FOUND));

    let counters = counters_of(&client);

    assert_eq!(counters.request_total, 1);
    assert_eq!(counters.attempt_total, 1);
    assert_eq!(counters.consecutive_failures, 1);
}

/// The relay axis: each 404 dials the NEXT endpoint in pool order, and the row refuses only when
/// the pool is exhausted (`local-strata.ts:243-261`).
#[tokio::test]
async fn a_relay_row_rotates_through_every_endpoint_before_it_refuses() {
    let not_found = || {
        Reply::Error(
            StatusCode::NOT_FOUND,
            json!({ "error": { "message": "no route" } }),
        )
    };

    let (address, upstream) = provider(vec![not_found(), not_found(), not_found()]).await;

    // Three DIFFERENT base URLs on one listener, so the captured request path proves which relay
    // each attempt dialed.
    let endpoints = ["relay-a", "relay-b", "relay-c"]
        .iter()
        .map(|name| Endpoint::labeled(&format!("http://{address}/{name}"), name))
        .collect();

    let client = client_on(row("local/strata").keyless_relay(), endpoints, vec![]);

    let failure = client
        .create(&json!({ "model": "x", "stream": false }))
        .await
        .expect_err("every relay answers 404");

    assert_eq!(
        upstream
            .seen()
            .into_iter()
            .map(|attempt| attempt.path)
            .collect::<Vec<String>>(),
        vec![
            "/relay-a/v1/chat/completions",
            "/relay-b/v1/chat/completions",
            "/relay-c/v1/chat/completions"
        ],
        "endpoint rotation order is pool order, never re-sorted"
    );
    assert_eq!(
        upstream.authorizations(),
        vec!["<none>", "<none>", "<none>"],
        "the relay axis never invents a credential"
    );
    assert_eq!(failure.status(), Some(StatusCode::NOT_FOUND));
    assert_eq!(
        client.upstream_label().as_deref(),
        Some("relay-c"),
        "the label read after the rotations is the LAST relay - the route captures it at the response head"
    );

    let counters = counters_of(&client);

    assert_eq!(counters.request_total, 1);
    assert_eq!(counters.attempt_total, 3);
    assert_eq!(counters.consecutive_failures, 1);
    assert!(
        client.is_discoverable(),
        "a credential-less relay stays discoverable even with no working relay"
    );
}

/// The open_router case: the pool head classifies `invalid`, so the row is HIDDEN while it keeps
/// sending `Bearer NotAvailable` and stays routable (`model-class.test.ts:288-296`).
#[tokio::test]
async fn an_invalid_pool_head_is_still_the_head_on_the_wire() {
    let (address, upstream) = provider(vec![Reply::Json(StatusCode::OK, completion(2))]).await;

    let credentials = pool_from(&vault_doc(
        r#"[
            { "label": "Default", "value": "NotAvailable", "validity": "invalid" },
            { "label": "Backup", "value": "sk-backup-backup-backup", "validity": "valid" }
        ]"#,
        "NotAvailable",
    ));

    assert_eq!(credentials.len(), 2, "document order is preserved");

    let client = client_with_pool(row("open-router/glm-5.3-flash"), &address, credentials);

    ok_json(&client, &json!({ "model": "x", "stream": false })).await;

    assert_eq!(
        upstream.seen()[0].header("authorization"),
        Some("Bearer NotAvailable"),
        "the advisory validity does NOT gate routing - the pool head is sent verbatim"
    );
    assert_eq!(
        upstream.seen()[0].path,
        "/v1/chat/completions",
        "and the request still reached the model"
    );
    assert!(
        !client.is_discoverable(),
        "while an invalid head hides the row from /models"
    );
    assert!(
        client.enabled(),
        "`enabled` is the declared flag, not the credential state"
    );
}

// ---------------------------------------------------------------------------
// Settlement (A3)
// ---------------------------------------------------------------------------

/// A streaming success settles EXACTLY ONCE: usage, elapsed, and the phase split fold on the
/// terminal item, and nothing folds afterwards.
#[tokio::test]
async fn a_streamed_success_settles_exactly_once() {
    // One 5ms pause per frame: every clock figure below then has a floor the test can state,
    // instead of depending on how fast loopback happened to be.
    let (address, _upstream) = provider(vec![Reply::paced_chunks(
        vec![content_chunk("hel"), content_chunk("lo"), usage_chunk(21)],
        Duration::from_millis(5),
    )])
    .await;

    let client = client_with_pool(row("test/glm-5.3"), &address, keyed(&["k1"]));
    let payload = json!({ "model": "x", "stream": true });
    let estimate = estimate_request_tokens(&payload);

    let answer = client.create(&payload).await.expect("the head is 200");
    let mut stream = answer.into_stream().expect("the body is a stream");
    let (chunks, failure) = collect(&mut stream).await;

    assert_eq!(
        chunks.len(),
        3,
        "every frame is delivered before the [DONE] ends the stream"
    );
    assert_eq!(failure, None, "a clean stream has no terminal failure");

    let counters = counters_of(&client);

    assert_eq!(counters.request_total, 1);
    assert_eq!(counters.attempt_total, 1);
    assert_eq!(
        counters.consecutive_failures, 0,
        "a completed stream is a success"
    );
    assert_eq!(counters.input_token_total, estimate);
    assert_eq!(
        counters.output_token_total, 21,
        "usage is added verbatim (model-class.ts:869)"
    );
    assert_eq!(
        counters.measured_prefill_token_total, estimate,
        "the prefill numerator is the request estimate"
    );
    assert_eq!(
        counters.measured_decode_token_total, 21,
        "and the decode numerator is REPORTED usage"
    );

    // The three clock figures are derived from three instants, so the exact assertion available
    // here is the documented containment: the client's span starts before the transport's and ends
    // after both phase spans.
    assert!(
        counters.millisecond_total
            >= counters.ttft_millisecond_total + counters.generation_millisecond_total,
        "the total contains both phases: {:?}",
        (
            counters.millisecond_total,
            counters.ttft_millisecond_total,
            counters.generation_millisecond_total
        )
    );
    assert!(
        counters.measured_decode_millisecond_total >= 5,
        "the usage-known stream recorded its generation span"
    );

    let after = counters_after_settlement(&client).await;

    assert_eq!(after, zeroed(), "nothing folded a second time: {after:?}");
}

/// A non-streaming response folds usage and elapsed but NO phase split - the rule the
/// TypeScript's `accountUpstreamCall` enforces by only calling `trackStream` on a stream
/// (`model-class.ts:724-730`).
#[tokio::test]
async fn a_settled_response_records_no_phase_split() {
    let (address, _upstream) = provider(vec![Reply::Json(StatusCode::OK, completion(9))]).await;

    let client = client_with_pool(row("test/glm-5.3"), &address, keyed(&["k1"]));

    ok_json(&client, &json!({ "model": "x", "stream": false })).await;

    let counters = counters_of(&client);

    assert_eq!(counters.request_total, 1);
    assert_eq!(counters.attempt_total, 1);
    assert_eq!(counters.consecutive_failures, 0);
    assert_eq!(counters.output_token_total, 9);
    assert_eq!(
        counters.ttft_millisecond_total, 0,
        "a settled response records no time-to-first-token"
    );
    assert_eq!(
        counters.generation_millisecond_total, 0,
        "and no generation span"
    );
    assert_eq!(
        counters.measured_prefill_token_total, 0,
        "so neither side of the prefill pair moves"
    );
    assert_eq!(counters.measured_decode_token_total, 0);
    assert_eq!(counters.measured_decode_millisecond_total, 0);
}

/// An undecodable frame ends the stream as a FAILURE, and the usage an earlier frame reported is
/// still folded exactly once (`model-class.ts:862-879`'s `finally`).
#[tokio::test]
async fn an_undecodable_frame_settles_the_stream_as_a_failure_once() {
    let frames = vec![
        data_frame(&content_chunk("hel")).into_bytes(),
        data_frame(&usage_chunk(7)).into_bytes(),
        b"data: this is not json\n\n".to_vec(),
    ];

    let (address, _upstream) = provider(vec![Reply::Sse {
        frames,
        gap: Some(Duration::from_millis(5)),
    }])
    .await;

    let client = client_with_pool(row("test/glm-5.3"), &address, keyed(&["k1"]));

    let answer = client
        .create(&json!({ "model": "x", "stream": true }))
        .await
        .expect("the head is 200");
    let mut stream = answer.into_stream().expect("the body is a stream");
    let (chunks, failure) = collect(&mut stream).await;

    assert_eq!(chunks.len(), 2, "both good frames are delivered first");
    let failure = failure.expect("the third frame is a decode failure");

    // The wire verdict is what a consumer branches on (`harness`'s supervisor contract), and the
    // recorded deviation: an undecodable frame is FATAL here, because this port's decoder is the
    // component that can no longer continue.
    assert_eq!(failure.code().as_str(), "decode");
    assert!(failure.is_fatal());

    let counters = counters_of(&client);

    assert_eq!(counters.request_total, 1);
    assert_eq!(counters.attempt_total, 1);
    assert_eq!(
        counters.consecutive_failures, 1,
        "a failed stream is a failure"
    );
    assert_eq!(
        counters.output_token_total, 7,
        "usage reported BEFORE the bad frame still counts"
    );
    assert_eq!(
        counters.measured_decode_token_total, 7,
        "and it is still usage-KNOWN"
    );
    assert!(
        counters.ttft_millisecond_total >= 5,
        "the first generated payload was observed"
    );

    let after = counters_after_settlement(&client).await;

    assert_eq!(after, zeroed(), "the failure settled once: {after:?}");
}

/// A consumer that walks away mid-stream is NOT a provider failure, and its abandonment settles
/// exactly once through the stream's `Drop` (the TypeScript's `finally`,
/// `model-class.ts:862-879`).
#[tokio::test]
async fn an_abandoned_stream_settles_once_and_is_not_a_failure() {
    let (address, _upstream) = provider(vec![Reply::paced_chunks(
        vec![content_chunk("a"), content_chunk("b"), content_chunk("c")],
        Duration::from_millis(20),
    )])
    .await;

    let client = client_with_pool(row("test/glm-5.3"), &address, keyed(&["k1"]));

    let answer = client
        .create(&json!({ "model": "x", "stream": true }))
        .await
        .expect("the head is 200");

    {
        let mut stream = answer.into_stream().expect("the body is a stream");

        // Reading one frame first puts a known 20ms inside the span, so the settlement's elapsed
        // figure is a floor the test can state instead of a race against loopback.
        assert!(
            stream
                .next()
                .await
                .expect("the first frame arrives")
                .is_ok(),
            "the consumer read one frame and then walked away"
        );

        drop(stream);
    }

    let counters = counters_of(&client);

    assert_eq!(counters.request_total, 1);
    assert_eq!(counters.attempt_total, 1);
    assert_eq!(
        counters.consecutive_failures, 0,
        "abandonment is neither a success nor a failure - a disconnect must not trip the row's failure signal"
    );
    assert_eq!(
        counters.output_token_total, 0,
        "no usage frame was absorbed, so none is invented"
    );
    assert!(
        counters.millisecond_total >= 20,
        "the abandonment itself settled: {counters:?}"
    );
    assert!(
        counters.ttft_millisecond_total >= 20,
        "the observed first payload sits inside the abandoned span"
    );
    assert_eq!(
        counters.measured_decode_token_total, 0,
        "an abandoned stream has no settled usage, so the decode pair stays empty"
    );

    let after = counters_after_settlement(&client).await;

    assert_eq!(after, zeroed(), "the Drop settled exactly once: {after:?}");
}

/// A cancelled stream settles as a failure, once - the cancellation path the TypeScript renders
/// as `APIUserAbortError`.
#[tokio::test]
async fn a_cancelled_stream_settles_once_as_a_failure() {
    let (address, _upstream) = provider(vec![Reply::paced_chunks(
        vec![content_chunk("a"), content_chunk("b"), content_chunk("c")],
        Duration::from_millis(30),
    )])
    .await;

    let client = client_with_pool(row("test/glm-5.3"), &address, keyed(&["k1"]));
    let signal = CancellationToken::new();

    let answer = client
        .create_with_signal(&json!({ "model": "x", "stream": true }), signal.clone())
        .await
        .expect("the head is 200");

    let mut stream = answer.into_stream().expect("the body is a stream");

    assert!(
        stream
            .next()
            .await
            .expect("the first frame arrives")
            .is_ok(),
        "the stream produced one frame before the caller cancelled"
    );

    signal.cancel();

    // The terminal item is the abort verdict; draining it is what runs the settlement.
    while let Some(item) = stream.next().await {
        if item.is_err() {
            break;
        }
    }

    let counters = counters_of(&client);

    assert_eq!(counters.request_total, 1);
    assert_eq!(counters.attempt_total, 1);
    assert_eq!(
        counters.consecutive_failures, 1,
        "a cancelled request counts as a failure, exactly once"
    );

    let after = counters_after_settlement(&client).await;

    assert_eq!(after, zeroed(), "the cancellation settled once: {after:?}");
}

/// A 429 is retried INSIDE the transport, so the client sees one attempt - and the pause it waited
/// through shows up in the elapsed total. `attempt_total` is a port addition defined as
/// client-visible issues, which this test pins.
#[tokio::test]
async fn a_rate_limited_upstream_retries_inside_the_transport() {
    let pause = Duration::from_millis(40);

    let (address, upstream) = provider(vec![
        Reply::Error(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "error": { "message": "slow down", "type": "rate_limit_exceeded" } }),
        ),
        Reply::Json(StatusCode::OK, completion(5)),
    ])
    .await;

    let spec = row("test/glm-5.3");
    let client = client_on_with(
        spec,
        single_endpoint(&address),
        keyed(&["k1"]),
        Policy::standard().with_rate_limit_pause(pause),
    );

    ok_json(&client, &json!({ "model": "x", "stream": false })).await;

    assert_eq!(upstream.attempts(), 2, "the transport made both attempts");

    let counters = counters_of(&client);

    assert_eq!(counters.request_total, 1);
    assert_eq!(
        counters.attempt_total, 1,
        "the client layer saw ONE issue: the retry is the transport's"
    );
    assert_eq!(counters.consecutive_failures, 0, "the retry succeeded");
    assert_eq!(counters.output_token_total, 5);
    assert!(
        counters.millisecond_total >= 40,
        "the 429 pause is inside the elapsed figure: {}",
        counters.millisecond_total
    );
}

// ---------------------------------------------------------------------------
// Discoverability and diagnostics
// ---------------------------------------------------------------------------

/// The validity matrix of `model-class.test.ts:224,288-296,301,308`, expressed through the pool
/// the row actually carries.
#[tokio::test]
async fn the_discoverability_table_matches_the_typescript_validity_matrix() {
    // (case, the pool entry's value, its vault validity, whether `isDiscoverable` may be true)
    let cases: Vec<(&str, &str, &str, bool)> = vec![
        ("valid", "sk-head-head-head", "valid", true),
        ("keyless", "none", "keyless", true),
        (
            "placeholder",
            "EXAMPLE_API_KEY_PLACEHOLDER",
            "placeholder",
            false,
        ),
        ("invalid", "NotAvailable", "invalid", false),
        ("empty", "   ", "empty", false),
    ];

    for (label, value, validity, want) in cases {
        let entries =
            format!(r#"[{{ "label": "Head", "value": "{value}", "validity": "{validity}" }}]"#,);
        let credentials = pool_from(&vault_doc(&entries, value));

        assert_eq!(credentials.len(), 1, "one pool entry per case ({label})");

        let client = ModelClient::new(
            row("test/glm-5.3"),
            Transport::with_http_client(
                // No listener is needed: discoverability is a read of the pool, never a request.
                Target::new(vec![Endpoint::new("http://127.0.0.1:1")], credentials)
                    .keyless(false)
                    .keyless_header(KeylessHeaderPolicy::SendNone),
                Policy::standard(),
                http_client(),
            ),
        );

        assert_eq!(client.is_discoverable(), want, "{label} credential");
    }

    // `enabled:false` hides a row whose credential is perfectly valid (the 18 hidden rows).
    let (address, _) = provider(vec![]).await;
    let hidden = client_with_pool(
        row("makora/glm5-flash").hidden(),
        &address,
        pool_from(&vault_doc(
            r#"[{ "label": "Head", "value": "sk-head-head-head", "validity": "valid" }]"#,
            "sk-head-head-head",
        )),
    );

    assert!(!hidden.enabled());
    assert!(
        !hidden.is_discoverable(),
        "a hidden row is undiscoverable even with a valid key"
    );

    // A row with NO credential slot is a deliberate no-credential upstream: valid
    // (`local-strata.ts:156`, `model-class.test.ts:301,308`).
    let relay = client_on(
        row("local/strata").keyless_relay(),
        vec![Endpoint::labeled(&format!("http://{address}"), "Carl")],
        vec![],
    );

    assert!(relay.is_discoverable(), "a keyless relay is discoverable");
}

/// A credential value must not reach the failure vocabulary, the Debug of the failure, or the
/// Debug of the client - even when the UPSTREAM echoes it back in an error body.
#[tokio::test]
async fn no_credential_value_reaches_the_failure_vocabulary_or_the_debug_output() {
    let echo = json!({
        "error": {
            "message": format!("invalid api key {SECRET}"),
            "type": "invalid_request_error",
            "code": "invalid_api_key"
        }
    });

    let (address, _upstream) = provider(vec![Reply::Error(StatusCode::BAD_REQUEST, echo)]).await;

    let client = client_with_pool(row("test/glm-5.3"), &address, keyed(&[SECRET]));

    let failure: TransportFailure = client
        .create(&json!({ "model": "x", "stream": false }))
        .await
        .expect_err("400 is not retryable");

    for surface in [
        failure.message(),
        format!("{failure:?}"),
        format!("{client:?}"),
    ] {
        assert!(
            !surface.contains(SECRET),
            "a credential value leaked into a diagnostic: {surface}"
        );
    }

    assert_eq!(failure.status(), Some(StatusCode::BAD_REQUEST));
}

/// `upstreamLabel` is user-visible upstream configuration, so a hostile label is refused rather
/// than logged. The allow-list must keep the REAL vault labels (A6) while rejecting
/// credential-shaped and control-character ones.
#[test]
fn a_usable_upstream_label_is_accepted_and_a_hostile_one_is_not() {
    // The labels the live artifact actually publishes for the two relay slots, copied from
    // `config/vault/generated/agentic-provider.json` (`vendors[strata].local_strata_api_endpoints`
    // and `vendors[basalt].local_basalt_api_endpoints`). The same lists are re-read from the
    // artifact itself in `tests/registry.rs`, so a label that stops being printable fails in both
    // places: the allow-list here, and the pool build there.
    const REAL: &[&str] = &[
        "Carl",
        "Candy",
        "Markton",
        "James",
        "Parma",
        "DevonNotebook",
        "Nqqtran",
        "FranSoullin",
        "MolabEndless",
        "MarktonWillonLatest",
        "FamialPhotoFast",
        "DracoDravenFast",
        "DrakeJohnsonFast",
        "CornethOneThreeFast",
        "LandarMcTyreFast",
        // The name every non-local endpoint slot files; not published as a log label, but a
        // label this layer would print if a relay row ever used one of those slots.
        "STANDARD",
    ];

    // The rule is a port addition (`model-class.ts:337-350` is prose only), so both directions are
    // pinned here: what the deployment sends, and what an attacker could smuggle through a vault
    // label.
    const HOSTILE: &[&str] = &[
        "",
        "   ",
        "a:b",
        "line\nbreak",
        "tab\tsep",
        "sk-abcdef123456",
        "NotAvailable",
        "a_key_with_digits_1",
        "\u{1b}[31mred\u{1b}[0m",
    ];

    for label in REAL {
        assert!(
            upstream_label_is_safe(label),
            "the deployment's own label {label} must be accepted"
        );
    }

    for label in HOSTILE {
        assert!(!upstream_label_is_safe(label), "{label:?} must be refused");
    }
}

/// The row vocabulary itself: `ClientSpec::plain` is the base shape every builder modifies, and
/// the builders compose without touching each other's fields. Pinned so a new row cannot silently
/// change what `plain` means for the other 28.
#[test]
fn the_row_builders_change_exactly_their_own_field() {
    let base = row("test/glm-5.3");

    assert_eq!(
        base,
        ClientSpec {
            id: "test/glm-5.3",
            provider: "Test",
            model: "glm-5.3",
            vault_vendor: "test",
            credential_slot: Some("test_api_keys"),
            endpoint_slot: "test_api_endpoints",
            pricing: Pricing::free(),
            enabled: true,
            requires_usage_option: true,
            forced_max_tokens: None,
            default_reasoning_effort: None,
            coerce_none_effort: false,
            keyless_header: KeylessHeaderPolicy::SendNone,
            behavior: Behavior::ChatPlain,
            context_length: 262_144,
        },
        "`plain` is the base shape, spelled out once"
    );

    // One builder per field, each asserted as a one-field diff against the base.
    let routed = base.routed(RoutingVendor::Modal);
    assert_eq!(
        routed,
        ClientSpec {
            behavior: Behavior::ChatRouted {
                vendor: RoutingVendor::Modal
            },
            ..base
        }
    );

    let drained = base.drained(CredentialAdvance::SameThenNext);
    assert_eq!(
        drained,
        ClientSpec {
            behavior: Behavior::ChatCredentialDrain {
                advance: CredentialAdvance::SameThenNext
            },
            ..base
        }
    );

    let relay = row("local/basalt").keyless_relay();
    assert_eq!(
        relay,
        ClientSpec {
            credential_slot: None,
            keyless_header: KeylessHeaderPolicy::Suppress,
            behavior: Behavior::ChatKeylessRelay,
            ..row("local/basalt")
        },
        "a relay row drops the credential slot AND suppresses the header, and changes nothing else"
    );

    let shaped = base
        .priced(1.25, 0.125, 2.5)
        .hidden()
        .effort("low")
        .output_cap(OUTPUT_CAP)
        .without_usage_option();

    assert_eq!(
        shaped,
        ClientSpec {
            pricing: Pricing::per_million(1.25, 0.125, 2.5),
            enabled: false,
            default_reasoning_effort: Some("low"),
            forced_max_tokens: Some(OUTPUT_CAP),
            requires_usage_option: false,
            ..base
        },
        "pricing, visibility and request shaping are independent axes"
    );
    assert_eq!(base.effort_with_none("low"), {
        ClientSpec {
            default_reasoning_effort: Some("low"),
            coerce_none_effort: true,
            ..base
        }
    });
}
