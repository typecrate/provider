//! Behavioural tests for the outbound OpenAI-compatible transport (`src/transport.rs`), its SSE
//! codec (`src/sse.rs`) and its failure vocabulary (`src/error.rs`).
//!
//! Every upstream here is a REAL in-process listener on `127.0.0.1:0` - the scripted-provider
//! pattern of `packages/typecrate/harness/tests/transport.rs:117-143` - so these tests exercise
//! reqwest's header writer, hyper's framing, and body disposal, none of which a mock can show.
//! Nothing leaves the loopback interface, no port is fixed, and no test waits on anything except
//! the pause it configures itself.
//!
//! The specification each test names is the TypeScript original:
//!   * `packages/agentic/provider/src/model-class.ts` - the request wrapper and retry loop
//!     (`:557-654`), the `stream_options` opt-in (per client: `nvidia/nvidia-glm5-flash.ts:76`, `local/local-strata.ts:233` — `model-class.ts` has none), the 404 gate (`:634-636`), the
//!     credential gate (`:28-44`), `hasGeneratedPayload` (`:206-241`), `estimateRequestTokens`
//!     (`:82-85`), `trackStream` (`:831-879`).
//!   * `runtime/endpoint/provider/private/responses/private-responses.ts` - `is404` (`:25-27`),
//!     the rotation loop and its terminal message (stream loop `:106-121`, settled loop `:133-147`, terminal throws `:118` and `:144`).
//!   * `runtime/endpoint/provider/private/chat-completion/private-chat-completion.ts` - the emitted
//!     wire format (`:161-186`).
//!   * `packages/underload/service/src/server/start.ts` - the code set (`:318-338`), redaction and
//!     flattening (`:421-466`), the errno tables (`:352-419`), `classifyStreamError` (`:581-621`),
//!     the frame sentences and wire key order (`:635-705`), emission (`:860-873`).
//!   * `node_modules/openai/core/error.js:21-38` (`makeMessage`) and `:72-93` (the abort,
//!     connection and timeout sentences) - the messages a consumer string-matches.
//!
//! WHY THE UPSTREAM IS AXUM (a normal dependency now, and with DEFAULT features). The route task
//! needs the answer to "does the SSE response need axum's `sse` feature?" -
//! `axum_sse_response_needs_no_sse_feature` proves it does not: the scripted SSE answer is built
//! from `axum::body::Body::from_stream` plus explicit headers, both of which the default feature
//! set already provides, and the transport consumes it end to end. `src/serve.rs` has since become
//! an axum handler itself, which is why the dependency moved out of `[dev-dependencies]`; the
//! feature set is the answer this test still owns.

use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    http::{StatusCode, header},
    response::Response,
    routing::any,
};
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use std::collections::BTreeSet;
use typecrate_provider::{
    error::{
        ABORT_MESSAGE, CONNECTION_MESSAGE, FAILURE_KINDS, FailureKind, FailureSignals,
        MAX_ERROR_DETAIL_LENGTH, REDACTION_MARKER, STATUS_NO_BODY_SUFFIX, StreamFailure,
        TIMEOUT_MESSAGE, TransportError, UNKNOWN_STREAM_ERROR_MESSAGE, classify_stream_error,
        find_transport_errno, flatten_error_detail, is_libuv_errno, redact_secrets,
        sanitize_upstream_target,
    },
    sse::{
        DONE_FRAME, DONE_SENTINEL, FRAME_SEPARATOR, SseDecoder, SseEvent, data_frame, data_payload,
        stream_body,
    },
    transport::{
        CHAT_COMPLETIONS_PATH, Credential, EXHAUSTED_BASE_URLS_MESSAGE, Endpoint,
        KeylessHeaderPolicy, MAX_RETRIES, Policy, RequestOptions, ResponseOrStream, Resume,
        StreamOutcome, StreamSettlement, Target, Transport, TransportFailure, UpstreamStream,
        estimate_request_tokens, has_generated_payload, normalize_base_url, output_tokens_of,
        prepare_body,
    },
    vault::Secret,
};

/// A credential shape long enough to trip `REDACT_MIN_LEN` (`src/vault.rs:102`) and `sk-`-shaped, so
/// `redact_secrets` rule 1 (`src/error.rs:182-232`) would redact it if it ever reached free text.
/// Every "no credential in any diagnostic" assertion uses this literal.
const SECRET: &str = "sk-live-9f3a7c21be4d0000feed";

/// The three credential shapes the tests need, so each case says which one it means instead of
/// rebuilding pools at the call site.
enum TestCredential {
    /// A usable key, sent as `Authorization: Bearer {SECRET}`.
    Keyed,
    /// The vault's `none` marker as it actually reaches this crate: a `VaultValidity::Keyless`
    /// credential that still CARRIES the sentinel value, which is what `Credential::from_entry`
    /// produces for `local-strata.ts:156`.
    KeylessSentinel,
    /// The credential-less state itself.
    Keyless,
}

impl TestCredential {
    fn build(self) -> Credential {
        match self {
            Self::Keyed => Credential::new(&Secret::new(SECRET)),
            Self::KeylessSentinel => Credential::new(&Secret::new("none")),
            Self::Keyless => Credential::keyless(),
        }
    }
}

/// One scripted answer, consumed by exactly one attempt.
enum Reply {
    /// `status` plus a JSON document (the transport's `read_document` path).
    Json(StatusCode, Value),
    /// `status` plus a non-JSON body - a proxy's HTML error page, the `makeMessage` fallback branch
    /// (`openai/core/error.js:26`).
    Text(StatusCode, &'static str),
    /// `status` and no body at all, which is the `{status} status code (no body)` case.
    Empty(StatusCode),
    /// A 200 SSE body written ONE FRAME PER POLL, so a frame can be split across polls by putting
    /// half of it in one entry and half in the next. `produced` and `dropped` observe the upstream's
    /// side of the body, which is what the drop test reads.
    Sse {
        frames: Vec<Vec<u8>>,
        /// When set, one delay sits in front of every frame. That is what makes "the upstream
        /// stopped producing after the consumer went away" observable instead of racy.
        spaced: Option<Duration>,
        /// Whether the body paces itself: a spaced body starts its first sleep eagerly, so the
        /// first frame cannot arrive before the delay.
        paced: bool,
        produced: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    },
}

impl Reply {
    /// An unobserved SSE answer: the counters exist because `Body::from_stream` needs a
    /// `Send + 'static` stream, and they are how the tests see inside it.
    fn sse(frames: Vec<Vec<u8>>) -> Self {
        Self::Sse {
            frames,
            spaced: None,
            paced: false,
            produced: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The same, spaced, with the counters a test keeps.
    fn spaced(
        frames: Vec<Vec<u8>>,
        spaced: Duration,
        produced: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    ) -> Self {
        Self::Sse {
            frames,
            spaced: Some(spaced),
            paced: true,
            produced,
            dropped,
        }
    }
}

/// What one attempt looked like on the wire, as the upstream saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Captured {
    method: String,
    path: String,
    /// Every header in arrival order, so both the presence of `authorization` and the canonical
    /// ordering of the generated set are assertable.
    headers: Vec<(String, String)>,
    body: String,
}

impl Captured {
    /// All values of one header name. A repeated header is precisely the bug this crate must not
    /// produce, so tests count values rather than reading the first one.
    fn values(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
            .collect()
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
    /// The requests the upstream answered, in order.
    fn seen(&self) -> Vec<Captured> {
        self.seen.lock().expect("ledger lock").clone()
    }

    /// How many attempts reached the upstream.
    fn attempts(&self) -> usize {
        self.seen.lock().expect("ledger lock").len()
    }
}

/// Every scripted upstream this test process started, by address, so a helper that only carries the
/// address can still read its ledger back.
static LEDGERS: OnceLock<Mutex<BTreeMap<String, Arc<Upstream>>>> = OnceLock::new();

/// Starts one scripted upstream and returns its `host:port`.
async fn provider(script: Vec<Reply>) -> String {
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

    LEDGERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("registry lock")
        .insert(address.clone(), Arc::clone(&upstream));

    tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/{*path}", any(handle))
                .with_state(upstream),
        )
        .await
        .expect("the scripted upstream lives until the process ends");
    });

    address
}

/// The ledger of the upstream a transport was built against.
fn ledger(address: &str) -> Arc<Upstream> {
    LEDGERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("registry lock")
        .get(address)
        .unwrap_or_else(|| panic!("no scripted upstream is registered for {address}"))
        .clone()
}

/// Answers the next scripted reply, after recording what arrived.
async fn handle(State(upstream): State<Arc<Upstream>>, request: Request) -> Response {
    let method = request.method().as_str().to_owned();
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
        method,
        path,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    });

    // Taken and released before any await, so no lock is ever held across a poll.
    match upstream.script.lock().expect("script lock").pop_front() {
        Some(Reply::Json(status, document)) => {
            response(status, "application/json", Body::from(document.to_string()))
        }
        Some(Reply::Text(status, text)) => response(status, "text/html", Body::from(text)),
        Some(Reply::Empty(status)) => response(status, "application/json", Body::empty()),
        Some(Reply::Sse {
            frames,
            spaced,
            paced,
            produced,
            dropped,
        }) => response(
            StatusCode::OK,
            "text/event-stream",
            Body::from_stream(ScriptedSse {
                frames,
                index: 0,
                spaced,
                pacer: paced.then_some(Box::pin(tokio::time::sleep(
                    spaced.unwrap_or(Duration::ZERO),
                ))),
                produced,
                dropped,
            }),
        ),
        // The script ran out: an answer impossible to mistake for a real one, so a failing test
        // names its own bug instead of hiding behind a transport verdict.
        None => response(
            StatusCode::IM_A_TEAPOT,
            "text/plain",
            Body::from("script exhausted: the transport made one attempt too many"),
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

/// The SSE body: one scripted frame per poll, then `None`, with a `Drop` that records whether the
/// consumer went away before the script finished.
struct ScriptedSse {
    frames: Vec<Vec<u8>>,
    index: usize,
    spaced: Option<Duration>,
    /// The delay in front of the NEXT frame. A completed sleep is dropped and re-armed, so spacing
    /// is per frame rather than once.
    pacer: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
    produced: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl Stream for ScriptedSse {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.index >= self.frames.len() {
            return Poll::Ready(None);
        }

        // A spaced script parks a waker instead of blocking a thread, so the frames arrive on their
        // own schedule and the drop test can tell "still producing" from "finished producing".
        if let Some(delay) = self.spaced {
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
        self.produced.fetch_add(1, Ordering::SeqCst);

        Poll::Ready(Some(Ok(Bytes::from(frame))))
    }
}

impl Drop for ScriptedSse {
    fn drop(&mut self) {
        // The whole point of the type: hyper drops the response body when the client stops reading
        // it, so this flag says "the upstream was torn down after frame N of M".
        self.dropped.store(true, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// Test-side construction helpers
// ---------------------------------------------------------------------------

/// A hermetic HTTP client.
///
/// `no_proxy` is load-bearing: `reqwest::Client::builder()` honours `HTTP_PROXY` from the developer's
/// environment, and a proxy would silently move every request off loopback.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("a proxy-free client")
}

/// A transport pointed at one scripted upstream, with a pause short enough that a retry case proves
/// the pause HAPPENED without costing the suite three seconds each.
fn build_transport(address: &str, credential: TestCredential, pause: Duration) -> Transport {
    let target = Target::new(
        vec![Endpoint::new(&format!("http://{address}"))],
        vec![credential.build()],
    );

    Transport::with_http_client(
        target,
        Policy::standard().with_rate_limit_pause(pause),
        client(),
    )
}

/// Rebuilds one scripted transport with `KeylessHeaderPolicy::Suppress`.
///
/// The keyless default flipped to `SendNone` (`Bearer none`, the `openai@6.42.0` wire behaviour -
/// see `src/transport.rs::KeylessHeaderPolicy`), so the two tests that pin the credential-FREE relays
/// name the suppression explicitly instead of inheriting it. The target is rebuilt from
/// `Transport::target()` (pools are immutable, the policy is the only field that changes), and the
/// fresh transport keeps the same scripted endpoint and credential.
fn with_keyless_suppression(transport: Transport) -> Transport {
    let target = transport
        .target()
        .clone()
        .keyless_header(KeylessHeaderPolicy::Suppress);

    Transport::with_http_client(target, transport.policy().clone(), client())
}

/// A settled request against the scripted upstream.
async fn settled(transport: &Transport) -> Result<ResponseOrStream, TransportFailure> {
    transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "messages": [] }),
            RequestOptions::new(),
        )
        .await
}

/// A streaming request with a settlement hook attached, unwrapped to the stream result.
async fn streaming(transport: &Transport, settled: &Settled) -> ResponseOrStream {
    transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "stream": true }),
            RequestOptions::new().with_settlement(settled.hook()),
        )
        .await
        .expect("a streaming request reaches the head")
}

/// Drains a chunk stream into the exact list of items it produces.
async fn collect(stream: &mut UpstreamStream) -> Vec<Value> {
    let mut chunks = Vec::new();

    while let Some(item) = stream.next().await {
        chunks.push(item.expect("every frame of this script is a chunk value"));
    }

    chunks
}

/// The same, keeping the terminal failure instead of unwrapping it away.
async fn collect_with_failure(stream: &mut UpstreamStream) -> (Vec<Value>, Option<StreamFailure>) {
    let mut chunks = Vec::new();

    while let Some(item) = stream.next().await {
        match item {
            Ok(chunk) => chunks.push(chunk),
            Err(failure) => return (chunks, Some(failure)),
        }
    }

    (chunks, None)
}

/// A settlement collector. The hook runs on the stream's own context, so the facts are read back
/// through a shared cell rather than returned.
#[derive(Clone, Default)]
struct Settled(Arc<Mutex<Vec<StreamSettlement>>>);

impl Settled {
    fn hook(&self) -> typecrate_provider::transport::SettlementHook {
        let cell = Arc::clone(&self.0);

        Box::new(move |settlement| {
            cell.lock().expect("settlement lock").push(settlement);
        })
    }

    fn take(&self) -> Vec<StreamSettlement> {
        std::mem::take(&mut *self.0.lock().expect("settlement lock"))
    }

    /// Exactly one settlement - the contract every case below pins.
    fn one(&self) -> StreamSettlement {
        let settlements = self.take();

        assert_eq!(
            settlements.len(),
            1,
            "the settlement fires exactly once, got {}",
            settlements.len()
        );

        settlements.into_iter().next().expect("one settlement")
    }
}

// ---------------------------------------------------------------------------
// The request: URL, headers, auth, body shape
// ---------------------------------------------------------------------------

/// `model-class.ts:32-35` deliberately keeps `none` OUT of the invalid-key set (`:39-44`), so `isValidApiKey` (`:52-60`) accepts it and the row stays usable, and the
/// deployed pool uses it (`local-strata.ts:156`, `daytona-qwen3-8.ts:451`).
///
/// THE TWO SPELLINGS ARE NOW POLICY, NOT CONSTANT (`src/transport.rs::KeylessHeaderPolicy`):
/// `openai@6.42.0` has no special case for `none` (`node_modules/openai/client.js:232-241` builds the
/// header from `apiKey` unconditionally), so the deployed TypeScript providers send the literal
/// `Bearer none` - the DEFAULT arm. This test pins the OTHER arm, `Suppress`, which is what
/// `local-strata.ts:161` / `local-basalt.ts:147` get with
/// `defaultHeaders: { Authorization: null }` ("verified: fetch capture shows the header absent").
#[tokio::test]
async fn a_keyless_credential_sends_no_authorization_header() {
    let address = provider(vec![Reply::Json(StatusCode::OK, json!({ "id": "cmpl-1" }))]).await;
    let transport = with_keyless_suppression(build_transport(
        &address,
        TestCredential::KeylessSentinel,
        Duration::from_millis(5),
    ));

    let answer = settled(&transport)
        .await
        .expect("a keyless request still succeeds");

    assert_eq!(
        answer.into_response(),
        Some(json!({ "id": "cmpl-1" })),
        "the settled document is passed through untouched"
    );

    let captured = ledger(&address).seen();
    assert_eq!(
        captured.len(),
        1,
        "exactly one request reached the upstream"
    );
    assert!(
        captured[0].values("authorization").is_empty(),
        "a keyless credential sends no `authorization` header at all, got {:?}",
        captured[0].headers
    );
    assert_eq!(captured[0].values("content-type"), ["application/json"]);
    assert_eq!(captured[0].values("accept"), ["application/json"]);
}

/// The credential-less marker carries no value at all, and the SUPPRESS arm still sends no header (the
/// `SendNone` arm for the same state is pinned by
/// `tests/clients.rs::a_keyless_client_sends_bearer_none_by_default`).
#[tokio::test]
async fn a_credential_less_target_sends_no_authorization_header() {
    let address = provider(vec![Reply::Json(
        StatusCode::OK,
        json!({ "id": "cmpl-1b" }),
    )])
    .await;
    let transport = with_keyless_suppression(build_transport(
        &address,
        TestCredential::Keyless,
        Duration::from_millis(5),
    ));

    settled(&transport)
        .await
        .expect("a credential-less request still succeeds");

    let captured = ledger(&address).seen();

    assert!(
        captured[0].values("authorization").is_empty(),
        "no header, not an empty one: {:?}",
        captured[0].headers
    );
}

/// The non-keyless half of the same rule: the value is sent verbatim, which is the
/// `Authorization: Bearer ${apiKey}` shape of `openai/client.js:232-241`.
#[tokio::test]
async fn a_keyed_credential_sends_the_bearer_header() {
    let address = provider(vec![Reply::Json(StatusCode::OK, json!({ "id": "cmpl-2" }))]).await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    settled(&transport).await.expect("a keyed request succeeds");

    let captured = ledger(&address).seen();

    assert_eq!(
        captured[0].values("authorization"),
        [format!("Bearer {SECRET}").as_str()],
        "the credential is sent verbatim, exactly as the SDK's `buildHeaders` does"
    );
}

/// `harness/src/transport.rs:222-262` is the precedent for the merge rule: reqwest's `.header()`
/// APPENDS, so a caller's `accept` would otherwise sit next to the generated one and the upstream
/// would see two values for one name.
#[tokio::test]
async fn a_custom_header_replaces_the_generated_one() {
    let address = provider(vec![Reply::Json(StatusCode::OK, json!({ "id": "cmpl-3" }))]).await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "messages": [] }),
            RequestOptions::new().with_header("accept", "text/plain"),
        )
        .await
        .expect("a custom accept still succeeds");

    let captured = ledger(&address).seen();

    assert_eq!(
        captured[0].values("accept"),
        ["text/plain"],
        "the caller's value REPLACES the generated one instead of joining it"
    );
}

/// The generated set is written once per name and in a canonical order, which keeps a built request
/// byte-stable (`harness/src/transport.rs:224-228` makes the same argument).
#[tokio::test]
async fn headers_arrive_in_one_canonical_order() {
    let address = provider(vec![Reply::Json(StatusCode::OK, json!({ "id": "cmpl-4" }))]).await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "messages": [] }),
            RequestOptions::new().with_header("x-request-id", "req-7"),
        )
        .await
        .expect("the request succeeds");

    let captured = ledger(&address).seen();

    // `host`, `content-length` and `connection` are hyper's own, not this layer's to order.
    let ours: Vec<&str> = captured[0]
        .headers
        .iter()
        .map(|(name, _)| name.as_str())
        .filter(|name| !matches!(*name, "host" | "content-length" | "connection"))
        .collect();

    assert_eq!(
        ours,
        ["accept", "authorization", "content-type", "x-request-id"],
        "the whole header set this layer owns, in canonical order"
    );
}

/// Every streaming client sends `stream_options: { include_usage: true }` only for a streaming
/// request, in its own `create()` (32 client files; `nvidia/nvidia-glm5-flash.ts:76`, `local/local-strata.ts:233`, `makora/makora-glm-5-3.ts:79`), which explains why the pair must agree: the OpenAI contract
/// rejects `stream_options` when `stream` is falsy. A non-streaming body therefore loses the key
/// ENTIRELY - never `"stream_options": null` - which is what `JSON.stringify` drops in TS and what
/// `prepare_body` removes here.
#[test]
fn a_settled_body_never_carries_stream_options() {
    let (bytes, streaming) = prepare_body(&json!({
        "model": "m",
        "stream": false,
        "stream_options": { "include_usage": true }
    }))
    .expect("a plain document serializes");

    assert!(
        !streaming,
        "the body's own `stream` flag decides the request kind"
    );

    let document: Value = serde_json::from_slice(&bytes).expect("the bytes are JSON");

    assert_eq!(
        document.get("stream_options"),
        None,
        "the key is removed, not nulled: {document}"
    );
    assert_eq!(
        document["stream"],
        Value::from(false),
        "the flag itself stays"
    );

    // A body with no `stream` at all is a settled request.
    let (_, streaming) = prepare_body(&json!({ "model": "m" })).expect("serializes");
    assert!(!streaming);
}

/// The streaming half of the pair, asserted on the WIRE: the caller supplies the usage option, the
/// transport keeps it, and `Accept` switches to the event stream.
#[tokio::test]
async fn a_streaming_request_forwards_the_usage_option_verbatim() {
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n".to_vec(),
        DONE_FRAME.as_bytes().to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({
                "model": "m",
                "stream": true,
                "stream_options": { "include_usage": true, "include": [{ "type": "message.output_text.log" }] }
            }),
            RequestOptions::new().with_settlement(settled.hook()),
        )
        .await
        .expect("a streaming request reaches the head");
    let mut stream = answer.into_stream().expect("the body is a stream");

    assert_eq!(
        collect(&mut stream).await,
        vec![json!({ "id": "a", "choices": [{ "delta": { "content": "hi" } }] })],
        "the one data frame is the one chunk"
    );

    let captured = ledger(&address).seen();
    let sent = captured[0].sent();

    assert_eq!(sent["stream"], Value::from(true));
    assert_eq!(
        sent["stream_options"],
        json!({ "include_usage": true, "include": [{ "type": "message.output_text.log" }] }),
        "the caller's option is forwarded verbatim, including the keys this crate knows nothing about"
    );
    assert_eq!(captured[0].values("accept"), ["text/event-stream"]);
    assert_eq!(captured[0].values("content-type"), ["application/json"]);
}

/// The transport normalises the `stream_options` PAIR, it never INVENTS the option: injection is the
/// endpoint body builder's rule (`private-chat-completion.ts:113`, ported by the client task), so a
/// streaming body that did not ask for usage goes out with the key ABSENT - not `"stream_options":
/// null`, which the OpenAI contract rejects.
#[tokio::test]
async fn a_streaming_request_invents_no_usage_option() {
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\"}\n\n".to_vec(),
        DONE_FRAME.as_bytes().to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("the body is a stream");

    assert_eq!(collect(&mut stream).await, vec![json!({ "id": "a" })]);

    let captured = ledger(&address).seen();
    let sent = captured[0].sent();

    assert_eq!(
        sent.as_object()
            .expect("an object body")
            .get("stream_options"),
        None,
        "no option in, no key out: `{}`",
        captured[0].body
    );
    assert_eq!(sent["stream"], Value::from(true));
}

/// A body that explicitly carries `"stream_options": null` is a streaming request whose option was
/// cleared, and `null` is what the TS ternary leaves behind before `JSON.stringify` erases it, so it
/// is removed rather than sent as a null object.
#[test]
fn an_explicit_null_stream_options_is_removed() {
    let (bytes, streaming) =
        prepare_body(&json!({ "model": "m", "stream": true, "stream_options": null }))
            .expect("serializes");

    assert!(streaming);

    let document: Value = serde_json::from_slice(&bytes).expect("the bytes are JSON");

    assert_eq!(document.get("stream_options"), None);
}

/// The four rebasing branches of `model-class.ts:141-164` (`rebaseRequestInput`) - which is the ONLY
/// place that rule lives; the `id` doc comment at `:322-327` says nothing about `/v1`.
#[test]
fn base_urls_rebase_in_four_branches() {
    // 1. The vendor's own host is already complete, so `api.openai.com` is kept as given.
    assert_eq!(
        normalize_base_url("https://api.openai.com"),
        "https://api.openai.com"
    );
    assert_eq!(
        normalize_base_url("https://api.openai.com/v1"),
        "https://api.openai.com/v1"
    );

    // 2. A base that already ends in `/v1` is left alone (10 of the 11 real vault endpoints are in
    //    this shape).
    assert_eq!(
        normalize_base_url("https://integrate.api.nvidia.com/v1"),
        "https://integrate.api.nvidia.com/v1"
    );
    assert_eq!(
        normalize_base_url("https://integrate.api.nvidia.com/v1/"),
        "https://integrate.api.nvidia.com/v1"
    );

    // 3. A base with a NON-`/v1` path gets `/v1` appended to that path - the daytona case, whose
    //    vault endpoint is `https://app.daytona.io/api`.
    assert_eq!(
        normalize_base_url("https://app.daytona.io/api"),
        "https://app.daytona.io/api/v1"
    );
    assert_eq!(
        normalize_base_url("http://127.0.0.1:8080"),
        "http://127.0.0.1:8080/v1"
    );

    // 4. Trimming, the trailing slash, and a query that is not part of the path at all.
    assert_eq!(
        normalize_base_url("  https://engine.example.com/  "),
        "https://engine.example.com/v1"
    );
    assert_eq!(
        normalize_base_url("https://engine.example.com/v1?api-version=2"),
        "https://engine.example.com/v1?api-version=2"
    );
    assert_eq!(
        normalize_base_url("https://engine.example.com/v1/"),
        "https://engine.example.com/v1"
    );
}

/// The URL is rebuilt from the CURRENT endpoint, which is what makes `switchBaseUrl()`
/// (`local-strata.ts:243-261`) safe to retry against: there is no captured URL to rebase. The `GET`
/// half is the sandbox engines' discovery call (`daytona-qwen3-8.ts:336`).
#[tokio::test]
async fn the_request_url_is_built_from_the_current_endpoint() {
    let address = provider(vec![
        Reply::Json(StatusCode::OK, json!({ "id": "cmpl-5" })),
        Reply::Json(StatusCode::OK, json!({ "data": [] })),
    ])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    settled(&transport).await.expect("the POST succeeds");

    let captured = ledger(&address).seen();

    assert_eq!(
        captured[0].path, "/v1/chat/completions",
        "the normalised base plus the path, exactly as `{{base}}/chat/completions` composes"
    );
    assert_eq!(captured[0].method, "POST");

    transport
        .get_json("models", RequestOptions::new())
        .await
        .expect("the GET is answered by the scripted upstream");

    let captured = ledger(&address).seen();
    let last = captured.last().expect("the GET was captured");

    assert_eq!(last.path, "/v1/models");
    assert_eq!(last.method, "GET");
    assert_eq!(
        last.body, "",
        "a GET sends no body, not an empty JSON document"
    );
    assert_eq!(
        last.values("content-type"),
        ["application/json"],
        "the generated pair is sent for every request kind (`src/transport.rs:1417-1429`)"
    );
    assert_eq!(
        transport
            .current_endpoint()
            .map(|endpoint| endpoint.base_url().to_owned()),
        Some(format!("http://{address}/v1")),
        "the pooled endpoint is the normalised one"
    );
}

// ---------------------------------------------------------------------------
// The failure verdicts: 404 failover, 429 pause, the budget, cancellation
// ---------------------------------------------------------------------------

/// The 404 gate: `private-responses.ts:25-27` (`is404`) and the handler `model-class.ts:634-636`
/// installs. It comes back as a TYPED OUTCOME plus the ledger and is NOT retried internally, because
/// only the caller knows whether another base URL or credential exists.
#[tokio::test]
async fn a_404_is_a_typed_failover_verdict_and_is_not_retried() {
    let address = provider(vec![Reply::Json(
        StatusCode::NOT_FOUND,
        json!({ "error": { "message": "model not found", "type": "invalid_request_error" } }),
    )])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    let failure = settled(&transport)
        .await
        .expect_err("a 404 is a failure, not an answer");

    assert_eq!(
        ledger(&address).attempts(),
        1,
        "the 404 is NOT retried by this layer"
    );
    assert!(
        failure.is_not_found(),
        "the `is404` gate is a typed verdict"
    );
    assert_eq!(failure.status(), Some(StatusCode::NOT_FOUND));
    assert_eq!(failure.kind(), FailureKind::UpstreamStatus);
    assert_eq!(
        failure.message(),
        "404 model not found",
        "`makeMessage` renders `{{status}} {{error.message}}` with a NUMERIC status \
         (`openai/core/error.js:29-30`)"
    );
    assert!(!failure.retries_exhausted(), "the caller may still rotate");

    let state = failure.state();
    assert_eq!(state.attempts, 1);
    assert_eq!(state.attempted.len(), 1);

    let attempt = &state.attempted[0];
    assert_eq!(
        attempt.index, 1,
        "the ledger is 1-based, as the routes log it"
    );
    assert_eq!(attempt.status, Some(StatusCode::NOT_FOUND));
    assert_eq!(attempt.kind, Some(FailureKind::UpstreamStatus));
    assert_eq!(attempt.endpoint_label, None);
    assert_eq!(attempt.credential_index, Some(0));
    assert!(
        !format!("{failure:?}").contains(SECRET),
        "a `Debug` of the failure cannot contain the credential"
    );

    // Nothing reached the consumer, so a rotated re-run starts from scratch.
    assert_eq!(failure.resume(), Resume::New);
}

/// The message half when the upstream sends no body at all (`openai/core/error.js:31-33`).
#[tokio::test]
async fn a_status_with_no_body_uses_the_sdk_fallback_message() {
    let address = provider(vec![Reply::Empty(StatusCode::NOT_FOUND)]).await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    let failure = settled(&transport)
        .await
        .expect_err("a 404 with no body is still a failure");

    assert_eq!(
        failure.message(),
        format!("404 {STATUS_NO_BODY_SUFFIX}"),
        "an empty detail means the SDK's `status code (no body)` line is the message"
    );
    assert_eq!(failure.error().detail(), "");
}

/// A non-JSON error page is the other `makeMessage` branch (`:26`,
/// `error ? JSON.stringify(error) : message`): the raw text, flattened and capped.
#[tokio::test]
async fn an_html_error_page_becomes_the_flattened_detail() {
    let address = provider(vec![Reply::Text(
        StatusCode::BAD_GATEWAY,
        "<html>\n  <body>502 Bad Gateway</body>\n</html>",
    )])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    let failure = settled(&transport).await.expect_err("502 is a failure");

    assert_eq!(
        failure.message(),
        "502 <html> <body>502 Bad Gateway</body> </html>",
        "whitespace collapses to single spaces (`flattenErrorDetail`, `start.ts:442-450`): the \
         newline before `<body>` becomes ONE space, and nothing else is removed"
    );
    assert_eq!(failure.status(), Some(StatusCode::BAD_GATEWAY));
    assert_eq!(failure.kind(), FailureKind::UpstreamStatus);
}

/// The 429 handler at `model-class.ts:572-577` waits before retrying, and the wait is the whole
/// point of the handler. `Policy::rate_limit_pause` makes it observable: the suite pins a 120 ms
/// pause and proves the pause happened by measuring the elapsed span against it. The production
/// default stays the deployed three seconds (`RATE_LIMIT_PAUSE`).
#[tokio::test]
async fn a_429_pauses_before_it_retries() {
    const PAUSE: Duration = Duration::from_millis(120);

    assert_eq!(
        typecrate_provider::transport::RATE_LIMIT_PAUSE,
        Duration::from_secs(3),
        "the shipped pause is the three seconds the TypeScript handler waits"
    );

    let address = provider(vec![
        Reply::Json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "error": "slow down" }),
        ),
        Reply::Json(StatusCode::OK, json!({ "id": "cmpl-6" })),
    ])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, PAUSE);

    let started = Instant::now();
    let answer = settled(&transport).await.expect("the retry succeeds");
    let elapsed = started.elapsed();

    assert_eq!(
        answer.into_response(),
        Some(json!({ "id": "cmpl-6" })),
        "the retry's answer is the caller's answer"
    );

    let captured = ledger(&address).seen();
    assert_eq!(captured.len(), 2, "one 429, one retry");

    // The one timing relation this file asserts rather than an exact value, and it is exact up to
    // the scheduler: the pause is a `tokio::time::sleep`, so the span cannot be SHORTER than the
    // configured pause.
    assert!(
        elapsed >= PAUSE,
        "the 429 pause delayed the retry: {elapsed:?} < {PAUSE:?}"
    );

    assert!(
        transport.current_credential().is_some(),
        "a 429 never rotates the credential"
    );
}

/// The retry budget, a DELIBERATE deviation from the TypeScript original: `model-class.ts:557-654`
/// loops `while (true)` over its status handlers, so an upstream that keeps answering 429 retries
/// forever. A Rust caller owns a task, so the budget is `1 + MAX_RETRIES` attempts and the terminal
/// verdict is typed.
#[tokio::test]
async fn the_retry_budget_is_one_attempt_plus_three_retries() {
    let address = provider(
        (0..4)
            .map(|_| {
                Reply::Json(
                    StatusCode::TOO_MANY_REQUESTS,
                    json!({ "error": "slow down" }),
                )
            })
            .collect(),
    )
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(1));

    let failure = settled(&transport).await.expect_err("the budget runs out");

    assert_eq!(
        ledger(&address).attempts(),
        1 + MAX_RETRIES,
        "the first attempt plus exactly `MAX_RETRIES` retries, no more"
    );
    assert!(
        failure.retries_exhausted(),
        "the budget, not the upstream, ended it"
    );
    assert_eq!(failure.status(), Some(StatusCode::TOO_MANY_REQUESTS));
    assert_eq!(failure.kind(), FailureKind::UpstreamStatus);
    assert_eq!(failure.state().attempted.len(), 1 + MAX_RETRIES);
    assert_eq!(failure.state().attempts, 1 + MAX_RETRIES);

    // The ledger keeps every verdict, in order, which is what lets the route report the whole
    // attempt set the way `private-responses.ts:106-121` does.
    let statuses: Vec<u16> = failure
        .state()
        .attempted
        .iter()
        .map(|attempt| attempt.status.expect("a status").as_u16())
        .collect();

    assert_eq!(statuses, [429, 429, 429, 429]);
    assert_eq!(failure.resume(), Resume::New);
}

/// A refused port is the connection verdict, and `TransportError::message()` is the SDK's constant,
/// not the transport's own wording (`openai/core/error.js:72-93`, which is what
/// `formatUpstreamError` at `private-responses.ts:31-40` renders).
#[tokio::test]
async fn a_refused_port_is_a_connection_failure_after_the_budget() {
    // Bind and drop: the port is essentially certainly free, which is the deterministic way to ask
    // for ECONNREFUSED on loopback without racing a real listener.
    let address = {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback listener to measure");

        listener
            .local_addr()
            .expect("the bound address of a loopback listener")
            .to_string()
    };

    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(1));

    let failure = settled(&transport).await.expect_err("nothing is listening");

    assert_eq!(failure.kind(), FailureKind::Connection);
    assert_eq!(
        failure.status(),
        None,
        "there is no upstream verdict to forward"
    );
    assert_eq!(failure.message(), CONNECTION_MESSAGE);
    assert!(
        failure.retries_exhausted(),
        "a connection failure is retryable"
    );
    assert_eq!(failure.resume(), Resume::New);
    assert_eq!(failure.state().attempted.len(), 1 + MAX_RETRIES);

    for attempt in &failure.state().attempted {
        assert_eq!(attempt.status, None);
        assert_eq!(attempt.kind, Some(FailureKind::Connection));
    }
}

/// Cancellation is FINAL and never retried: TS's abort path raises `APIUserAbortError`, and the
/// SDK's message is the abort constant.
#[tokio::test]
async fn a_cancelled_request_reports_the_abort_constant() {
    let address = provider(vec![Reply::Json(StatusCode::OK, json!({ "id": "cmpl-7" }))]).await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let signal = CancellationToken::new();

    signal.cancel();

    let failure = transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "messages": [] }),
            RequestOptions::new().with_signal(signal),
        )
        .await
        .expect_err("an aborted request is a failure");

    assert_eq!(failure.kind(), FailureKind::Aborted);
    assert_eq!(failure.message(), ABORT_MESSAGE);
    assert_eq!(failure.status(), None);
    assert!(
        !failure.retries_exhausted(),
        "a cancellation is final, not a budget verdict"
    );
    assert_eq!(
        ledger(&address).attempts(),
        0,
        "a cancellation before the head sends nothing and retries nothing"
    );
}

// ---------------------------------------------------------------------------
// Rotation and the resume ledger
// ---------------------------------------------------------------------------

/// Rotation is the typed `switchBaseUrl()`: forward-only over the pool, and on exhaustion the LAST
/// entry stays in force - `local-strata.ts:244-250` returns false WITHOUT clearing `baseURL`.
#[tokio::test]
async fn rotation_walks_the_pools_and_then_reports_exhaustion() {
    let target = Target::new(
        vec![
            Endpoint::labeled("https://one.example/v1", "relay-one"),
            Endpoint::new("https://two.example/v1"),
            Endpoint::new("https://three.example/v1"),
        ],
        vec![
            Credential::new(&Secret::new("sk-first-key-0000")),
            Credential::new(&Secret::new("sk-second-key-000")),
        ],
    );
    let transport = Transport::with_policy(target, Policy::standard());

    assert_eq!(transport.endpoint_index(), Some(0));
    assert_eq!(
        transport.upstream_label().as_deref(),
        Some("relay-one"),
        "`upstreamLabel` is the relay NAME of the endpoint in force (`private-chat-completion.ts:106-122`)"
    );

    assert_eq!(
        transport
            .rotate_endpoint()
            .map(|endpoint| endpoint.base_url().to_owned()),
        Some("https://two.example/v1".to_owned())
    );
    assert_eq!(transport.endpoint_index(), Some(1));
    assert!(!transport.endpoints_exhausted());

    assert_eq!(
        transport
            .rotate_endpoint()
            .map(|endpoint| endpoint.base_url().to_owned()),
        Some("https://three.example/v1".to_owned())
    );
    assert!(transport.endpoints_exhausted());

    // Exhaustion: no endpoint left, and the cursor STAYS on the last one.
    assert_eq!(transport.rotate_endpoint(), None);
    assert_eq!(
        transport
            .current_endpoint()
            .map(|endpoint| endpoint.base_url().to_owned()),
        Some("https://three.example/v1".to_owned())
    );

    assert_eq!(transport.credential_index(), Some(0));
    assert_eq!(
        transport
            .rotate_credential()
            .and_then(|credential| credential.expose().map(str::to_owned)),
        Some("sk-second-key-000".to_owned())
    );
    assert_eq!(transport.rotate_credential(), None);
    assert!(transport.credentials_exhausted());
    assert_eq!(
        transport
            .current_credential()
            .and_then(|credential| credential.expose().map(str::to_owned)),
        Some("sk-second-key-000".to_owned())
    );

    transport.reset_rotation();
    assert_eq!(transport.endpoint_index(), Some(0));
    assert_eq!(transport.credential_index(), Some(0));

    // The pool is shared by clone: a cloned transport is the same singleton, which is how the
    // TypeScript clients behave (process-wide, one credential/base-URL pair that mutates in place).
    let clone = transport.clone();
    assert!(clone.rotate_endpoint().is_some());
    assert_eq!(
        transport.endpoint_index(),
        Some(1),
        "the cursors are shared, not copied"
    );
}

/// A 404 that survives a rotation carries the WHOLE ledger, because the terminal message has to name
/// every base URL the request burned through (`private-responses.ts:106-121`).
#[tokio::test]
async fn a_resumed_request_extends_the_ledger() {
    let address = provider(vec![
        Reply::Json(
            StatusCode::NOT_FOUND,
            json!({ "error": { "message": "no route" } }),
        ),
        Reply::Json(
            StatusCode::NOT_FOUND,
            json!({ "error": { "message": "no route" } }),
        ),
    ])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    let first = settled(&transport)
        .await
        .expect_err("the first attempt 404s");

    assert_eq!(first.state().attempted.len(), 1);

    let second = transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "messages": [] }),
            RequestOptions::new().with_resume(first.state().clone()),
        )
        .await
        .expect_err("the rotated attempt 404s too");

    assert_eq!(
        second.state().attempted.len(),
        2,
        "a resumed run EXTENDS the ledger, it does not reset it"
    );
    assert_eq!(
        second.state().attempted[0].status,
        Some(StatusCode::NOT_FOUND),
        "the carried-over attempt is still there, in order"
    );
    assert_eq!(second.state().attempts, 2);

    // What "resume" means, precisely (`src/transport.rs:691-729`): the state a MID-STREAM failure
    // carries is `Resumed`, and that decision is made by whether the consumer saw anything.
    let delivered = typecrate_provider::transport::AttemptState {
        chunks: 2,
        first_payload: true,
        ..typecrate_provider::transport::AttemptState::default()
    };

    assert_eq!(
        delivered.resume(),
        Resume::Resumed(delivered.clone()),
        "text already reached the consumer, so the retry must not re-stamp TTFT"
    );
    assert_eq!(
        typecrate_provider::transport::AttemptState::default().resume(),
        Resume::New,
        "nothing was delivered, so the retry is a fresh request"
    );
}

/// The terminal condition of a base-URL failover that ran out of base URLs, asserted BYTE FOR BYTE
/// because consumers match on it. Both branches throw the same string
/// (`private-responses.ts:118` and `:144`): "Service unavailable: all base URLs are exhausted
/// (404 Not Found)." - NOT the wording the task brief quoted, which exists nowhere in the repository.
#[test]
fn the_exhausted_pool_reports_the_deployed_sentence() {
    let failure = TransportFailure::all_base_urls_exhausted(
        typecrate_provider::transport::AttemptState::default(),
    );

    assert_eq!(
        EXHAUSTED_BASE_URLS_MESSAGE,
        "Service unavailable: all base URLs are exhausted (404 Not Found)."
    );
    assert_eq!(
        failure.message(),
        format!("404 {EXHAUSTED_BASE_URLS_MESSAGE}")
    );
    assert!(
        failure.is_not_found(),
        "the routes still render their 404 branch"
    );
    assert_eq!(failure.status(), Some(StatusCode::NOT_FOUND));
    assert_eq!(failure.kind(), FailureKind::UpstreamStatus);
    assert_eq!(failure.resume(), Resume::New);
}

// ---------------------------------------------------------------------------
// The SSE stream: decoding, the sentinel, error frames, settlement, disposal
// ---------------------------------------------------------------------------

/// The decoder's buffer is the whole point of `SseDecoder` (`src/sse.rs:174-182`): an upstream that
/// cuts a frame in half must not produce half a chunk.
#[tokio::test]
async fn a_frame_split_across_tcp_writes_yields_one_chunk() {
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\",\"choices\":[{\"delta\":{\"content\":\"he".to_vec(),
        b"llo\"}}]}\n".to_vec(),
        b"\ndata: {\"id\":\"b\",\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n".to_vec(),
        DONE_FRAME.as_bytes().to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(
        collect(&mut stream).await,
        vec![
            json!({ "id": "a", "choices": [{ "delta": { "content": "hello" } }] }),
            json!({ "id": "b", "choices": [{ "delta": { "content": "x" } }] }),
        ],
        "the split frame reassembles, and the frame straddling the same write still arrives"
    );

    let settlement = settled.one();
    assert_eq!(settlement.chunks, 2);
    assert!(settlement.sentinel);
    assert_eq!(settlement.outcome, StreamOutcome::Completed);
    assert_eq!(settlement.failure, None);
}

/// One TCP segment carrying three complete frames must yield three items across three polls, which is
/// what `StreamBody::queued` exists for (`src/transport.rs:1719-1721`).
#[tokio::test]
async fn one_write_carrying_three_frames_yields_three_items() {
    let segment = [
        b"data: {\"id\":\"one\"}\n\n".as_slice(),
        b"data: {\"id\":\"two\"}\n\n".as_slice(),
        b"data: {\"id\":\"three\"}\n\n".as_slice(),
        DONE_FRAME.as_bytes(),
    ]
    .concat();

    let address = provider(vec![Reply::sse(vec![segment])]).await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    let answer = streaming(&transport, &Settled::default()).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(
        collect(&mut stream).await,
        vec![
            json!({ "id": "one" }),
            json!({ "id": "two" }),
            json!({ "id": "three" })
        ]
    );
}

/// `private-chat-completion.ts:161-186` emits `data: <json>` per chunk and then its OWN trailing
/// `data: [DONE]`; the sentinel is never a chunk. A body that ends without it is still a clean end
/// for the provider surface - the relay emits `[DONE]` after any exhausted loop (`:178`) - so the
/// truncation signal is the settlement's `sentinel` flag, not an error.
#[tokio::test]
async fn the_sentinel_is_not_a_chunk_and_its_absence_is_reported() {
    let with_sentinel = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\"}\n\n".to_vec(),
        DONE_FRAME.as_bytes().to_vec(),
    ])])
    .await;
    let transport = build_transport(
        &with_sentinel,
        TestCredential::Keyed,
        Duration::from_millis(5),
    );
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(collect(&mut stream).await, vec![json!({ "id": "a" })]);

    let settlement = settled.one();
    assert!(settlement.sentinel);
    assert_eq!(settlement.outcome, StreamOutcome::Completed);

    let without_sentinel =
        provider(vec![Reply::sse(vec![b"data: {\"id\":\"b\"}\n\n".to_vec()])]).await;
    let transport = build_transport(
        &without_sentinel,
        TestCredential::Keyed,
        Duration::from_millis(5),
    );
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(collect(&mut stream).await, vec![json!({ "id": "b" })]);

    let settlement = settled.one();
    assert!(
        !settlement.sentinel,
        "the body was exhausted without the sentinel - the caller's truncation signal"
    );
    assert_eq!(
        settlement.outcome,
        StreamOutcome::Completed,
        "the provider surface does NOT fail on this (`private-chat-completion.ts:186`)"
    );
    assert_eq!(settlement.chunks, 1);
    assert_eq!(settlement.failure, None);
}

/// A mid-stream error frame with NO `status` is the `unknown` class, whose message is the frozen
/// constant `Upstream stream terminated` (`start.ts:682-685`, `UNKNOWN_STREAM_ERROR_MESSAGE`) and
/// whose payload therefore has NO `status` key at all.
#[tokio::test]
async fn a_mid_stream_error_frame_without_a_status_is_an_unknown_code() {
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n".to_vec(),
        b"data: {\"error\":{\"message\":\"Rate limit exceeded\",\"type\":\"rate_limit_error\",\"code\":\"rate_limit_exceeded\"}}\n\n".to_vec(),
        b"data: {\"id\":\"never\"}\n\n".to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    let (chunks, failure) = collect_with_failure(&mut stream).await;

    assert_eq!(
        chunks,
        vec![json!({ "id": "a", "choices": [{ "delta": { "content": "hi" } }] })],
        "nothing after an error frame is delivered: TS threw out of the iterator at that point"
    );

    let failure = failure.expect("the error frame is an item of the stream");
    assert_eq!(failure.code(), FailureKind::Unknown);
    assert!(
        failure.is_upstream(),
        "the model DID answer, so a supervisor must not treat this as fatal"
    );
    assert_eq!(failure.status(), None, "the frame carried no status");
    assert_eq!(failure.chunks(), 1, "one chunk reached the consumer first");
    assert_eq!(
        failure.errno(),
        None,
        "a provider code is not a transport errno"
    );

    let payload = failure.payload(None);

    assert_eq!(
        payload,
        json!({
            "error": {
                "message": UNKNOWN_STREAM_ERROR_MESSAGE,
                "type": "stream_error",
                "code": "unknown",
                "param": null
            }
        }),
        "the frame exactly as `start.ts:695-702` stamps it"
    );
    assert_eq!(
        payload["error"]
            .as_object()
            .expect("an error object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["message", "type", "code", "param"],
        "the key ORDER is the contract, and `status` is absent rather than null"
    );

    // The stream is fused after the failure item, and the settlement named it.
    assert!(
        stream.next().await.is_none(),
        "the stream is fused after `Err`"
    );

    let settlement = settled.one();
    assert_eq!(settlement.outcome, StreamOutcome::Failed);
    assert_eq!(settlement.chunks, 1);
    assert_eq!(settlement.failure, Some(failure));
}

/// The same frame WITH a `status`: it is STILL the `unknown` class, because a status written INSIDE a
/// 200 stream body is not a status the deployed gateway can read.
///
/// The chain, verified on 2026-11-19: the SDK wraps an in-band `{"error":{…}}` payload in
/// `new APIError(undefined, data.error, undefined, response.headers)`
/// (`node_modules/openai/core/streaming.js:50`) — the third argument is the status, and `APIError`
/// assigns exactly what it is given (`node_modules/openai/core/error.js:12`), so `err.status` is
/// `undefined`. `readUpstreamStatus` (`packages/underload/service/src/server/start.ts:506-517`) only
/// reads `error.status` off the thrown object, so `upstreamStatusCode` stays `undefined`, the status
/// arm of `classifyStreamError` (`start.ts:600-601`) cannot fire, and `start.ts:644`
/// (`const status = code === 'upstream_status' ? readUpstreamStatus(chain) : undefined`) omits the key from
/// the frame literal (`start.ts:695-702` writes `status: typeof status === 'number' ? status : undefined`
/// at `:699`, and `JSON.stringify` drops the key when `status` is `undefined`). So the emitted frame is
/// byte-identical to the frame of the
/// test above: `code: "unknown"`, the frozen message, and NO `status` key.
#[tokio::test]
async fn a_mid_stream_error_frame_with_a_status_stays_unknown_and_carries_no_status() {
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\"}\n\n".to_vec(),
        b"data: {\"error\":{\"message\":\"Too many requests\",\"type\":\"rate_limit\",\"status\":429}}\n\n".to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    let answer = streaming(&transport, &Settled::default()).await;
    let mut stream = answer.into_stream().expect("a stream");

    let (chunks, failure) = collect_with_failure(&mut stream).await;

    assert_eq!(chunks.len(), 1);

    let failure = failure.expect("the frame is an item");
    assert_eq!(
        failure.code(),
        FailureKind::Unknown,
        "an in-band `status` is not an upstream status signal (`start.ts:506-517`)"
    );
    assert_eq!(
        failure.status(),
        None,
        "the classified failure has no status, exactly as the SDK's `APIError` has none"
    );
    assert_eq!(
        failure.frame_status(),
        Some(StatusCode::TOO_MANY_REQUESTS),
        "the number the upstream wrote is kept as a diagnostic (`src/error.rs`, \
         `StreamFailure::frame_status`) that is never on the wire and has no reader in `src/`"
    );
    assert!(failure.is_upstream());
    assert_eq!(failure.chunks(), 1, "one chunk reached the consumer first");

    let payload = failure.payload(Some("responses"));

    assert_eq!(
        payload,
        json!({
            "error": {
                "message": UNKNOWN_STREAM_ERROR_MESSAGE,
                "type": "stream_error",
                "code": "unknown",
                "param": null
            }
        }),
        "the SAME frame the status-less in-band error produces — the in-band `status` changes nothing"
    );
    assert_eq!(
        payload["error"]
            .as_object()
            .expect("an error object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["message", "type", "code", "param"],
        "`status` is ABSENT, not null, for an in-band frame (`start.ts:644`, `:695-702`)"
    );
    assert_eq!(
        serde_json::to_string(&payload).expect("a serialisable frame"),
        r#"{"error":{"message":"Upstream stream terminated","type":"stream_error","code":"unknown","param":null}}"#,
        "the exact bytes a browser reads: the 429 the upstream wrote appears nowhere in them"
    );
}

/// The ONE shape that does stamp a `status`: a real HTTP verdict on the response head, i.e. a
/// `TransportError::Status`, which is the Rust counterpart of the thrown `ApiError` whose own `status`
/// property `readUpstreamStatus` reads (`start.ts:506-517` — the ONLY source of
/// `upstreamStatusCode`). `start.ts:644` then emits `status`, and `start.ts:695-702` places it between
/// `code` and `errno`/`param`.
///
/// This is a PRE-STREAM verdict: the route answers the gateway's constant 500 for it
/// (`start.ts:908-911`, `src/serve.rs` deviation 3), so the frame below is what the frame builder emits
/// whenever a status-carrying transport failure is classified — the classification and the wire shape
/// are pinned here because no mid-stream failure can carry a status (see the test above).
#[test]
fn a_pre_stream_status_is_the_only_shape_that_stamps_a_status() {
    let target = Some("https://engine.example/v1");
    let error = TransportError::Status {
        status: StatusCode::TOO_MANY_REQUESTS,
        detail: "Too many requests".to_owned(),
    };

    // `StreamErrorContext.chunks` is 0 before the first chunk, and the sentence says so
    // (`start.ts:623-627`, `:652-654`).
    let before = StreamFailure::transport(&error, 0, target);

    assert_eq!(before.code(), FailureKind::UpstreamStatus);
    assert_eq!(before.status(), Some(StatusCode::TOO_MANY_REQUESTS));
    assert_eq!(
        before.frame_status(),
        None,
        "there is no in-band frame here, so the diagnostic field stays empty"
    );
    assert_eq!(
        before.sentence(Some("chat-completion")),
        "upstream https://engine.example/v1 returned HTTP 429 before the first chunk: Too many requests"
    );
    assert_eq!(
        serde_json::to_string(&before.payload(Some("chat-completion"))).expect("a frame"),
        r#"{"error":{"message":"upstream https://engine.example/v1 returned HTTP 429 before the first chunk: Too many requests","type":"stream_error","code":"upstream_status","status":429,"param":null}}"#,
        "`status` is the number 429, in the position `start.ts:699` stamps it, never a string"
    );

    // After a chunk the phase changes and nothing else does (`start.ts:652-654`).
    let after = StreamFailure::transport(&error, 1, target);

    assert_eq!(
        after.sentence(None),
        "upstream https://engine.example/v1 returned HTTP 429 after 1 chunk: Too many requests"
    );
    assert_eq!(
        after.payload(None)["error"]["status"],
        Value::from(429),
        "the emitted status is the upstream's own status, unmodified"
    );
}

/// A `data:` line that is not JSON is the `decode` class (`start.ts:609`), and it is terminal.
#[tokio::test]
async fn an_undecodable_frame_is_a_terminal_decode_failure() {
    let address = provider(vec![Reply::sse(vec![
        b"data: not json at all\n\n".to_vec(),
        b"data: {\"id\":\"never\"}\n\n".to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    let (chunks, failure) = collect_with_failure(&mut stream).await;

    assert!(chunks.is_empty());

    let failure = failure.expect("the bad line is an item");
    assert_eq!(failure.code(), FailureKind::Decode);
    assert!(
        !failure.is_upstream(),
        "the DECODER gave up, so a supervisor treats this as fatal (a deliberate deviation from \
         the TS origin, documented at `src/transport.rs`, `StreamBody::absorb`)"
    );

    // The frame names the target the transport actually dialed, which is the ONLY way the sentence
    // can be diagnosable: the TS gateway never sees a URL (it gets an async iterable) and has to
    // thread the value onto the thrown object with a Symbol (`start.ts:536-547`), while this layer
    // knows it directly. `sanitize_upstream_target` keeps origin + pathname and drops userinfo,
    // query and hash (`start.ts:452-466`).
    let expected_target = format!("http://{address}/v1/chat/completions");

    assert_eq!(failure.upstream_target(), Some(expected_target.as_str()));
    assert_eq!(
        failure.sentence(Some("chat-completion")),
        format!(
            "upstream {expected_target} sent an undecodable payload before the first chunk: \
             expected ident at line 1 column 2"
        )
    );

    assert!(stream.next().await.is_none());

    let settlement = settled.one();
    assert_eq!(settlement.outcome, StreamOutcome::Failed);
    assert_eq!(settlement.chunks, 0);
}

/// Dropping the stream must stop reading upstream, and must settle exactly once. TS has no name for
/// this ending: the consumer leaving IS the request's end, and `trackStream`'s `finally`
/// (`model-class.ts:799-801`) runs the settlement - which is why `Drop` performs it here.
#[tokio::test]
async fn dropping_the_stream_stops_the_upstream_reads() {
    const TOTAL: usize = 12;
    const SPACING: Duration = Duration::from_millis(10);

    let produced = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));

    let frames: Vec<Vec<u8>> = (0..TOTAL)
        .map(|index| format!("data: {{\"id\":\"{index}\"}}\n\n").into_bytes())
        .chain(std::iter::once(DONE_FRAME.as_bytes().to_vec()))
        .collect();

    let address = provider(vec![Reply::spaced(
        frames.clone(),
        SPACING,
        Arc::clone(&produced),
        Arc::clone(&dropped),
    )])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();
    let signal = CancellationToken::new();

    let answer = transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "stream": true }),
            RequestOptions::new()
                .with_settlement(settled.hook())
                .with_signal(signal.clone()),
        )
        .await
        .expect("the head arrives");

    let mut stream = answer.into_stream().expect("a stream");
    assert_eq!(
        stream.next().await.expect("a frame"),
        Ok(json!({ "id": "0" }))
    );

    drop(stream);

    // The upstream body is dropped by hyper once the client stops reading. The spacing is what makes
    // this observable rather than racy: the body produces ONE frame per 10 ms, so delivering all 13
    // would take 130 ms, and the drop lands almost immediately. The bound is therefore a real bound.
    let deadline = Instant::now() + Duration::from_millis(500);

    while !dropped.load(Ordering::SeqCst) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert!(
        dropped.load(Ordering::SeqCst),
        "dropping the stream tore the response body down on the upstream side"
    );
    assert!(
        produced.load(Ordering::SeqCst) < frames.len(),
        "the upstream stopped producing: {} of {} frames",
        produced.load(Ordering::SeqCst),
        frames.len()
    );

    // Abandonment cancels the REQUEST's child token, not the caller's own token
    // (`src/transport.rs:1237`), so a caller that shares one scope across several requests is not
    // poisoned by one stream it walked away from.
    assert!(
        !signal.is_cancelled(),
        "abandoning one stream does not cancel the caller's scope"
    );

    let settlement = settled.one();
    assert_eq!(settlement.outcome, StreamOutcome::Abandoned);
    assert_eq!(settlement.chunks, 1);
    assert_eq!(settlement.failure, None, "abandonment is not a failure");
}

/// The settlement fires EXACTLY ONCE for every way a stream can end - exhaustion, failure,
/// cancellation, abandonment - because the facts `trackStream`'s `finally` records
/// (`model-class.ts:859-878`) are recorded together and must not be recorded twice.
#[tokio::test]
async fn the_settlement_fires_once_for_clean_failure_and_cancellation() {
    // (a) clean exhaustion, with the full settlement asserted.
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\",\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n".to_vec(),
        b"data: {\"id\":\"b\",\"choices\":[{\"delta\":{\"content\":\"y\"}}],\"usage\":{\"completion_tokens\":7}}\n\n".to_vec(),
        DONE_FRAME.as_bytes().to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(collect(&mut stream).await.len(), 2);

    let settlement = settled.one();
    assert_eq!(settlement.outcome, StreamOutcome::Completed);
    assert_eq!(settlement.chunks, 2);
    assert!(settlement.sentinel);
    assert_eq!(settlement.output_tokens, Some(7));
    assert!(settlement.usage_reported);
    let first = settlement
        .time_to_first_token
        .expect("a generated payload stamped TTFT");
    assert!(
        settlement.generation.is_some(),
        "TTFT plus generation is the whole span the dashboard's rate figures need"
    );
    assert!(
        settlement.elapsed >= first,
        "elapsed covers entry to settlement"
    );
    assert_eq!(settlement.failure, None);

    // (b) a delivered failure item.
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"error\":{\"message\":\"boom\"}}\n\n".to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    while let Some(item) = stream.next().await {
        if item.is_err() {
            break;
        }
    }

    assert_eq!(settled.one().outcome, StreamOutcome::Failed);

    // (c) cancellation mid-stream.
    let address = provider(vec![Reply::spaced(
        vec![
            b"data: {\"id\":\"a\"}\n\n".to_vec(),
            DONE_FRAME.as_bytes().to_vec(),
        ],
        Duration::from_millis(20),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
    )])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();
    let signal = CancellationToken::new();

    let answer = transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "stream": true }),
            RequestOptions::new()
                .with_settlement(settled.hook())
                .with_signal(signal.clone()),
        )
        .await
        .expect("the head arrives");

    let mut stream = answer.into_stream().expect("a stream");
    signal.cancel();

    let failure = stream
        .next()
        .await
        .expect("an item")
        .expect_err("a cancelled stream reports the abort");
    assert_eq!(failure.code(), FailureKind::Aborted);

    let settlement = settled.one();
    assert_eq!(settlement.outcome, StreamOutcome::Cancelled);
    assert_eq!(
        settlement.failure.as_ref().map(StreamFailure::code),
        Some(FailureKind::Aborted)
    );

    // The stream is fused, so nothing settles a second time.
    assert!(stream.next().await.is_none());
    assert!(
        settled.take().is_empty(),
        "the settlement is not fired a second time"
    );
}

/// The usage rules of `trackStream` (`model-class.ts:838-853`): an explicit `0` is KNOWN (it adds no
/// tokens but still counts its span in the decode denominator), absent usage is not, and the last
/// frame that reports usage wins.
#[tokio::test]
async fn usage_rules_distinguish_absent_from_explicit_zero() {
    // Absent usage: the denominator must not be inflated.
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\",\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n".to_vec(),
        DONE_FRAME.as_bytes().to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(collect(&mut stream).await.len(), 1);

    let settlement = settled.one();
    assert_eq!(settlement.output_tokens, None);
    assert!(
        !settlement.usage_reported,
        "absent usage is not a report, so it must not inflate the decode denominator"
    );

    // Explicit zero: KNOWN, and it adds nothing. Both spellings are read, at both placements.
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\",\"usage\":{\"completion_tokens\":3}}\n\n".to_vec(),
        b"data: {\"id\":\"b\",\"usage\":{\"completion_tokens\":0}}\n\n".to_vec(),
        DONE_FRAME.as_bytes().to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(collect(&mut stream).await.len(), 2);

    let settlement = settled.one();
    assert_eq!(
        settlement.output_tokens,
        Some(0),
        "the LAST reporting frame wins"
    );
    assert!(settlement.usage_reported, "an explicit 0 is still a report");

    // The Responses surface reports output tokens on a nested terminal event
    // (`model-class.ts:846-853`).
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"output_tokens\":11}}}\n\n".to_vec(),
        DONE_FRAME.as_bytes().to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(collect(&mut stream).await.len(), 1);
    assert_eq!(settled.one().output_tokens, Some(11));
}

/// `model-class.ts:816-819` stamps TTFT on the FIRST frame that carries generated text - a role delta
/// or a usage-only frame is not output - and the transport uses the same predicate, asserted both
/// through the settlement and directly against `hasGeneratedPayload`.
#[tokio::test]
async fn the_first_generated_payload_stamps_ttft() {
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\",\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n".to_vec(),
        b"data: {\"id\":\"b\",\"usage\":{\"completion_tokens\":1}}\n\n".to_vec(),
        b"data: {\"id\":\"c\",\"choices\":[{\"delta\":{\"content\":\"go\"}}]}\n\n".to_vec(),
        DONE_FRAME.as_bytes().to_vec(),
    ])])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(collect(&mut stream).await.len(), 3);

    let settlement = settled.one();
    assert_eq!(settlement.chunks, 3);
    assert!(
        settlement.time_to_first_token.is_some(),
        "the third frame is the first GENERATED payload"
    );

    // The predicate itself, in both shapes (`hasGeneratedPayload`, `model-class.ts:206-241`).
    assert!(
        !has_generated_payload(&json!({ "choices": [{ "delta": { "role": "assistant" } }] })),
        "a role delta is not output"
    );
    assert!(
        !has_generated_payload(&json!({ "usage": { "completion_tokens": 1 } })),
        "usage alone is not output"
    );
    assert!(has_generated_payload(
        &json!({ "choices": [{ "delta": { "content": "go" } }] })
    ));
    assert!(
        has_generated_payload(&json!({
            "choices": [{ "delta": { "tool_calls": [{ "function": { "arguments": "{\"a\"" } }] } }]
        })),
        "a tool-call argument fragment is output (`model-class.ts:228-238`)"
    );
    assert!(has_generated_payload(
        &json!({ "choices": [{ "delta": { "reasoning_content": "hm" } }] })
    ));
    assert!(has_generated_payload(&json!({
        "type": "response.output_text.delta",
        "delta": "x"
    })));
    assert!(
        !has_generated_payload(&json!({ "type": "response.output_text.delta", "delta": "" })),
        "an empty delta is not output"
    );
    assert!(
        !has_generated_payload(&json!({ "type": "response.created", "delta": "x" })),
        "a non-generated `type` is not output"
    );
    assert!(
        !has_generated_payload(&json!({ "choices": [{ "delta": null }] })),
        "a null delta is not output"
    );
    assert!(
        !has_generated_payload(&json!({ "choices": null })),
        "a null `choices` is not output"
    );
}

/// The route task's open question, answered end to end: an SSE response needs NO axum `sse` feature.
/// The scripted answer is `Body::from_stream` plus two headers - both DEFAULT axum items - and the
/// transport decodes it through the same path a real provider uses. Adding `sse` to
/// `packages/typecrate/services` (which declares axum with `json,query,ws`) would be a feature grant
/// it does not need.
#[tokio::test]
async fn axum_sse_response_needs_no_sse_feature() {
    // TWO scripted answers: the upstream pops one reply per request, and this test makes two of them
    // - the raw GET probe first, then the transport's POST.
    let address = provider(vec![
        Reply::sse(vec![
            b"data: {\"id\":\"probe\"}\n\n".to_vec(),
            DONE_FRAME.as_bytes().to_vec(),
        ]),
        Reply::sse(vec![
            b"data: {\"id\":\"first\",\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n".to_vec(),
            b"data: {\"id\":\"second\",\"choices\":[{\"delta\":{\"content\":\"b\"}}],\"usage\":{\"completion_tokens\":2}}\n\n".to_vec(),
            DONE_FRAME.as_bytes().to_vec(),
        ]),
    ])
    .await;

    // The upstream's own headers are the DEFAULT-feature construction in `handle`/`response`.
    let probe = client()
        .get(format!("http://{address}/v1/probe"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .expect("the scripted upstream answers a plain GET");

    assert_eq!(probe.status(), StatusCode::OK);
    assert_eq!(
        probe
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );

    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));
    let settled = Settled::default();

    let answer = streaming(&transport, &settled).await;
    let mut stream = answer.into_stream().expect("a stream");

    assert_eq!(
        collect(&mut stream).await,
        vec![
            json!({ "id": "first", "choices": [{ "delta": { "content": "a" } }] }),
            json!({
                "id": "second",
                "choices": [{ "delta": { "content": "b" } }],
                "usage": { "completion_tokens": 2 }
            })
        ]
    );

    let settlement = settled.one();
    assert_eq!(settlement.chunks, 2);
    assert!(settlement.sentinel);
    assert_eq!(settlement.output_tokens, Some(2));
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

/// The anti-leak contract, end to end: the credential value appears in NO verdict, NO frame and NO
/// `Debug` this layer produces. The structures make this structural rather than a scrubbing step
/// (`Attempt` names a credential by INDEX, label and validity - `src/transport.rs:640-676`), and
/// `redact_secrets` catches an upstream that echoes the key back inside an error body
/// (`start.ts:419-439`).
#[tokio::test]
async fn no_credential_reaches_a_diagnostic_or_the_wire_detail() {
    let address = provider(vec![
        Reply::Json(
            StatusCode::BAD_REQUEST,
            json!({ "error": { "message": format!("invalid key {SECRET}") } }),
        ),
        Reply::Json(
            StatusCode::UNAUTHORIZED,
            json!({ "error": { "message": format!("Bearer {SECRET} is not valid") } }),
        ),
    ])
    .await;
    let transport = build_transport(&address, TestCredential::Keyed, Duration::from_millis(5));

    let failure = settled(&transport).await.expect_err("400 is a failure");

    // The upstream ECHOED the key; the message must not carry it.
    assert_eq!(
        failure.message(),
        format!("400 invalid key {REDACTION_MARKER}"),
        "`redactSecrets` rule 1 (`start.ts:421`) removes an `sk-` shaped key from the detail"
    );
    assert!(!format!("{failure:?}").contains(SECRET));
    assert!(!failure.error().detail().contains(SECRET));
    assert!(!format!("{:?}", failure.state()).contains(SECRET));
    assert!(
        !format!("{:?}", failure.state().attempted).contains(SECRET),
        "the ledger names a credential by index, label and validity only"
    );

    // The `Bearer …` spelling is redacted too, case-insensitively (rule 2, `start.ts:421`).
    let failure = settled(&transport).await.expect_err("401 is a failure");

    assert_eq!(
        failure.message(),
        format!("401 {REDACTION_MARKER} is not valid"),
        "the whole `Bearer …` span disappears, the scheme word included"
    );

    // A mid-stream frame built from an upstream echo is redacted before it can reach the wire.
    let frame = StreamFailure::upstream(
        &json!({ "message": format!("key {SECRET} rejected") }),
        4,
        Some("https://engine.example/v1"),
    );

    assert!(!frame.detail().contains(SECRET));
    assert!(!frame.sentence(None).contains(SECRET));
    assert!(!frame.payload(None).to_string().contains(SECRET));
    assert!(!format!("{frame:?}").contains(SECRET));

    // And the `Debug` of the type that OWNS the value.
    let credential = Credential::new(&Secret::new(SECRET));

    assert!(!format!("{credential:?}").contains(SECRET));
    assert_eq!(
        credential.expose(),
        Some(SECRET),
        "the value stays readable on purpose"
    );
    assert_eq!(
        credential.bearer().as_deref(),
        Some(format!("Bearer {SECRET}").as_str())
    );
    assert!(!format!("{:?}", RequestOptions::new()).contains(SECRET));
}

// ---------------------------------------------------------------------------
// Codec and vocabulary units (no listener needed)
// ---------------------------------------------------------------------------

/// `SseDecoder` is the port of the SDK decoder `node_modules/openai/core/streaming.js:222-285` (`iterSSEChunks` `:222-244` + `SSEDecoder` `:245-285`), and its contract is "any chunk boundary": a
/// frame only leaves when its newline has arrived, the tail still counts when the body ends, and
/// everything after the sentinel is not this stream's business.
#[test]
fn the_decoder_buffers_lines_and_stops_at_the_sentinel() {
    let mut decoder = SseDecoder::new();

    // A partial line is held, not emitted.
    assert_eq!(decoder.push(b"data: {\"id\":\"a"), Vec::new());
    assert!(
        !decoder.pending().is_empty(),
        "the unfinished line is buffered"
    );

    // Completing the line emits it; the blank separator line emits nothing.
    assert_eq!(
        decoder.push(b"\"}\n\n"),
        vec![SseEvent::Data("{\"id\":\"a\"}".to_owned())]
    );
    assert!(
        decoder.pending().is_empty(),
        "a completed line leaves the buffer"
    );

    // A comment line and a non-`data:` line carry no payload, and CRLF is tolerated.
    assert_eq!(
        decoder.push(b": keep-alive\r\nevent: message\r\n"),
        Vec::new()
    );
    assert_eq!(
        decoder.push(b"data: {\"id\":\"b\"}\r\n\r\n"),
        vec![SseEvent::Data("{\"id\":\"b\"}".to_owned())]
    );

    // Several frames in one segment arrive together, in order - and each becomes its own item.
    assert_eq!(
        decoder.push(b"data: 1\n\ndata: 2\n\n"),
        vec![
            SseEvent::Data("1".to_owned()),
            SseEvent::Data("2".to_owned())
        ]
    );

    // The sentinel finishes the decoder, and bytes after it are dropped on the floor.
    assert_eq!(
        decoder.push(b"data: [DONE]\n\ndata: {\"id\":\"late\"}\n\n"),
        vec![SseEvent::Done]
    );
    assert!(decoder.is_finished());
    assert!(
        decoder.pending().is_empty(),
        "the buffer is cleared at the sentinel"
    );
    assert_eq!(decoder.push(b"data: {\"id\":\"more\"}\n\n"), Vec::new());

    // A body that ends mid-line still has a frame to give (`harness/src/transport.rs:343-354`).
    let mut tail = SseDecoder::new();
    assert_eq!(tail.push(b"data: {\"id\":\"tail\"}"), Vec::new());
    assert_eq!(
        tail.finish(),
        vec![SseEvent::Data("{\"id\":\"tail\"}".to_owned())]
    );

    // An empty tail gives nothing, and `finish` after the sentinel gives nothing.
    let mut empty = SseDecoder::new();
    assert_eq!(empty.finish(), Vec::new());
    let mut done = SseDecoder::new();
    assert_eq!(done.push(DONE_FRAME.as_bytes()), vec![SseEvent::Done]);
    assert_eq!(done.finish(), Vec::new());

    // `data_payload` is the line classifier.
    assert_eq!(data_payload(b"data:x"), Some("x".to_owned()));
    assert_eq!(data_payload(b"  data:  y  "), Some("y".to_owned()));
    assert_eq!(
        data_payload(b"data: [DONE]"),
        Some(DONE_SENTINEL.to_owned())
    );
    assert_eq!(data_payload(b"data"), None);
    assert_eq!(data_payload(b"event: message"), None);
    assert_eq!(data_payload(b"data: \n"), Some("".to_owned()));
}

/// The emitted direction of the codec, which is what the provider surface writes
/// (`private-chat-completion.ts:163` and `:186`, which the gateway writes at `start.ts:853`): one `data:` frame per chunk,
/// then the sentinel, each terminated by a blank line.
#[test]
fn the_emitted_frames_are_the_pinned_strings() {
    assert_eq!(DONE_FRAME, "data: [DONE]\n\n");
    assert_eq!(FRAME_SEPARATOR, "\n\n");
    assert_eq!(DONE_SENTINEL, "[DONE]");

    assert_eq!(
        data_frame(&json!({ "id": "a" })),
        "data: {\"id\":\"a\"}\n\n",
        "the value is re-serialised, never echoed as upstream bytes"
    );

    assert_eq!(
        stream_body(&[json!({ "id": "a" }), json!({ "id": "b" })]),
        "data: {\"id\":\"a\"}\n\ndata: {\"id\":\"b\"}\n\ndata: [DONE]\n\n"
    );

    assert_eq!(
        stream_body(&[]),
        "data: [DONE]\n\n",
        "an empty stream still terminates the contract"
    );

    // The key order of the upstream document survives re-serialisation, which is what makes the
    // `serde_json` `preserve_order` feature load-bearing (`src/sse.rs:22-30`).
    assert_eq!(
        data_frame(&json!({ "object": "x", "id": "a" })),
        "data: {\"object\":\"x\",\"id\":\"a\"}\n\n"
    );
}

/// The frame sentences and wire key order for every class (`start.ts:672-702`), asserted with a FIXED
/// upstream so each sentence is a byte-exact expectation rather than a substring check.
#[test]
fn the_frame_contract_names_the_target_the_phase_and_the_detail() {
    let target = Some("https://engine.example/v1");

    // `upstream_status`: the status in the sentence and in the frame. The ONLY source of that class
    // is a real HTTP verdict on the response head (`TransportError::Status`, the counterpart of the
    // thrown `ApiError` whose own `status` `readUpstreamStatus` reads, `start.ts:506-517`) — an
    // in-band frame's `status` number never gets here, see
    // `a_mid_stream_error_frame_with_a_status_stays_unknown_and_carries_no_status`.
    let status = StreamFailure::transport(
        &TransportError::Status {
            status: StatusCode::TOO_MANY_REQUESTS,
            detail: "Too many requests".to_owned(),
        },
        1,
        target,
    );

    assert_eq!(
        status.sentence(None),
        "upstream https://engine.example/v1 returned HTTP 429 after 1 chunk: Too many requests"
    );
    assert_eq!(status.code(), FailureKind::UpstreamStatus);
    assert_eq!(status.status(), Some(StatusCode::TOO_MANY_REQUESTS));
    assert_eq!(status.chunks(), 1);
    assert_eq!(
        serde_json::to_string(&status.payload(None)).expect("a serialisable frame"),
        r#"{"error":{"message":"upstream https://engine.example/v1 returned HTTP 429 after 1 chunk: Too many requests","type":"stream_error","code":"upstream_status","status":429,"param":null}}"#,
        "the `status` key and its exact position (`start.ts:644`, `:695-702`)"
    );

    // The singular/plural phase distinction the wire prints (`start.ts:652-654`).
    let zero = StreamFailure::transport(
        &TransportError::Connection {
            detail: "connection reset".to_owned(),
        },
        0,
        target,
    );
    assert_eq!(
        zero.sentence(None),
        "upstream https://engine.example/v1 failed before the first chunk: connection reset"
    );
    assert_eq!(zero.code(), FailureKind::Connection);

    let many = StreamFailure::transport(
        &TransportError::Connection {
            detail: "other side closed".to_owned(),
        },
        4,
        target,
    );
    assert_eq!(
        many.sentence(None),
        "upstream https://engine.example/v1 closed the response body after 4 chunks: other side closed"
    );

    // `timeout` and `aborted`, whose SDK constants the transport's messages are.
    let timeout = StreamFailure::transport(
        &TransportError::Timeout {
            detail: TIMEOUT_MESSAGE.to_owned(),
        },
        2,
        target,
    );
    assert_eq!(
        timeout.sentence(None),
        "upstream https://engine.example/v1 timed out after 2 chunks: Request timed out."
    );
    assert_eq!(timeout.code(), FailureKind::Timeout);
    assert_eq!(
        TransportError::Timeout {
            detail: "request to https://engine.example/v1/chat/completions exceeded 1000ms"
                .to_owned()
        }
        .message(),
        TIMEOUT_MESSAGE,
        "the SDK's constant, not the transport's own wording (`openai/core/error.js:72-93`)"
    );

    let aborted = StreamFailure::transport(
        &TransportError::Aborted {
            detail: "operation was aborted".to_owned(),
        },
        2,
        target,
    );
    assert_eq!(
        aborted.sentence(None),
        "upstream https://engine.example/v1 was aborted after 2 chunks: operation was aborted"
    );
    assert_eq!(aborted.code(), FailureKind::Aborted);

    // `decode`.
    let decode = StreamFailure::transport(
        &TransportError::Decode {
            detail: "expected value at line 1 column 1".to_owned(),
        },
        3,
        target,
    );
    assert_eq!(
        decode.sentence(None),
        "upstream https://engine.example/v1 sent an undecodable payload after 3 chunks: expected value at line 1 column 1"
    );
    assert_eq!(decode.code(), FailureKind::Decode);

    // `unknown`: the message is the frozen constant whatever the upstream said (`start.ts:682-685`),
    // which is what keeps every consumer that string-matches the legacy frame working.
    let unknown = StreamFailure::upstream(
        &json!({ "message": "Something unclassifiable happened", "code": "rate_limit_exceeded" }),
        2,
        target,
    );
    assert_eq!(unknown.sentence(None), UNKNOWN_STREAM_ERROR_MESSAGE);
    assert_eq!(unknown.code(), FailureKind::Unknown);
    assert_eq!(
        unknown.detail(),
        "Something unclassifiable happened",
        "the detail half is still available for the log line"
    );

    // The route subject, which this layer cannot know and its caller supplies (`start.ts:647`).
    let routeless = StreamFailure::transport(
        &TransportError::Timeout {
            detail: String::new(),
        },
        0,
        None,
    );
    assert_eq!(
        routeless.sentence(Some("chat-completions")),
        "stream from endpoint chat-completions timed out before the first chunk"
    );
    assert_eq!(
        routeless.sentence(None),
        "stream from endpoint unknown timed out before the first chunk"
    );
    assert_eq!(routeless.upstream_target(), None);

    // The truncation guard: a VALUE the caller may ignore, classified `unknown` because nothing about
    // it is a transport or decode signal.
    let truncated = StreamFailure::truncated(2, target);
    assert_eq!(truncated.code(), FailureKind::Unknown);
    assert_eq!(truncated.sentence(None), UNKNOWN_STREAM_ERROR_MESSAGE);
    assert_eq!(
        truncated.upstream_target(),
        Some("https://engine.example/v1")
    );

    // An errno-carrying transport failure: `errno` joins the payload between `status` and `param`,
    // and is not duplicated into the sentence when the message already spells it
    // (`start.ts:659-663`, `:699-701`).
    let errno = StreamFailure::transport(
        &TransportError::Connection {
            detail: "connect ECONNREFUSED 127.0.0.1:8080".to_owned(),
        },
        0,
        target,
    );
    assert_eq!(errno.errno(), Some("ECONNREFUSED"));

    let payload = errno.payload(Some("responses"));

    assert_eq!(payload["error"]["errno"], Value::from("ECONNREFUSED"));
    assert_eq!(
        payload["error"]
            .as_object()
            .expect("an error object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["message", "type", "code", "errno", "param"],
        "no `status` key for a transport failure, and `errno` sits before `param`"
    );
    assert_eq!(
        payload["error"]["param"],
        Value::Null,
        "`param` is always present"
    );
    assert_eq!(payload["error"]["type"], Value::from("stream_error"));

    // The closed code set and its wire spellings, in `STREAM_ERROR_CODES` order (`start.ts:318-325`).
    assert_eq!(
        FAILURE_KINDS
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        [
            "timeout",
            "connection",
            "aborted",
            "upstream_status",
            "decode",
            "unknown"
        ]
    );
}

/// `classifyStreamError` (`start.ts:581-621`) is a precedence ladder, and the ladder IS the contract:
/// a timeout that contains an abort still classifies as a timeout.
#[test]
fn the_classifier_respects_the_precedence_ladder() {
    // Rule 1 outranks the abort it is implemented with (`start.ts:355-361`).
    let racing = FailureSignals {
        names: vec![
            "APIConnectionTimeoutError".to_owned(),
            "AbortError".to_owned(),
        ],
        errno: None,
        status: Some(StatusCode::PAYLOAD_TOO_LARGE),
        messages: vec!["operation was aborted due to timeout".to_owned()],
    };
    assert_eq!(classify_stream_error(&racing), FailureKind::Timeout);

    let abort = FailureSignals {
        names: vec!["AbortError".to_owned()],
        ..FailureSignals::default()
    };
    assert_eq!(classify_stream_error(&abort), FailureKind::Aborted);

    let status = FailureSignals {
        status: Some(StatusCode::INTERNAL_SERVER_ERROR),
        ..FailureSignals::default()
    };
    assert_eq!(
        classify_stream_error(&status),
        FailureKind::UpstreamStatus,
        "a definite verdict outranks every heuristic"
    );

    let errno = FailureSignals {
        errno: Some("ECONNRESET".to_owned()),
        ..FailureSignals::default()
    };
    assert_eq!(classify_stream_error(&errno), FailureKind::Connection);

    let decode = FailureSignals {
        names: vec!["SyntaxError".to_owned()],
        ..FailureSignals::default()
    };
    assert_eq!(classify_stream_error(&decode), FailureKind::Decode);

    let odd = FailureSignals {
        errno: Some("EUNKNOWN".to_owned()),
        ..FailureSignals::default()
    };
    assert_eq!(
        classify_stream_error(&odd),
        FailureKind::Connection,
        "an errno outside the enumerated sets is still a transport failure"
    );

    let wording = FailureSignals {
        messages: vec!["fetch failed".to_owned()],
        ..FailureSignals::default()
    };
    assert_eq!(classify_stream_error(&wording), FailureKind::Connection);

    let soft_timeout = FailureSignals {
        messages: vec!["the deadline exceeded our budget".to_owned()],
        ..FailureSignals::default()
    };
    assert_eq!(classify_stream_error(&soft_timeout), FailureKind::Timeout);

    let nothing = FailureSignals {
        messages: vec!["Rate limit exceeded".to_owned()],
        ..FailureSignals::default()
    };
    assert_eq!(classify_stream_error(&nothing), FailureKind::Unknown);

    // The SDK-shaped verdicts this crate's own errors produce, which is why
    // `TransportError::kind()` stays one implementation.
    assert_eq!(
        TransportError::Timeout {
            detail: "request exceeded 1000ms".to_owned()
        }
        .kind(),
        FailureKind::Timeout
    );
    assert_eq!(
        TransportError::Aborted {
            detail: "operation was aborted".to_owned()
        }
        .kind(),
        FailureKind::Aborted
    );
    assert_eq!(
        TransportError::Connection {
            detail: "connect ECONNREFUSED".to_owned()
        }
        .kind(),
        FailureKind::Connection
    );
    assert_eq!(
        TransportError::Status {
            status: StatusCode::NOT_FOUND,
            detail: "model not found".to_owned()
        }
        .kind(),
        FailureKind::UpstreamStatus
    );
    assert_eq!(
        TransportError::Build {
            detail: "header name is invalid".to_owned()
        }
        .kind(),
        FailureKind::Unknown,
        "a build failure carries NO status, which is what makes the route render 500"
    );
    assert_eq!(
        TransportError::Build {
            detail: "header name is invalid".to_owned()
        }
        .status(),
        None
    );
}

/// The text hygiene the frame contract depends on: redaction, flattening, capping, target sanitising
/// and the errno tables (`start.ts:421-466`, `:352-419`).
#[test]
fn the_text_hygiene_rules_are_the_deployed_ones() {
    assert_eq!(
        redact_secrets(&format!("key {SECRET} and sk-abc")),
        format!("key {REDACTION_MARKER} and sk-abc"),
        "rule 1 needs SIX characters after `sk-`"
    );
    assert_eq!(
        redact_secrets("Authorization: bearer  x"),
        format!("Authorization: {REDACTION_MARKER}"),
        "rule 2 is case-insensitive and swallows the scheme word too"
    );
    assert_eq!(redact_secrets("no secrets here"), "no secrets here");

    assert_eq!(
        flatten_error_detail("  line\n one\r\n  line two   "),
        "line one line two",
        "whitespace collapses FIRST, so an SSE frame can never carry a newline"
    );

    let capped = flatten_error_detail(&"x".repeat(MAX_ERROR_DETAIL_LENGTH + 40));
    assert_eq!(capped.chars().count(), MAX_ERROR_DETAIL_LENGTH + 3);
    assert!(
        capped.ends_with("..."),
        "the truncation marker is the literal `...`"
    );

    // A target is reported as the transport dialed it: userinfo and query never survive.
    assert_eq!(
        sanitize_upstream_target(
            "https://user:sk-live-9f3a7c21be4d0000feed@engine.example/v1?token=abc#frag"
        ),
        Some("https://engine.example/v1".to_owned())
    );
    assert_eq!(
        sanitize_upstream_target("http://127.0.0.1:8080/v1"),
        Some("http://127.0.0.1:8080/v1".to_owned())
    );
    assert_eq!(
        sanitize_upstream_target("http://127.0.0.1:8080/"),
        Some("http://127.0.0.1:8080".to_owned()),
        "a bare `/` path is not part of the identity"
    );
    assert_eq!(
        sanitize_upstream_target("relay:alpha?x=1"),
        Some("relay:alpha".to_owned())
    );
    assert_eq!(sanitize_upstream_target("   "), None);

    // The errno tables.
    assert!(is_libuv_errno("ECONNREFUSED"));
    assert!(is_libuv_errno("E123"));
    assert!(!is_libuv_errno("E"), "a bare `E` is not an errno");
    assert!(!is_libuv_errno("errno"), "the shape is uppercase");
    assert_eq!(
        find_transport_errno("read ECONNRESET while connecting"),
        Some("ECONNRESET".to_owned())
    );
    assert_eq!(
        find_transport_errno("insufficient_quota"),
        None,
        "a provider code is not a transport errno"
    );
    assert_eq!(
        find_transport_errno("UND_ERR_SOCKET hang up"),
        Some("UND_ERR_SOCKET".to_owned())
    );
}

/// `estimateRequestTokens` (`model-class.ts:82-85`) is a character-count heuristic including the
/// surrogate-pair doubling, and it is what the accounting seam charges per request.
#[test]
fn the_request_token_estimate_is_the_heuristic() {
    // 48 characters, four units each.
    assert_eq!(
        estimate_request_tokens(&json!({ "messages": [{ "role": "user", "content": "hello" }] })),
        12
    );

    // An astral character counts as TWO, which is the JS `.length` behaviour the port reproduces:
    // `{"c":"` (6) + 2 + `"}` (2) = 10 -> 3.
    assert_eq!(estimate_request_tokens(&json!({ "c": "\u{1F600}" })), 3);

    // `JSON.stringify({})` is `"{}"`, two units, and `Math.ceil(2 / 4)` is one - the estimator is a
    // quarter of the SERIALIZED document, never a constant floor (`model-class.ts:82-85`).
    assert_eq!(estimate_request_tokens(&json!({})), 1);

    // `output_tokens_of` reads both spellings and tolerates a float.
    assert_eq!(
        output_tokens_of(&json!({ "completion_tokens": 7 })),
        Some(7)
    );
    assert_eq!(output_tokens_of(&json!({ "output_tokens": 9 })), Some(9));
    assert_eq!(
        output_tokens_of(&json!({ "completion_tokens": 7.9 })),
        Some(7)
    );
    assert_eq!(output_tokens_of(&json!({ "completion_tokens": "7" })), None);
    assert_eq!(output_tokens_of(&json!({})), None);
}

// ---------------------------------------------------------------------------
// The dependency-grant gate (acceptance item A8, kept in the repository)
// ---------------------------------------------------------------------------

/// This crate is a STANDALONE workspace with no vendored registry, so every package its lockfile
/// names must already appear in a sibling lockfile: otherwise a checkout whose cargo cache was seeded
/// by `packages/typecrate/harness` or `packages/typecrate/services` could not build this crate
/// offline. `Temp/ptp-transport/lockset.py` produced this same result at authoring time; this test
/// keeps the rule enforced from here on.
#[test]
fn the_lock_package_set_is_a_strict_subset_of_the_siblings() {
    let lockfile = package_names(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.lock"));
    let provider: BTreeSet<&str> = lockfile
        .iter()
        .map(String::as_str)
        // The workspace's own root package is in no sibling lock by definition.
        .filter(|name| *name != env!("CARGO_PKG_NAME"))
        .collect();

    let siblings = [
        (
            "harness",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../harness/Cargo.lock"),
        ),
        (
            "services",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../services/Cargo.lock"),
        ),
    ];

    for (label, path) in siblings {
        let names = package_names(path);
        let sibling: BTreeSet<&str> = names
            .iter()
            .map(String::as_str)
            .filter(|name| *name != label)
            .collect();

        let missing: Vec<&&str> = provider
            .iter()
            .filter(|name| !sibling.contains(**name))
            .collect();

        assert!(
            missing.is_empty(),
            "{label} is missing {} packages this lockfile needs: {missing:?}",
            missing.len()
        );
        assert!(
            sibling.len() > provider.len(),
            "{label} should carry more packages than this crate, i.e. the subset is strict: \
             provider={} sibling={}",
            provider.len(),
            sibling.len()
        );
    }
}

/// The `name = "…"` set of one `Cargo.lock`.
fn package_names(path: &str) -> Vec<String> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{path}: {error}"));

    text.lines()
        .filter_map(|line| line.trim().strip_prefix("name = "))
        .map(|value| value.trim_matches('"').to_owned())
        .collect()
}
