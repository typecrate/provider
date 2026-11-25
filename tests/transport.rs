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
        Arc, Barrier, Mutex, OnceLock,
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

/// Rotation, through the documented SEAM, is forward-only over the pool, and on exhaustion the LAST
/// entry stays in force - `local-strata.ts:244-250` returns false WITHOUT clearing `baseURL`.
///
/// What changed with the concurrency fix is what the endpoint cursor MEANS: it is a verified-prefix
/// head, so `rotate_endpoint` publishes past the entry the head itself names rather than claiming a
/// slot per caller (`src/transport.rs::Transport::rotate_endpoint` explains why production rotation no
/// longer runs through it). The observable walk of a SINGLE caller is identical, which is what the
/// assertions below pin - and the head is now asserted alongside the selection, because "one step per
/// call" is the property the old claim-a-slot cursor could not promise under load.
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
    assert_eq!(
        transport.endpoint_head(),
        1,
        "one seam step publishes exactly one entry as verified"
    );
    assert!(!transport.endpoints_exhausted());

    assert_eq!(
        transport
            .rotate_endpoint()
            .map(|endpoint| endpoint.base_url().to_owned()),
        Some("https://three.example/v1".to_owned())
    );
    assert_eq!(transport.endpoint_head(), 2);
    assert!(transport.endpoints_exhausted());

    // Exhaustion: no endpoint left, and the cursor STAYS on the last one.
    assert_eq!(transport.rotate_endpoint(), None);
    assert_eq!(
        transport.endpoint_head(),
        2,
        "a refused rotation publishes nothing: there was no entry to verify"
    );
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
    assert_eq!(
        transport.endpoint_head(),
        0,
        "the seam rewinds the head as well"
    );
    assert_eq!(transport.credential_index(), Some(0));

    // The HEAD is shared by clone: a cloned transport is the same singleton, which is how the
    // TypeScript clients behave (process-wide, one credential/base-URL pair that mutates in place).
    // What a clone does NOT share is a walk position - no such state exists on the transport - so two
    // clones starting at the same head dial the same entry, which is the lockstep the contract chose
    // over per-caller slot claiming.
    let clone = transport.clone();
    assert!(clone.rotate_endpoint().is_some());
    assert_eq!(
        transport.endpoint_index(),
        Some(1),
        "the cursors are shared, not copied"
    );
    assert_eq!(
        transport.endpoint_head(),
        1,
        "and the clone's publish is visible through the original, because it is the same head"
    );
}

/// A transport over `relays`, one labelled endpoint per name, so the captured request path proves
/// which relay an attempt reached. The pool shape `build_transport` does not give: one listener,
/// several NAMED bases.
fn pool_transport(
    address: &str,
    relays: &[&str],
    credential: TestCredential,
    pause: Duration,
) -> Transport {
    let endpoints = relays
        .iter()
        .map(|relay| Endpoint::labeled(&format!("http://{address}/{relay}"), relay))
        .collect();

    Transport::with_http_client(
        Target::new(endpoints, vec![credential.build()]),
        Policy::standard().with_rate_limit_pause(pause),
        client(),
    )
}

/// THE PIN: a caller that names the endpoint it wants dials THAT endpoint, and the shared selection
/// neither moves nor gets consulted.
///
/// This is the transport half of the per-request walk (`src/transport.rs::RequestOptions::endpoint`):
/// the transport must not re-read shared state to pick a dial target while a request is in flight. The
/// contrast is asserted in one test - pinned call dials the third relay, unpinned call dials the pool
/// head - and both leave the head exactly where they found it, because only a 404 the CALLER collected
/// is allowed to publish.
#[tokio::test]
async fn a_pinned_request_dials_the_pin_and_never_the_shared_selection() {
    let address = provider(vec![
        Reply::Json(StatusCode::OK, json!({ "id": "cmpl-pinned" })),
        Reply::Json(StatusCode::OK, json!({ "id": "cmpl-head" })),
    ])
    .await;

    let transport = pool_transport(
        &address,
        &["relay-one", "relay-two", "relay-three"],
        TestCredential::Keyed,
        Duration::from_millis(5),
    );

    let pinned = transport
        .endpoint_at(2)
        .expect("the third relay of the pool exists");

    transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "messages": [] }),
            RequestOptions::new().with_endpoint(pinned),
        )
        .await
        .expect("the pinned relay answers");

    let seen = ledger(&address).seen();

    assert_eq!(
        seen[0].path, "/relay-three/v1/chat/completions",
        "the PIN decided the dial target, not the shared selection sitting on relay-one"
    );
    assert_eq!(
        transport.endpoint_index(),
        Some(0),
        "and dialling a pinned entry advanced nothing"
    );
    assert_eq!(
        transport.endpoint_head(),
        0,
        "a success publishes nothing, pinned or otherwise"
    );

    // The same transport, unpinned: model discovery and the direct-driving suites keep the fallback,
    // which is the shared selection as it stands when the CALL starts.
    transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "messages": [] }),
            RequestOptions::new(),
        )
        .await
        .expect("the pool head answers");

    assert_eq!(
        ledger(&address).seen()[1].path,
        "/relay-one/v1/chat/completions",
        "an unpinned call dials the selection"
    );
}

/// An INTERNAL retry stays on the pinned endpoint.
///
/// The transport retries 429s, timeouts and dead sockets by itself, and a retry that silently moved to
/// another upstream would break the client layer's per-request walk: the client logs and publishes
/// against the endpoint IT pinned, so every attempt of one `post` call has to hit that same entry. The
/// scripted 429 then 200 pair below is the cheapest way to make two attempts happen inside one call.
#[tokio::test]
async fn a_pinned_retry_stays_on_the_pinned_endpoint() {
    let address = provider(vec![
        Reply::Json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "error": { "message": "slow down" } }),
        ),
        Reply::Json(StatusCode::OK, json!({ "id": "cmpl-after-pause" })),
    ])
    .await;

    // A zero pause: the retry is the behaviour under test, not the wait (`tests/transport.rs`'s own
    // 429 test takes the same shortcut for the same reason).
    let transport = pool_transport(
        &address,
        &["relay-one", "relay-two", "relay-three"],
        TestCredential::Keyed,
        Duration::ZERO,
    );

    let pinned = transport
        .endpoint_at(2)
        .expect("the third relay of the pool exists");

    transport
        .post(
            CHAT_COMPLETIONS_PATH,
            &json!({ "model": "m", "messages": [] }),
            RequestOptions::new().with_endpoint(pinned),
        )
        .await
        .expect("the retry succeeds on the same relay");

    let attempts = ledger(&address).attempts();

    assert_eq!(attempts, 2, "the 429 consumed one retry inside this call");
    assert_eq!(
        ledger(&address)
            .seen()
            .iter()
            .map(|attempt| attempt.path.clone())
            .collect::<Vec<String>>(),
        vec![
            "/relay-three/v1/chat/completions".to_owned(),
            "/relay-three/v1/chat/completions".to_owned()
        ],
        "both attempts dialed the pinned relay: an internal retry never changes the upstream"
    );
    assert_eq!(
        transport.endpoint_head(),
        0,
        "and the shared head is untouched by another request's retries"
    );
}

/// Credential rotation raced from twenty threads: every racer is handed the credential IT claimed, no
/// credential is handed out twice, and none is skipped.
///
/// The endpoint axis moved to a `fetch_max` head precisely because it does not hand out slots; the
/// credential axis still does, because taking a key out of the pool takes it out for every caller
/// (`nvidia-glm5-flash.ts:13-18`, `makora-glm-5-3.ts:6-11`). That is what makes the exactness of
/// `advance` observable from outside the module, and the assertion is an exact multiset rather than a
/// property: eight entries give seven claims, so twenty racers must produce those seven values once
/// each plus thirteen exhaustion answers. The pre-fix `advance` returned a post-hoc `load`, so two
/// racers could report the same later slot and the entry neither of them claimed went unserved.
///
/// The primitive-level version of this test is the inline unit in `src/transport.rs`
/// (`transport::tests::advance_hands_each_racer_the_slot_it_claimed`), which lives there because
/// `advance` itself is private.
#[test]
fn credential_rotation_hands_every_racer_the_credential_it_claimed() {
    let pool: Vec<Credential> = (0..8)
        .map(|index| Credential::new(&Secret::new(format!("sk-contention-key-{index:02}"))))
        .collect();

    let transport = Transport::with_policy(
        Target::new(vec![Endpoint::new("https://one.example/v1")], pool.clone()),
        Policy::standard(),
    );

    let racers = 20_usize;
    let barrier = Barrier::new(racers);

    let claims: Vec<Option<String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..racers)
            .map(|_| {
                let transport = transport.clone();
                // A `Copy` reference into the scope, since twenty `move` closures cannot each take the
                // `Barrier` itself.
                let gate: &Barrier = &barrier;

                scope.spawn(move || {
                    gate.wait();

                    transport
                        .rotate_credential()
                        .and_then(|credential| credential.expose().map(str::to_owned))
                })
            })
            .collect();

        // Spawned before joined, or the barrier never opens.
        handles
            .into_iter()
            .map(|handle| handle.join().expect("the racing thread did not panic"))
            .collect()
    });

    let mut served: Vec<String> = claims.iter().flatten().cloned().collect();
    served.sort();

    assert_eq!(
        served,
        (1..8)
            .map(|index| format!("sk-contention-key-{index:02}"))
            .collect::<Vec<String>>(),
        "seven claims for seven slots: every credential handed out exactly once, and never the pool \
         head, which is the entry the first rotation left behind"
    );
    assert_eq!(
        claims.iter().filter(|claim| claim.is_none()).count(),
        racers - 7,
        "the racers beyond the last claim get the exhaustion answer, not a repeat of someone else's \
         credential"
    );
    assert_eq!(
        transport.credential_index(),
        Some(7),
        "and the cursor ends on the last entry, the documented exhaustion fallback"
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

/// The residual `unknown`: an in-band error frame that declares NOTHING the classifier can use — no
/// numeric `error.status`, no error class in `UPSTREAM_TYPE_STATUSES` (`src/error.rs`), no
/// errno-shaped `code`, and no wording needle — stays the `unknown` class (`start.ts:581-621`, rule 9)
/// and its payload still carries NO `status` key (`start.ts:644`, `:699`). Note what that list now
/// excludes: a frame with a status, or with `type: server_error`/`invalid_request_error`/
/// `rate_limit_error`/…, is `upstream_status` (see the two tests below), so this class is the narrow
/// residue it was not before 2026-11-24 — and the table has since grown twice more, which is why the
/// frame below names `insufficient_quota`: a real OpenAI class the closed table deliberately refuses to
/// price. What changed even for the residue is the SENTENCE: the frozen constant stays the LEAD, so a
/// consumer string-matching the legacy frame keeps matching, and the upstream's own text is tailed
/// after `: ` — a deployed `unknown` that quoted nothing was the log nobody could read (`src/error.rs`,
/// the 2026-11-24 deviation in its module header).
#[tokio::test]
async fn a_mid_stream_error_frame_without_a_status_is_an_unknown_code() {
    let address = provider(vec![Reply::sse(vec![
        b"data: {\"id\":\"a\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n".to_vec(),
        b"data: {\"error\":{\"message\":\"You exceeded your current quota, please check your plan and billing details\",\"type\":\"insufficient_quota\",\"code\":\"insufficient_quota\"}}\n\n".to_vec(),
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
                // The frozen constant LEADS and the upstream's own sentence follows it: the class
                // may not know a code, but it is not allowed to throw away what the model server
                // said (`src/error.rs`, the `unknown` arm of `sentence`).
                "message": "Upstream stream terminated: You exceeded your current quota, please check your plan and billing details",
                "type": "stream_error",
                "code": "unknown",
                "param": null
            }
        }),
        "the frame exactly as `start.ts:695-702` stamps it, with the 2026-11-24 detail tail"
    );
    assert!(
        payload["error"]["message"]
            .as_str()
            .expect("a message")
            .starts_with(UNKNOWN_STREAM_ERROR_MESSAGE),
        "the legacy prefix is intact for a consumer that string-matches it"
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

/// An in-band `{"error":{…}}` frame that WRITES a `status` is now the `upstream_status` class, and
/// the frame carries that `status` key on the wire.
///
/// THE RULE THIS REPLACED, AND WHY IT CHANGED ON 2026-11-24. Until then this crate reproduced the
/// SDK byte for byte: the SDK wraps an in-band payload in
/// `new APIError(undefined, data.error, undefined, response.headers)`
/// (`node_modules/openai/core/streaming.js:50`) — the third argument is the status, and `APIError`
/// assigns exactly what it is given (`node_modules/openai/core/error.js:12`), so `err.status` is
/// `undefined`; `readUpstreamStatus` (`packages/underload/service/src/server/start.ts:506-517`)
/// therefore finds nothing, the status arm of `classifyStreamError` cannot fire, and `start.ts:644`
/// leaves the `status` key out of the frame literal. Reproducing that loss meant a verdict the model
/// server stated plainly — 429 here, and in the field a dead engine or a rejected prompt — reached
/// the operator as a bare `unknown`. The status an in-band frame declares IS an upstream verdict, and
/// the consumer's retry decision reads it (`providerWireFailureOf`,
/// `packages/underload/harness/src/engine/transport/provider-timeouts.ts:666`), so the crate now
/// lifts it (`src/error.rs`, `FailureSignals::from_upstream_frame`). The TS gateway's own behaviour is
/// unchanged; this crate answers more than it used to.
#[tokio::test]
async fn a_mid_stream_error_frame_with_a_status_is_an_upstream_status_frame() {
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
        FailureKind::UpstreamStatus,
        "the status the frame declares is a status signal now (`src/error.rs`)"
    );
    assert_eq!(
        failure.status(),
        Some(StatusCode::TOO_MANY_REQUESTS),
        "the number the upstream wrote is the emitted status, unmodified"
    );
    assert_eq!(
        failure.frame_status(),
        Some(StatusCode::TOO_MANY_REQUESTS),
        "and it is kept verbatim beside it (`src/error.rs`, `StreamFailure::frame_status`), which \
         is what distinguishes a sent status from an inferred one in the log line"
    );
    assert!(failure.is_upstream());
    assert_eq!(failure.chunks(), 1, "one chunk reached the consumer first");
    assert_eq!(
        failure.errno(),
        None,
        "`rate_limit` is an upstream class, not a transport errno"
    );

    let payload = failure.payload(Some("responses"));

    assert_eq!(
        payload,
        json!({
            "error": {
                "message": format!(
                    "upstream http://{address}/v1/chat/completions returned HTTP 429 after 1 chunk: Too many requests"
                ),
                "type": "stream_error",
                "code": "upstream_status",
                "status": 429,
                "param": null
            }
        }),
        "the sentence names the status the frame declared, and the payload stamps the `status` key"
    );
    assert_eq!(
        payload["error"]
            .as_object()
            .expect("an error object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["message", "type", "code", "status", "param"],
        "`status` sits between `code` and `param`, the position `start.ts:699` stamps it"
    );
    assert_eq!(
        serde_json::to_string(&payload).expect("a serialisable frame"),
        format!(
            r#"{{"error":{{"message":"upstream http://{address}/v1/chat/completions returned HTTP 429 after 1 chunk: Too many requests","type":"stream_error","code":"upstream_status","status":429,"param":null}}}}"#
        ),
        "the exact bytes a browser reads: the 429 the upstream wrote is on the wire twice, in the \
         message and in its own key"
    );
}

/// What the deployed failure actually looked like, and the whole reason the 2026-11-24 rule exists:
/// the local relay reports a dead engine IN-BAND, as
/// `{"error":{"type":"server_error","message":"the engine stopped unexpectedly (exit code 1)…"}}`
/// followed by `[DONE]` (`Temp/basalt-check/serve/handler.py:732-737`), and under the SDK-parity rule
/// that frame came out of the gateway as `code: "unknown"` with the frozen sentence and nothing else —
/// the log line this crate used to print said only `unknown: Upstream stream terminated`.
///
/// It now classifies as `upstream_status` from the CLASS the frame names, the upstream's own sentence
/// survives into the wire `message`, and the log line names every fact the frame hides.
#[test]
fn an_in_band_server_error_frame_becomes_an_upstream_status_failure() {
    let engine_died = "the engine stopped unexpectedly (exit code 1); the next request restarts it";
    let target = Some("https://basalt.example/v1");

    let failure = StreamFailure::upstream(
        &json!({ "type": "server_error", "message": engine_died }),
        0,
        target,
    );

    assert_eq!(failure.code(), FailureKind::UpstreamStatus);
    assert_eq!(
        failure.status(),
        Some(StatusCode::INTERNAL_SERVER_ERROR),
        "`server_error` states a 500 (`src/error.rs`, `UPSTREAM_TYPE_STATUSES`)"
    );
    assert_eq!(
        failure.frame_status(),
        None,
        "the status is INFERRED, so the log must not claim the frame wrote a number"
    );
    assert!(
        failure.is_upstream(),
        "the model was reached, so this is not fatal"
    );
    assert_eq!(failure.chunks(), 0);
    assert_eq!(
        failure.detail(),
        engine_died,
        "the upstream's own words survive"
    );

    assert_eq!(
        failure.sentence(Some("chat-completion")),
        format!(
            "upstream https://basalt.example/v1 returned HTTP 500 before the first chunk: {engine_died}"
        )
    );
    assert_eq!(
        serde_json::to_string(&failure.payload(Some("chat-completion"))).expect("a frame"),
        format!(
            r#"{{"error":{{"message":"upstream https://basalt.example/v1 returned HTTP 500 before the first chunk: {engine_died}","type":"stream_error","code":"upstream_status","status":500,"param":null}}}}"#
        ),
        "the consumer sees a coded, status-bearing frame instead of a bare `unknown`"
    );
    assert_eq!(
        failure.log_line(Some("chat-completion")),
        "stream error in chat-completion: code=upstream_status origin=upstream chunks=0 \
         upstream=https://basalt.example/v1 status=500 \
         detail=\"the engine stopped unexpectedly (exit code 1); the next request restarts it\"",
        "one greppable line with the code, the origin, the chunk count, the target, the status and \
         the reason (`src/error.rs`, `StreamFailure::log_line`)"
    );

    // An upstream that echoes the caller's key back inside its error body must not put that key in
    // the log: `detail` is redacted at CAPTURE, and the log line quotes `detail` and nothing else.
    let echoed = StreamFailure::upstream(
        &json!({
            "type": "server_error",
            "message": "engine refused the key sk-live-secretvalue123"
        }),
        0,
        target,
    );

    assert!(
        !echoed
            .log_line(Some("chat-completion"))
            .contains("sk-live-secretvalue123"),
        "a credential value reached the log line: {}",
        echoed.log_line(Some("chat-completion"))
    );
    assert!(
        echoed
            .log_line(Some("chat-completion"))
            .contains(REDACTION_MARKER),
        "the redaction marker is what replaces it"
    );
}

/// The inference table is CLOSED, and a numeric `error.status` always outranks it — both halves are
/// the contract, because an open-ended inference would guess at vendor spellings this crate has never
/// seen.
#[test]
fn an_in_band_error_class_infers_only_a_listed_status_and_a_number_always_wins() {
    // Every row of `UPSTREAM_TYPE_STATUSES` (`src/error.rs`), asserted as data rather than sampled.
    for (class, status) in [
        ("server_error", 500_u16),
        ("model_busy", 409),
        ("invalid_request_error", 400),
        ("authentication_error", 401),
        ("forbidden", 403),
        ("not_found", 404),
        // The two rows added on 2026-11-24 after review: the class a throttling proxy relays, and the
        // size-refusal spelling, which prices 400 — the same verdict the sentence rule and the local
        // server's `/v1/responses` path give the identical fault
        // (`Temp/basalt-check/serve/handler.py:774-776`), not `server_error`'s 500.
        ("rate_limit_error", 429),
        ("context_length_exceeded", 400),
    ] {
        let inferred = StreamFailure::upstream(
            &json!({ "type": class, "message": "the relay named this class" }),
            0,
            None,
        );

        assert_eq!(inferred.code(), FailureKind::UpstreamStatus, "{class}");
        assert_eq!(
            inferred.status(),
            Some(StatusCode::from_u16(status).expect("a status the table bounds")),
            "{class} states {status}"
        );
    }

    // A class outside the table infers NOTHING: `usage_limit_exceeded` is a real OpenAI spelling this
    // crate deliberately refuses to guess a status for, so the frame keeps the old verdict.
    let unmapped = StreamFailure::upstream(
        &json!({ "type": "usage_limit_exceeded", "message": "Too many requests" }),
        0,
        None,
    );

    assert_eq!(unmapped.code(), FailureKind::Unknown);
    assert_eq!(
        unmapped.status(),
        None,
        "no number, no listed class, no status"
    );

    // A number the frame WROTE beats the class: an upstream that sends `server_error` with a 400
    // means 400, and the number is kept verbatim as `frame_status` as well.
    let explicit = StreamFailure::upstream(
        &json!({ "type": "server_error", "status": 400, "message": "unknown field `foo`" }),
        0,
        None,
    );

    assert_eq!(explicit.code(), FailureKind::UpstreamStatus);
    assert_eq!(explicit.status(), Some(StatusCode::BAD_REQUEST));
    assert_eq!(explicit.frame_status(), Some(StatusCode::BAD_REQUEST));
    assert_eq!(
        explicit.log_line(Some("responses")),
        "stream error in responses: code=upstream_status origin=upstream chunks=0 status=400 \
         detail=\"unknown field `foo`\"",
        "one status on the line when the frame's number and the emitted status agree"
    );

    // A status outside the range `readUpstreamStatus` accepts (`start.ts:506-517`: an integer in
    // 100..=599) is not a status, and a `type` that is not a string infers nothing — both are hostile
    // upstream shapes, and neither may throw or invent a class.
    let out_of_range = StreamFailure::upstream(
        &json!({ "status": 999, "message": "not a status" }),
        0,
        None,
    );

    assert_eq!(out_of_range.code(), FailureKind::Unknown);
    assert_eq!(out_of_range.status(), None);

    let object_type = StreamFailure::upstream(
        &json!({ "type": { "nested": true }, "message": "a non-string class" }),
        0,
        None,
    );

    assert_eq!(object_type.code(), FailureKind::Unknown);
    assert_eq!(object_type.status(), None);
}

/// The two cases where the emitted `status` and the status the frame WROTE legitimately differ, and
/// the log line has to say both — this is why `frame_status` survived the 2026-11-24 change instead of
/// collapsing into `status()`.
#[test]
fn a_status_a_stronger_rule_withholds_is_still_named_in_the_log() {
    // An errno-shaped `code` is a TIMEOUT verdict by rule 1 of `classify_stream_error`, so the class
    // withholds the status the frame wrote (`start.ts:643`: a status belongs to `upstream_status`
    // alone). Without `frame_status` the upstream's own 503 would be gone from every diagnostic.
    let timeout = StreamFailure::upstream(
        &json!({ "code": "ETIMEDOUT", "status": 503, "message": "the engine ran out of time" }),
        3,
        Some("https://basalt.example/v1"),
    );

    assert_eq!(timeout.code(), FailureKind::Timeout);
    assert_eq!(
        timeout.status(),
        None,
        "the class withholds a status it is not allowed to carry"
    );
    assert_eq!(
        timeout.frame_status(),
        Some(StatusCode::SERVICE_UNAVAILABLE),
        "and the number the frame wrote is kept"
    );
    assert_eq!(timeout.errno(), Some("ETIMEDOUT"));
    assert_eq!(
        timeout.log_line(Some("chat-completion")),
        "stream error in chat-completion: code=timeout origin=upstream chunks=3 \
         upstream=https://basalt.example/v1 frame_status=503 errno=ETIMEDOUT \
         detail=\"the engine ran out of time\"",
        "the withheld status appears as `frame_status`, next to the errno that outranked it"
    );

    // A transport failure prints the OTHER origin, which is the fact a supervisor branches on
    // (`src/error.rs`, `StreamOrigin`).
    let broken = StreamFailure::transport(
        &TransportError::Connection {
            detail: "connection reset by peer".to_owned(),
        },
        0,
        Some("https://basalt.example/v1"),
    );

    assert_eq!(
        broken.log_line(Some("responses")),
        "stream error in responses: code=connection origin=transport chunks=0 \
         upstream=https://basalt.example/v1 detail=\"connection reset by peer\""
    );

    // A quote inside an upstream detail stays ONE greppable field: the log escapes `"` rather than
    // ending the quoted value halfway through the reason.
    let quoted = StreamFailure::upstream(&json!({ "message": "engine said \"stop\"" }), 0, None);

    assert_eq!(
        quoted.log_line(None),
        "stream error in unknown: code=unknown origin=upstream chunks=0 \
         detail=\"engine said \\\"stop\\\"\"",
        "no endpoint and no target still produce a complete line"
    );
}

/// A mid-stream size refusal must keep its WORDING on the wire, because prose is the only signal the
/// consumer has for that class
/// (`isContextOverflowSignal`, `packages/underload/harness/src/engine/transport/provider-timeouts.ts:467`):
/// its gate reads `413`, `400` + wording, or NO status + wording. Dropping the upstream's sentence
/// into the frozen constant is what made a relayed overflow unreadable, and the local server sends
/// exactly this text (`Temp/basalt-check/serve/prompt.py:15-28`).
///
/// SINCE 2026-11-24 the frame also carries the **400** that gate needs beside the wording (deviation
/// D7, `src/error.rs`, `status_declared_by_size_refusal`): the moment the relay wraps the sentence in
/// `server_error` the class table answers 500, which that gate refuses to read as an overflow however
/// well the prose matches — so the wording itself is a status rule, ranked below the frame's own
/// number and above the table.
///
/// WHAT THE 400 DOES NOT BUY, stated because "the 400 unlocks compaction" overstates it (review, N1).
/// The gate accepts wording ALONE when the frame carries no status, and this crate's four
/// `CONTEXT_OVERFLOW_MARKERS` are WIDER than the consumer's eight `CONTEXT_OVERFLOW_PATTERNS`
/// (`provider-timeouts.ts:424-433`): a sentence like the engine's raw
/// `prompt (N tokens) + max_new (M) exceeds the context (C)` matches ours and none of its, so with our
/// 400 attached the consumer classifies it as a plain 400 refusal — the terminal
/// `provider-rejected` (`:751-758`), not `context-overflow` and not the compaction policy. That is
/// still the better answer for that wording (a terminal refusal where the pre-2026-11-24 500 walked
/// the whole 1 + 10 transient ladder), but it is a TERMINAL REFUSAL, and only a sentence in BOTH lists
/// — anything quoting "context window", "maximum context length", "prompt is too long", — turns into
/// compaction. The assertions below pin the frame this crate emits; the consumer's reading of it is a
/// harness contract, pinned on the harness side.
#[test]
fn an_in_band_context_overflow_frame_keeps_its_wording_on_the_wire() {
    let overflow = "request exceeds the context window: prompt (9000 tokens) + max tokens (1024) \
                    exceeds the context (8192); requests are never truncated";

    // The shape with NOTHING but the sentence: no number, no class, no errno. SINCE 2026-11-24 (D7,
    // `src/error.rs`, `status_declared_by_size_refusal`) the sentence alone is a verdict, and the
    // verdict is 400 — the same status the upstream gives this identical message when the refusal
    // happens BEFORE the response head (`Temp/basalt-check/serve/handler.py:551-555`, the
    // `ValueError` -> 400 / `invalid_request_error` arm; `Temp/basalt-check/serve/prompt.py:15-28` is
    // where the two sentences are raised). Until that date this frame was the `unknown` residue with
    // no `status` key at all, which the consumer could only retry.
    let bare = StreamFailure::upstream(&json!({ "message": overflow }), 0, None);

    assert_eq!(
        bare.code(),
        FailureKind::UpstreamStatus,
        "a message that names a size refusal states a status, so the declared-status rescue claims \
         the frame (`src/error.rs`, rule 9 — nothing in rules 1-8 names this sentence)"
    );
    assert_eq!(
        bare.status(),
        Some(StatusCode::BAD_REQUEST),
        "400, because the remedy is a smaller request, not more time"
    );
    assert_eq!(
        bare.frame_status(),
        None,
        "the status is INFERRED from the wording, so the log must not claim the frame wrote a number"
    );
    assert_eq!(bare.detail(), overflow, "the engine's words survive");
    assert_eq!(
        bare.sentence(Some("chat-completion")),
        format!(
            "stream from endpoint chat-completion returned HTTP 400 before the first chunk: {overflow}"
        )
    );
    assert_eq!(
        serde_json::to_string(&bare.payload(Some("chat-completion"))).expect("a frame"),
        format!(
            r#"{{"error":{{"message":"stream from endpoint chat-completion returned HTTP 400 before the first chunk: {overflow}","type":"stream_error","code":"upstream_status","status":400,"param":null}}}}"#
        ),
        "the frame's `message` carries the size refusal, so a prose matcher can fire on it, and the \
         status the consumer's overflow gate needs is on the same frame"
    );
    assert!(
        bare.payload(Some("chat-completion"))["error"]["message"]
            .as_str()
            .expect("a message")
            .contains("context window"),
        "the needle the consumer matches is in the emitted message, not only in the log"
    );

    // The shape the local server ACTUALLY sends mid-stream: the same sentence inside a
    // `server_error` envelope (`Temp/basalt-check/serve/handler.py:736-738` — an engine `ERR` line is
    // relayed as `server_error`, under that arm's own comment "engine ERR after the stream started:
    // no 400 now"). The wording rule outranks the class table, so this is a 400 as well: the class is
    // what the RELAY stamps on every engine fault, which is exactly why it is too coarse to decide
    // this case and the sentence is not.
    let enveloped = StreamFailure::upstream(
        &json!({ "type": "server_error", "message": overflow }),
        1,
        Some("https://basalt.example/v1"),
    );

    assert_eq!(enveloped.code(), FailureKind::UpstreamStatus);
    assert_eq!(
        enveloped.status(),
        Some(StatusCode::BAD_REQUEST),
        "`server_error` alone would say 500; the size-refusal wording outranks it (D7)"
    );
    assert_eq!(
        serde_json::to_string(&enveloped.payload(Some("chat-completion"))).expect("a frame"),
        format!(
            r#"{{"error":{{"message":"upstream https://basalt.example/v1 returned HTTP 400 after 1 chunk: {overflow}","type":"stream_error","code":"upstream_status","status":400,"param":null}}}}"#
        )
    );
}

/// The size-refusal marker list is CLOSED, and a numeric `error.status` outranks it — both halves are
/// the contract, because an open-ended prose match would guess at vendor wording this crate has never
/// seen, and a wording rule above the number would overrule a verdict the upstream actually stated.
#[test]
fn a_size_refusal_wording_infers_only_a_400_and_a_number_always_outranks_it() {
    // Every row of `CONTEXT_OVERFLOW_MARKERS` (`src/error.rs`), asserted as data rather than sampled.
    // The sentences are the relay's and the engine's own wordings, not paraphrases:
    //   * `Temp/basalt-check/serve/tests/test_protocol_facts.py:21-23` pins the engine's `ERR` line
    //     for `context_overflow`, `context_no_room` and the per-slot variant;
    //   * `Temp/basalt-check/serve/prompt.py:15-28` is the server's rewrite of the first two
    //     (`room_error` / `overflow_error`), which is what a chat client sees on the pre-head path.
    for (marker, message) in [
        (
            "context_overflow",
            "context_overflow prompt=5000 max_new=20 context=4096: the prompt does not fit",
        ),
        (
            "context_no_room",
            "context_no_room prompt=4090 context=4096: no room left",
        ),
        (
            "exceeds the context",
            "request exceeds the context window: prompt (5000 tokens) + max tokens (20) exceeds the context (4096); requests are never truncated",
        ),
        (
            "leaves no room to answer",
            "request exceeds the context window: prompt (4090 tokens) leaves no room to answer in the context (4096); requests are never truncated",
        ),
    ] {
        let refusal = StreamFailure::upstream(&json!({ "message": message }), 0, None);

        assert_eq!(refusal.code(), FailureKind::UpstreamStatus, "{marker}");
        assert_eq!(
            refusal.status(),
            Some(StatusCode::BAD_REQUEST),
            "{marker} states the 400"
        );
        assert_eq!(
            refusal.frame_status(),
            None,
            "{marker} is inferred, so the frame still wrote no number"
        );
    }

    // Case-insensitive, like every other text rule in this module.
    let shouted = StreamFailure::upstream(
        &json!({ "message": "REQUEST EXCEEDS THE CONTEXT (4096)" }),
        0,
        None,
    );

    assert_eq!(shouted.code(), FailureKind::UpstreamStatus);
    assert_eq!(shouted.status(), Some(StatusCode::BAD_REQUEST));

    // A number the frame WROTE outranks the wording, in BOTH directions. `status: 500` beside overflow
    // wording is a 500 — the upstream stated a verdict and this crate repeats it rather than
    // second-guessing it, which is the same reason the number outranks the class table.
    let explicit_500 = StreamFailure::upstream(
        &json!({ "type": "server_error", "status": 500, "message": "request exceeds the context window: prompt (9000 tokens) + max tokens (1024) exceeds the context (8192)" }),
        1,
        Some("https://basalt.example/v1"),
    );

    assert_eq!(explicit_500.code(), FailureKind::UpstreamStatus);
    assert_eq!(
        explicit_500.status(),
        Some(StatusCode::INTERNAL_SERVER_ERROR),
        "the wording rule sits BELOW the number, so an explicit 500 is NOT rewritten to 400"
    );
    assert_eq!(
        explicit_500.frame_status(),
        Some(StatusCode::INTERNAL_SERVER_ERROR)
    );
    assert_eq!(
        serde_json::to_string(&explicit_500.payload(Some("chat-completion"))).expect("a frame"),
        r#"{"error":{"message":"upstream https://basalt.example/v1 returned HTTP 500 after 1 chunk: request exceeds the context window: prompt (9000 tokens) + max tokens (1024) exceeds the context (8192)","type":"stream_error","code":"upstream_status","status":500,"param":null}}"#,
        "the wire status is the frame's own number, and the wording is still preserved beside it"
    );

    // A number BELOW the inferred one wins too, so the rule cannot quietly upgrade a frame.
    let explicit_418 = StreamFailure::upstream(
        &json!({ "status": 418, "message": "exceeds the context (8192)" }),
        0,
        None,
    );

    assert_eq!(explicit_418.code(), FailureKind::UpstreamStatus);
    assert_eq!(explicit_418.status(), Some(StatusCode::IM_A_TEAPOT));

    // The documented NON-match, pinned so the gap in the list stays visible rather than becoming a
    // surprise: the per-slot engine sentence `… exceeds slot S's context (C)` reaches the relay
    // UN-rewritten (`Temp/basalt-check/serve/prompt.py:86-95`, `explain` rewrites only the coded
    // shapes that carry full fields) and matches no marker, so it keeps the class table's verdict —
    // here no class at all, which is the old `unknown` residue.
    let per_slot = StreamFailure::upstream(
        &json!({ "message": "prompt (5000 tokens) + max_new (20) exceeds slot 1's context (4096)" }),
        0,
        None,
    );

    assert_eq!(per_slot.code(), FailureKind::Unknown);
    assert_eq!(
        per_slot.status(),
        None,
        "the marker list is closed: prose outside it infers nothing, and the wording rules keep \
         their chance"
    );
}

/// THE PRECEDENCE RULE, end to end on the failure object (2026-11-24 review correction, C1): the
/// status an in-band frame DECLARES rescues a failure nothing else named, and never overrides one the
/// names/errno/wording rules reached. Both halves matter, and the second is the regression this closed —
/// an earlier revision of the in-band read fed the declared status to rule 3, so a frame that had
/// ALWAYS been `connection` became `upstream_status` and lost the consumer's short dial ladder for the
/// codeless 1 + 10 transient one (`provider-timeouts.ts:714-721` vs `:761-790`, restated in
/// `src/error.rs`'s module header).
#[test]
fn a_worded_in_band_frame_keeps_its_code_and_emits_no_status() {
    // (a) THE CASE THE REVIEW REPORTED, byte for byte: the relay's `server_error` envelope around a
    // sentence rule 7 already matched. The class table's 500 must not reach the wire.
    let target = Some("https://basalt.example/v1");
    let worded = StreamFailure::upstream(
        &json!({ "type": "server_error", "message": "upstream stream terminated" }),
        1,
        target,
    );

    assert_eq!(
        worded.code(),
        FailureKind::Connection,
        "the wording named it before any status was read (`src/error.rs`, rule 7)"
    );
    assert_eq!(
        worded.status(),
        None,
        "a named class carries no status (`start.ts:643`), so the wire has no `status` key at all"
    );
    assert_eq!(
        worded.frame_status(),
        None,
        "the frame wrote no number, so the log may not claim one"
    );
    assert_eq!(worded.errno(), None, "the class name is not an errno");
    assert_eq!(
        serde_json::to_string(&worded.payload(Some("chat-completion"))).expect("a frame"),
        r#"{"error":{"message":"upstream https://basalt.example/v1 closed the response body after 1 chunk: upstream stream terminated","type":"stream_error","code":"connection","param":null}}"#,
        "the frame is the one this crate emitted before the in-band status existed — `connection`, \
         and the key set has no `status`"
    );
    assert_eq!(
        worded.payload(Some("chat-completion"))["error"]
            .as_object()
            .expect("an error object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["message", "type", "code", "param"],
        "no `status` between `code` and `param`"
    );

    // THE SAME FRAME WITH A NUMBER WRITTEN BESIDE THE SENTENCE: still `connection`, still no emitted
    // status, and the number survives exactly where the field comment says it must — `frame_status`,
    // printed on the operator's line.
    let worded_with_number = StreamFailure::upstream(
        &json!({ "type": "server_error", "status": 503, "message": "upstream stream terminated" }),
        1,
        target,
    );

    assert_eq!(worded_with_number.code(), FailureKind::Connection);
    assert_eq!(
        worded_with_number.status(),
        None,
        "the number is DECLARED, not observed, so it cannot buy this frame a status"
    );
    assert_eq!(
        worded_with_number.frame_status(),
        Some(StatusCode::SERVICE_UNAVAILABLE),
        "and it is the only place the upstream's 503 survives"
    );
    assert_eq!(
        worded_with_number.log_line(Some("chat-completion")),
        "stream error in chat-completion: code=connection origin=upstream chunks=1 \
         upstream=https://basalt.example/v1 frame_status=503 \
         detail=\"upstream stream terminated\"",
        "the withheld status is still greppable, beside the code the wording earned"
    );

    // (b) THE WORDING-ONLY VERDICTS ARE UNCHANGED, one per wording class, each with a declared class
    // or number present so the assertion is about PRECEDENCE and not about the needles themselves.
    for (frame, expected) in [
        (
            json!({ "type": "server_error", "message": "invalid json in the relay's frame" }),
            FailureKind::Decode,
        ),
        (
            json!({ "type": "server_error", "status": 500, "message": "the engine reply timed out" }),
            FailureKind::Timeout,
        ),
    ] {
        let named = StreamFailure::upstream(&frame, 0, None);

        assert_eq!(named.code(), expected, "{frame}");
        assert_eq!(
            named.status(),
            None,
            "{frame} declares a status it is not allowed to carry"
        );
        assert_eq!(
            named.payload(None)["error"]
                .as_object()
                .expect("an error object")
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["message", "type", "code", "param"],
            "{frame}: the key set is the pre-2026-11-24 one"
        );
    }
}

/// THE OTHER ARM OF THE SAME RULE (C1 pin (c)): with nothing in rules 1-8 to match, the declared
/// status IS the verdict — which is the whole point of lifting it. These are the frames the deviation
/// was added for, and the rescue is what still delivers them.
#[test]
fn a_frame_nothing_names_is_rescued_by_the_status_it_declares() {
    // Class only, no message at all: the detail falls back to the object's own JSON text.
    let bare_class = StreamFailure::upstream(&json!({ "type": "server_error" }), 0, None);

    assert_eq!(bare_class.code(), FailureKind::UpstreamStatus);
    assert_eq!(
        bare_class.status(),
        Some(StatusCode::INTERNAL_SERVER_ERROR),
        "the class states 500, and nothing else had a say"
    );
    assert_eq!(
        bare_class.frame_status(),
        None,
        "the status is inferred, so the log must not claim the frame wrote a number"
    );

    // Number only, no message and no class: the strongest declaration wins on the wire AND in
    // `frame_status`, because the frame really wrote it.
    let bare_number = StreamFailure::upstream(&json!({ "status": 429 }), 2, None);

    assert_eq!(bare_number.code(), FailureKind::UpstreamStatus);
    assert_eq!(bare_number.status(), Some(StatusCode::TOO_MANY_REQUESTS));
    assert_eq!(
        bare_number.frame_status(),
        Some(StatusCode::TOO_MANY_REQUESTS),
        "written, not inferred"
    );
    assert_eq!(
        serde_json::to_string(&bare_number.payload(Some("responses"))).expect("a frame"),
        r#"{"error":{"message":"stream from endpoint responses returned HTTP 429 after 2 chunks: {\"status\":429}","type":"stream_error","code":"upstream_status","status":429,"param":null}}"#,
        "the rescue emits the `status` key the pre-2026-11-24 `unknown` frame never carried"
    );
}

/// The shapes `error.status` arrives in, read the way BOTH deployed readers read it (C3, 2026-11-24):
/// `readUpstreamStatus` accepts any value `Number.isInteger` takes (`start.ts:505-512`) — and JSON
/// parses `500.0` to an integer — while the consumer's `wireStatusOf` additionally accepts a quoted
/// three-digit string, "some proxies serialise it quoted"
/// (`packages/underload/harness/src/engine/transport/provider-timeouts.ts:655-660`). `Value::as_u64`
/// alone rejected the last two, so a real verdict was silently dropped to `unknown`.
#[test]
fn a_declared_status_is_read_from_every_shape_the_consumer_reads() {
    for (written, status) in [
        (json!(500), 500_u16),
        (json!(500.0), 500),
        (json!("500"), 500),
        (json!(" 502 "), 502),
        (json!("0503"), 503),
    ] {
        let frame = json!({ "status": written, "message": "the relay wrote a verdict" });
        let failure = StreamFailure::upstream(&frame, 0, None);

        assert_eq!(failure.code(), FailureKind::UpstreamStatus, "{frame}");
        assert_eq!(
            failure.status(),
            Some(StatusCode::from_u16(status).expect("a status the range check bounds")),
            "{written} is the status the frame declared"
        );
        assert_eq!(
            failure.frame_status(),
            Some(StatusCode::from_u16(status).expect("a status the range check bounds")),
            "{written} was WRITTEN, so `frame_status` says so too"
        );
    }

    // And the shapes BOTH readers refuse: a fraction, a negative, an out-of-range value in either
    // shape, a string that is not pure ASCII digits, and every non-scalar. Dropping the status is not
    // a fault — it leaves the wording rules and then `unknown`, exactly as before.
    for written in [
        json!(500.5),
        json!(-500),
        json!(999),
        json!("999"),
        json!("500.0"),
        json!("five hundred"),
        json!(""),
        json!(true),
        json!(null),
        json!({ "code": 500 }),
    ] {
        let frame = json!({ "status": written, "message": "a shape this crate refuses" });
        let failure = StreamFailure::upstream(&frame, 0, None);

        assert_eq!(
            failure.code(),
            FailureKind::Unknown,
            "{frame} states no status"
        );
        assert_eq!(failure.status(), None, "{written}");
        assert_eq!(failure.frame_status(), None, "{written}");
    }
}

/// The size-refusal marker is read from `error.code` as well as from the message (C4, 2026-11-24):
/// `src/error.rs`'s own marker note describes an `error.code` written beside the message, and until
/// this fix the ONLY read was the message, so the frame below — which says its code and nothing else —
/// inferred the class table's 500.
#[test]
fn a_size_refusal_marker_written_in_error_code_is_a_400() {
    // The frame the sentence describes: no `message` member at all, so the message half has nothing
    // to match and only the `code` can state the verdict.
    let coded = StreamFailure::upstream(
        &json!({ "type": "server_error", "code": "context_overflow" }),
        0,
        None,
    );

    assert_eq!(coded.code(), FailureKind::UpstreamStatus);
    assert_eq!(
        coded.status(),
        Some(StatusCode::BAD_REQUEST),
        "`context_overflow` in `error.code` is the same refusal the sentence states, so it is the \
         same 400 (`Temp/basalt-check/serve/engine/errors.py:5` `ERR_CODES`)"
    );
    assert_eq!(
        coded.frame_status(),
        None,
        "the 400 is inferred from the code, so the log must not claim a number"
    );
    assert_eq!(
        coded.errno(),
        None,
        "a lower-case provider code is still not a transport errno"
    );

    // Both coded markers, with real prose beside them (the shape a structured relay sends), and
    // case-insensitively like every other text read in this module.
    for (code, message) in [
        ("context_no_room", "the prompt does not fit the slot"),
        ("CONTEXT_OVERFLOW", "engine reported the prompt over budget"),
    ] {
        let refusal = StreamFailure::upstream(
            &json!({ "type": "server_error", "code": code, "message": message }),
            1,
            None,
        );

        assert_eq!(refusal.code(), FailureKind::UpstreamStatus, "{code}");
        assert_eq!(
            refusal.status(),
            Some(StatusCode::BAD_REQUEST),
            "{code} states the 400 whatever the prose says"
        );
    }

    // A `code` that is NOT a size marker changes nothing: the class table keeps its verdict, which is
    // what stops this rule from becoming a general guess about vendor codes.
    let other_code = StreamFailure::upstream(
        &json!({ "type": "server_error", "code": "engine_offline" }),
        0,
        None,
    );

    assert_eq!(
        other_code.status(),
        Some(StatusCode::INTERNAL_SERVER_ERROR),
        "an unrelated `code` does not move the class table's 500"
    );
}

/// The content-free in-band error object: `{"error":{}}` on a 200 stream. It must degrade — no panic,
/// no new code spelling, no fabricated status — and the operator line must still name the endpoint,
/// the code, the origin and the chunk count, which is the point of a log line for a failure that has
/// no reason in it.
#[test]
fn a_content_free_in_band_error_frame_degrades_without_a_new_code() {
    let empty = StreamFailure::upstream(&json!({}), 0, None);

    assert_eq!(empty.code(), FailureKind::Unknown);
    assert_eq!(empty.status(), None);
    assert_eq!(empty.frame_status(), None);
    assert_eq!(empty.errno(), None);
    // `StreamFailure::upstream` falls back to the JSON text of the object when it names no message,
    // which is the fallback `APIError.makeMessage` uses (`core/error.js:22-27`). That text IS the
    // detail, so the sentence is the constant plus it — and the constant's prefix is intact.
    assert_eq!(empty.detail(), "{}");
    assert_eq!(empty.sentence(None), "Upstream stream terminated: {}");
    assert_eq!(
        serde_json::to_string(&empty.payload(None)).expect("a frame"),
        r#"{"error":{"message":"Upstream stream terminated: {}","type":"stream_error","code":"unknown","param":null}}"#,
        "the key set is unchanged — no `status`, no invented key"
    );
    assert_eq!(
        empty.log_line(Some("chat-completion")),
        "stream error in chat-completion: code=unknown origin=upstream chunks=0 detail=\"{}\"",
        "endpoint, code, origin and chunk count are named even when the frame said nothing"
    );

    // A scalar `error` member is the other content-free shape: the SDK stringifies it WITH its
    // quotes, and this crate classifies it as `unknown` rather than guessing.
    let scalar = StreamFailure::upstream(&Value::String("boom".to_owned()), 2, None);

    assert_eq!(scalar.code(), FailureKind::Unknown);
    assert_eq!(scalar.status(), None);
    assert_eq!(scalar.detail(), "\"boom\"");
    assert_eq!(
        scalar.sentence(None),
        "Upstream stream terminated: \"boom\""
    );
}

/// A real HTTP verdict on the response head — a `TransportError::Status`, the Rust counterpart of the
/// thrown `ApiError` whose own `status` property `readUpstreamStatus` reads (`start.ts:506-517`) —
/// stamps a `status` too, and it always has. `start.ts:644` emits it and `start.ts:695-702` places it
/// between `code` and `errno`/`param`.
///
/// This used to be the ONLY shape that could stamp one; since 2026-11-24 an in-band error frame can
/// as well (see `a_mid_stream_error_frame_with_a_status_is_an_upstream_status_frame` above). What is
/// still distinct here is the SOURCE: a pre-stream verdict reaches the failure as a real
/// [`TransportError::status`] and never as `frame_status`, which the assertions below pin.
///
/// A pre-stream verdict is also the case the ROUTE answers with the gateway's constant 500
/// (`start.ts:908-911`, `src/serve.rs` deviation 3), so the frame below is what the frame builder
/// emits whenever a status-carrying transport failure is classified — the classification and the wire
/// shape are pinned here because the route never streams one.
#[test]
fn a_pre_stream_status_stamps_the_status_the_response_head_carried() {
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

    // `upstream_status`: the status in the sentence and in the frame. A real HTTP verdict on the
    // response head (`TransportError::Status`, the counterpart of the thrown `ApiError` whose own
    // `status` `readUpstreamStatus` reads, `start.ts:506-517`) is one source of that class; since
    // 2026-11-24 the status an IN-BAND error frame declares is the other, pinned by
    // `a_mid_stream_error_frame_with_a_status_is_an_upstream_status_frame`.
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

    // `unknown`: the frozen constant LEADS the message whatever the upstream said
    // (`start.ts:682-685`), which is what keeps every consumer that string-matches the legacy frame
    // working, and since 2026-11-24 the upstream's own text is tailed after `: ` — the class that
    // cannot name a code is no longer allowed to also forget the sentence.
    let unknown = StreamFailure::upstream(
        &json!({ "message": "Something unclassifiable happened", "code": "rate_limit_exceeded" }),
        2,
        target,
    );
    assert_eq!(
        unknown.sentence(None),
        "Upstream stream terminated: Something unclassifiable happened"
    );
    assert!(
        unknown
            .sentence(None)
            .starts_with(UNKNOWN_STREAM_ERROR_MESSAGE),
        "the legacy prefix survives the detail tail"
    );
    assert_eq!(unknown.code(), FailureKind::Unknown);
    assert_eq!(
        unknown.status(),
        None,
        "the frame declared no status at all"
    );
    assert_eq!(
        unknown.frame_status(),
        None,
        "and wrote no number of its own, so the log line has nothing to distinguish"
    );
    assert_eq!(
        unknown.detail(),
        "Something unclassifiable happened",
        "the detail half is still available for the log line"
    );
    assert_eq!(
        unknown.log_line(Some("chat-completions")),
        "stream error in chat-completions: code=unknown origin=upstream chunks=2 \
         upstream=https://engine.example/v1 detail=\"Something unclassifiable happened\"",
        "the operator line names the endpoint, the code, the origin, the chunk count, the target and \
         the detail — everything the wire sentence leaves out (`src/error.rs`, `log_line`)"
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
    // it is a transport or decode signal. It has a message of its own, so the frozen constant leads
    // and that message follows it.
    let truncated = StreamFailure::truncated(2, target);
    assert_eq!(truncated.code(), FailureKind::Unknown);
    assert_eq!(
        truncated.sentence(None),
        "Upstream stream terminated: The model stream ended before reporting a completion."
    );
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
        declared_status: None,
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

    // THE DECLARED-STATUS RESCUE (2026-11-24), pinned at the ladder level as well as through the
    // frame tests below: `declared_status` is not an OBSERVED status, so it lifts a failure ONLY when
    // rules 1-8 produced `unknown`. The two assertions either side of that line are the whole rule.
    let rescued = FailureSignals {
        messages: vec!["Rate limit exceeded".to_owned()],
        declared_status: Some(StatusCode::TOO_MANY_REQUESTS),
        ..FailureSignals::default()
    };
    assert_eq!(
        classify_stream_error(&rescued),
        FailureKind::UpstreamStatus,
        "a frame nothing else could name takes the verdict it declared instead of `unknown`"
    );

    // ...and the same status may NOT demote a verdict the wording rules reached. One row per wording
    // class, because all three are the regression: `connection` was the case the review caught, and
    // `decode`/`timeout` sit under the same rules on either side of it.
    for (message, expected) in [
        ("upstream stream terminated", FailureKind::Connection),
        ("invalid json in the relay's frame", FailureKind::Decode),
        ("the request operation timed out", FailureKind::Timeout),
    ] {
        let named = FailureSignals {
            messages: vec![message.to_owned()],
            declared_status: Some(StatusCode::INTERNAL_SERVER_ERROR),
            ..FailureSignals::default()
        };

        assert_eq!(
            classify_stream_error(&named),
            expected,
            "{message}: a declared status never outranks a named verdict (`src/error.rs`, \
             the rule 9 rescue)"
        );
    }

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
