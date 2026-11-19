//! Coverage for the gateway surface `src/serve.rs` publishes for a host that is NOT
//! `typecrate-services`.
//!
//! # What this file is for
//!
//! `src/serve.rs` gained four public items — [`serve::with_gateway_headers`],
//! [`serve::is_cors_preflight`], [`serve::gateway`] and [`serve::health_document`] — so that a
//! second host can mount the four contract routes without re-inventing the cross-origin policy.
//! `packages/typecrate/services/src/server/mod.rs:354-417` holds the same behaviour as PRIVATE
//! items inside the services router; these are the reusable spelling, and `src/bin/server.rs` is
//! the first consumer. The dedup follow-up (services calling these instead of its own copies) is
//! deliberately not taken here: it edits the services crate and its `tests/server_http.rs` pins,
//! which is outside this change.
//!
//! WHY A REAL LISTENER: the same reason `tests/routes.rs:19-24` gives. `gateway` is a middleware,
//! and what a middleware OWNS is the response a caller receives — the header set, the preflight
//! short-circuit, and the fact that a fallback 404 is decorated too. Calling the function with a
//! hand-built `Next` would exercise the code while proving nothing about the wire, and `axum` is
//! already a dependency of this crate, so a real socket costs nothing. The predicates and the
//! document builder, by contrast, are asserted as pure values.
//!
//! WHAT LIVES ELSEWHERE: the bin's own routing (`/health`, `ANY`, the disabled body limit, the
//! credential gate, the log filter) is `tests/bin_server.rs`; the four handlers' bodies are
//! `tests/routes.rs`.

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderName, Method, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::any,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use typecrate_provider::{
    registry::Registry,
    serve::{
        self, GATEWAY_ALLOW_ALL, HANDLER_NOT_FOUND, JSON_CONTENT_TYPE, MODELS_ROUTE,
        PRIVATE_NETWORK_ALLOW_HEADER, ServeState,
    },
};

/// The private-network header spelled the way a client asks for it, so an assertion failure names
/// the header instead of an index into a map.
const PRIVATE_NETWORK: &str = "access-control-allow-private-network";

/// The three `Access-Control-Allow-*` names, as a client reads them.
const ALLOW_HEADERS: [&str; 3] = [
    "access-control-allow-origin",
    "access-control-allow-methods",
    "access-control-allow-headers",
];

/// A hermetic HTTP client — `no_proxy` is load-bearing for the same reason `tests/routes.rs:289-295`
/// gives: a proxy from the developer's environment would move a loopback request off this machine.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("a proxy-free client")
}

/// A ready state with NO rows, so the catalog is exactly `{"object":"list","data":[]}`: the
/// smallest body that still proves a route answered. `Registry::from_clients` is the same
/// constructor `tests/routes.rs:327` uses, so nothing here depends on the generated vault artifact.
fn empty_ready_state() -> ServeState {
    ServeState::Ready(Registry::from_clients(Vec::new()))
}

/// An unavailable state, built the way `tests/routes.rs:774` builds one: an explicit path source
/// that does not exist, because `std::env::set_var` is `unsafe` in edition 2024 and this crate
/// forbids `unsafe`.
fn unavailable_state() -> ServeState {
    serve::state_from_source(&serve::VaultSource::Path(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/no-such-vault.json"),
    ))
}

/// One answer as it arrived on the wire.
struct Answer {
    status: StatusCode,
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

/// The four routes of one state, served over a real loopback listener. Only `/models` is mounted:
/// this file is about the layer, and one contract route is enough to reach the handler's verb gate.
///
/// The adapter below is the pattern `tests/routes.rs:378-391` states — axum needs a state type to
/// build a `Router`, while `serve`'s handlers take `&ServeState` — so the layer under test is wrapped
/// around a router built exactly the way a host builds one.
async fn bind(state: ServeState) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback listener for the gateway surface");
    let address = listener
        .local_addr()
        .expect("the bound address of a loopback listener")
        .to_string();

    let service = Router::new()
        .route(MODELS_ROUTE, any(catalog_route))
        .route("/echo", any(echo))
        // Added BEFORE `.with_state`, so it wraps every mounted route AND the fallback 404 — the
        // position `services/src/server/mod.rs:538` uses, and the property the 404 assertion below
        // pins.
        .layer(middleware::from_fn(serve::gateway))
        .with_state(state);

    tokio::spawn(async move {
        axum::serve(listener, service)
            .await
            .expect("the gateway listener lives until the process ends");
    });

    address
}

/// `serve::models` behind the extractor axum needs.
async fn catalog_route(State(state): State<ServeState>, request: Request) -> Response {
    serve::models(&state, request).await
}

/// Echoes the method it was reached with, so a caller can tell "the middleware answered" from
/// "the route answered".
async fn echo(request: Request) -> Response {
    (
        StatusCode::IM_A_TEAPOT,
        [(axum::http::header::CONTENT_TYPE, JSON_CONTENT_TYPE)],
        request.method().to_string(),
    )
        .into_response()
}

/// One request against the bound surface.
async fn request(address: &str, method: Method, path: &str, headers: &[(&str, &str)]) -> Answer {
    let mut request = http_client().request(method.clone(), format!("http://{address}{path}"));

    for (name, value) in headers {
        request = request.header(*name, *value);
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
        body: String::from_utf8(bytes.to_vec()).expect("every gateway body is UTF-8"),
    }
}

/// Asserts the whole policy on one answer, in one place, so no test can check three of four
/// headers and call the contract covered. `private_network` is `true`, not `*` — the value is the
/// answer, not an echo.
fn assert_policy(answer: &Answer, context: &str) {
    for name in ALLOW_HEADERS {
        assert_eq!(
            answer.header(name),
            Some(GATEWAY_ALLOW_ALL),
            "{name} is missing from the {context}"
        );
    }

    assert_eq!(
        answer.header(PRIVATE_NETWORK),
        Some("true"),
        "{PRIVATE_NETWORK} is missing from the {context}"
    );
}

// ---------------------------------------------------------------------------
// The pure half: what counts as a preflight, and what the header set is
// ---------------------------------------------------------------------------

/// Builds a request without touching the network, for the pure predicate.
fn request_with(method: Method, declares: Option<&str>) -> Request {
    let mut builder = Request::builder().method(method).uri(MODELS_ROUTE);

    if let Some(verb) = declares {
        builder = builder.header("access-control-request-method", verb);
    }

    builder
        .body(Body::empty())
        .expect("a request from a method and a header")
}

#[test]
fn a_preflight_is_the_method_plus_the_header_and_neither_alone() {
    // The PAIR is the rule: an `OPTIONS` that does not declare a verb is a ROUTED request on this
    // mount, because every group is `ANY` and the handler prints the gateway's own verb-miss 400.
    // Short-circuiting every `OPTIONS` would retire that behaviour silently — the same reasoning as
    // `services/src/server/mod.rs:340-346` for its own router.
    assert!(serve::is_cors_preflight(&request_with(
        Method::OPTIONS,
        Some("POST")
    )));
    assert!(
        !serve::is_cors_preflight(&request_with(Method::OPTIONS, None)),
        "a bare OPTIONS is a routed request, not a preflight"
    );
    assert!(
        !serve::is_cors_preflight(&request_with(Method::POST, Some("POST"))),
        "the declaration header alone does not make a preflight"
    );
}

#[test]
fn the_policy_answers_every_origin_verb_and_header_and_the_private_network_ask() {
    let response = serve::with_gateway_headers(Response::new(Body::empty()));
    let headers = response.headers();

    for name in ALLOW_HEADERS {
        let name = HeaderName::from_static(name);
        assert_eq!(
            headers.get(&name).and_then(|value| value.to_str().ok()),
            Some(GATEWAY_ALLOW_ALL),
            "{name}"
        );
    }

    // The published constant is the name a client actually reads, and the value is `true` rather
    // than an echo, so a client that asked never re-negotiates after a redirect.
    assert_eq!(PRIVATE_NETWORK_ALLOW_HEADER.as_str(), PRIVATE_NETWORK);
    assert_eq!(
        headers
            .get(&PRIVATE_NETWORK_ALLOW_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );
}

#[test]
fn the_policy_replaces_a_policy_a_handler_tried_to_narrow() {
    // The insert is an OVERWRITE, and that is the point: the advertised policy belongs to the
    // transport, and a route that set its own origin must not be able to quiet the host.
    let response = Response::builder()
        .header("access-control-allow-origin", "https://example.test")
        .body(Body::empty())
        .expect("a response with a narrow origin");

    let response = serve::with_gateway_headers(response);

    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|value| value.to_str().ok()),
        Some(GATEWAY_ALLOW_ALL)
    );
}

#[test]
fn the_health_body_is_the_exact_document_a_probe_reads() {
    // Key ORDER is asserted as bytes, not only as a parsed map: `serde_json`'s `preserve_order`
    // feature is what lets this crate state an order at all, and a probe that prints the document
    // sees this string.
    let document = serve::health_document();

    assert_eq!(
        document,
        json!({
            "ok": true,
            "service": "typecrate-provider",
            "version": typecrate_provider::version(),
        })
    );
    assert_eq!(
        serde_json::to_string(&document).expect("a document that serialises"),
        format!(
            "{{\"ok\":true,\"service\":\"typecrate-provider\",\"version\":\"{}\"}}",
            typecrate_provider::version()
        )
    );
    // The banner identity and the body name the same package, and that package is the cargo one.
    assert_eq!(serve::SERVICE_NAME, "typecrate-provider");
}

// ---------------------------------------------------------------------------
// The live half: what a caller actually receives
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_preflight_answers_204_with_the_policy_and_no_body() {
    let address = bind(empty_ready_state()).await;

    let answer = request(
        &address,
        Method::OPTIONS,
        MODELS_ROUTE,
        &[("access-control-request-method", "POST")],
    )
    .await;

    assert_eq!(answer.status, StatusCode::NO_CONTENT);
    // `204` means no body, and no media type is claimed for one.
    assert_eq!(answer.header("content-type"), None);
    assert_eq!(answer.body, "");
    assert_policy(&answer, "preflight");
}

#[tokio::test]
async fn a_bare_options_reaches_the_route_and_takes_the_gateways_400() {
    // The other half of the predicate: without `Access-Control-Request-Method` the request is
    // ROUTED, and on an `ANY` mount the handler answers it with the deployed gateway's sentence
    // (`packages/underload/service/src/server/handler.ts:38-46`), not with axum's body-less 405.
    let address = bind(empty_ready_state()).await;

    let answer = request(&address, Method::OPTIONS, MODELS_ROUTE, &[]).await;

    assert_eq!(answer.status, StatusCode::BAD_REQUEST);
    assert_eq!(answer.json(), json!({ "error": HANDLER_NOT_FOUND }));
    assert_eq!(answer.header("content-type"), Some(JSON_CONTENT_TYPE));
    assert_policy(&answer, "routed OPTIONS refusal");
}

#[tokio::test]
async fn a_routed_answer_a_fallback_and_an_uncatalogued_path_carry_the_same_policy() {
    let address = bind(empty_ready_state()).await;

    let models = request(&address, Method::GET, MODELS_ROUTE, &[]).await;
    assert_eq!(models.status, StatusCode::OK);
    assert_eq!(
        models.json(),
        json!({ "object": "list", "data": [] }),
        "a registry with no rows is an empty catalog, not an error"
    );
    assert_policy(&models, "the catalog");

    // The fallback 404 is decorated as well, which is only true when the layer is the OUTERMOST
    // one (`services/src/server/mod.rs:535-538` states the same property for its own gateway).
    let missing = request(&address, Method::GET, "/providers/private/v1/nope", &[]).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    assert_policy(&missing, "the fallback 404");

    // And the layer hands the request through untouched: the echo names the verb it received.
    let echoed = request(&address, Method::GET, "/echo", &[]).await;
    assert_eq!(echoed.status, StatusCode::IM_A_TEAPOT);
    assert_eq!(echoed.body, "GET", "the route answered, not the middleware");
    assert_policy(&echoed, "the echo route");
}

#[tokio::test]
async fn a_preflight_is_answered_even_while_the_vault_is_down() {
    // The vault-down half of the CORS story: the preflight is answered BEFORE routing, so a
    // deployment with no artifact still answers a browser's handshake with 204, and only the routed
    // requests carry the 503. That ordering is what keeps a credential problem from looking like a
    // CORS problem in a browser console.
    let address = bind(unavailable_state()).await;

    let preflight = request(
        &address,
        Method::OPTIONS,
        MODELS_ROUTE,
        &[("access-control-request-method", "GET")],
    )
    .await;
    assert_eq!(preflight.status, StatusCode::NO_CONTENT);
    assert_policy(&preflight, "a vault-down preflight");

    let models = request(&address, Method::GET, MODELS_ROUTE, &[]).await;
    assert_eq!(models.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_policy(&models, "the vault-down 503");

    let reason = models.json()["error"]
        .as_str()
        .expect("`error` is a bare string")
        .to_owned();
    assert!(
        reason.contains(serve::REGENERATION_COMMAND),
        "the 503 body names the regeneration command: {reason}"
    );
}
