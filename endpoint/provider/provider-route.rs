//! Route bridge for `/providers/private/v1`, discovered by `typecrate-services`.
//!
//! `packages/typecrate/services/build.rs` walks the repository root and compiles every
//! `<stem>-route.rs` inside an `endpoint`/`endpoints` tree into the services crate as a module of
//! its own (`build_support/mod.rs`, "Route file contract"); this file is a route source because of
//! its NAME and its directory, never because of its contents, and the generated `service_routes.rs`
//! pulls it in with an absolute `#[path]`. The provider crate is a path dependency of services
//! (`packages/typecrate/services/Cargo.toml`), and services never depends on the provider crate's
//! build support, so there is no cycle: this file reaches OUT into the library, the library never
//! reaches back.
//!
//! There is no provider logic in here. The whole `/providers/private/v1` contract — vault gate,
//! model lookup, upstream relay, SSE framing, token log line, stats document — lives in
//! `typecrate_provider::serve` (`packages/typecrate/provider/src/serve.rs`), whose module doc maps
//! every handler onto the TypeScript original it reproduces. This file only addresses those
//! handlers and declares the ports.
//!
//! # Why the handlers take no `State`
//!
//! The generated router mounts every handler into `Router<crate::server::AppState>`, i.e. the
//! SERVICES state. The provider's state is its own — `typecrate_provider::serve::ServeState`, the
//! vault-derived model registry plus its counters — and services has no reason to own a vault
//! artifact it never reads, so these adapters take nothing out of the services state and hand the
//! request to the provider handler with the provider's own lazily-loaded process state
//! (`serve::process_state`: the artifact is read on first use, and until it can be, every route
//! answers `503` with the regeneration command instead of aborting the host process).
//!
//! The shape is also what keeps the contract testable. `serve`'s handlers are plain
//! `(state, request) -> Response` functions, so
//! `packages/typecrate/provider/tests/routes.rs` can mount them on an in-process listener against a
//! SYNTHETIC registry — no services binary, no vault artifact on disk, no test-only bridge inside the
//! services crate.
//!
//! # Why every group is `ANY`
//!
//! The TypeScript route objects declare a VERB-KEYED handler map — `{ POST: privateChatCompletion }`,
//! `{ GET: getModelsList }`, `{ GET: privateStats, POST: privateStatsReset }` — and the dispatcher
//! that reads it answers a verb the map does not contain with
//! `400 {"error":"handler not found"}` (`packages/underload/service/src/server/handler.ts:38-46`),
//! not with a 405. Mounting `POST`/`GET` only would therefore change the contract twice over: axum
//! answers verb misses with its own `405` + `Allow` header, and an `OPTIONS` preflight would never
//! reach a handler at all. So the mount delivers EVERY verb to the handler and lets the handler print
//! the contract's refusal, which `typecrate_provider::serve` reproduces verbatim.
//!
//! That is the same reasoning as the harness bridge
//! (`packages/typecrate/harness/endpoint/harness/harness-route.rs`), with one difference worth naming:
//! the harness needs `ANY` because its dispatcher prints its own verb-specific 405 sentences, while
//! the provider needs it because the gateway's verb miss is a 400. Both are in-handler verdicts that
//! a router-level verb filter would pre-empt.
//!
//! # Why every group declares `port: 8800`
//!
//! All four TypeScript route objects carry `port: LOCAL_AREA_NETWORK_PROVIDER_PORT` — `6660`, from
//! `config/environment/src/port.ts:4` — and the TypeScript gateway, like the services server here,
//! binds one listener per distinct declared port (`packages/underload/service/src/server/start.ts:780`).
//! The Rust surface publishes that listener as **8800** (the deviation is recorded in
//! `packages/typecrate/provider/src/serve.rs`, on `PROVIDER_PORT`, and in `src/lib.rs`). The TS
//! provider process also owns the `/playground*` and `/v1/services/*` builtins on that port; those
//! belong to services and stay on the services default listener.

use axum::{extract::Request, response::Response};

/// `ANY /providers/private/v1/chat/completions` — the OpenAI-compatible relay
/// (`runtime/endpoint/provider/private/chat-completion/{service-route.ts,private-chat-completion.ts}`).
pub async fn chat_completions(request: Request) -> Response {
    typecrate_provider::serve::chat_completions(typecrate_provider::serve::process_state(), request).await
}

/// `ANY /providers/private/v1/responses` — the Responses-shaped relay
/// (`runtime/endpoint/provider/private/responses/{service-route.ts,private-responses.ts}`).
pub async fn responses(request: Request) -> Response {
    typecrate_provider::serve::responses(typecrate_provider::serve::process_state(), request).await
}

/// `ANY /providers/private/v1/models` — the model catalog
/// (`runtime/endpoint/provider/private/models/service-route.ts`).
pub async fn models(request: Request) -> Response {
    typecrate_provider::serve::models(typecrate_provider::serve::process_state(), request).await
}

/// `ANY /providers/private/v1/stats` — the stats document, and the `?id=` reset
/// (`runtime/endpoint/provider/private/stats/{service-route.ts,private-stats.ts}`).
pub async fn stats(request: Request) -> Response {
    typecrate_provider::serve::stats(typecrate_provider::serve::process_state(), request).await
}

// The one place the four mounts are written: path first, `ANY` inside that path's handler group,
// then the `, port:` trailer that says which listener the group serves. `ANY` has no
// `axum::routing::MethodFilter` constant, so the `service_routes!` macro routes it through
// `axum::routing::any` and lists it as the literal `"ANY"`
// (`packages/typecrate/services/src/server/metadata.rs`); the `/v1/services/routes` listing is
// generated from these same tokens, so it cannot disagree with the router.
//
// The trailer is per GROUP (the health sample shows both spellings side by side: the same path with
// and without `port: 8010`), so all four groups carry it — the TypeScript puts
// `port: LOCAL_AREA_NETWORK_PROVIDER_PORT` on all four route objects, and a group that omitted it
// here would quietly merge into the services default listener instead. `8800` is written as a literal
// because the macro grammar is `, port: <literal>` and a route file cannot name a constant it did not
// define; `packages/typecrate/provider/tests/routes.rs` pins `serve::PROVIDER_PORT` to that same
// number, so the two spellings cannot drift.
crate::service_routes! {
    "/providers/private/v1/chat/completions" => {
        ANY: chat_completions, "OpenAI-compatible chat completions relay";
    }, port: 8800;

    "/providers/private/v1/responses" => {
        ANY: responses, "OpenAI-compatible responses relay";
    }, port: 8800;

    "/providers/private/v1/models" => {
        ANY: models, "List the served model catalog";
    }, port: 8800;

    "/providers/private/v1/stats" => {
        ANY: stats, "Report provider token stats, or reset one model's stats";
    }, port: 8800;
}
