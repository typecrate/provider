//! The failure taxonomy: one closed code vocabulary, the SDK-compatible request messages,
//! and the mid-stream error frame the gateway writes.
//!
//! TypeScript origins, one per block:
//!   * the code set, the frame key set and the sentence table come from
//!     `packages/underload/service/src/server/start.ts` - `STREAM_ERROR_CODES` (`:318-325`, the
//!     `StreamErrorCode` alias is `:327`), `StreamErrorPayload` (`:329-337`),
//!     `UNKNOWN_STREAM_ERROR_MESSAGE` (`:341`), `MAX_ERROR_DETAIL_LENGTH` (`:346`), the vocabulary
//!     sets (`:358-417`), the secret / flatten / sanitize helpers (`:421-466`), the chain reads
//!     (`:472-527`), `classifyStreamError` (`:581-621`) and `buildStreamErrorPayload`
//!     (`:635-705`, the frame literal is `:695-702`), written inline by the gateway's stream
//!     `catch` at `:860-873` (the log is `:861`, the `writeSSE` call is `:866-871`);
//!   * the REQUEST-side message strings come from the OpenAI SDK those TypeScript clients
//!     actually throw - `APIError.makeMessage` (`node_modules/openai/core/error.js:21-38`),
//!     `APIUserAbortError` (`:72-76`, `Request was aborted.`), `APIConnectionError`
//!     (`:78-86`, `Connection error.`) and `APIConnectionTimeoutError` (`:88-92`,
//!     `Request timed out.`) - because `packages/agentic/provider/src/model-class.ts` surfaces
//!     upstream failures as exactly those objects and the routes key off them:
//!     `runtime/endpoint/provider/private/responses/private-responses.ts:25-27` (`is404`) and
//!     `:31-40` (`formatUpstreamError`, which renders `error.status` and `error.message`);
//!   * the MID-STREAM, in-band rule - what an `{"error":{…}}` object on an otherwise-200 stream
//!     becomes before the gateway ever sees it - comes from the SDK's stream loop, not from
//!     `start.ts`: `node_modules/openai/core/streaming.js:50` throws
//!     `new APIError(undefined, data.error, undefined, response.headers)`, and that constructor
//!     assigns `this.status = status` (`core/error.js:12`) and `this.code = data?.['code']`
//!     (`:17`), so the object the gateway classifies carries the upstream's `error.message`
//!     (through `makeMessage`) and its `error.code`, and NO numeric status.
//!     [`FailureSignals::from_upstream_frame`] reproduces the message and the code reads of that
//!     object; the ONE read it no longer reproduces is the missing status, which the deviation
//!     block below owns.
//!
//! WHY A SEPARATE MODULE. The route task maps a failure onto an HTTP verdict in three different
//! shapes - `status ?? 500` for a settled response, `{"error":"Internal Server Error"}` for a
//! failure that happens before the stream head is written, and the classified SSE frame for a
//! failure after it - and all three read the SAME three accessors ([`TransportError::status`],
//! [`TransportError::kind`], [`TransportError::message`]). One taxonomy is what stops a fourth
//! shape drifting.
//!
//! DEVIATION (deliberate, recorded rather than silent) - THE CLASSIFIER READS DATA, NOT AN
//! ERROR GRAPH. TypeScript's `classifyStreamError` inspects a thrown `unknown`: the `name` of
//! every link in the `cause` chain, the `code` (errno) of the deepest link, a `status` anywhere
//! in the chain, and the message text (`collectErrorChain` / `readChainNames` / `readErrorErrno`
//! / `readUpstreamStatus`, `start.ts:472-527`). A Rust failure is already typed, so there is no
//! chain to walk: [`FailureSignals`] carries those SAME four signals as data, every rule and
//! every rule ORDER of `start.ts:581-621` is reproduced in [`classify_stream_error`], and the
//! two ways this crate builds signals ([`FailureSignals::from_transport_error`] and
//! [`FailureSignals::from_upstream_frame`]) feed the classifier exactly as the JS objects fed it -
//! with ONE exception, named in the block below: the status an in-band error frame declares, which
//! travels in its OWN field ([`FailureSignals::declared_status`]) precisely so the rule order above
//! stays intact.
//! The free-text rules stay live on purpose: the messages reqwest and hyper produce ARE the text
//! the TypeScript rules match against (`other side closed`, `operation timed out`,
//! `connection reset by peer`).
//!
//! DEVIATION (deliberate, recorded rather than silent, 2026-11-24) - AN IN-BAND ERROR FRAME'S
//! DECLARED STATUS RESCUES A FAILURE NOTHING ELSE NAMED. Until this date the classifier reproduced
//! the SDK exactly here, and the SDK discards the value: `streaming.js:50` throws
//! `new APIError(undefined, data.error, undefined, headers)`, so `err.status` is `undefined` whatever
//! the upstream wrote in `error.status` (`core/error.js:12`), and an in-band frame could therefore
//! NEVER reach `upstream_status`. What that parity threw away was the class of verdict with NO signal
//! the wording rules could read: a frame that wrote a number, named an error class, or only stated
//! its size came out as `code: "unknown"` with the frozen sentence and no `status` key - including
//! the two the operator actually has to act on, an engine that died
//! (`{"error":{"type":"server_error","message":"the engine stopped unexpectedly (exit code 1)…"}}`,
//! `Temp/basalt-check/serve/handler.py:733-735`) and a context refusal relayed after the stream head
//! (`Temp/basalt-check/serve/prompt.py:15-28`). It is NOT true that every in-band failure came out
//! `unknown`: a frame whose sentence happened to spell a transport needle - `upstream stream
//! terminated`, `invalid json`, `operation timed out` - was already `connection` / `decode` /
//! `timeout` on rules 5, 7 and 8, and that half of the behaviour is what the rule below deliberately
//! preserves. The rule NOW: [`FailureSignals::from_upstream_frame`] reads a valid numeric
//! `error.status` (validated by [`FailureSignals::frame_status`] across the shapes
//! [`status_from_json`] accepts, the range test being the one `readUpstreamStatus` applies,
//! `start.ts:506-517`) and, failing that, infers one from the size refusal the sentence or the
//! `code` names ([`status_declared_by_size_refusal`]) or from the error class the frame names
//! ([`status_declared_by_error_type`]) - and all three become
//! [`FailureSignals::declared_status`]. [`classify_stream_error`] then uses that field for ONE
//! thing: rescuing the fallthrough, so a frame nothing else could name becomes `upstream_status` and
//! [`StreamFailure::payload`] stamps a real `status` key. A frame the names, errno or wording rules
//! already named keeps its code and its ABSENT `status` key.
//!
//! THE SAME-DAY REFINEMENT (D7) - A MESSAGE THAT NAMES A SIZE REFUSAL IS A 400, AHEAD OF THE CLASS
//! TABLE. The class table above answers the local relay's context refusal with 500, because the relay
//! stamps `server_error` on every engine `ERR` line
//! (`Temp/basalt-check/serve/handler.py:739-741`) - and 500 is a verdict the consumer can act on only
//! by retrying, which is precisely the wrong remedy for a prompt that is too long. So
//! [`status_declared_by_size_refusal`] runs BETWEEN the numeric status and the class table, matching
//! the small explicit phrase list in [`CONTEXT_OVERFLOW_MARKERS`] case-insensitively - in the frame's
//! MESSAGE and in its `error.code` - and produces 400. WHY 400 IS THE RIGHT ANSWER AND NOT AN
//! INVENTION: the upstream answers this IDENTICAL message with HTTP 400 the moment it happens before
//! the response head - the engine sentence travels as a `ValueError` and the handler's `ValueError`
//! arm writes `400` / `invalid_request_error` (`Temp/basalt-check/serve/handler.py:550-555`) - so an
//! in-band occurrence is the same refusal that merely arrived after the head was committed to 200. A
//! numeric `error.status` still outranks this rule as it outranks the table: where the upstream states
//! a verdict, this crate repeats it.
//!
//! WHY THE RESCUE CANNOT COST THE CONSUMER AN ACTION: `@underload/harness` translates the frame
//! through `providerWireFailureOf`
//! (`packages/underload/harness/src/engine/transport/provider-timeouts.ts:666`), which reads
//! `code` first and `status` second; an unrecognised code falls to the codeless transient ladder,
//! and a non-terminal 5xx under `upstream_status` falls to that SAME ladder
//! (`isTerminalProviderStatus` names only the statuses about OUR request, `:510-511`). So a frame
//! that was `unknown` and becomes `upstream_status` + 500 retries exactly where it retried, and gains
//! the status in the message. The consumer does not read a wire code this crate does not emit, so the
//! closed six-code set ([`FAILURE_KINDS`]) is unchanged: a seventh spelling would map to the codeless
//! ladder and buy nothing.
//!
//! WHY THE SAME TRANSLATION MAKES THE STATUS A RESCUE AND NEVER AN OVERRIDE (the 2026-11-24
//! correction): that "same ladder" is only the same for a frame that had NO code. A frame the wording
//! rules name carries a CODE, and the codes are asymmetric: wire `connection` maps to
//! `PROVIDER_CONNECTION_CODE` (`:714-721`), whose ladder is a few attempts with single-digit-second
//! waits and then a terminal failure (`:101-106`), while `upstream_status` with 408/429/5xx reaches
//! the codeless arm at `:761-790` - 1 + 10 attempts and ~17 min of backoff (`:118-125`). Attaching a
//! declared status to an already-named frame would therefore have traded a short, quickly-terminal
//! ladder for eleven more attempts at a failure the transport had already diagnosed. Hence the rule,
//! which is the ladder note on [`classify_stream_error`]: NEVER DEMOTE A NAMED TRANSPORT VERDICT INTO
//! A LONGER RETRY LADDER.
//!
//! WHAT THE 400 DOES AND DOES NOT BUY: the consumer's overflow gate
//! (`isContextOverflowSignal`, `provider-timeouts.ts:467-473`) refuses to read matching prose as an
//! overflow behind a 5xx, so for a sentence ITS OWN pattern list recognises, the 400 is what turns a
//! retry into the compaction remedy. It is not a general unlock, and the crate's marker list is
//! WIDER than the consumer's pattern list: an engine-raw line such as `prompt (N tokens) + max_new (M)
//! exceeds the context (C)` gets our 400 but matches none of `CONTEXT_OVERFLOW_PATTERNS`
//! (`:424-433`), so the consumer lands on the terminal `provider-rejected` refusal (`:751-758`) rather
//! than on compaction. Stated plainly because both answers are defensible and only one is claimed:
//! for the wording only this crate recognises, the 400 converts eleven futile retries into a terminal
//! refusal, not into a smaller prompt.
//!
//! DEVIATION (deliberate, recorded rather than silent, 2026-11-24) - THE `unknown` SENTENCE QUOTES
//! THE UPSTREAM ONCE IT HAS SAID ANYTHING. `start.ts:682-685` answers that class with the frozen
//! [`UNKNOWN_STREAM_ERROR_MESSAGE`] and NOTHING else, and that is still the rule for the LEAD of
//! the sentence: a consumer string-matching the legacy prefix keeps matching. What changed is that
//! the class may now TAIL its own detail (`Upstream stream terminated: <detail>`, see
//! [`StreamFailure::sentence`]), because the only frames that still reach it are the ones that
//! carried no status, no known class, no errno and no matching wording - and for those, the frozen
//! constant alone threw the upstream's own sentence away, which is what made the deployed
//! `unknown` unanswerable. The constant itself is unchanged and still exported byte-identical, and
//! a frame with no detail at all still gets the bare constant.
//!
//! DEVIATION (deliberate) - TEXT RULES USE NEEDLE LISTS, NOT REGEXES. The three
//! `*_TEXT_PATTERN` expressions (`start.ts:411-417`) are alternations of literal phrases with
//! exactly one optional-character rule (`socket hang ?up`); expanding every alternative into a
//! lower-case needle reproduces them token for token and keeps this crate free of the `regex`
//! dependency the harness crate needed for its marker grammar. The expansion is visible in the
//! tables below, one row per alternative, so the equivalence is reviewable without running
//! anything. `char::is_whitespace` (Rust's `White_Space` property) is the stand-in for JS `\s`,
//! which additionally matches `U+FEFF`; no upstream message in this workspace carries it.
//!
//! DEVIATION (deliberate) - THE UPSTREAM'S OWN MESSAGE IS STORED REDACTED. TypeScript keeps the
//! raw text on the error object and redacts only when the wire frame is built
//! (`redactSecrets`, `start.ts:435-438`). Here the detail is flattened + redacted at CAPTURE time
//! (crate rule 12: a credential value must never appear in an error, a log line, or a `Debug`
//! output), so an upstream that echoes the caller's key back inside an error body cannot put that
//! key into a `{:?}`. The redaction itself is byte-identical to the gateway's (`**redacted**`),
//! and the 200-character cap it applies is the cap the gateway applied anyway.
//!
//! NOTHING IN THIS MODULE CAN PRINT A CREDENTIAL. No variant holds a [`crate::vault::Secret`],
//! every free-text field goes through [`flatten_error_detail`], and the credential identity a
//! caller may log is an entry LABEL plus a [`crate::vault::VaultValidity`] - which is how
//! [`crate::transport::Attempt`] is shaped.

use serde_json::{Map, Value};

use reqwest::StatusCode;

// ---------------------------------------------------------------------------
// The closed code vocabulary
// ---------------------------------------------------------------------------

/// The closed vocabulary a consumer may branch on (`start.ts:318-325 STREAM_ERROR_CODES`).
///
/// The set is CLOSED because it is the public contract of the SSE frame, and a consumer that
/// branches on `timeout` vs `connection` vs `aborted` is making a RETRY decision. The TypeScript
/// supervisor treats a transport failure as fatal for the worker while an upstream mid-stream
/// failure is not, which is why [`StreamFailure::is_upstream`] exists next to the code: the code
/// alone cannot carry that fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FailureKind {
    /// A clock expired (`APIConnectionTimeoutError`, `TimeoutError`, `UND_ERR_*_TIMEOUT`).
    Timeout,
    /// A hard transport failure: DNS, connect, reset, premature close.
    Connection,
    /// Explicit cancellation - by the caller's signal, or by the consumer that dropped a stream.
    Aborted,
    /// The upstream answered with a definite HTTP status.
    UpstreamStatus,
    /// A payload could not be decoded (`SyntaxError` in TS, a non-JSON `data:` frame here).
    Decode,
    /// Nothing the classifier reads matched: no OBSERVED status, no timeout / abort / connection /
    /// decode name, errno or wording, and no status an in-band frame DECLARED (rule 9's rescue is
    /// what keeps a frame that stated a verdict out of here - see [`classify_stream_error`]).
    ///
    /// THE MESSAGE IS THE FROZEN CONSTANT FIRST, THEN THE UPSTREAM'S OWN WORDS. `start.ts:682-685`
    /// emits only [`UNKNOWN_STREAM_ERROR_MESSAGE`], and that constant is still the sentence's LEAD, so
    /// a consumer string-matching the legacy frame keeps matching; since 2026-11-24 the class may TAIL
    /// the detail (`Upstream stream terminated: <detail>`, [`StreamFailure::sentence`]) because the
    /// only frames that still land here are ones that said something nothing could classify, and the
    /// bare constant alone threw that sentence away. A frame with no detail still gets the bare
    /// constant.
    Unknown,
}

impl FailureKind {
    /// The wire spelling: the value of the frame's `code` key.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Connection => "connection",
            Self::Aborted => "aborted",
            Self::UpstreamStatus => "upstream_status",
            Self::Decode => "decode",
            Self::Unknown => "unknown",
        }
    }
}

/// Every code, in `STREAM_ERROR_CODES` order - the closed set, iterable by any test.
pub const FAILURE_KINDS: [FailureKind; 6] = [
    FailureKind::Timeout,
    FailureKind::Connection,
    FailureKind::Aborted,
    FailureKind::UpstreamStatus,
    FailureKind::Decode,
    FailureKind::Unknown,
];

/// The only message any pre-existing consumer string-matches for the class no rule can name
/// (`start.ts:341 UNKNOWN_STREAM_ERROR_MESSAGE`), kept byte-identical.
///
/// It is the LEAD of the `unknown` sentence, never necessarily the whole of it: since 2026-11-24
/// the upstream's own detail is appended after `: ` when the frame carried one (see the
/// `unknown` arm of [`StreamFailure::sentence`]), which is what lets a prose-matching consumer see
/// a size refusal the class cannot name. A prefix match still succeeds either way.
pub const UNKNOWN_STREAM_ERROR_MESSAGE: &str = "Upstream stream terminated";

/// Longest free-text fragment that reaches the wire (`start.ts:346`).
///
/// Upstream bodies can be kilobytes of proxy HTML; the consumer needs the class and the first
/// line, not the page. The cap also bounds how much untrusted text a `Debug` of any failure in
/// this crate can print.
pub const MAX_ERROR_DETAIL_LENGTH: usize = 200;

/// The SDK's own sentence for a connection failure (`core/error.js:78-86`).
pub const CONNECTION_MESSAGE: &str = "Connection error.";

/// The SDK's own sentence for a timeout (`core/error.js:88-92`).
pub const TIMEOUT_MESSAGE: &str = "Request timed out.";

/// The SDK's own sentence for a cancellation (`core/error.js:72-76`).
pub const ABORT_MESSAGE: &str = "Request was aborted.";

/// The SDK's wording when a status arrived with no body (`core/error.js:31-32`).
pub const STATUS_NO_BODY_SUFFIX: &str = "status code (no body)";

/// The marker [`redact_secrets`] writes in place of a credential-shaped fragment.
pub const REDACTION_MARKER: &str = "**redacted**";

// ---------------------------------------------------------------------------
// Wire-facing text hygiene: redaction, flattening, target sanitising
// ---------------------------------------------------------------------------

/// The `[A-Za-z0-9_-]` class of rule 1 (an ASCII-only class, so a byte test is exact).
fn is_key_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

/// The byte length of a leading whitespace run.
fn whitespace_run(text: &str) -> usize {
    text.char_indices()
        .take_while(|(_, character)| character.is_whitespace())
        .map(|(offset, character)| offset + character.len_utf8())
        .last()
        .unwrap_or(0)
}

/// The byte length of a leading non-whitespace run (`\S+`).
fn non_space_run(text: &str) -> usize {
    text.char_indices()
        .find(|(_, character)| character.is_whitespace())
        .map_or(text.len(), |(offset, _)| offset)
}

/// Replaces credential-shaped fragments with [`REDACTION_MARKER`]
/// (`start.ts:421-438 SECRET_PATTERNS` + `redactSecrets`).
///
/// Two rules, ported literally:
///   1. `sk-` followed by SIX OR MORE `[A-Za-z0-9_-]` characters, maximal run, CASE-SENSITIVE
///      (the JS literal has no `i` flag). A shorter tail is left alone: `sk-` is only a
///      credential SHAPE when it carries a real key.
///   2. `Bearer` + whitespace + one non-whitespace run, CASE-INSENSITIVE (the JS literal carries
///      the `i` flag). The whole match - the scheme word included - is replaced.
///
/// This is what stops an upstream that echoes the caller's key back inside an error body from
/// putting that key on the wire, into a log line, or into a `Debug`.
#[must_use]
pub fn redact_secrets(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        // Rule 1: `sk-[A-Za-z0-9_-]{6,}`, matched at byte level because the class is ASCII-only.
        // A run shorter than six does NOT match, and scanning resumes at the `s`.
        if value[index..].starts_with("sk-") {
            let mut end = index + 3;

            while end < bytes.len() && is_key_char(bytes[end]) {
                end += 1;
            }

            if end - index >= 9 {
                out.push_str(REDACTION_MARKER);
                index = end;
                continue;
            }
        }

        // Rule 2: `[Bb][Ee][Aa][Rr][Ee][Rr]\s+\S+`. The scheme word is case-folded; the span that
        // disappears is the whole match, so `Bearer sk-live-...` becomes one marker, not two.
        if value[index..].len() >= 6 && value[index..index + 6].eq_ignore_ascii_case("bearer") {
            let after = &value[index + 6..];
            let spaces = whitespace_run(after);
            let run = non_space_run(&after[spaces..]);

            // `\s+` and `\S+` both need at least one character, or the rule does not fire.
            if spaces > 0 && run > 0 {
                out.push_str(REDACTION_MARKER);
                index += 6 + spaces + run;
                continue;
            }
        }

        let character = value[index..].chars().next().unwrap_or('\0');
        out.push(character);
        index += character.len_utf8();
    }

    out
}

/// Collapses free text onto one line, redacts it, and caps it
/// (`start.ts:442-448 flattenErrorDetail`).
///
/// Order matters and is the TypeScript order: whitespace collapse and trim FIRST (an SSE `data:`
/// field must not embed a raw newline), redaction SECOND, cap THIRD - so the marker itself counts
/// toward [`MAX_ERROR_DETAIL_LENGTH`] exactly as it does in JS, and the truncation marker is the
/// literal `...`.
#[must_use]
pub fn flatten_error_detail(value: &str) -> String {
    let mut collapsed = String::with_capacity(value.len());
    // A whitespace run is remembered, not written: the space is flushed only in front of the NEXT
    // non-space character, which is byte-for-byte `.replace(/\s+/g, ' ').trim()` (`start.ts:445`).
    // Writing the separator eagerly instead would leave a trailing space behind when the input ENDS
    // in whitespace, which is exactly the case an upstream error body hit (`"...>\n</html>\n"`).
    let mut pending_space = false;

    for character in value.chars() {
        if character.is_whitespace() {
            pending_space = true;
            continue;
        }

        // A leading run vanishes because nothing has been written yet, which is the `.trim()` half
        // of the same expression.
        if pending_space && !collapsed.is_empty() {
            collapsed.push(' ');
        }

        pending_space = false;
        collapsed.push(character);
    }

    let redacted = redact_secrets(&collapsed);

    if redacted.chars().count() <= MAX_ERROR_DETAIL_LENGTH {
        return redacted;
    }

    let truncated: String = redacted.chars().take(MAX_ERROR_DETAIL_LENGTH).collect();

    format!("{truncated}...")
}

/// Normalises an upstream target for the wire (`start.ts:453-466 sanitizeUpstreamTarget`).
///
/// An absolute URL keeps ORIGIN + PATHNAME only, so an embedded userinfo credential, a query
/// token or a hash can never be printed; a bare `label` or `host:port` keeps its text up to the
/// first `?`, `#` or `@`. An empty or whitespace-only input has no target at all, which the
/// caller renders as `stream from endpoint ...` instead.
#[must_use]
pub fn sanitize_upstream_target(value: &str) -> Option<String> {
    let candidate = value.trim();

    if candidate.is_empty() {
        return None;
    }

    let normalised = match candidate.split_once("://") {
        Some((scheme, rest)) => {
            // Origin = scheme + "//" + authority, where authority runs to the first path/query/
            // hash separator. Userinfo is dropped: `user:token@host` must print as `host`, which
            // is the whole reason this function exists.
            let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
            let host = authority
                .rsplit_once('@')
                .map_or(authority, |(_, host)| host);
            let path = rest[authority.len()..]
                .split(['?', '#'])
                .next()
                .unwrap_or_default();
            let path = if path == "/" { "" } else { path };

            format!("{scheme}://{host}{path}")
        }
        // Not absolute (a relay label, or a bare `host:port`): strip the halves that can carry a
        // credential.
        None => candidate
            .split(['?', '#', '@'])
            .next()
            .unwrap_or(candidate)
            .to_owned(),
    };

    let flat = flatten_error_detail(&normalised);

    (!flat.is_empty()).then_some(flat)
}

/// Drops the leading `{status} ` the SDK (and some relays) prefix onto their own message
/// (`start.ts:664-670`), because the frame names the status in its own key and in the sentence.
#[must_use]
fn strip_status_prefix(message: &str, status: Option<StatusCode>) -> String {
    let Some(status) = status else {
        return message.to_owned();
    };

    let prefix = format!("{status} ");

    message
        .strip_prefix(prefix.as_str())
        .map_or_else(|| message.to_owned(), ToOwned::to_owned)
}

/// Validates an integer as an HTTP status the way `readUpstreamStatus` does
/// (`start.ts:506-517`: an integer in 100..=599, nothing else).
#[must_use]
fn status_code(value: u64) -> Option<StatusCode> {
    if !(100..=599).contains(&value) {
        return None;
    }

    StatusCode::from_u16(
        u16::try_from(value).expect("the range check above bounds the value to u16"),
    )
    .ok()
}

/// Reads a JSON `status` VALUE as an HTTP status, accepting every shape the deployed readers
/// accept. Added 2026-11-24: this crate read the field through `Value::as_u64` alone, which silently
/// dropped the two shapes real proxies write,
///   * `500.0` - a JSON number with a fraction part is a FLOAT, and both readers treat a float as an
///     integer: `readUpstreamStatus` tests `Number.isInteger` on a value JSON has already parsed
///     (`start.ts:510`), and the consumer's `wireStatusOf` takes `Math.trunc` of any finite number
///     (`packages/underload/harness/src/engine/transport/provider-timeouts.ts:655-660`); and
///   * `"500"` - the quoted form, which `wireStatusOf` accepts on purpose ("some proxies serialise it
///     quoted", same lines: `/^\d{3}$/` over `value.trim()`).
///
/// WHAT STILL MEANS "NO STATUS": a non-integral float (`500.5`), a negative number, anything outside
/// 100..=599 in ANY shape (`"0999"` parses to 999 and is rejected by the same range check), a string
/// that is not pure ASCII digits (`"+500"`, `"5e2"`, `"500.0"`, `" 5 0 0 "`, a non-ASCII digit), a
/// bool, `null`, an object or an array. Dropping a status is never a fault: it leaves the wording
/// rules, and then `unknown`, exactly as before.
#[must_use]
fn status_from_json(value: &Value) -> Option<StatusCode> {
    match value {
        Value::String(text) => {
            let trimmed = text.trim();

            // The digit test is explicit rather than left to `parse`, because `parse::<u64>` also
            // accepts a leading `+` - and `" 500 "` is what a quoted header value looks like.
            if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }

            trimmed.parse::<u64>().ok().and_then(status_code)
        }
        Value::Number(number) => {
            if let Some(integer) = number.as_u64() {
                return status_code(integer);
            }

            let float = number.as_f64()?;

            // `fract` is exact inside the range a status can occupy, and the negative test comes
            // first so `-500` cannot saturate its way into the accepted range. A magnitude above
            // `u64::MAX` saturates and is then rejected by the range check.
            if float < 0.0 || float.fract() > 0.0 {
                return None;
            }

            status_code(float as u64)
        }
        _ => None,
    }
}

/// The status an upstream error OBJECT means when it names an error CLASS but writes no numeric
/// `status` (the 2026-11-24 in-band deviation in the module header).
///
/// WHY INFERENCE IS DEFENSIBLE HERE AND WOULD NOT BE ELSEWHERE IN THIS MODULE: every other
/// classifier input is a fact some layer OBSERVED (a status, an errno, a class name); `error.type`
/// is the upstream's own statement about the verdict it is reporting, so it is the weakest signal
/// that still beats "no idea". The eight rows are the OpenAI-compatible class spellings the relays
/// this crate serves really write - `server_error` is what the local Basalt server sends for a dead
/// engine and for an engine `ERR` line (`Temp/basalt-check/serve/handler.py:732-737`,
/// `:739-741`) - and the rest are the OpenAI error classes a compatible proxy passes through.
///
/// THE TWO ROWS ADDED ON 2026-11-24 (the review correction) are both spellings of a verdict the
/// consumer can act on: `rate_limit_error` is the OpenAI class a throttling proxy relays, whose HTTP
/// occurrence carries 429, and `context_length_exceeded` is the OpenAI size-refusal spelling, whose
/// HTTP occurrence carries 400 - the same 400 [`status_declared_by_size_refusal`] produces from the
/// sentence, and the same 400 the local server answers on its `/v1/responses` path, where it writes
/// THAT WORD (as `error.code`, beside `type: invalid_request_error`) and a 400 together
/// (`Temp/basalt-check/serve/handler.py:774-776`, `api/responses.py:31-43`). A row here changes
/// NOTHING for a frame the wording rules can name: like every other read in
/// [`FailureSignals::declared_status`], it only rescues an otherwise-`unknown` frame
/// ([`classify_stream_error`]).
///
/// WHAT THE TABLE DELIBERATELY DOES NOT COVER: a class outside it infers NOTHING, which is exactly
/// the pre-2026-11-24 behaviour (`usage_limit_exceeded`, `insufficient_quota`, the relay's
/// `not_found_error` spelling of `not_found`, and any vendor coinage stay status-less here, and the
/// wording rules still get their chance); and a numeric `error.status` ALWAYS wins over an inferred
/// one, because [`FailureSignals::from_upstream_frame`] tries the number first. The one inference that
/// outranks THIS table is [`status_declared_by_size_refusal`]: a `server_error` sentence that names a
/// size refusal means 400, not the 500 this table's `server_error` row states, and that specific case
/// is why the table is second rather than first.
///
/// WHY A WRONG ROW IS CHEAP: the consumer's translator
/// (`packages/underload/harness/src/engine/transport/provider-timeouts.ts:666`) sends a
/// non-terminal 5xx down the same transient ladder an `unknown` frame used to walk, so the worst a
/// wrong inference can do is name a retry it would have retried anyway.
const UPSTREAM_TYPE_STATUSES: [(&str, u16); 8] = [
    ("server_error", 500),
    ("model_busy", 409),
    ("invalid_request_error", 400),
    ("authentication_error", 401),
    ("forbidden", 403),
    ("not_found", 404),
    ("rate_limit_error", 429),
    ("context_length_exceeded", 400),
];

/// The `error.status` an error object infers from its own `error.type`, or `None`.
///
/// The class is read case- and space-insensitively (`Server_Error ` and `server_error` are the same
/// upstream verdict, and a spelling this table fails to match silently downgrades the frame), and a
/// non-string `type` - an object, a number, absent - infers nothing rather than throwing.
///
/// FOR AN IN-BAND FRAME THIS IS THE WEAKEST OF THE THREE STATUS READS: it is outranked by the frame's
/// own `error.status` and by [`status_declared_by_size_refusal`], in that order
/// ([`FailureSignals::from_upstream_frame`]) - and all three of them together are outranked by ANY
/// verdict rules 1-8 of [`classify_stream_error`] reach, because what they fill is
/// [`FailureSignals::declared_status`], the `unknown` rescue, not an observed status.
#[must_use]
fn status_declared_by_error_type(error: &Value) -> Option<StatusCode> {
    let declared = error
        .get("type")
        .and_then(Value::as_str)
        .map(|name| name.trim().to_lowercase())?;

    UPSTREAM_TYPE_STATUSES
        .iter()
        .find(|(name, _)| *name == declared)
        .and_then(|(_, status)| StatusCode::from_u16(*status).ok())
}

/// The two CODED size-refusal markers: the engine's own error codes
/// (`Temp/basalt-check/serve/engine/errors.py:5` `ERR_CODES`), which are the two rows of
/// [`CONTEXT_OVERFLOW_MARKERS`] that can arrive in a frame's `error.code` as well as in its message.
///
/// A FRAME CARRYING ITS CODE IS THE SHAPE THE SENTENCE DOCUMENTS, NOT A HYPOTHETICAL ONE (added as
/// its own read on 2026-11-24, after review): the `/v1/responses` surface writes a structured error
/// object with a `code` field beside the `type` (`Temp/basalt-check/serve/api/responses.py:42-43`),
/// and a proxy passing an engine `ERR` line through can put the code word where only the message was
/// looked for. Before that date only `error.message` was consulted, so
/// `{"type":"server_error","code":"context_overflow"}` - a frame that states its code and nothing
/// else - inferred this table's 500 instead of the 400 the same code means.
const CONTEXT_OVERFLOW_CODES: [&str; 2] = ["context_overflow", "context_no_room"];

/// The phrases that unambiguously name a CONTEXT-SIZE REFUSAL (D7, 2026-11-24) - the two
/// [`CONTEXT_OVERFLOW_CODES`] plus the two sentences the engine writes for them.
///
/// WHY A HANDWRITTEN LIST AND NOT A FUZZY MATCH: this is a status INFERENCE on untrusted prose, so
/// the cost of a false positive is a retry decision the consumer would not otherwise have made, and
/// the cost of a false negative is only the pre-2026-11-24 behaviour (the `type` table, then
/// `unknown`). Each row is therefore a phrase the relays in this workspace are KNOWN to emit, not a
/// guess at vendor wording - the same discipline as the `*_TEXT_NEEDLES` tables below. All rows are
/// matched lower-cased, hence the literal spellings here.
///
/// The two coded markers are the engine's own error codes (`Temp/basalt-check/serve/engine/errors.py:5`
/// `ERR_CODES`); the two phrases are the sentences the engine writes for them, in the server's own
/// words (`Temp/basalt-check/serve/prompt.py:15-28`, `room_error` / `overflow_error`) and in the
/// engine's raw `ERR` line (`Temp/basalt-check/serve/tests/test_protocol_facts.py:21-23` pins all
/// three spellings).
///
/// WHAT THE CODED MARKERS ARE WORTH ON THE MESSAGE PATH: little, on the shape this crate's own relay
/// produces. The relay strips the code word out of the message before it raises
/// (`Temp/basalt-check/serve/engine/process.py:178-179`, `message = split_err(err)[2]` keeps only the
/// sentence, and that sentence is what the mid-stream arm relays), so a real mid-stream overflow
/// reaches this crate as prose and is caught by `exceeds the context` / `leaves no room to answer`.
/// The two code rows earn their place twice over: they document what the two phrases mean, and since
/// 2026-11-24 they are matched against `error.code` as well ([`status_declared_by_size_refusal`]), so
/// a frame that carries its code - a proxy passing the engine line through, or a structured
/// `{"type":…,"code":…}` body - classifies the way the sentence alone does.
///
/// WHY `exceeds the context` AND NOT `exceeds the context window`: the shorter needle also catches
/// the ENGINE's own wording, `prompt (N tokens) + max_new (M) exceeds the context (C)`, which is what
/// arrives when the server does not rewrite the line (`prompt.py:86-95` `explain` rewrites only the
/// two coded shapes that carry full fields; anything else travels verbatim).
///
/// THE ONE SIZE REFUSAL THIS LIST DELIBERATELY DOES NOT MATCH: the per-slot sentence
/// `prompt (N tokens) + max_new (M) exceeds slot S's context (C)`
/// (`Temp/basalt-check/serve/tests/test_protocol_facts.py:23`). `explain` passes it through un-rewritten
/// (`prompt.py:89` requires `"slot" not in f`), so it contains neither needle and the stripped code
/// word is gone - that frame keeps the `type` table's verdict unless it also writes `context_overflow`
/// in its message or `error.code`. It is left unmatched because widening the needle to `exceeds`
/// alone would sweep in unrelated prose, and a false negative here only restores the pre-2026-11-24
/// behaviour.
const CONTEXT_OVERFLOW_MARKERS: [&str; 4] = [
    "context_overflow",
    "context_no_room",
    "exceeds the context",
    "leaves no room to answer",
];

/// The status an in-band error's MESSAGE states when it names a size refusal, or `None`.
///
/// WHY 400 AND NOT THE 500 THE `server_error` CLASS WOULD INFER (D7, 2026-11-24): the upstream
/// answers this IDENTICAL message with a CLIENT error whenever it hits before the response head. Two
/// independent arms state it: the handler's `ValueError` arm writes
/// `400` / `invalid_request_error` (`Temp/basalt-check/serve/handler.py:551-555`), and the
/// `/v1/responses` path classifies the same "found when the engine counted" `ValueError` as
/// `context_length_exceeded` (`Temp/basalt-check/serve/handler.py:774-776`). The overflow sentence is a
/// plain `ValueError` - `Temp/basalt-check/serve/prompt.py:15-28` returns `ValueError(...)`, and
/// `Temp/basalt-check/serve/engine/process.py:170-179` picks `ValueError` for any engine message
/// starting `prompt (`, which every overflow sentence does.
///
/// The in-band occurrence is the SAME refusal one step later: the mid-stream arm catches the same
/// `ValueError` and relays it as `server_error` under the comment "engine ERR after the stream
/// started: no 400 now" (`Temp/basalt-check/serve/handler.py:736-738`). That comment is about the
/// transport being unable to re-send a status, not about the request being at fault - which is exactly
/// why the class is too coarse to decide this case and the sentence is not. The verdict the consumer
/// needs is the one the pre-head path states: the remedy is a smaller request, not more time.
///
/// WHY THE CONSUMER CARES ABOUT THE NUMBER SPECIFICALLY - AND WHAT THE NUMBER DOES NOT BUY: its
/// overflow gate (`packages/underload/harness/src/engine/transport/provider-timeouts.ts`,
/// `isContextOverflowSignal`, `:467-473`) refuses to call a 5xx an overflow even when the prose
/// matches, so a 500 here keeps the request on the transient retry ladder for its full height, for a
/// problem that only a shorter request fixes. With the 400 it reaches the compaction remedy ONLY for
/// wording its own `CONTEXT_OVERFLOW_PATTERNS` (`:424-433`) recognises; this crate's four markers are
/// WIDER, so a frame carrying the engine's raw `prompt (N) + max_new (M) exceeds the context (C)`
/// lands on the consumer's terminal `provider-rejected` (`:751-758`) - a terminal refusal instead of
/// eleven retries, not a smaller prompt. The module header states the same limit.
///
/// WHAT IS CONSULTED (C4, 2026-11-24): the MESSAGE and the `error.code`, never `error.type` and never
/// the rest of the frame. The message is the shape the served relay actually produces (it strips the
/// code word out of the sentence, `Temp/basalt-check/serve/engine/process.py:178-179`); the code is
/// the shape the same fault takes on the surfaces that keep one
/// (`Temp/basalt-check/serve/api/responses.py:42-43`), and reading only the message used to answer
/// `{"type":"server_error","code":"context_overflow"}` with the class table's 500. Restricting the
/// read to those two fields is still what stops an unrelated CLASS from smuggling the inference in,
/// and a frame that WROTE a numeric `error.status` never reaches here at all
/// ([`FailureSignals::from_upstream_frame`] tries the number first).
#[must_use]
fn status_declared_by_size_refusal(message: &str, code: Option<&str>) -> Option<StatusCode> {
    let text = message.to_lowercase();

    let named_in_message = CONTEXT_OVERFLOW_MARKERS
        .iter()
        .any(|marker| text.contains(marker));

    // The same test over the CODE, which is a token rather than a sentence: trimmed and folded like
    // every other text read in this module, and matched with `contains` for consistency with the
    // message rule - a proxy that forwards the engine's whole `ERR` line as its code
    // (`context_overflow prompt=5000 …`) should classify the same way a frame naming the phrase does.
    let named_in_code = code.is_some_and(|code| {
        let code = code.trim().to_lowercase();

        CONTEXT_OVERFLOW_CODES
            .iter()
            .any(|marker| code.contains(marker))
    });

    (named_in_message || named_in_code)
        // 400 is the one status this rule can produce, so no lookup table is warranted; the
        // constant keeps it unmistakable and out of the class table above.
        .then(|| StatusCode::from_u16(400).ok())
        .flatten()
}

// ---------------------------------------------------------------------------
// The classifier: `classifyStreamError`, rule for rule
// ---------------------------------------------------------------------------

/// The `name` values that mean "a clock expired" (`start.ts:358-364`).
const TIMEOUT_ERROR_NAMES: [&str; 5] = [
    "APIConnectionTimeoutError",
    "TimeoutError",
    "ConnectTimeoutError",
    "HeadersTimeoutError",
    "BodyTimeoutError",
];

/// The errno codes that mean "a clock expired", not "a socket broke" (`start.ts:367-372`).
const TIMEOUT_ERRNO_CODES: [&str; 4] = [
    "ETIMEDOUT",
    "UND_ERR_CONNECT_TIMEOUT",
    "UND_ERR_HEADERS_TIMEOUT",
    "UND_ERR_BODY_TIMEOUT",
];

/// Explicit cancellation (`start.ts:377`).
const ABORT_ERROR_NAMES: [&str; 2] = ["APIUserAbortError", "AbortError"];

/// The transport itself failed (`start.ts:381`). `APIConnectionError` is listed by exact name so
/// its timeout subclass can never reach this rule.
const CONNECTION_ERROR_NAMES: [&str; 4] = [
    "APIConnectionError",
    "SocketError",
    "FetchError",
    "ClientNetworkError",
];

/// errno codes for a socket/connection failure (`start.ts:385-400`).
const CONNECTION_ERRNO_CODES: [&str; 14] = [
    "ECONNREFUSED",
    "ENOTFOUND",
    "EAI_AGAIN",
    "ENETUNREACH",
    "EHOSTUNREACH",
    "ECONNABORTED",
    "ECONNRESET",
    "EPIPE",
    "ENETDOWN",
    "EHOSTDOWN",
    "ERR_STREAM_PREMATURE_CLOSE",
    "ERR_STREAM_DESTROYED",
    "UND_ERR_SOCKET",
    "UND_ERR_CONNECT",
];

/// Anything the JSON/SSE decoder raises (`start.ts:405`). `TypeError` is deliberately absent:
/// undici's `TypeError: terminated` is a CONNECTION failure.
const DECODE_ERROR_NAMES: [&str; 3] = ["SyntaxError", "ParseError", "JSONParseError"];

/// `TIMEOUT_TEXT_PATTERN` (`start.ts:411`), expanded into lower-case needles.
const TIMEOUT_TEXT_NEEDLES: [&str; 6] = [
    "timed out",
    "timeout",
    "timedout",
    "deadline exceeded",
    "stalled",
    "etimedout",
];

/// `CONNECTION_TEXT_PATTERN` (`start.ts:415`), expanded into lower-case needles - one row per
/// regex alternative, with `socket hang ?up` and the `(?:...)` groups expanded explicitly.
const CONNECTION_TEXT_NEEDLES: [&str; 18] = [
    "fetch failed",
    "failed to fetch",
    "premature close",
    "other side closed",
    "socket hangup",
    "socket hang up",
    "connection error",
    "connection reset",
    "connection refused",
    "connection closed",
    "connection ended",
    "connection terminated",
    "connection aborted",
    "stream reset",
    "stream terminated",
    "unexpected end of stream",
    "network error",
    // The bare `terminated` is undici's whole message for a body cut with no cause code.
    "terminated",
];

/// `DECODE_TEXT_PATTERN` (`start.ts:417`), expanded into lower-case needles.
const DECODE_TEXT_NEEDLES: [&str; 13] = [
    "json",
    "malformed",
    "invalid line",
    "invalid json",
    "invalid sse",
    "invalid payload",
    "invalid event",
    "not valid json",
    "unexpected token",
    "unexpected end of input",
    "unexpected end of json",
    "cannot deserialize",
    "parse error",
];

/// True for the generic libuv errno spelling `^E[A-Z0-9]+$` (`start.ts:407-409`): `E` plus at
/// least one upper-case letter or digit. Used only for codes outside the enumerated sets above.
#[must_use]
pub fn is_libuv_errno(code: &str) -> bool {
    let Some(rest) = code.strip_prefix('E') else {
        return false;
    };

    !rest.is_empty()
        && rest
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

/// True for a code that really IS a transport errno, as opposed to an upstream body's error code.
///
/// `readErrorErrno` (`start.ts:493-503`) is deliberately paranoid about the same thing: on an
/// OpenAI `APIError` the top-level `code` is the upstream JSON error code (`insufficient_quota`,
/// lower case), not a transport errno, so it is only accepted when it spells like one. The three
/// accepted shapes are the enumerated sets, the `ERR_STREAM_*` / `UND_ERR_*` undici families
/// (`start.ts:396-399`, which are not LIBUV-shaped but are real codes), and a LIBUV spelling of
/// at least 7 characters.
///
/// WHY THE LENGTH FLOOR. In TS the errno comes from a FIELD (`cause.code`), so any string is
/// plausible. Here the only field-shaped source is gone and a text scan is the substitute, so a
/// bare uppercase word inside an upstream message (`ERROR 500`, `FAILED`, `HTTP`) would otherwise
/// be scraped into the frame as a fabricated errno. `ECONNRESET` (10), `EHOSTUNREACH` (10),
/// `ETIMEDOUT` (9) and `EPIPE` (5, enumerated) all survive; five-letter dictionary words do not.
#[must_use]
pub fn is_transport_errno(code: &str) -> bool {
    TIMEOUT_ERRNO_CODES.contains(&code)
        || CONNECTION_ERRNO_CODES.contains(&code)
        || code.starts_with("ERR_STREAM_")
        || code.starts_with("UND_ERR_")
        || (is_libuv_errno(code) && code.len() >= 7)
}

/// Pulls the first transport-errno token out of free text.
///
/// This is the Rust stand-in for `error.cause.code`: the Rust transport stack carries no errno
/// field, but reqwest and hyper print one inside their messages (`Connection reset by peer
/// (os error 104)`, `error sending request: ECONNRESET`), and a word scan for the accepted
/// spellings finds what the JS chain read would have found. Everything else yields `None`, which
/// the frame renders as an ABSENT `errno` key - never a wrong one.
#[must_use]
pub fn find_transport_errno(text: &str) -> Option<String> {
    text.split(|character: char| !matches!(character, 'A'..='Z' | '0'..='9' | '_'))
        .find(|token| !token.is_empty() && is_transport_errno(token))
        .map(ToOwned::to_owned)
}

/// Everything the TypeScript classifier can read off a thrown error chain, as DATA.
///
/// The first four fields are exactly the four reads `start.ts:491-527` performs (`names`, `errno`,
/// `status`, and the message/name text the soft rules match). Building one from a Rust failure is
/// three lines, and building one from an arbitrary JSON error object is
/// [`FailureSignals::from_upstream_frame`] - so the classifier keeps ONE implementation for both,
/// the way one `classifyStreamError` serves every thrown object in TS.
///
/// THE FIFTH FIELD IS THE ONE DEVIATION, AND IT EXISTS TO PROTECT THE RULE ORDER. A single
/// `status` field would have to mean two different things - "a layer SAW a response head" and "the
/// upstream SENT a number in its body" - and the classifier's rule 3 (`start.ts:600`) only
/// outranks the wording rules for the first of those. Splitting the two reads is what lets rule 3
/// stay byte-identical to TS while the in-band status still reaches the wire: it is a
/// [`FailureSignals::declared_status`], and rule 9's rescue arm - not rule 3 - is what consumes it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FailureSignals {
    /// `readChainNames` (`start.ts:484-490`): the `name` of every chain link.
    pub names: Vec<String>,
    /// `readErrorErrno` (`start.ts:493-503`): the deepest `cause.code`, or a LIBUV-shaped
    /// top-level `code`.
    pub errno: Option<String>,
    /// `readUpstreamStatus` (`start.ts:506-517`): a definite HTTP status, anywhere in the chain.
    ///
    /// PARITY FIELD - and it means OBSERVED, not DECLARED. In TS the only chain that ever carries
    /// one is an `APIError` built from a response head, i.e. a status some layer saw; that is also
    /// the only thing that fills this field here ([`TransportError::Status`], through
    /// [`FailureSignals::from_transport_error`]). A number an upstream wrote inside a 200 stream's
    /// error object is NOT this field - the SDK throws it away (`core/error.js:12`), so no JS chain
    /// ever had it, and handing it to rule 3 would demote a named transport verdict
    /// ([`FailureSignals::declared_status`], and the note on [`classify_stream_error`]'s ladder).
    pub status: Option<StatusCode>,
    /// The status an IN-BAND error OBJECT declares without any layer having observed it
    /// (2026-11-24): the number it wrote in `error.status` ([`FailureSignals::frame_status`]), or
    /// the one its size-refusal sentence ([`status_declared_by_size_refusal`]) or its error class
    /// ([`status_declared_by_error_type`]) states.
    ///
    /// It is kept apart from [`FailureSignals::status`] for exactly one reason: precedence. Rule 3
    /// of [`classify_stream_error`] is the TS read of an OBSERVED status and outranks every wording
    /// rule; a declared status may only RESCUE a failure nothing else could name, which is rule
    /// 9's arm. One field for both would have made every in-band verdict outrank `connection`.
    pub declared_status: Option<StatusCode>,
    /// The chain's message texts. The soft rules test the joined text - in TS `text` is an
    /// array of TWO haystacks, the joined names and the joined messages (`start.ts:588-589`),
    /// so a class name alone can satisfy a wording rule when the message is empty.
    pub messages: Vec<String>,
}

impl FailureSignals {
    /// Signals for one of this crate's request-side failures.
    ///
    /// The `name` is the SDK CLASS a JS caller would have thrown for the same verdict, and that
    /// mapping is what keeps the rule ORDER correct: a reqwest timeout carries
    /// `APIConnectionTimeoutError`, so rule 1 claims it before the abort rule can (the comment at
    /// `start.ts:355-361` documents exactly that trap - the SDK cancels its own fetch on timeout,
    /// so a timeout routinely contains an `AbortError`), and a connect failure carries
    /// `APIConnectionError`, so rule 4 claims it before the wording rules can.
    #[must_use]
    pub fn from_transport_error(error: &TransportError) -> Self {
        let (name, detail, scrape_errno) = match error {
            TransportError::Timeout { detail } => ("APIConnectionTimeoutError", detail, true),
            TransportError::Aborted { detail } => ("APIUserAbortError", detail, true),
            TransportError::Connection { detail } => ("APIConnectionError", detail, true),
            TransportError::Decode { detail } => ("SyntaxError", detail, false),
            TransportError::Build { detail } => ("", detail, false),
            // A status-bearing error has a STATUS, which is the signal; scraping the upstream's
            // own prose for an uppercase token would fabricate an errno the JS chain never had.
            TransportError::Status { detail, .. } => ("", detail, false),
        };

        Self {
            names: (!name.is_empty())
                .then(|| name.to_owned())
                .into_iter()
                .collect(),
            errno: scrape_errno.then(|| find_transport_errno(detail)).flatten(),
            status: error.status(),
            // Nothing here is DECLARED: this constructor reads a Rust transport failure, and the
            // only status it can carry is one a response head actually wrote.
            declared_status: None,
            messages: vec![detail.clone()],
        }
    }

    /// Signals for one upstream error OBJECT (`{"error":{...}}` on an otherwise-200 stream).
    ///
    /// WHAT IS STILL READ THE SDK'S WAY. The deployed relay reads that frame through the SDK, and
    /// the SDK throws `new APIError(undefined, data.error, undefined, response.headers)`
    /// (`node_modules/openai/core/streaming.js:50`), which assigns `this.code = data?.['code']`
    /// (`core/error.js:17`). So `message` is the root text (the SDK's `makeMessage`,
    /// `core/error.js:21-38`), `code` is an errno ONLY when it spells like a transport errno
    /// (filtered by `readErrorErrno`, `start.ts:493-503` - `insufficient_quota` does not survive
    /// it), and NO JS class name is invented: the SDK reports its own `APIError`, which is in none
    /// of the classifier's name sets.
    ///
    /// THE ONE READ THAT CHANGED ON 2026-11-24 - THE STATUS IS LIFTED, AS A RESCUE. The SDK also
    /// throws away whatever the upstream wrote in `error.status` (`core/error.js:12` assigns the
    /// `undefined` `streaming.js:50` passes), and this constructor used to reproduce that loss on
    /// purpose, for parity with the deployed TypeScript's `unknown`. Reproducing it meant an in-band
    /// verdict from the model server - the engine died, the prompt does not fit - reached the
    /// operator as `code: "unknown"` with no status and, until the same date, no detail either. An
    /// in-band error object is a real upstream verdict and the consumer's retry decision needs its
    /// status, so the number is now read: [`FailureSignals::frame_status`] validates it exactly as
    /// `readUpstreamStatus` does (`start.ts:506-517`), and when the frame names an error CLASS
    /// without writing a status, [`status_declared_by_error_type`] infers the one that class states.
    /// A numeric `status` always wins over an inferred one.
    ///
    /// WHERE THAT STATUS MAY BE USED, AND WHERE IT MAY NOT (the 2026-11-24 correction). All three
    /// reads land in [`FailureSignals::declared_status`], never in
    /// [`FailureSignals::status`], and only rule 9 of [`classify_stream_error`] consults them: a
    /// frame the names/errno/wording rules already named keeps its code and its ABSENT `status` key,
    /// and the status rescues only the frames that would otherwise be `unknown`. So a numeric status
    /// does NOT outrank a timeout, abort, connection or decode verdict here - the opposite of what
    /// an earlier revision of this constructor did by feeding the number to rule 3, which cost
    /// `{"error":{"type":"server_error","message":"upstream stream terminated"}}` its `connection`
    /// code and the short dial ladder that goes with it.
    ///
    /// THE SECOND STATUS RULE, ADDED THE SAME DAY (D7): BETWEEN the number and the class table sits
    /// [`status_declared_by_size_refusal`], which reads the frame's MESSAGE and its `error.code`. So
    /// the precedence inside this field is `error.status` -> size refusal -> `error.type`, and the
    /// reason the size refusal outranks the class table is that the class table would answer a
    /// context overflow with 500 (`server_error`) while the upstream answers the identical sentence
    /// with 400 on its pre-head path (`Temp/basalt-check/serve/handler.py:550-555`). An explicit
    /// number still outranks the wording for exactly the reason it outranks the class: where the
    /// upstream states a verdict, this crate repeats it instead of second-guessing it.
    #[must_use]
    pub fn from_upstream_frame(error: &Value) -> Self {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .map(flatten_error_detail)
            .unwrap_or_default();

        let code = error.get("code").and_then(Value::as_str);

        Self {
            // A plain upstream error object has no JS class name; the SDK reports its own
            // `APIError`, which is in none of the classifier's name sets, so no name is invented
            // here either.
            names: Vec::new(),
            errno: code
                .filter(|code| is_transport_errno(code))
                .map(ToOwned::to_owned),
            // NOTHING here is OBSERVED: an in-band frame travels inside a 200 body, so no response
            // head exists. The parity field stays `None` and rule 3 cannot fire on this path.
            status: None,
            // The frame's own declared verdict, in order of how directly it was STATED: the number it
            // wrote, then the size refusal its sentence or its `code` names, then the status its error
            // class states. All three are validated; anything else stays `None`, which leaves the
            // wording rules the last word.
            declared_status: Self::frame_status(error)
                .or_else(|| status_declared_by_size_refusal(&message, code))
                .or_else(|| status_declared_by_error_type(error)),
            messages: (!message.is_empty())
                .then_some(message)
                .into_iter()
                .collect(),
        }
    }

    /// The `status` field an in-band error OBJECT carries, validated the way `readUpstreamStatus`
    /// validates one (`start.ts:506-517`: an integer in 100..=599) across the three JSON shapes that
    /// reference - and the consumer's `wireStatusOf` - both accept
    /// ([`status_from_json`], 2026-11-24).
    ///
    /// WHAT THIS VALUE IS ALLOWED TO DO (corrected the same day it became a signal). It is a DECLARED
    /// status, and it reaches the classifier only through
    /// [`FailureSignals::declared_status`] - i.e. only as rule 9's RESCUE of an otherwise-`unknown`
    /// frame ([`classify_stream_error`]). It outranks the two message/class inferences in that field
    /// for the reason it has always outranked them: a number the upstream wrote is a verdict it
    /// stated, and this crate repeats it rather than second-guessing it. It does NOT outrank a
    /// timeout / abort / connection / decode verdict the names, errno or wording rules reach first,
    /// and a frame that keeps one of those keeps its absent `status` key on the wire.
    ///
    /// WHY IT ALSO STAYS A SEPARATE ACCESSOR: the emitted [`StreamFailure::status`] and the number
    /// the frame wrote genuinely differ in two cases, and both are worth printing on the operator's
    /// log line ([`StreamFailure::log_line`] prints both):
    ///   * a frame whose `code` spells a transport errno is `timeout`/`connection` by rules 1/4, and
    ///     a frame whose SENTENCE names one - `upstream stream terminated` is `connection` by rule 7 -
    ///     is the same case without an errno: either way the class withholds the status, so
    ///     [`StreamFailure::status`] is `None` while the frame still wrote a number, and this field
    ///     is the ONLY place that number survives;
    ///   * a frame whose status comes from a RULE rather than a number - its error class alone
    ///     ([`status_declared_by_error_type`]) or its size-refusal wording alone
    ///     ([`status_declared_by_size_refusal`]) - has an emitted status and NO number of its own, so
    ///     this is `None` while [`StreamFailure::status`] is `Some`.
    #[must_use]
    pub fn frame_status(error: &Value) -> Option<StatusCode> {
        error.get("status").and_then(status_from_json)
    }

    /// The `hasName` test (`start.ts:591`).
    fn has_name(&self, set: &[&str]) -> bool {
        self.names.iter().any(|name| set.contains(&name.as_str()))
    }

    /// The `matchesText` test (`start.ts:592`), over both JS haystacks.
    fn matches_text(&self, needles: &[&str]) -> bool {
        let names = self.names.join(" ").to_lowercase();
        let messages = self.messages.join(" ").to_lowercase();

        needles
            .iter()
            .any(|needle| names.contains(needle) || messages.contains(needle))
    }
}

/// Maps a failure onto the closed code vocabulary - `classifyStreamError` (`start.ts:581-621`).
///
/// Precedence is reproduced rule for rule; each rule is a STRONGER signal than the ones below it:
///   1. `timeout`         - an explicit timeout verdict (name or errno).
///   2. `aborted`         - an explicit cancellation verdict (name).
///   3. `upstream_status` - a definite HTTP status SOME LAYER OBSERVED: the upstream answered.
///   4. `connection`      - an enumerated errno, or a known transport class name.
///   5. `decode`          - a decoder class name, or decoder wording.
///   6. `connection`      - an errno OUTSIDE the enumerated sets is still a transport failure.
///   7. `connection`      - soft: transport wording, no errno.
///   8. `timeout`         - soft: timeout wording, no errno.
///   9. `unknown`         - nothing matched - UNLESS the in-band frame DECLARED a status, which
///      makes it `upstream_status` (the rescue below). The `unknown` sentence leads with the frozen
///      constant and tails whatever detail the upstream actually sent (`start.ts:682-685`).
///
/// RULES 1-8 DECIDE THE CODE; A DECLARED STATUS ONLY RESCUES. That is the 2026-11-24 rule, and the
/// correction that put it back the same day: rules 1, 2, 4, 5, 6, 7 and 8 read names, errnos and
/// wording, and they run to a verdict BEFORE any in-band status is consulted. A frame like
/// `{"error":{"type":"server_error","message":"upstream stream terminated"}}` is `connection` on its
/// sentence (rule 7) and STAYS `connection`, with no `status` key on the wire, whatever its class
/// table entry or its own `error.status` says. Only when rules 1-8 reach `unknown` does
/// [`FailureSignals::declared_status`] - the number the frame wrote, the size refusal its wording or
/// its `code` names, or the status its error class states
/// ([`FailureSignals::from_upstream_frame`]) - turn the frame into `upstream_status`, at the single
/// `if` below rule 8. THE STATUS IS THE RESCUE FOR A FAILURE NOTHING ELSE COULD NAME. WHY, in the
/// words the rule is named by: NEVER DEMOTE A NAMED TRANSPORT VERDICT INTO A LONGER RETRY LADDER. The
/// consumer reads `code` before `status`: a wire `connection` gets the short bounded dial ladder
/// (`packages/underload/harness/src/engine/transport/provider-timeouts.ts:714-721`, and the ladder is
/// described at `:94-108`), while `upstream_status` with a non-terminal 5xx falls past every coded arm
/// to the codeless generic transient ladder - 1 + 10 attempts, ~17 min of backoff (`:761-790`,
/// `:118-125`). Letting the declared status win would have traded a precise, quickly-abandoned
/// transport verdict for eleven more attempts at a request the transport already knows died.
///
/// THE ONE STATUS THAT STILL OUTRANKS THE WORDING RULES IS THE OBSERVED ONE. Rule 3 is TS's read of a
/// status a response head actually WROTE ([`FailureSignals::status`], which only
/// [`FailureSignals::from_transport_error`] fills), and it keeps `start.ts:601` verbatim - "a definite
/// status wins over every heuristic: the upstream answered" - because a pre-stream HTTP verdict IS the
/// answer and its body is a document, not a verdict. That asymmetry between observed and declared is
/// the whole reason [`FailureSignals`] carries two status fields instead of one.
#[must_use]
pub fn classify_stream_error(signals: &FailureSignals) -> FailureKind {
    // 1. The timeout verdict outranks the abort it is implemented with.
    if signals.has_name(&TIMEOUT_ERROR_NAMES)
        || signals
            .errno
            .as_deref()
            .is_some_and(|errno| TIMEOUT_ERRNO_CODES.contains(&errno))
    {
        return FailureKind::Timeout;
    }

    // 2. Explicit cancellation.
    if signals.has_name(&ABORT_ERROR_NAMES) {
        return FailureKind::Aborted;
    }

    // 3. A definite status wins over every heuristic: the upstream answered. OBSERVED only - see the
    //    ladder note: an in-band frame's declared number is NOT here, it is rule 9's rescue.
    if signals.status.is_some() {
        return FailureKind::UpstreamStatus;
    }

    // 4. Hard transport signal.
    if signals
        .errno
        .as_deref()
        .is_some_and(|errno| CONNECTION_ERRNO_CODES.contains(&errno))
        || signals.has_name(&CONNECTION_ERROR_NAMES)
    {
        return FailureKind::Connection;
    }

    // 5. Decode, decided BEFORE the generic errno rule so a truncated body whose errno is not
    //    enumerated still reports what was undecodable.
    if signals.has_name(&DECODE_ERROR_NAMES) || signals.matches_text(&DECODE_TEXT_NEEDLES) {
        return FailureKind::Decode;
    }

    // 6/7. Transport failure by elimination of the transport markers.
    if signals.errno.as_deref().is_some_and(is_libuv_errno)
        || signals.matches_text(&CONNECTION_TEXT_NEEDLES)
    {
        return FailureKind::Connection;
    }

    // 8. Timeout by wording only.
    if signals.matches_text(&TIMEOUT_TEXT_NEEDLES) {
        return FailureKind::Timeout;
    }

    // 9. Fallthrough - and THE single decision point this crate's 2026-11-24 deviation adds to it.
    //    Nothing above named this failure, so the status an IN-BAND frame declared (its own number,
    //    its size-refusal wording, or its error class - all three already resolved into
    //    `declared_status` by `FailureSignals::from_upstream_frame`) is the only verdict available,
    //    and it beats "no idea". Rules 1-8 having run first is what makes this a RESCUE rather than a
    //    precedence: a failure the names, errno or wording rules could name never gets here, so it
    //    never trades a short-ladder code for `upstream_status` (the ladder note above is why that
    //    ordering is not cosmetic). An OBSERVED status is handled by rule 3, as in TS, and never
    //    reaches this arm - `from_transport_error` leaves `declared_status` empty.
    if signals.declared_status.is_some() {
        return FailureKind::UpstreamStatus;
    }

    FailureKind::Unknown
}

// ---------------------------------------------------------------------------
// The request-side taxonomy
// ---------------------------------------------------------------------------

/// A request-side failure, in the shape the routes need.
///
/// The rule the route layer depends on: EVERY variant answers `status()` / `kind()` / `message()`,
/// so one match there covers the whole surface - `status ?? 500` for a settled response,
/// `{"error":"Internal Server Error"}` before the stream head is written, and the classified
/// frame after it. There is deliberately no variant that cannot answer all three, and no variant
/// the routes cannot map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportError {
    /// The upstream answered with a non-2xx status.
    ///
    /// `detail` is the SDK's OWN message text for that body minus the status prefix
    /// (`core/error.js:21-38`): `{"error":{"message":"nope"}}` at 404 gives `nope`; an `error`
    /// object with no `message` gives `JSON.stringify(error)`; an empty body gives an empty
    /// `detail`, which [`TransportError::message`] renders as `404 status code (no body)`.
    Status { status: StatusCode, detail: String },
    /// The attempt exceeded the 30-minute ceiling (`model-class.ts:624`).
    Timeout { detail: String },
    /// DNS / connect / reset / premature close (`APIConnectionError`).
    Connection { detail: String },
    /// The caller's cancellation token fired (`APIUserAbortError`).
    Aborted { detail: String },
    /// A body or a frame could not be decoded - the SDK re-raises `SyntaxError` for both
    /// (`harness/src/transport.rs:435-443` is the same rule on the harness side).
    Decode { detail: String },
    /// The request could not be built at all: an invalid header value (a credential with
    /// non-header bytes) or an unparseable base URL.
    ///
    /// TS parity: the SDK's `buildHeaders` throws a plain `Error` for undecodable header bytes,
    /// and a plain `Error` carries no `status`, so the route renders 500 +
    /// `{"error":"Internal Server Error"}`. `status()` is `None` and `kind()` is `Unknown` for
    /// exactly that reason.
    Build { detail: String },
}

impl TransportError {
    /// The HTTP status a caller should forward; `None` means "no upstream verdict exists", which
    /// is what makes the routes' `status ?? 500` total.
    #[must_use]
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::Status { status, .. } => Some(*status),
            Self::Timeout { .. }
            | Self::Connection { .. }
            | Self::Aborted { .. }
            | Self::Decode { .. }
            | Self::Build { .. } => None,
        }
    }

    /// The code half of the frame contract for this variant's class.
    #[must_use]
    pub fn kind(&self) -> FailureKind {
        classify_stream_error(&FailureSignals::from_transport_error(self))
    }

    /// The verdict the failover loops act on: `is404` in both routes
    /// (`private-responses.ts:25-27`, and the 404 handler `model-class.ts:634-636` installs).
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Status { status, .. } if *status == StatusCode::NOT_FOUND)
    }

    /// The untrusted, already-redacted text behind the verdict (empty when the upstream sent no
    /// body). The frame builder turns this into the detail half; [`TransportError::message`] is
    /// what a consumer string-matches.
    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Status { detail, .. }
            | Self::Timeout { detail, .. }
            | Self::Connection { detail, .. }
            | Self::Aborted { detail, .. }
            | Self::Decode { detail, .. }
            | Self::Build { detail, .. } => detail,
        }
    }

    /// The message the SDK would have produced, byte-comparable with what the TypeScript providers
    /// throw today: `{status} {detail}`, or `{status} status code (no body)` for an empty body,
    /// and the three SDK constants for the three transport verdicts.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Status { status, detail } => {
                // `as_u16`, NOT `StatusCode`'s `Display`: the SDK interpolates the NUMBER
                // (`core/error.js:28-32`), so the message is `404 …`, never `404 Not Found …`. The
                // reason phrase would break every consumer that string-matches the message, which is
                // what the TypeScript providers do today.
                let code = status.as_u16();

                if detail.is_empty() {
                    format!("{code} {STATUS_NO_BODY_SUFFIX}")
                } else {
                    format!("{code} {detail}")
                }
            }
            // The SDK prints its constant, not the transport text: `formatUpstreamError`
            // (private-responses.ts:31-40) renders `request error: {status} ({message})` from
            // exactly these fields. The transport's own wording stays available through
            // `detail()`, and it is what the frame's detail half uses.
            Self::Timeout { .. } => TIMEOUT_MESSAGE.to_owned(),
            Self::Connection { .. } => CONNECTION_MESSAGE.to_owned(),
            Self::Aborted { .. } => ABORT_MESSAGE.to_owned(),
            // Neither has an SDK constant: a decode failure surfaces as the decoder's own
            // message, and a build failure as the plain Error message the SDK throws.
            Self::Decode { detail } | Self::Build { detail } => detail.clone(),
        }
    }
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message())
    }
}

impl std::error::Error for TransportError {}

// ---------------------------------------------------------------------------
// The mid-stream failure and its frame
// ---------------------------------------------------------------------------

/// Where a mid-stream failure came from.
///
/// Load-bearing, not cosmetic: the TypeScript supervisor treats a TRANSPORT failure as fatal for
/// the worker while an upstream failure (the model answered, then the body died) is not.
/// `start.ts:860-873` writes the classified frame and closes the stream normally either way; the
/// caller's fatal/non-fatal branch reads this signal, which the code set alone cannot carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamOrigin {
    /// The upstream reached the model and then sent an error object on an otherwise-200 stream.
    Upstream,
    /// The transport itself failed: timeout, abort, socket, or an undecodable frame.
    Transport,
}

/// A mid-stream failure, carrying every input the frame needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamFailure {
    origin: StreamOrigin,
    code: FailureKind,
    /// The flattened, redacted root message (`readRootMessage`, `start.ts:520-527`: the DEEPEST
    /// message, because the outermost one is a wrapper that names nothing).
    message: String,
    /// Emitted for the `upstream_status` class only; every other class either has no status or
    /// would be mislabelled by one (`start.ts:643`). Since 2026-11-24 an IN-BAND error frame reaches
    /// that class too - but ONLY through rule 9's rescue of a failure nothing else named, from the
    /// number it wrote, the size refusal it stated, or the class it named
    /// ([`FailureSignals::from_upstream_frame`], [`classify_stream_error`]). A frame the wording rules
    /// could name keeps its code and this stays `None`, so `payload` emits no `status` key for it.
    status: Option<StatusCode>,
    /// The `status` an IN-BAND error frame WROTE, kept verbatim next to the classified one.
    ///
    /// WHY THIS FIELD EXISTS AT ALL, restated on 2026-11-24 after the precedence was corrected: the
    /// emitted [`StreamFailure::status`] and the number the frame wrote differ in BOTH directions, and
    /// the operator's log line ([`StreamFailure::log_line`] prints whichever of the two applies) is
    /// the only place either difference is visible:
    ///   * a frame the classifier NAMED - by an errno-shaped `code` (rules 1/4) or by its own sentence
    ///     (`upstream stream terminated` is `connection` by rule 7; `invalid json` is `decode` by
    ///     rule 5; `operation timed out` is `timeout` by rule 8) - carries NO emitted status, so this
    ///     field is the only surviving record that the upstream also wrote a number. That is the case
    ///     the earlier version of this comment got wrong: it named only the errno half, while the
    ///     wording half is the one the 2026-11-24 correction actually protects;
    ///   * a frame whose status is INFERRED from its error class
    ///     ([`status_declared_by_error_type`]) or its size-refusal sentence
    ///     ([`status_declared_by_size_refusal`]) has an emitted status and wrote no number at all, so
    ///     this stays `None` and the log must not claim the upstream sent one.
    ///
    /// Set by [`StreamFailure::upstream`] and by nothing else: every other path has no error object.
    /// The SDK parity note that used to justify hiding it (`streaming.js:50` passes `undefined`,
    /// `core/error.js:12` assigns it) still describes the deployed TypeScript, not this crate.
    frame_status: Option<StatusCode>,
    errno: Option<String>,
    /// Frames already on the wire when the failure happened (`StreamErrorContext.chunks`,
    /// `start.ts:623-627`): the difference between "nothing was lost, retry safely" and "partial
    /// text is on the wire, a retry duplicates it". Counted BEFORE the write, so a write that
    /// itself failed still reports what was attempted (`start.ts:846,851`).
    chunks: u64,
    /// The sanitized upstream target: the transport knows the endpoint it dialed, which the TS
    /// gateway never can (it only ever sees an async iterable, so TS has to thread the value onto
    /// the thrown object with a Symbol - `annotateStreamErrorUpstream`, `start.ts:536-547`).
    upstream: Option<String>,
}

impl StreamFailure {
    /// A transport-caused failure: timeout, abort, socket, or an undecodable frame.
    #[must_use]
    pub fn transport(error: &TransportError, chunks: u64, upstream: Option<&str>) -> Self {
        Self::from_signals(
            StreamOrigin::Transport,
            FailureSignals::from_transport_error(error),
            chunks,
            upstream,
        )
    }

    /// A failure the UPSTREAM reported: an `{"error":{...}}` object on a 200 stream
    /// (`openai.ts:783-788`, read through `harness/src/transport.rs:447-455`).
    ///
    /// This is the IN-BAND path, and since 2026-11-24 it is the path that CAN carry a status: the
    /// number the upstream wrote (or, failing that, the size refusal its sentence or its `code` names,
    /// or the status its error class names) makes the frame `upstream_status` with a `status` key on
    /// the wire - but ONLY for a frame nothing else could name. A frame whose own sentence spells a
    /// transport needle stays `connection` / `decode` / `timeout` with NO `status` key, exactly as it
    /// did before this path read statuses at all, and the module header's ladder note says why.
    /// [`FailureSignals::from_upstream_frame`] is where the three reads live.
    ///
    /// The frame's own number is ALSO retained verbatim as [`StreamFailure::frame_status`], because
    /// the emitted [`StreamFailure::status`] is withheld whenever rules 1-8 claim another
    /// class, and the operator's log line needs the number the upstream actually sent either way.
    #[must_use]
    pub fn upstream(error: &Value, chunks: u64, upstream: Option<&str>) -> Self {
        // The root message: the error's own `message`, or `JSON.stringify` of the object when it
        // carries none - the same fallback `APIError.makeMessage` uses (`core/error.js:22-27`).
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| error.to_string(), ToOwned::to_owned);

        let frame_status = FailureSignals::frame_status(error);
        let mut signals = FailureSignals::from_upstream_frame(error);
        signals.messages = vec![message];

        Self {
            frame_status,
            ..Self::from_signals(StreamOrigin::Upstream, signals, chunks, upstream)
        }
    }

    /// A body that ended without the `[DONE]` sentinel AND without a `finish_reason`: the
    /// generation was cut short, so an empty answer is not a completed one
    /// (`harness/src/transport.rs:365-367`, the R1 truncation guard).
    ///
    /// The provider surface does NOT fail on this today - the TS relay emits `[DONE]` after any
    /// clean exhaustion (`private-chat-completion.ts:186`) - so the transport surfaces it as a
    /// value the caller may ignore, not as a forced error.
    #[must_use]
    pub fn truncated(chunks: u64, upstream: Option<&str>) -> Self {
        Self::from_signals(
            StreamOrigin::Transport,
            FailureSignals {
                names: Vec::new(),
                errno: None,
                status: None,
                declared_status: None,
                messages: vec!["The model stream ended before reporting a completion.".to_owned()],
            },
            chunks,
            upstream,
        )
    }

    /// Classifies the signals and applies the wire-text rules. This is the single constructor
    /// every failure path funnels through, so origin, code, status and errno cannot disagree.
    #[must_use]
    fn from_signals(
        origin: StreamOrigin,
        mut signals: FailureSignals,
        chunks: u64,
        upstream: Option<&str>,
    ) -> Self {
        // Every free-text half is flattened + redacted before it can reach a field, so no
        // `Debug` of this type can print a credential an upstream echoed back.
        signals.messages = signals
            .messages
            .iter()
            .map(|message| flatten_error_detail(message))
            .collect();

        let code = classify_stream_error(&signals);

        // `status` is kept only for its own class (see the field comment): `start.ts:643` emits a
        // status for `upstream_status` alone, and this is the single place that decides whether one is
        // emitted at all. When the code IS `upstream_status`, exactly one of the two status fields is
        // set - rule 3 fired on an OBSERVED status, or rule 9's rescue fired on a DECLARED one - so
        // `or` picks the right one without either constructor having to know which rule won. When the
        // rules named any other class, the status is withheld even though the frame wrote one: a
        // `connection` frame whose upstream also stamped `status: 503` emits NO `status` key, which is
        // exactly the wire answer this crate gave before the in-band deviation existed (and the number
        // survives in `frame_status`, below, for the operator's log line).
        let emitted_status = (code == FailureKind::UpstreamStatus)
            .then_some(signals.status.or(signals.declared_status))
            .flatten();

        Self {
            origin,
            code,
            // The deepest message is the root cause; with several links TS prints the deepest
            // non-empty one, and the Rust callers hand in exactly one message, so index 0 IS the
            // root here.
            message: signals
                .messages
                .iter()
                .rev()
                .find(|message| !message.is_empty())
                .cloned()
                .unwrap_or_default(),
            status: emitted_status,
            // The frame's own number is set by [`StreamFailure::upstream`], the only constructor
            // that has an error OBJECT in hand; every other path has no frame to read. It is kept
            // beside `status` rather than merged into it, for the two cases listed on the field.
            frame_status: None,
            errno: signals.errno,
            chunks,
            upstream: upstream.and_then(sanitize_upstream_target),
        }
    }

    /// The closed code the consumer branches on.
    #[must_use]
    pub fn code(&self) -> FailureKind {
        self.code
    }

    /// `true` when the upstream reached the model - i.e. when a supervisor must NOT treat this as
    /// fatal for the worker.
    #[must_use]
    pub fn is_upstream(&self) -> bool {
        self.origin == StreamOrigin::Upstream
    }

    /// `true` when the transport itself failed: the fatal case above.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        !self.is_upstream()
    }

    /// The definite upstream status, present only for `upstream_status`.
    ///
    /// For an in-band error frame this is the status THAT FRAME DECLARED since 2026-11-24 - the
    /// number it wrote in `error.status`, the 400 its size-refusal sentence or `error.code` names, or
    /// the one its `error.type` states ([`FailureSignals::from_upstream_frame`]) - and it is `None`
    /// for such a frame whenever rules 1-8 named the failure first: by an errno-shaped `code` (a
    /// `timeout`/`connection` verdict) or by its own sentence (`upstream stream terminated` is
    /// `connection`, `invalid json` is `decode`). Either way the number the upstream wrote is still
    /// available through [`StreamFailure::frame_status`], and the wire frame carries no `status` key at
    /// all, which is the whole point of the rescue ordering. A pre-stream HTTP verdict arrives here as
    /// the real [`TransportError::status`], unchanged.
    #[must_use]
    pub fn status(&self) -> Option<StatusCode> {
        self.status
    }

    /// The numeric `error.status` an IN-BAND frame wrote, kept verbatim whatever the classifier made
    /// of the frame.
    ///
    /// This is the difference between "the upstream sent 500" and "the upstream named a class this
    /// crate maps to 500": an INFERRED status appears in [`StreamFailure::status`] and never here.
    /// It also survives EVERY class that withholds a status - whether the frame was named by an
    /// errno-shaped `code` (rules 1/4) or by its own sentence (rules 5/7/8), which is the case the
    /// 2026-11-24 correction protects - and only this accessor can still say the upstream reported a
    /// number. It is emitted on the wire only when it IS the emitted status (`payload` reads `status`,
    /// never this); its dedicated reader is the operator log line, [`StreamFailure::log_line`]. A
    /// pre-stream HTTP verdict is NOT reported here - it is the real [`StreamFailure::status`].
    #[must_use]
    pub fn frame_status(&self) -> Option<StatusCode> {
        self.frame_status
    }

    /// The transport errno, present only when the failure really carried one.
    #[must_use]
    pub fn errno(&self) -> Option<&str> {
        self.errno.as_deref()
    }

    /// Frames that reached the wire first.
    #[must_use]
    pub fn chunks(&self) -> u64 {
        self.chunks
    }

    /// The upstream target only the transport knows.
    #[must_use]
    pub fn upstream_target(&self) -> Option<&str> {
        self.upstream.as_deref()
    }

    /// The free-text half of the frame, already flattened and redacted.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.message
    }

    /// The frame's `message` value, from the sentence table (`start.ts:672-686`). The `unknown`
    /// class LEADS with the frozen [`UNKNOWN_STREAM_ERROR_MESSAGE`] (`start.ts:682-685`) so every
    /// consumer that only ever string-matched the legacy frame keeps working, and since 2026-11-24
    /// tails the upstream's own detail after it (`start.ts` kept the constant alone - see this
    /// module's header for why that stopped being good enough).
    ///
    /// `endpoint` is the ROUTE's folder name (`StreamErrorContext.endpoint`, `start.ts:623-625`):
    /// the transport does not know it, so the caller of [`StreamFailure::payload`] supplies it and
    /// `None` renders the `unknown` subject the TS fallback renders.
    #[must_use]
    pub fn sentence(&self, endpoint: Option<&str>) -> String {
        let subject = self.upstream.clone().map_or_else(
            || {
                format!(
                    "stream from endpoint {}",
                    endpoint
                        .filter(|name| !name.is_empty())
                        .unwrap_or("unknown")
                )
            },
            |target| format!("upstream {target}"),
        );

        // The phase wording, including the singular/plural distinction the wire prints
        // (`start.ts:652-654`).
        let phase = match self.chunks {
            0 => "before the first chunk".to_owned(),
            1 => "after 1 chunk".to_owned(),
            chunks => format!("after {chunks} chunks"),
        };

        // The errno is the machine-readable half: it either leads the detail or is confirmed in
        // parentheses, unless the message already spells it (`start.ts:656-663`).
        let detail = match (&self.errno, self.message.is_empty()) {
            (Some(errno), false) if self.message.to_uppercase().contains(&errno.to_uppercase()) => {
                self.message.clone()
            }
            (Some(errno), false) => format!("{} ({errno})", self.message),
            (Some(errno), true) => errno.clone(),
            (None, false) => self.message.clone(),
            (None, true) => String::new(),
        };

        // The SDK prefixes its own status onto the message; this frame names the status in its own
        // key and in the sentence, so the duplicate prefix is dropped (`start.ts:664-670`).
        let detail = strip_status_prefix(&detail, self.status);
        let tail = if detail.is_empty() {
            String::new()
        } else {
            format!(": {detail}")
        };

        match self.code {
            FailureKind::Timeout => format!("{subject} timed out {phase}{tail}"),
            // Mid-stream the socket died AFTER bytes arrived (a retry would duplicate text);
            // before the first byte nothing was lost at all (`start.ts:674-678`).
            FailureKind::Connection if self.chunks == 0 => {
                format!("{subject} failed {phase}{tail}")
            }
            FailureKind::Connection => {
                format!("{subject} closed the response body {phase}{tail}")
            }
            FailureKind::Aborted => format!("{subject} was aborted {phase}{tail}"),
            FailureKind::UpstreamStatus => format!(
                "{subject} returned HTTP {} {phase}{tail}",
                // JS interpolates `undefined` when the status is missing; the classifier makes
                // that combination unreachable, so this is only defensive.
                self.status.map_or_else(
                    || "undefined".to_owned(),
                    |status| status.as_u16().to_string(),
                )
            ),
            FailureKind::Decode => {
                format!("{subject} sent an undecodable payload {phase}{tail}")
            }
            // 2026-11-24: the frozen constant stays the LEAD, so a consumer string-matching the
            // legacy frame keeps matching, and the frame's own detail is TAILED on when it has one.
            // Before this the class threw the upstream's sentence away entirely, which is what made
            // a deployed `unknown` unanswerable from the wire alone; the subject and the phase stay
            // out of it, because `payload`'s `message` is a wire value other consumers match, and the
            // full picture is [`StreamFailure::log_line`]'s job. A frame with nothing to say still
            // gets the bare constant, byte-identical to `start.ts:685`.
            FailureKind::Unknown => format!("{UNKNOWN_STREAM_ERROR_MESSAGE}{tail}"),
        }
    }

    /// The ONE line the gateway logs before it writes the frame (`serve.rs::Relay`'s error arm,
    /// standing in for `start.ts:861`), in `key=value` form so it is greppable.
    ///
    /// WHY THIS EXISTS NEXT TO [`StreamFailure::sentence`]: the sentence is a WIRE value, shaped for
    /// a consumer that string-matches it, and it deliberately leaves facts out (no code, no origin,
    /// no chunk count, and no status for the classes that withhold one). The log line is for the
    /// operator asking "what actually happened to this request", so it names every fact the crate
    /// holds, including the ones the frame is not allowed to carry: the closed code, whether the
    /// upstream or the transport failed, how much already reached the caller, the sanitized target,
    /// the status the frame declared AND the status it wrote, and the redacted detail verbatim.
    ///
    /// EVERY FREE-TEXT FIELD HERE IS ALREADY SANITISED AT CAPTURE, which is the crate rule this line
    /// depends on: `detail` is flattened + redacted ([`flatten_error_detail`]) and `upstream` is
    /// origin+pathname only ([`sanitize_upstream_target`]), so no credential can reach this line. The
    /// detail is quoted, with `"` and `\` escaped, so a detail containing spaces or quotes stays one
    /// greppable field rather than three.
    #[must_use]
    pub fn log_line(&self, endpoint: Option<&str>) -> String {
        // The route's folder name; `unknown` is the same fallback the sentence's subject uses.
        let endpoint = endpoint
            .filter(|name| !name.is_empty())
            .unwrap_or("unknown");

        let origin = if self.is_upstream() {
            "upstream"
        } else {
            "transport"
        };

        let mut line = format!(
            "stream error in {endpoint}: code={} origin={origin} chunks={}",
            self.code.as_str(),
            self.chunks,
        );

        if let Some(target) = &self.upstream {
            line.push_str(&format!(" upstream={target}"));
        }

        if let Some(status) = self.status {
            line.push_str(&format!(" status={}", status.as_u16()));
        }

        // The frame's OWN number, printed only when it differs from the emitted status: an inferred
        // status must not look like a number the upstream sent, and a status the stronger rules
        // withheld would otherwise disappear from the log entirely.
        if let Some(frame) = self
            .frame_status
            .filter(|frame| Some(*frame) != self.status)
        {
            line.push_str(&format!(" frame_status={}", frame.as_u16()));
        }

        if let Some(errno) = &self.errno {
            line.push_str(&format!(" errno={errno}"));
        }

        line.push_str(&format!(
            " detail=\"{}\"",
            self.message.replace('\\', "\\\\").replace('"', "\\\"")
        ));

        line
    }

    /// The wire frame as a JSON value, in the exact key set and key ORDER the contract stamps
    /// (`start.ts:695-702`).
    ///
    /// WHY `param: null` IS ALWAYS PRESENT AND `status`/`errno` NEVER APPEAR AS `null`: the two
    /// optional keys are written by setting them to `undefined` in the TS literal, and
    /// `JSON.stringify` DROPS undefined values, so the key simply does not reach the wire. A null
    /// status would read as "no status" to one consumer and "status 0" to another, and the
    /// contract types them as number/string only. `param` is the reserved OpenAI-shaped key and
    /// is always `null`.
    ///
    /// Key order (message, type, code, status, errno, param) is part of the contract, so this
    /// builds a [`Map`] - the `serde_json` `preserve_order` feature keeps insertion order, which a
    /// `BTreeMap`-backed object would not.
    #[must_use]
    pub fn payload(&self, endpoint: Option<&str>) -> Value {
        let mut error = Map::new();

        error.insert("message".to_owned(), Value::String(self.sentence(endpoint)));
        error.insert("type".to_owned(), Value::String("stream_error".to_owned()));
        error.insert(
            "code".to_owned(),
            Value::String(self.code.as_str().to_owned()),
        );

        if let Some(status) = self.status {
            error.insert("status".to_owned(), Value::from(status.as_u16()));
        }

        if let Some(errno) = &self.errno {
            error.insert("errno".to_owned(), Value::String(errno.clone()));
        }

        error.insert("param".to_owned(), Value::Null);

        let mut frame = Map::new();
        frame.insert("error".to_owned(), Value::Object(error));

        Value::Object(frame)
    }
}

impl std::fmt::Display for StreamFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The endpoint name is the route's, not this layer's, so the subject falls back to the
        // upstream target (which the transport DOES know) or to TS's `unknown`.
        formatter.write_str(&self.sentence(None))
    }
}

impl std::error::Error for StreamFailure {}
