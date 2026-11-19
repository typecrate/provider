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
//!     (through `makeMessage`) and its `error.code`, and NO numeric status. That single fact is
//!     what [`FailureSignals::from_upstream_frame`] reproduces; see its doc for the consequence on
//!     the emitted frame.
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
//! including the one signal the JS object does NOT carry in the second case, which is what keeps
//! an in-band error frame out of `upstream_status` (see [`FailureSignals::from_upstream_frame`]).
//! The free-text rules stay live on purpose: the messages reqwest and hyper produce ARE the text
//! the TypeScript rules match against (`other side closed`, `operation timed out`,
//! `connection reset by peer`).
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
    /// Nothing matched; the message stays the frozen legacy constant.
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
/// The four fields are exactly the four reads `start.ts:491-527` performs (`names`, `errno`,
/// `status`, and the message/name text the soft rules match). Building one from a Rust failure is
/// three lines, and building one from an arbitrary JSON error object is
/// [`FailureSignals::from_upstream_frame`] - so the classifier keeps ONE implementation for both,
/// the way one `classifyStreamError` serves every thrown object in TS.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FailureSignals {
    /// `readChainNames` (`start.ts:484-490`): the `name` of every chain link.
    pub names: Vec<String>,
    /// `readErrorErrno` (`start.ts:493-503`): the deepest `cause.code`, or a LIBUV-shaped
    /// top-level `code`.
    pub errno: Option<String>,
    /// `readUpstreamStatus` (`start.ts:506-517`): a definite HTTP status, anywhere in the chain.
    pub status: Option<StatusCode>,
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
            messages: vec![detail.clone()],
        }
    }

    /// Signals for one upstream error OBJECT (`{"error":{...}}` on an otherwise-200 stream).
    ///
    /// THE ONE SIGNAL THE SDK DOES NOT CARRY, SO NEITHER DOES THIS. The deployed relay reads that
    /// frame through the SDK, and the SDK throws `new APIError(undefined, data.error, undefined,
    /// response.headers)` (`node_modules/openai/core/streaming.js:50`): the constructor assigns
    /// `this.status = status` (`node_modules/openai/core/error.js:12`), so the object the gateway's
    /// `classifyStreamError` inspects has **no numeric status at all**, whatever the upstream wrote
    /// in `error.status`. `readUpstreamStatus` (`start.ts:506-517`) therefore finds nothing, rule 3
    /// of `start.ts:581-621` cannot fire, and the frame falls through to the errno/name/text rules;
    /// for an ordinary upstream message that matches no needle, that is `unknown`, whose sentence
    /// is the frozen [`UNKNOWN_STREAM_ERROR_MESSAGE`] (`start.ts:685`) and whose payload carries no
    /// `status` key (`start.ts:699` writes `undefined`, which `JSON.stringify` drops).
    ///
    /// So the reads below mirror the SDK object, not the JSON body: `message` as the root text (the
    /// SDK's `makeMessage`, `core/error.js:21-38`), `code` as an errno ONLY when it spells like a
    /// transport errno (`this.code = data?.['code']`, `core/error.js:17`, filtered by
    /// `readErrorErrno`, `start.ts:493-503` - `insufficient_quota` does not survive it), and
    /// `status` deliberately NOT lifted, because lifting it is what made this crate answer
    /// `upstream_status` where the deployed TypeScript answers `unknown`. The value is not lost:
    /// [`StreamFailure::frame_status`] keeps it for diagnostics, and it never reaches the wire.
    #[must_use]
    pub fn from_upstream_frame(error: &Value) -> Self {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .map(flatten_error_detail)
            .unwrap_or_default();

        Self {
            // A plain upstream error object has no JS class name; the SDK reports its own
            // `APIError`, which is in none of the classifier's name sets, so no name is invented
            // here either.
            names: Vec::new(),
            errno: error
                .get("code")
                .and_then(Value::as_str)
                .filter(|code| is_transport_errno(code))
                .map(ToOwned::to_owned),
            // See the doc above: an in-band frame's `status` is NOT a status signal.
            status: None,
            messages: (!message.is_empty())
                .then_some(message)
                .into_iter()
                .collect(),
        }
    }

    /// The `status` field an in-band error OBJECT carries, validated the way
    /// `readUpstreamStatus` validates one (`start.ts:506-517`: an integer in 100..=599).
    ///
    /// This is the diagnostic half only: the SDK discards the value (`core/error.js:12` with
    /// `streaming.js:50`), so it must not feed the classifier and must not reach the emitted
    /// frame. [`FailureSignals::from_upstream_frame`] deliberately returns `status: None`; this is
    /// the accessor that keeps the number readable as a diagnostic — nothing in `src/` prints it.
    #[must_use]
    pub fn frame_status(error: &Value) -> Option<StatusCode> {
        error
            .get("status")
            .and_then(Value::as_u64)
            .and_then(status_code)
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
///   3. `upstream_status` - a definite HTTP status: the upstream answered.
///   4. `connection`      - an enumerated errno, or a known transport class name.
///   5. `decode`          - a decoder class name, or decoder wording.
///   6. `connection`      - an errno OUTSIDE the enumerated sets is still a transport failure.
///   7. `connection`      - soft: transport wording, no errno.
///   8. `timeout`         - soft: timeout wording, no errno.
///   9. `unknown`         - nothing matched; the message stays the frozen constant.
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

    // 3. A definite status wins over every heuristic: the upstream answered.
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

    // 9. Fallthrough.
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
    /// would be mislabelled by one (`start.ts:643`).
    status: Option<StatusCode>,
    /// The `status` an IN-BAND error frame carried, kept as a diagnostic that never reaches the wire.
    ///
    /// The SDK discards it (`node_modules/openai/core/streaming.js:50` passes `undefined` as
    /// the status, and `core/error.js:12` assigns that), so the classified failure genuinely has
    /// none - which is why this is a separate field rather than a second source for
    /// [`StreamFailure::status`]: the emitted frame reads `status`, never this. Set by
    /// [`StreamFailure::upstream`] and by nothing else.
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
    /// This is the IN-BAND path, so it is the one that must not carry a status: the SDK has already
    /// thrown the frame away from its status by the time the gateway classifies it
    /// (`node_modules/openai/core/streaming.js:50`), which is why the emitted frame is the
    /// `unknown` / frozen-message frame with no `status` key - see
    /// [`FailureSignals::from_upstream_frame`] for the full chain. What the upstream wrote in
    /// `error.status` is retained on this type as [`StreamFailure::frame_status`], a diagnostic that
    /// is never emitted on the wire and that no code in `src/` reads today (its only readers are the
    /// `tests/transport.rs` assertions that pin this rule).
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
            // `status` is kept only for its own class (see the field comment), and an in-band
            // frame never reaches that class (see [`FailureSignals::from_upstream_frame`]).
            status: (code == FailureKind::UpstreamStatus)
                .then_some(signals.status)
                .flatten(),
            // The in-band diagnostic is set by [`StreamFailure::upstream`], the only constructor
            // that has an error OBJECT in hand; every other path has no frame to read.
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
    /// For an in-band error frame this is `None` even when the frame carried an `error.status`,
    /// because the SDK strips it before the gateway classifies anything
    /// (`node_modules/openai/core/streaming.js:50`); the number the upstream wrote stays readable
    /// through [`StreamFailure::frame_status`], which is a diagnostic with no production reader.
    #[must_use]
    pub fn status(&self) -> Option<StatusCode> {
        self.status
    }

    /// The `error.status` an IN-BAND frame carried, retained as a diagnostic that is NEVER emitted
    /// on the wire.
    ///
    /// [`StreamFailure::payload`] and [`StreamFailure::sentence`] never read this, so it can never
    /// reach the wire: parity with the deployed relay, whose classifier has no status to read for
    /// this failure class (`node_modules/openai/core/error.js:12`). Nothing in `src/` reads it
    /// either — the request log line (`serve.rs::format_token_line`, `private-chat-completion.ts:49-61`)
    /// has no status field — so its only readers are the `tests/transport.rs` assertions that pin
    /// this rule. A pre-stream HTTP verdict is NOT reported here - it is the real
    /// [`StreamFailure::status`].
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

    /// The frame's `message` value, from the sentence table (`start.ts:672-686`), with the
    /// `unknown` class deliberately constant (`start.ts:682-685`) so every consumer that only
    /// ever string-matched the legacy frame keeps working.
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
            FailureKind::Unknown => UNKNOWN_STREAM_ERROR_MESSAGE.to_owned(),
        }
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
