//! The SSE wire codec: incremental `data:` frame decoding, and the two frames this provider
//! emits.
//!
//! THE DECODER THIS ONE HAS TO MATCH, AND THE CITATION LINEAGE
//! -----------------------------------------------------------
//! Written out again on 2026-11-19, because the first cut of this module cited an IN-REPO SIBLING
//! as if it were the reference, and the two do not have the same rules.
//!
//!   * The read half has to match the decoder the DEPLOYED TypeScript relay actually runs, which
//!     is the OpenAI SDK's own `SSEDecoder`: `node_modules/openai/core/streaming.js:245-285`
//!     (`openai@6.42.0`). It accumulates the fields of ONE event, joins that event's consecutive
//!     `data:` values with `\n` (`streaming.js:261` `data: this.data.join('\n')`), and hands the
//!     event to its caller on the BLANK separator line only (`streaming.js:255-268`). The caller
//!     ends the stream on a payload that STARTS WITH the sentinel
//!     (`streaming.js:35` `if (sse.data.startsWith('[DONE]'))`) - prefix matching, not equality -
//!     and turns a mid-body `{"error":{…}}` payload into a thrown `APIError`
//!     (`streaming.js:50`, which is the fact `crate::error`'s in-band rule is built on).
//!     Every streaming request this provider makes goes through that code path
//!     (`client.chat.completions.create({ stream: true })` -> `Stream.fromSSEResponse` ->
//!     `_iterSSEMessages`, `streaming.js:193-243`), so it - not the harness - is the reference.
//!   * `packages/typecrate/harness/src/transport.rs:303-354` is a SIBLING, not the source of these
//!     rules: it ports the HARNESS client's own decoder
//!     (`packages/underload/harness/src/transport/openai.ts`), which is deliberately stricter than
//!     the SDK - one `data:` line is one payload, and `[DONE]` is compared EXACTLY. Both rules are
//!     equivalent for a relay that emits one JSON object per frame (every relay in this workspace),
//!     and neither is what the deployed provider's inbound leg does. `data_payload` below still
//!     follows the sibling's line classifier, because that part IS identical.
//!
//! The write half is the relay's own output: `runtime/endpoint/provider/private/chat-completion/
//! private-chat-completion.ts:163` yields `data: ${JSON.stringify(chunk)}` per chunk and
//! `:186` yields `data: [DONE]` once, after the loop, and the gateway turns each yielded STRING
//! into a frame by writing it plus `\n\n` (`packages/underload/service/src/server/start.ts:853`
//! `await stream.write(chunk + '\n\n')`). So the bytes on the wire ARE `data: <json>\n\n`, and
//! `[DONE]` is the relay's own sentinel, appended only after the upstream was exhausted cleanly.
//!
//! WHAT THIS MODULE GUARANTEES, AND WHY IT MATTERS
//!   * partial frames: a `data:` line may arrive split across any number of TCP chunks, and a
//!     chunk may carry several frames; the decoder holds the tail and emits whole payloads only;
//!   * ONE EVENT, ONE PAYLOAD: consecutive `data:` lines of one event are joined with `\n` and
//!     emitted as one payload at the event boundary (the blank separator line), exactly as
//!     `streaming.js:255-268` does - so a frame the SDK would hand over as one payload can never
//!     be split into two here, and vice versa;
//!   * a payload is emitted EXACTLY once, including the `[DONE]` sentinel, and after the sentinel
//!     the decoder is finished - reading further is a caller bug, and the flag makes it visible;
//!   * a non-`data:` field line (`event:`, `id:`, `retry:`, a `:` comment, or any other line that
//!     is not a data field) carries no payload and is skipped, so a relay that adds an `event:`
//!     name does not produce a decode failure;
//!   * the emitted chunk objects are RE-SERIALISED VALUES, never upstream bytes: the relay
//!     re-stringifies each chunk (`private-chat-completion.ts:163`), which is what makes the
//!     `serde_json` `preserve_order` feature load-bearing - a chunk's own key order survives the
//!     round trip, and a `BTreeMap`-backed object would reorder `id`/`object`/`choices` and break
//!     byte-level comparisons downstream.
//!
//! DEVIATION (deliberate, recorded in knowledge.md) - THREE ONE-DIRECTIONAL LENIENCIES vs THE SDK
//! DECODER. Every one of them ACCEPTS bytes the deployed decoder drops or rejects, and that makes
//! the flow UNEVEN IN BOTH DIRECTIONS, so it is stated here rather than glossed: leniency 3 can only
//! ever deliver LESS, while leniencies 1 and 2 can deliver a chunk the deployed decoder never
//! produced. An event that is not closed by a blank line is relayed HERE and DROPPED there: the SDK
//! builds an event only when it reads an empty line (`streaming.js:255-268`), and its end-of-body
//! flush loop feeds the leftover lines to that same decoder and yields only what comes back
//! (`streaming.js:212-216`, `node_modules/openai/core/streaming.js`), which a trailing `data:` line
//! never does - so a caller of this decoder CAN observe one extra chunk. An indented `data:` line is
//! a chunk here and nothing there, for the same reason (`streaming.js:273`). What none of them does
//! is invent a payload the deployed RELAY would not have sent: every chunk this decoder hands over is
//! one the TypeScript relay put on the wire, and the whole difference is in what the inbound decoder
//! keeps. The leniencies, then:
//!   1. every line, and every payload, is TRIMMED (`data_payload`), where the SDK strips only a
//!      trailing `\r` and ONE leading space (`streaming.js:252-254`, `:274-276`) and splits the
//!      field name at the first colon (`streaming.js:273`). So `  data: x  ` is a data field here
//!      and a field literally named `  data` there;
//!   2. a body that ends WITHOUT the blank separator still delivers the event it had started
//!      ([`SseDecoder::finish`], the sibling's last rule at `harness/src/transport.rs:343-354`).
//!      The SDK emits an event ONLY when it reads an empty line
//!      (`streaming.js:255-268` with `streaming.js:205-216`), so it DROPS that final event
//!      altogether - strictly less reaches the TypeScript caller;
//!   3. an event with a field line and no `data:` field produces nothing here, while the SDK
//!      yields it with `data: ''` and its caller then fails to `JSON.parse` it
//!      (`streaming.js:41-48`), i.e. a keep-alive comment or a bare `event:` line is a `decode`
//!      failure there and a skipped line here.

use serde_json::Value;

/// The sentinel that ends a provider stream: the prefix the deployed decoder compares against
/// (`node_modules/openai/core/streaming.js:35`), the literal the relay yields
/// (`private-chat-completion.ts:186`), and the constant the harness sibling shares
/// (`harness/src/transport.rs:46`).
pub const DONE_SENTINEL: &str = "[DONE]";

/// The whole sentinel frame as it reaches the client: the payload the route yields
/// (`private-chat-completion.ts:186`) plus the separator the gateway appends
/// (`start.ts:853`).
pub const DONE_FRAME: &str = "data: [DONE]\n\n";

/// The separator between SSE fields, and the blank line that closes one frame.
pub const FRAME_SEPARATOR: &str = "\n\n";

/// One decoded SSE payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SseEvent {
    /// The payload of ONE event: its `data:` values joined with `\n`
    /// (`streaming.js:261`), with the `data:` name and surrounding whitespace removed (a JSON
    /// chunk, or anything else - this module does not parse, so a malformed frame stays visible
    /// to the caller, which is what has to report `decode`).
    Data(String),
    /// The `[DONE]` sentinel, i.e. the upstream was exhausted cleanly. The relay emits it and
    /// nothing after it.
    Done,
}

impl SseEvent {
    /// The payload text, `None` for the sentinel.
    #[must_use]
    pub fn payload(&self) -> Option<&str> {
        match self {
            Self::Data(payload) => Some(payload),
            Self::Done => None,
        }
    }

    /// The wire frame this event would produce on the write side, sentinel included.
    #[must_use]
    pub fn frame(&self) -> String {
        match self {
            Self::Data(payload) => format!("data: {payload}{FRAME_SEPARATOR}"),
            Self::Done => DONE_FRAME.to_owned(),
        }
    }
}

/// Extracts the payload of one SSE line, or `None` for anything that is not a data field
/// (`harness/src/transport.rs:552-560 data_payload`, i.e. `openai.ts:519-524`).
///
/// Lines are split on `\n`, so the `\r` of a CRLF pair is dropped first and the line is then
/// trimmed exactly as TypeScript trims it. The payload itself is TRIMMED, not "one space
/// removed" as the SSE spec would have it: that is what the sibling does, and a JSON payload
/// cannot tell the difference. It is also the first of this module's three recorded leniencies
/// against the SDK decoder, which splits the field name at the first colon instead
/// (`streaming.js:273`) - see the module doc.
#[must_use]
pub fn data_payload(line: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(line);

    text.trim_end_matches(['\n', '\r'])
        .trim()
        .strip_prefix("data:")
        .map(|rest| rest.trim().to_owned())
}

/// True for the blank line that CLOSES one event - the boundary the SDK compares an empty line
/// against (`streaming.js:255`). The crate's trimming rule is applied here too (recorded
/// leniency 1), so a separator carrying stray spaces still closes the event.
#[must_use]
fn is_event_boundary(line: &[u8]) -> bool {
    String::from_utf8_lossy(line)
        .trim_end_matches(['\n', '\r'])
        .trim()
        .is_empty()
}

/// The `[DONE]` test the deployed decoder applies: a PREFIX match on the joined payload
/// (`streaming.js:35`), so `[DONE]` followed by anything is still the sentinel, while a payload
/// that merely CONTAINS the sentinel text (`{"delta":{"content":"[DONE]"}}`) is an ordinary chunk.
#[must_use]
fn is_sentinel(payload: &str) -> bool {
    payload.starts_with(DONE_SENTINEL)
}

/// The incremental decoder: bytes in, whole payloads out.
///
/// `push` may be called with any chunk boundary - mid-frame, mid-line, several frames at once -
/// and `finish` releases the final event when the body ends without a trailing blank line
/// (recorded leniency 2; the SDK would drop it, `streaming.js:255-268`).
#[derive(Clone, Debug, Default)]
pub struct SseDecoder {
    /// The bytes of a line that has not been terminated yet.
    buffer: Vec<u8>,
    /// The `data:` values of the event being assembled, in wire order. Joined with `\n` when the
    /// event boundary arrives, exactly as `streaming.js:261` joins them.
    fields: Vec<String>,
    /// Set once the sentinel has been seen; a finished decoder emits nothing more.
    finished: bool,
}

impl SseDecoder {
    /// An empty decoder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            buffer: Vec::new(),
            fields: Vec::new(),
            finished: false,
        }
    }

    /// True once the `[DONE]` sentinel has been emitted.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// The bytes held back for a line that has not been terminated yet.
    #[must_use]
    pub fn pending(&self) -> &[u8] {
        &self.buffer
    }

    /// Feeds one upstream chunk and returns the frames it completed.
    ///
    /// A frame is returned only when its newline has arrived, and a PAYLOAD only when its event
    /// boundary has arrived, which is the whole point of the two buffers: an upstream that cuts a
    /// frame in half must not produce half a chunk, and an upstream that splits one event across
    /// writes must not produce half a payload.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        let mut events = Vec::new();

        if self.finished {
            return events;
        }

        self.buffer.extend_from_slice(bytes);

        // Manual splitting, keeping the trailing partial line for the next chunk
        // (`harness/src/transport.rs:326-340`).
        while let Some(position) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=position).collect();

            if let Some(event) = self.consume(&line) {
                if event == SseEvent::Done {
                    self.finished = true;
                    events.push(event);
                    // Anything after the sentinel in the same chunk is not this stream's
                    // business: the sentinel is the end of the contract. The half-assembled
                    // event goes with it, because a payload after the sentinel is not part of
                    // this stream either.
                    self.buffer.clear();
                    self.fields.clear();
                    return events;
                }

                events.push(event);
            }
        }

        events
    }

    /// Ends the stream: an event that had started but never reached its blank separator is still
    /// one event (recorded leniency 2), and an unterminated final line is still a line.
    pub fn finish(&mut self) -> Vec<SseEvent> {
        if self.finished {
            self.buffer.clear();
            self.fields.clear();

            return Vec::new();
        }

        let tail = std::mem::take(&mut self.buffer);

        // The tail goes through the same classifier as a terminated line, so a tail that is a
        // `data:` field joins the event it belongs to; whatever the boundary did not flush is
        // then flushed explicitly, because there is no further byte that could close it.
        let mut events = Vec::new();

        match self.consume(&tail) {
            // The tail itself was the blank separator, so it already closed the event.
            Some(event) => events.push(event),
            // The tail was a field line (or nothing): the event still in progress has no
            // closing byte coming, so close it here — recorded leniency 2.
            None => events = self.take_event().into_iter().collect(),
        }

        if events.contains(&SseEvent::Done) {
            self.finished = true;
        }

        events
    }

    /// One complete line: a `data:` field extends the event in progress, the blank separator
    /// closes it, and any other line is a field this decoder has no use for.
    fn consume(&mut self, line: &[u8]) -> Option<SseEvent> {
        match data_payload(line) {
            Some(value) => {
                self.fields.push(value);

                None
            }
            // Not a data field. Only the blank line can produce an event
            // (`streaming.js:255-268`); `event:`/`id:`/`retry:`/`:` comments and any other
            // non-data line carry nothing (recorded leniency 3).
            None if is_event_boundary(line) => self.take_event(),
            None => None,
        }
    }

    /// Closes the event in progress: its `data:` values joined with `\n`
    /// (`streaming.js:261`), or nothing at all when the event carried no `data:` field.
    fn take_event(&mut self) -> Option<SseEvent> {
        if self.fields.is_empty() {
            return None;
        }

        let payload = self.fields.join("\n");
        self.fields.clear();

        Some(if is_sentinel(&payload) {
            SseEvent::Done
        } else {
            SseEvent::Data(payload)
        })
    }
}

/// The frame one chunk produces on the wire: `data: <json>\n\n`
/// (`private-chat-completion.ts:163` for the yielded text, `start.ts:853` for the appended
/// separator).
///
/// The value is re-serialised, never echoed as upstream bytes, and `serde_json`'s `preserve_order`
/// feature is what keeps the upstream's own key order intact through that round trip.
#[must_use]
pub fn data_frame(chunk: &Value) -> String {
    format!(
        "data: {}{FRAME_SEPARATOR}",
        serde_json::to_string(chunk).unwrap_or_else(|_| "null".to_owned())
    )
}

/// The whole body of one served stream: every chunk frame, then the sentinel.
///
/// The sentinel is APPENDED ONLY ON CLEAN EXHAUSTION. A mid-stream failure ends the body with the
/// classified error frame and no sentinel, because the TypeScript relay's `yield 'data: [DONE]'`
/// is unreachable once the upstream threw (`private-chat-completion.ts:163-186`) and the gateway
/// writes the error frame instead of continuing (`start.ts:860-873`).
#[must_use]
pub fn stream_body(chunks: &[Value]) -> String {
    let mut body = String::new();

    for chunk in chunks {
        body.push_str(&data_frame(chunk));
    }

    body.push_str(DONE_FRAME);

    body
}

#[cfg(test)]
mod tests {
    // The decoder rules are pinned against the SDK's own `SSEDecoder`, not against the harness
    // sibling: `node_modules/openai/core/streaming.js:245-285` (accumulation + boundary dispatch),
    // `:261` (the `\n` join), `:35` (the PREFIX sentinel test) and `:255-268` (an event closes on an
    // EMPTY line only). The three places this codec is deliberately more forgiving than the SDK are
    // the module doc's recorded leniencies, and each of those tests says which one it pins.
    use super::{DONE_SENTINEL, SseDecoder, SseEvent, data_frame, data_payload, stream_body};
    use serde_json::json;

    /// The SDK joins one event's `data:` values with `\n` (`streaming.js:261
    /// `data: this.data.join('\n')``), so several field lines are ONE payload, never several.
    #[test]
    fn several_data_lines_of_one_event_are_one_payload_joined_with_a_newline() {
        let mut decoder = SseDecoder::new();

        assert_eq!(
            decoder.push(b"data: {\"id\":\"a\"}\ndata: {\"index\":1}\n\n"),
            vec![SseEvent::Data("{\"id\":\"a\"}\n{\"index\":1}".to_owned())],
            "one event boundary, one payload, joined exactly the way the SDK joins it"
        );

        // The event in progress is visible between the two lines: nothing may be emitted before the
        // boundary, because the SDK holds the event until it reads the empty line.
        let mut half = SseDecoder::new();

        assert_eq!(half.push(b"data: {\"id\":\"a\"}\n"), Vec::new());
        assert_eq!(
            half.push(b"data: {\"index\":1}\n\n"),
            vec![SseEvent::Data("{\"id\":\"a\"}\n{\"index\":1}".to_owned())]
        );
    }

    /// `event:` / `id:` / `retry:` / a `:` comment carry no payload (recorded leniency 3). The SDK
    /// yields such an event with `data: ''` and its caller then fails to parse it
    /// (`streaming.js:41-48`), so strictly LESS reaches a TypeScript caller than reaches this one.
    #[test]
    fn a_field_line_that_is_not_data_carries_no_payload() {
        let mut decoder = SseDecoder::new();

        assert_eq!(
            decoder.push(b"event: message\nid: 7\nretry: 1000\n: keep-alive\n\n"),
            Vec::new(),
            "a keep-alive or a named event is not a chunk, and not a decode failure either"
        );

        // The same event WITH a data field still produces exactly one payload: the skipped lines
        // must not swallow the event they belong to.
        assert_eq!(
            decoder.push(b"event: message\ndata: {\"id\":\"a\"}\n\n"),
            vec![SseEvent::Data("{\"id\":\"a\"}".to_owned())]
        );
    }

    /// The sentinel test is a PREFIX match, exactly as the deployed decoder compares it
    /// (`streaming.js:35 if (sse.data.startsWith('[DONE]'))`). A payload that merely CONTAINS the
    /// sentinel text is an ordinary chunk.
    #[test]
    fn the_sentinel_is_matched_as_a_prefix_and_a_payload_containing_it_is_not() {
        let mut exact = SseDecoder::new();

        assert_eq!(exact.push(b"data: [DONE]\n\n"), vec![SseEvent::Done]);

        // Trailing WHITESPACE after the sentinel is still the sentinel — but only because this
        // crate's own trimming rule (`data_payload`) removes it, so the payload that reaches the
        // sentinel test is exactly `[DONE]` and the prefix rule is NOT what this case exercises.
        // The non-whitespace case, the one trimming cannot hide, is pinned by
        // `the_sentinel_prefix_ends_the_stream_even_with_text_after_it`.
        let mut trailing = SseDecoder::new();

        assert_eq!(
            trailing.push(b"data: [DONE] \n\ndata: ignored\n\n"),
            vec![SseEvent::Done],
            "the sentinel ends the stream, so the frame after it is not delivered"
        );

        let mut inside = SseDecoder::new();

        assert_eq!(
            inside.push(b"data: {\"delta\":{\"content\":\"[DONE]\"}}\n\n"),
            vec![SseEvent::Data(
                "{\"delta\":{\"content\":\"[DONE]\"}}".to_owned()
            )],
            "a model that GENERATES the sentinel text is not an end of stream"
        );
        assert!(!inside.is_finished());
        assert_eq!(DONE_SENTINEL, "[DONE]");
    }

    /// The PREFIX half of the sentinel rule, pinned where the trimming rule CANNOT hide it: text
    /// that is not whitespace after `[DONE]` is still the sentinel, because the deployed decoder
    /// compares a prefix and not the whole payload
    /// (`node_modules/openai/core/streaming.js:35 if (sse.data.startsWith('[DONE]'))`).
    ///
    /// Why this case is the one that guards the rule: an EXACT comparison against `[DONE]` rejects
    /// every payload below, so the decoder would emit it as an ordinary `Data` chunk and stay open —
    /// the stream would not terminate and the frames after the sentinel would reach the caller. That
    /// is an observable behavioural difference, not a formatting one, so these assertions fail if the
    /// prefix rule is replaced by equality (verified by perturbation, 2026-11-19).
    #[test]
    fn the_sentinel_prefix_ends_the_stream_even_with_text_after_it() {
        // `data_payload` trims the line, so `tail` is still inside the payload the sentinel test
        // sees: the joined payload here is `[DONE]tail`, exactly what an equality test rejects.
        let mut suffixed = SseDecoder::new();

        assert_eq!(
            suffixed.push(b"data: [DONE]tail\n\n"),
            vec![SseEvent::Done],
            "`[DONE]tail` is the sentinel, and only a prefix match makes it one"
        );
        assert!(
            suffixed.is_finished(),
            "the sentinel was seen, so the stream is over and nothing may follow"
        );
        assert_eq!(
            suffixed.push(b"data: {\"id\":\"after\"}\n\n"),
            Vec::new(),
            "a frame after the sentinel is not delivered"
        );
        assert_eq!(
            suffixed.finish(),
            Vec::new(),
            "`finish` stays empty once finished"
        );

        // The sentinel and a FOLLOW-ON FRAME in the same write: the sentinel closes its own event and
        // ends the stream, so the frame behind it is dropped with the rest of the buffer
        // (`push`'s sentinel branch clears both buffers). A decoder that compared the payload with
        // `[DONE]` would instead emit `[DONE]tail` as a chunk and then `{"id":"late"}` as a second
        // one, i.e. two chunks where the contract says "stream over".
        let mut burst = SseDecoder::new();

        assert_eq!(
            burst.push(b"data: [DONE]tail\n\ndata: {\"id\":\"late\"}\n\n"),
            vec![SseEvent::Done],
            "one write: the sentinel wins, and the frame that follows it in the same write is dropped"
        );
        assert!(burst.is_finished());
        assert_eq!(
            burst.pending(),
            b"".as_slice(),
            "nothing is held back after the sentinel"
        );

        // Same rule one level up: a payload assembled from SEVERAL `data:` lines of one event is
        // still tested as a whole against the sentinel prefix (`streaming.js:261` joins, `:35`
        // compares the joined string), so a sentinel with a second field line attached still ends it.
        let mut joined = SseDecoder::new();

        assert_eq!(
            joined.push(b"data: [DONE]tail\ndata: {\"id\":\"b\"}\n\n"),
            vec![SseEvent::Done],
            "the joined payload `[DONE]tail` plus the second field line still starts with the sentinel"
        );
        assert!(joined.is_finished());
    }

    /// An event that never reached its blank separator is still one event (recorded leniency 2).
    /// The SDK emits an event ONLY on an empty line (`streaming.js:255-268`, with `:211-216`), so it
    /// DROPS this final event — strictly less reaches the TypeScript caller.
    #[test]
    fn finish_flushes_the_event_a_truncated_body_left_open() {
        let mut decoder = SseDecoder::new();

        assert_eq!(decoder.push(b"data: {\"id\":\"tail\"}"), Vec::new());
        assert_eq!(
            decoder.finish(),
            vec![SseEvent::Data("{\"id\":\"tail\"}".to_owned())],
            "the unterminated tail line and the un-closed event are both flushed once"
        );
        assert!(decoder.pending().is_empty(), "the tail was consumed");

        // `finish` is idempotent, and a body that ended ON a boundary has nothing left to flush.
        assert_eq!(decoder.finish(), Vec::new());

        let mut clean = SseDecoder::new();

        assert_eq!(
            clean.push(b"data: {\"id\":\"a\"}\n\n"),
            vec![SseEvent::Data("{\"id\":\"a\"}".to_owned())]
        );
        assert_eq!(clean.finish(), Vec::new());
    }

    /// A line cut at any byte offset, and several events in one write, are the whole point of the two
    /// buffers: a payload is emitted exactly once and never half.
    #[test]
    fn a_line_split_at_any_offset_is_emitted_once_and_whole() {
        let mut decoder = SseDecoder::new();

        assert_eq!(decoder.push(b"data: {\"id\""), Vec::new());
        assert_eq!(
            decoder.pending(),
            b"data: {\"id\"".as_slice(),
            "the unterminated line is held, not dropped"
        );
        assert_eq!(
            decoder.push(b":\"a\"}\n"),
            Vec::new(),
            "a line is not an event"
        );
        assert_eq!(
            decoder.push(b"\n"),
            vec![SseEvent::Data("{\"id\":\"a\"}".to_owned())]
        );

        // One write, three complete events, in wire order.
        let mut burst = SseDecoder::new();

        assert_eq!(
            burst.push(b"data: {\"id\":\"1\"}\n\ndata: {\"id\":\"2\"}\n\ndata: {\"id\":\"3\"}\n\n"),
            vec![
                SseEvent::Data("{\"id\":\"1\"}".to_owned()),
                SseEvent::Data("{\"id\":\"2\"}".to_owned()),
                SseEvent::Data("{\"id\":\"3\"}".to_owned())
            ]
        );
    }

    /// CRLF separators are the SSE spec's own, and the CR is dropped before the line is classified
    /// (`streaming.js:252-254`). A boundary carrying stray spaces is recorded leniency 1: the SDK
    /// would read that line as a field literally named ` `.
    #[test]
    fn crlf_separators_close_an_event_and_a_spaced_boundary_does_too() {
        let mut decoder = SseDecoder::new();

        assert_eq!(
            decoder.push(b"data: {\"id\":\"a\"}\r\n\r\n"),
            vec![SseEvent::Data("{\"id\":\"a\"}".to_owned())]
        );
        assert_eq!(
            decoder.push(b"data: {\"id\":\"b\"}\r\n \r\n"),
            vec![SseEvent::Data("{\"id\":\"b\"}".to_owned())],
            "recorded leniency 1: a separator with a space in it still closes the event"
        );

        // The field-name rule that differs from the SDK, pinned so the leniency stays visible: a
        // leading space before `data:` is a data field here and a DIFFERENT field there.
        assert_eq!(
            data_payload(b"  data: {\"id\":\"c\"}  "),
            Some("{\"id\":\"c\"}".to_owned())
        );
    }

    /// Nothing follows the sentinel: the relay emits it last and closes
    /// (`private-chat-completion.ts:186`), so a caller reading past it is a bug this flag exposes.
    #[test]
    fn the_decoder_is_finished_after_the_sentinel() {
        let mut decoder = SseDecoder::new();

        assert_eq!(
            decoder.push(b"data: {\"id\":\"a\"}\n\ndata: [DONE]\n\ndata: {\"id\":\"late\"}\n\n"),
            vec![SseEvent::Data("{\"id\":\"a\"}".to_owned()), SseEvent::Done]
        );
        assert!(decoder.is_finished());
        assert_eq!(decoder.push(b"data: {\"id\":\"more\"}\n\n"), Vec::new());
        assert_eq!(decoder.finish(), Vec::new());
    }

    /// The write half: the frame the relay produces, and the whole body with the sentinel appended
    /// ONLY on clean exhaustion (`private-chat-completion.ts:163`, `:186`, `start.ts:853`).
    #[test]
    fn the_write_half_is_the_frame_the_relay_yields() {
        assert_eq!(
            data_frame(&json!({ "id": "a", "object": "chat.completion.chunk" })),
            "data: {\"id\":\"a\",\"object\":\"chat.completion.chunk\"}\n\n",
            "`data: <json>\\n\\n`, with the value RE-SERIALISED and its own key order intact"
        );

        assert_eq!(
            stream_body(&[json!({ "id": "1" }), json!({ "id": "2" })]),
            "data: {\"id\":\"1\"}\n\ndata: {\"id\":\"2\"}\n\ndata: [DONE]\n\n"
        );

        // The sentinel is emitted exactly once, whatever came before it.
        assert_eq!(stream_body(&[]), "data: [DONE]\n\n");
    }
}
