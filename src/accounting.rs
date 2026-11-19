//! Per-client accounting: the token/cost bookkeeping and the cumulative counters.
//!
//! # Typecrate port - source lineage
//!
//! TypeScript origin: `packages/agentic/provider/src/model-class.ts` - the counter block
//! (`:417-495`), `resetTotals` (`:507-519`), the private baseline cell
//! `lastRequestTokens` (`:525`), the cost getters (`:533-548`), the accounting wrapper
//! `accountUpstreamCall` (`:700-734`), the input/cache diff in `recordRequestTokens`
//! (`:770-786`), and the stream settlement in `trackStream` (`:831-879`).
//!
//! The counters are this module's whole public surface:
//! `runtime/endpoint/provider/private/stats/private-stats.ts:197-341` projects exactly ten of
//! them per model row and rolls the rows into one fleet summary, and
//! `runtime/endpoint/provider/private/chat-completion/private-chat-completion.ts:174-184,206-215`
//! prints five of them (id, input, cache, input-cost, output, output-cost) as the request log
//! line. Both read LIFETIME totals, never per-request figures - which is why this module
//! exposes counters plus cost getters rather than a per-request usage object.
//!
//! ## The ten TypeScript counters, mirrored one-for-one
//!
//! | Rust field | TypeScript field |
//! |---|---|
//! | `input_token_total` | `inputTokenTotal` (new, non-cached input) |
//! | `cache_token_total` | `cacheTokenTotal` (replayed prefix) |
//! | `output_token_total` | `outputTokenTotal` |
//! | `request_total` | `requestTotal` |
//! | `millisecond_total` | `millisecondTotal` |
//! | `ttft_millisecond_total` | `ttftMillisecondTotal` |
//! | `generation_millisecond_total` | `generationMillisecondTotal` |
//! | `measured_prefill_token_total` | `measuredPrefillTokenTotal` |
//! | `measured_decode_token_total` | `measuredDecodeTokenTotal` |
//! | `measured_decode_millisecond_total` | `measuredDecodeMillisecondTotal` |
//!
//! Cost is NOT a stored counter upstream either: `costInputTotal` / `costOutputTotal` /
//! `costTotal` (`:533-548`) multiply the token totals by the pinned per-million rates on read,
//! "so no extra bookkeeping runs per request" (`:530-532`). [`Accounting::input_cost`],
//! [`Accounting::output_cost`] and [`Accounting::total_cost`] reproduce that lazy shape, which
//! is why a pricing correction is reflected immediately in a live process in BOTH systems.
//!
//! ## The two extra counters are PORT ADDITIONS
//!
//! `attempt_total` and `consecutive_failures` have no TypeScript counterpart: `attemptTotal`
//! and `consecutiveFailures` appear nowhere in `packages/agentic/provider/src`. They exist
//! because this port reports attempts and outcomes as VALUES (`AttemptState::attempts`,
//! [`crate::transport::StreamOutcome`]) rather than as thrown errors, so a supervisor can read
//! a circuit-breaker figure without parsing messages. They are kept out of every parity figure: the
//! per-model `/stats` ROW omits them entirely (`serve::stats_row`), and only the fleet roll-up
//! (`serve::stats_totals`) appends them — as two EXTRA keys after the parity keys, never folded into
//! one — and [`Totals`] sums them for the same reason: a sum of counters is the whole point of that
//! type.
//!
//! DEVIATION (deliberate, recorded): two counters are added for the port's own observability, and no
//! parity figure depends on them (they ride alongside the parity keys in `stats_totals` only).
//!
//! ## Latency totals are integers here
//!
//! TypeScript accumulates `performance.now()` DIFFERENCES, which are fractional milliseconds.
//! Every span here is truncated to whole milliseconds at settlement, so a fleet total can read
//! a few ms below the Node figure after many sub-millisecond requests. Nothing in this crate
//! divides two of these counters together - the dashboard computes its own Prefill/Decode rates
//! from the paired numerator and denominator (`private-stats.ts:84-101`) - so truncation cannot
//! distort a figure beyond the rounding the dashboard already applies.
//!
//! DEVIATION (deliberate, recorded): latency totals are whole milliseconds, truncated per
//! settlement rather than summed as fractions.
//!
//! ## One lock, one critical section
//!
//! The counters live inside a single `Mutex<Ledger>` together with the cache baseline, and every
//! documented rule that reads one figure and writes another - above all the input/cache diff -
//! happens inside ONE acquisition. TypeScript gets that atomicity for free from a single-threaded
//! runtime (`recordRequestTokens` reads `lastRequestTokens` and writes two totals with no
//! possible interleaving); a Rust client is shared across worker threads, so the same
//! read-modify-write sequence needs the lock to stay equivalent. A `RefCell` would not do: the
//! ledger sits behind the `Arc` of a client that is `Send + Sync`, and a `RefCell` is not
//! `Sync`.

use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::pricing::Pricing;
use crate::transport::StreamSettlement;

/// Running per-client token and latency counters.
///
/// One field per TypeScript counter, in the declaration order of `model-class.ts:417-495`:
/// `inputTokenTotal`, `cacheTokenTotal`, `outputTokenTotal`, `requestTotal`, `millisecondTotal`,
/// `ttftMillisecondTotal`, `generationMillisecondTotal`, `measuredPrefillTokenTotal`,
/// `measuredDecodeTokenTotal`, `measuredDecodeMillisecondTotal`, plus the two port additions at
/// the end.
///
/// Every value is cumulative since construction or the last [`Accounting::reset`], which is
/// upstream `resetTotals` (`:507-519`) - the endpoint the dashboard's per-model reset drives.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counters {
    /// Estimated prompt tokens charged as FRESH input, cache prefix excluded.
    pub input_token_total: u64,
    /// Estimated prompt tokens charged as the replayed cache prefix.
    pub cache_token_total: u64,
    /// Output tokens as reported by upstream usage, added verbatim.
    ///
    /// NOT a difference: `outputTokenTotal += readOutputTokens(usage)`
    /// (`model-class.ts:725,863`). An absent usage block adds nothing rather than zero, because
    /// upstream guards on `!== undefined` (`:728`).
    pub output_token_total: u64,
    /// Requests seen by this client, pass or fail.
    ///
    /// `requestTotal` is incremented BEFORE the upstream call (`model-class.ts:708`), so a failed
    /// request counts, and it counts once per logical request however many times the failover loop
    /// rotates base URLs and credentials.
    pub request_total: u64,
    /// Wall-clock milliseconds across every settled request, start to completion.
    pub millisecond_total: u64,
    /// Time-to-first-generated-payload, summed over streams that produced one.
    ///
    /// A non-streaming response records NO phase split at all (`model-class.ts:726-730`), and a
    /// stream that never emitted content records none either (`:870`).
    pub ttft_millisecond_total: u64,
    /// First-payload-to-settlement spans, summed over the same streams.
    pub generation_millisecond_total: u64,
    /// The NEW (non-cached) input estimate of each first-payload stream.
    ///
    /// This is the prefill NUMERATOR, paired with `ttft_millisecond_total`: the input figure
    /// `record_request` already computed, never a second serialization
    /// (`model-class.ts:705-707,872`). Upstream's own comment used to describe the field as
    /// "input+cache"; the T1 correction made it input-only, so a replayed prefix cannot inflate
    /// the reported Prefill tok/s.
    pub measured_prefill_token_total: u64,
    /// Settled output usage of first-payload streams whose usage was REPORTED.
    pub measured_decode_token_total: u64,
    /// Generation spans of exactly those usage-known streams.
    ///
    /// The pairing is the point: an absent usage block contributes its generation span to
    /// `generation_millisecond_total` but NOT to this denominator, so "unknown" never masquerades
    /// as zero and drags the Decode tok/s figure down (`model-class.ts:873-876`).
    pub measured_decode_millisecond_total: u64,
    /// HTTP attempts, including failover rotations.
    ///
    /// PORT ADDITION - see the module doc. Defined as the attempts the CLIENT layer can see, i.e.
    /// one per request issue, because the transport's internal 429 retries are not observable on
    /// a success path.
    pub attempt_total: u64,
    /// Consecutive settled failures; cleared by [`Accounting::record_success`].
    ///
    /// PORT ADDITION - see the module doc.
    pub consecutive_failures: u32,
}

/// The phase-split and usage figures of one settled stream.
///
/// This is the local state of `trackStream` (`model-class.ts:834-879`) reduced to the values its
/// `finally` block folds into the counters. It exists as a type so that fold happens in ONE
/// place, which is what keeps numerator and denominator describing the same set of requests -
/// the invariant the matched-rate counters exist to protect (`model-class.ts:460-490`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamRecord {
    /// Wrapper entry -> settlement, the figure `millisecondTotal` takes.
    pub total_milliseconds: u64,
    /// Wrapper entry -> first generated payload; `None` when no payload arrived, and `None` is
    /// NOT zero (`model-class.ts:870-872`).
    pub ttft_milliseconds: Option<u64>,
    /// First payload -> settlement; `None` under the same rule.
    pub generation_milliseconds: Option<u64>,
    /// The LAST usage figure seen on any chunk, or 0 when the upstream reported none. Added to
    /// `output_token_total` either way (`:869`).
    pub output_tokens: u64,
    /// Whether a usage block was SEEN at all - `outputUsageKnown` (`:838-839`). An explicit `0` is
    /// known; absent usage is not.
    pub usage_reported: bool,
}

impl StreamRecord {
    /// Fold a transport settlement plus the caller's own wall clock into one record.
    ///
    /// WHY the caller supplies the total: the transport's settlement clock starts when its stream
    /// adapter is built, i.e. AT THE RESPONSE HEAD (`transport.rs:1884`), while TypeScript's span
    /// starts at the accounting wrapper's entry - before connect, before transport retries, and
    /// before any 404 rotation (`model-class.ts:710`). Only the client can measure that span, so
    /// `total_milliseconds` is the client's single clock read at settlement.
    ///
    /// Everything the transport spent before the first chunk therefore belongs to
    /// time-to-first-token, exactly as TypeScript attributes everything from entry to first
    /// payload to it: `ttft = settlement.ttft + head_latency`, generation unchanged. When the
    /// settlement carries no first payload, both phase figures stay `None` and the head latency is
    /// dropped with them - matching `:870`, which records NO phase split in that case.
    #[must_use]
    pub fn new(settlement: &StreamSettlement, total_milliseconds: u64) -> Self {
        // A clock skew, or a settlement that raced the client's entry stamp, must not underflow:
        // the head latency is a derived remainder, so it saturates at zero.
        let head_latency = total_milliseconds.saturating_sub(millis(settlement.elapsed));

        Self {
            total_milliseconds,
            ttft_milliseconds: settlement
                .time_to_first_token
                .map(|first| millis(first) + head_latency),
            generation_milliseconds: settlement.generation.map(millis),
            output_tokens: settlement.output_tokens.unwrap_or_default(),
            usage_reported: settlement.usage_reported,
        }
    }
}

/// A fair surrogate for a poisoned lock: the ledger is plain integer arithmetic with no user
/// callback inside a critical section, so a panic elsewhere in the process cannot have left it
/// half-updated in a way a caller could act on. Recovering the value keeps accounting infallible,
/// which is what the transport's settlement hook requires (a hook cannot fail).
fn recover<T>(poisoned: std::sync::PoisonError<T>) -> T {
    poisoned.into_inner()
}

/// The whole mutable state of one client's ledger, guarded by one lock.
#[derive(Debug, Default)]
struct Ledger {
    counters: Counters,
    /// Whole-body estimate of this client's most recent request body, the baseline the next
    /// request diffs against. `lastRequestTokens` (`model-class.ts:525`); cleared by
    /// `resetTotals` (`:518`) for exactly the reason given there - a reset model must diff against
    /// a fresh conversation, not the pre-reset one.
    last_request_tokens: u64,
}

/// Per-client accounting: the counters plus the cache-hit baseline.
#[derive(Debug, Default)]
pub struct Accounting {
    ledger: Mutex<Ledger>,
}

impl Accounting {
    /// A fresh ledger: every counter zero and no baseline.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// One acquisition, one critical section - see the module's "one lock" section.
    fn ledger(&self) -> MutexGuard<'_, Ledger> {
        self.ledger.lock().unwrap_or_else(recover)
    }

    /// Snapshot the counters.
    #[must_use]
    pub fn counters(&self) -> Counters {
        self.ledger().counters
    }

    /// The whole-body estimate of the most recent request (the baseline cell).
    #[must_use]
    pub fn last_request_tokens(&self) -> u64 {
        self.ledger().last_request_tokens
    }

    /// Zero every counter AND the baseline - `resetTotals` (`model-class.ts:507-519`).
    pub fn reset(&self) {
        *self.ledger() = Ledger::default();
    }

    // -- cost, computed on read (model-class.ts:533-548) --------------------

    /// Fresh input plus replayed prefix, in dollars: `costInputTotal`.
    #[must_use]
    pub fn input_cost(&self, pricing: &Pricing) -> f64 {
        let counters = self.counters();

        pricing.cost_input(counters.input_token_total, counters.cache_token_total)
    }

    /// Lifetime output dollars: `costOutputTotal`.
    #[must_use]
    pub fn output_cost(&self, pricing: &Pricing) -> f64 {
        pricing.cost_output(self.counters().output_token_total)
    }

    /// The two brackets added: `costTotal`, which is why the dashboard's three cost lines always
    /// sum to its total line.
    #[must_use]
    pub fn total_cost(&self, pricing: &Pricing) -> f64 {
        self.input_cost(pricing) + self.output_cost(pricing)
    }

    // -- settlement points --------------------------------------------------

    /// Record the prompt side of a request and return its NEW (non-cached) input figure, the
    /// prefill numerator `record_stream` needs.
    ///
    /// This is `accountUpstreamCall`'s opening pair: `recordRequestTokens`
    /// (`model-class.ts:770-786`) plus `requestTotal += 1` (`:708`). It runs exactly once per
    /// logical request however many rotations follow, because the accounting wrapper sits OUTSIDE
    /// the failover loop (`:655-656` wraps the whole `create`).
    ///
    /// The split rule, term for term:
    ///   * `is_continuation = last > 0 && current >= last` - a body SHORTER than its predecessor
    ///     means the conversation restarted and shares no prior prefix: cache 0, whole body fresh
    ///     input, and it becomes the new baseline (`:772-779`);
    ///   * `cache = if is_continuation { last } else { 0 }`, `input = current - cache`: a disjoint
    ///     partition of the whole-body estimate, so `input + cache == current` always holds - which
    ///     is the identity the tests pin;
    ///   * the baseline is REPLACED, not accumulated, so the next request diffs against a full
    ///     conversation body rather than a running sum (`:783`).
    ///
    /// `prompt_tokens` must be `transport::estimate_request_tokens` over the CALLER's body:
    /// TypeScript estimates `body` BEFORE `withModel` rewrites `model` (`:707` vs `:733`), and the
    /// port keeps both bodies for that reason (see `crate::client`).
    #[must_use]
    pub fn record_request(&self, prompt_tokens: u64) -> u64 {
        let mut ledger = self.ledger();

        // "Continuation" is a size comparison, not a similarity test: the first request has no
        // baseline (`last == 0`) and a shrunken body is treated as a fresh conversation, both of
        // which fall out of the single condition below (`model-class.ts:772`).
        let cache_tokens =
            if ledger.last_request_tokens > 0 && prompt_tokens >= ledger.last_request_tokens {
                ledger.last_request_tokens
            } else {
                0
            };

        let input_tokens = prompt_tokens - cache_tokens;

        ledger.counters.cache_token_total += cache_tokens;
        ledger.counters.input_token_total += input_tokens;
        ledger.counters.request_total += 1;
        ledger.last_request_tokens = prompt_tokens;

        input_tokens
    }

    /// Settle a NON-streaming success: the reported usage and the elapsed span.
    ///
    /// `model-class.ts:724-730`: `outputTokenTotal += readOutputTokens(usage)`, then `settle()`. A
    /// missing usage block passes `None` and adds nothing. No phase split and no matched counters
    /// are recorded, because a settled response has no observable first token (`:729-731`).
    pub fn record_response(&self, output_tokens: Option<u64>, elapsed_milliseconds: u64) {
        let mut ledger = self.ledger();

        ledger.counters.output_token_total += output_tokens.unwrap_or_default();
        ledger.counters.millisecond_total += elapsed_milliseconds;
    }

    /// Settle the elapsed span of a FAILED request, adding no tokens.
    ///
    /// The `catch` half of `accountUpstreamCall` (`model-class.ts:716-720`): "a failed attempt
    /// still consumed wall-clock time, so its elapsed figure settles before the ORIGINAL error
    /// object is re-thrown" (`:680-683`). The request itself was already counted by
    /// `record_request`, so a failed request still appears in `request_total` - which is what
    /// `private-stats.ts:270-284` relies on when it treats `requests === 0` as "never attempted".
    pub fn record_elapsed(&self, elapsed_milliseconds: u64) {
        self.ledger().counters.millisecond_total += elapsed_milliseconds;
    }

    /// Fold one settled stream into the counters - `trackStream`'s `finally`
    /// (`model-class.ts:862-879`), which runs on clean exhaustion, on a mid-stream failure, on
    /// cancellation, and on consumer abandonment alike.
    ///
    /// `input_tokens` is the figure `record_request` returned for THIS request, carried by the
    /// caller so the prefill numerator costs no second serialization.
    ///
    /// The order below is the TypeScript's, and the guard matters: the token total and the elapsed
    /// span settle UNCONDITIONALLY, while the phase split - and with it both matched-rate
    /// counters - is recorded ONLY once a generated payload was observed (`:870`). The decode pair
    /// has a second guard on `usage_reported`, so a stream whose provider never sent usage
    /// contributes to `generation_millisecond_total` but not to the decode denominator.
    pub fn record_stream(&self, input_tokens: u64, record: &StreamRecord) {
        let mut ledger = self.ledger();

        ledger.counters.output_token_total += record.output_tokens;
        ledger.counters.millisecond_total += record.total_milliseconds;

        let (Some(ttft), Some(generation)) =
            (record.ttft_milliseconds, record.generation_milliseconds)
        else {
            // No observed first token => no phase split, and NOTHING in the matched-rate pair
            // either (`model-class.ts:870`).
            return;
        };

        ledger.counters.ttft_millisecond_total += ttft;
        ledger.counters.generation_millisecond_total += generation;
        ledger.counters.measured_prefill_token_total += input_tokens;

        if record.usage_reported {
            ledger.counters.measured_decode_token_total += record.output_tokens;
            ledger.counters.measured_decode_millisecond_total += generation;
        }
    }

    // -- port-addition observability ---------------------------------------

    /// Count `attempts` HTTP attempts issued by the client layer.
    pub fn record_attempt(&self, attempts: u64) {
        self.ledger().counters.attempt_total += attempts;
    }

    /// Clear the consecutive-failure run after a settled success.
    pub fn record_success(&self) {
        self.ledger().counters.consecutive_failures = 0;
    }

    /// Extend the consecutive-failure run after a settled failure.
    pub fn record_failure(&self) {
        self.ledger().counters.consecutive_failures += 1;
    }
}

/// Aggregates per-client counters the way `private-stats.ts:286-300` rolls the projected rows up
/// into one fleet summary: plain field-wise sums.
///
/// Deliberately sums ONLY counters. Cost cannot be aggregated this way, because each row prices
/// its own tokens at its own pinned rates (`private-stats.ts:229-231`), so a fleet dollar figure
/// is the sum of per-client [`Accounting::total_cost`] calls, which the caller has the pricing
/// for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    /// The field-wise sum of every folded [`Counters`].
    pub counters: Counters,
}

impl Totals {
    /// Fold one client's counters in.
    pub fn add(&mut self, counters: &Counters) {
        let total = &mut self.counters;

        total.input_token_total += counters.input_token_total;
        total.cache_token_total += counters.cache_token_total;
        total.output_token_total += counters.output_token_total;
        total.request_total += counters.request_total;
        total.millisecond_total += counters.millisecond_total;
        total.ttft_millisecond_total += counters.ttft_millisecond_total;
        total.generation_millisecond_total += counters.generation_millisecond_total;
        total.measured_prefill_token_total += counters.measured_prefill_token_total;
        total.measured_decode_token_total += counters.measured_decode_token_total;
        total.measured_decode_millisecond_total += counters.measured_decode_millisecond_total;
        total.attempt_total += counters.attempt_total;
        total.consecutive_failures += counters.consecutive_failures;
    }
}

/// Truncating millisecond conversion, the one place a latency `Duration` becomes a counter value.
/// The `u128 -> u64` saturation covers a clock span that could exceed `u64::MAX` ms only on the
/// order of hundreds of millions of years.
fn millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}
