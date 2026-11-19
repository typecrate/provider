//! The counter semantics of `model-class.ts`, pinned against the TypeScript's own arithmetic.
//!
//! Every expected figure below is derived from the rule in the TypeScript, not from an observed
//! Rust output: the input/cache diff is `isContinuation ? lastRequestTokens : 0`
//! (`model-class.ts:772-777`), the output total is `+= readOutputTokens(usage)` (`:725,863`), the
//! phase split and the matched-rate pair are recorded together behind ONE first-token guard
//! (`:870-877`), and the cost figures are the lazy getters at `:533-548`.

use typecrate_provider::accounting::{Accounting, Counters, StreamRecord, Totals};
use typecrate_provider::pricing::Pricing;

/// The rates `makora/makora-glm-5-3.ts:32-39` pins, used wherever a dollar figure is asserted.
fn makora_pricing() -> Pricing {
    Pricing::per_million(0.72, 0.12, 2.38)
}

/// The all-zero baseline every test starts from, spelled out so a later `assert_eq!` on a whole
/// `Counters` value cannot pass by inheriting an unexpected non-zero field.
const ZERO: Counters = Counters {
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
};

#[test]
fn a_fresh_ledger_is_all_zeros_with_no_baseline() {
    let accounting = Accounting::new();

    assert_eq!(accounting.counters(), ZERO);
    assert_eq!(accounting.last_request_tokens(), 0);
    assert_eq!(accounting.input_cost(&makora_pricing()), 0.0);
    assert_eq!(accounting.output_cost(&makora_pricing()), 0.0);
    assert_eq!(accounting.total_cost(&makora_pricing()), 0.0);
}

#[test]
fn the_first_request_has_no_baseline_so_it_is_entirely_fresh_input() {
    let accounting = Accounting::new();

    // `recordRequestTokens` returns the NEW (non-cached) figure, which is the whole body here
    // because `lastRequestTokens` starts at 0 (`model-class.ts:772,785`).
    assert_eq!(accounting.record_request(100), 100);
    assert_eq!(
        accounting.counters(),
        Counters {
            input_token_total: 100,
            // `requestTotal += 1` happens with the prompt split, BEFORE the upstream call
            // (`model-class.ts:708`).
            request_total: 1,
            ..ZERO
        }
    );
    assert_eq!(accounting.last_request_tokens(), 100);
}

#[test]
fn a_continuation_bills_only_the_newly_appended_turns() {
    let accounting = Accounting::new();

    let _ = accounting.record_request(100);

    // The body embeds the whole prior conversation, so the overlapping 100 is the cache hit and
    // only the 30 newly appended tokens are fresh input (`model-class.ts:772-779`).
    assert_eq!(accounting.record_request(130), 30);
    assert_eq!(
        accounting.counters(),
        Counters {
            // 100 + 30 fresh across the two requests.
            input_token_total: 130,
            cache_token_total: 100,
            request_total: 2,
            ..ZERO
        }
    );

    // The partition identity the accounting rests on: every request's input + cache equals its
    // own whole-body estimate, never more.
    assert_eq!(
        accounting.counters().input_token_total + accounting.counters().cache_token_total,
        100 + 130
    );
}

#[test]
fn an_identical_body_is_fully_cached_but_stays_the_baseline() {
    let accounting = Accounting::new();

    let _ = accounting.record_request(120);

    // `current >= last` includes EQUAL, so a replay of the same conversation is a pure cache hit.
    assert_eq!(accounting.record_request(120), 0);
    assert_eq!(accounting.last_request_tokens(), 120);
    assert_eq!(
        accounting.counters(),
        Counters {
            input_token_total: 120,
            cache_token_total: 120,
            request_total: 2,
            ..ZERO
        }
    );
}

#[test]
fn a_shrunk_body_restarts_the_conversation_and_caches_nothing() {
    let accounting = Accounting::new();

    let _ = accounting.record_request(130);

    // The smaller body "means the conversation restarted and shares no prior prefix"
    // (`model-class.ts:765-768`): whole body fresh, cache zero, and it becomes the new baseline.
    assert_eq!(accounting.record_request(100), 100);
    assert_eq!(accounting.last_request_tokens(), 100);
    assert_eq!(
        accounting.counters(),
        Counters {
            input_token_total: 230,
            cache_token_total: 0,
            request_total: 2,
            ..ZERO
        }
    );

    // And the NEXT request diffs against the shrunken baseline, not the old larger one.
    assert_eq!(accounting.record_request(140), 40);
    assert_eq!(accounting.counters().cache_token_total, 100);
}

#[test]
fn reset_clears_every_counter_and_the_baseline_together() {
    let accounting = Accounting::new();

    let _ = accounting.record_request(100);
    let _ = accounting.record_request(130);
    accounting.record_response(Some(40), 1200);
    accounting.record_attempt(4);
    accounting.record_failure();
    assert_ne!(accounting.counters(), ZERO);

    accounting.reset();

    // `resetTotals` zeroes the ten counters AND `lastRequestTokens` (`model-class.ts:507-519`),
    // so a reset model diffs against a fresh conversation. The port additions clear too.
    assert_eq!(accounting.counters(), ZERO);
    assert_eq!(accounting.last_request_tokens(), 0);
}

#[test]
fn a_non_streaming_answer_adds_usage_and_elapsed_and_no_phase_split_at_all() {
    let accounting = Accounting::new();
    let _ = accounting.record_request(100);

    accounting.record_response(Some(40), 1200);

    // `model-class.ts:724-730`: the usage figure and the duration settle, and a non-streaming call
    // gets NO phase timing and NO matched counters - the latency split belongs to streams only.
    assert_eq!(
        accounting.counters(),
        Counters {
            input_token_total: 100,
            output_token_total: 40,
            request_total: 1,
            millisecond_total: 1200,
            ..ZERO
        }
    );
}

#[test]
fn an_absent_usage_block_adds_nothing_but_still_settles_the_elapsed_span() {
    let accounting = Accounting::new();

    let _ = accounting.record_request(100);
    accounting.record_response(None, 500);

    assert_eq!(
        accounting.counters(),
        Counters {
            input_token_total: 100,
            request_total: 1,
            millisecond_total: 500,
            ..ZERO
        }
    );
}

#[test]
fn a_failed_request_still_settles_its_wall_clock() {
    let accounting = Accounting::new();

    let _ = accounting.record_request(100);
    accounting.record_failure();
    accounting.record_elapsed(900);

    // The `catch` path (`model-class.ts:716-720`): elapsed settles, no token figure does, and the
    // request was already counted - which is why `requests === 0` means "never attempted"
    // (`private-stats.ts:270-284`).
    assert_eq!(
        accounting.counters(),
        Counters {
            input_token_total: 100,
            request_total: 1,
            millisecond_total: 900,
            consecutive_failures: 1,
            ..ZERO
        }
    );

    accounting.record_success();

    assert_eq!(accounting.counters().consecutive_failures, 0);
}

/// A settled stream that produced a first token, upstream usage included.
fn settled_stream() -> StreamRecord {
    StreamRecord {
        total_milliseconds: 1000,
        ttft_milliseconds: Some(200),
        generation_milliseconds: Some(800),
        output_tokens: 64,
        usage_reported: true,
    }
}

#[test]
fn a_first_payload_pairs_the_phase_split_with_both_rate_counters() {
    let accounting = Accounting::new();

    let prefill = accounting.record_request(100);
    accounting.record_stream(prefill, &settled_stream());

    // `trackStream`'s `finally` (`model-class.ts:862-877`), exact: the totals always settle, and
    // the phase split plus BOTH matched counters fold in behind the one first-token guard.
    assert_eq!(
        accounting.counters(),
        Counters {
            input_token_total: 100,
            // `settledOutputTokens`, verbatim.
            output_token_total: 64,
            request_total: 1,
            // `endedAt - startedAt`, the whole span - not ttft + generation of the transport's
            // head-only clock.
            millisecond_total: 1000,
            ttft_millisecond_total: 200,
            generation_millisecond_total: 800,
            // The prefill numerator is the request's NEW input figure, not input+cache.
            measured_prefill_token_total: 100,
            measured_decode_token_total: 64,
            measured_decode_millisecond_total: 800,
            ..ZERO
        }
    );
}

#[test]
fn a_stream_without_a_usage_block_keeps_the_decode_denominator_clean() {
    let accounting = Accounting::new();

    let prefill = accounting.record_request(100);

    let record = StreamRecord {
        usage_reported: false,
        output_tokens: 0,
        ..settled_stream()
    };

    accounting.record_stream(prefill, &record);

    // `outputUsageKnown` gates the decode PAIR only (`model-class.ts:873-876`): the generation
    // span still lands in `generationMillisecondTotal`, but an unknown figure must not enter the
    // decode denominator while contributing no numerator.
    assert_eq!(
        accounting.counters(),
        Counters {
            input_token_total: 100,
            request_total: 1,
            millisecond_total: 1000,
            ttft_millisecond_total: 200,
            generation_millisecond_total: 800,
            measured_prefill_token_total: 100,
            ..ZERO
        }
    );
}

#[test]
fn a_stream_that_never_produced_a_token_gets_no_phase_split_but_still_settles() {
    let accounting = Accounting::new();

    let prefill = accounting.record_request(100);

    let record = StreamRecord {
        total_milliseconds: 700,
        ttft_milliseconds: None,
        generation_milliseconds: None,
        output_tokens: 12,
        usage_reported: true,
    };

    accounting.record_stream(prefill, &record);

    // No observed first token => no phase split AND no matched counters (`model-class.ts:870`),
    // while the output total and the duration settle unconditionally (`:869-871`).
    assert_eq!(
        accounting.counters(),
        Counters {
            input_token_total: 100,
            output_token_total: 12,
            request_total: 1,
            millisecond_total: 700,
            ..ZERO
        }
    );
}

#[test]
fn the_cost_is_the_lifetime_tokens_at_the_pinned_rates_computed_on_read() {
    let accounting = Accounting::new();

    let _ = accounting.record_request(150_000);
    accounting.record_response(Some(10_000), 1000);

    // The split the getters see: the first request has no baseline, so all 150000 are fresh
    // input. `costInputTotal` = input*inputCost/1e6 + cache*cacheCost/1e6 (`model-class.ts:534`),
    // asserted through the four-decimal spelling the deployed log line and dashboard actually
    // print, because these products are binary-inexact floats and only their rendered form is
    // exact (the deployed formatter is `($X.XX)`, `private-chat-completion.ts:49-61`).
    assert_eq!(
        format!("{:.4}", accounting.input_cost(&makora_pricing())),
        "0.1080"
    );
    assert_eq!(
        format!("{:.4}", accounting.output_cost(&makora_pricing())),
        "0.0238"
    );

    // `costTotal` is the two brackets ADDED, never an independent product
    // (`model-class.ts:546-548`) - asserted as the same expression, so the identity is exact in
    // floating point rather than approximated.
    assert_eq!(
        accounting.total_cost(&makora_pricing()),
        accounting.input_cost(&makora_pricing()) + accounting.output_cost(&makora_pricing())
    );

    // A pricing change is reflected immediately, because nothing is accumulated per request: this
    // is the behaviour the "subclasses pinning real prices get live costs for free" comment
    // promises (`model-class.ts:530-532`).
    accounting.reset();
    let _ = accounting.record_request(150_000);

    assert_eq!(accounting.output_cost(&Pricing::free()), 0.0);
    assert_eq!(
        accounting.input_cost(&makora_pricing()),
        makora_pricing().cost_input(150_000, 0)
    );
}

#[test]
fn a_cached_prefix_is_priced_at_the_cache_rate_and_only_at_that_rate() {
    let accounting = Accounting::new();

    let _ = accounting.record_request(100_000);
    let _ = accounting.record_request(120_000);

    // 100000 fresh then 20000 fresh + 100000 cached.
    assert_eq!(
        accounting.counters().cache_token_total,
        100_000,
        "the replayed prefix is the second request's whole prior body"
    );
    assert_eq!(
        accounting.input_cost(&makora_pricing()),
        makora_pricing().cost_input(120_000, 100_000)
    );
    assert_eq!(
        format!("{:.4}", accounting.input_cost(&makora_pricing())),
        "0.0984",
        "0.12M x $0.72 (0.0864) plus 0.1M x $0.12 (0.012)"
    );
}

#[test]
fn totals_sum_counters_field_by_field() {
    let first = Counters {
        input_token_total: 10,
        cache_token_total: 20,
        output_token_total: 30,
        request_total: 40,
        millisecond_total: 50,
        ttft_millisecond_total: 60,
        generation_millisecond_total: 70,
        measured_prefill_token_total: 80,
        measured_decode_token_total: 90,
        measured_decode_millisecond_total: 100,
        attempt_total: 110,
        consecutive_failures: 3,
    };

    let second = Counters {
        input_token_total: 1,
        cache_token_total: 2,
        output_token_total: 3,
        request_total: 4,
        millisecond_total: 5,
        ttft_millisecond_total: 6,
        generation_millisecond_total: 7,
        measured_prefill_token_total: 8,
        measured_decode_token_total: 9,
        measured_decode_millisecond_total: 10,
        attempt_total: 11,
        consecutive_failures: 4,
    };

    let mut totals = Totals::default();
    totals.add(&first);
    totals.add(&second);

    // The stats route's fleet roll-up is a plain sum of every field (`private-stats.ts:286-300`).
    assert_eq!(
        totals.counters,
        Counters {
            input_token_total: 11,
            cache_token_total: 22,
            output_token_total: 33,
            request_total: 44,
            millisecond_total: 55,
            ttft_millisecond_total: 66,
            generation_millisecond_total: 77,
            measured_prefill_token_total: 88,
            measured_decode_token_total: 99,
            measured_decode_millisecond_total: 110,
            attempt_total: 121,
            consecutive_failures: 7,
        }
    );
}
