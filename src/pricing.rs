//! Per-model price list and the dollar arithmetic the routes render.
//!
//! TypeScript origin: the three cost fields of `StandardClient`
//! (`packages/agentic/provider/src/model-class.ts:398-409` — `inputCost`, `outputCost`, `cacheCost`,
//! each "dollars per ONE MILLION tokens") and the three derived getters at `:534-550`
//! (`costInputTotal`, `costOutputTotal`, `costTotal`). Every concrete client pins its own published
//! rates as `readonly inputCost = 0.72;`-style overrides (e.g.
//! `packages/agentic/provider/src/makora/makora-glm-5-3.ts:32-39`), and the base class pins three
//! zeros, which is why [`Pricing::free`] exists: four of the twenty-nine registered rows (the whole
//! Nvidia family, plus `merge/kimi-k2-6`) publish no price list at all and legitimately render
//! `$0.0000`.
//!
//! Consumers, pinned against the real files rather than paraphrase:
//!   * `runtime/endpoint/provider/private/stats/private-stats.ts:237-261` — the per-row
//!     `inputCost` / `cacheCost` / `outputCost` / `totalCost` brackets and the `inputPrice` /
//!     `outputPrice` / `cachePrice` labels, all computed from these same three numbers;
//!   * `runtime/endpoint/provider/private/chat-completion/private-chat-completion.ts:174-184` — the
//!     `[$X.XX]` brackets of the request-token line, which read `client.costInputTotal` /
//!     `client.costOutputTotal` after the request settles.
//!
//! NAMING, and the one place this port is deliberately richer than the TypeScript: the TS field names
//! are `inputCost` / `cacheCost` / `outputCost`, and `cacheCost` prices the prefix an upstream prompt
//! cache ALREADY holds — i.e. a cache READ. The names here are `input` / `cache_read` / `output` to
//! say what each one actually buys, and `cache_write` is the slot the TypeScript has no field for.
//! DEVIATION (deliberate, recorded): `cache_write` is CARRIED and validated but priced into nothing,
//! because no TypeScript client has a cache-write rate and inventing a formula would be worse than
//! carrying a zero. A row that ever gets a published cache-write rate wires it into
//! [`Pricing::cost_input`] in one place.
//!
//! MONEY IS `f64` ON PURPOSE. JavaScript has one numeric type and the deployed figures are JS
//! numbers end to end (`(tokens * price) / 1_000_000`, `model-class.ts:534-544`); a fixed-point
//! decimal would change what the dashboard prints. The divisor is therefore the same literal, the
//! multiplication happens before the division, and the token counts stay integers the way the
//! accounting layer produces them.

/// The divisor every price is quoted against: "per ONE MILLION tokens"
/// (`model-class.ts:536-549`, `private-stats.ts:237-239`). Named once so no call site writes a
/// bare `1e6`.
pub const TOKENS_PER_DOLLAR: f64 = 1_000_000.0;

/// What one model costs per million tokens, in dollars.
///
/// All four rates are validated at row-build time ([`Pricing::is_sane`]) rather than coerced: a
/// negative or non-finite price is a transcription bug in a client row, and the registry refuses it
/// with a problem list instead of quietly rendering `NaN` dollars.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Pricing {
    /// Fresh, non-cached input, `inputCost` (`model-class.ts:398-400`).
    pub input: f64,
    /// The replayed prefix an upstream prompt cache already holds, `cacheCost` (`:405-409`).
    pub cache_read: f64,
    /// Cache WRITE tokens. No TypeScript field exists; see the module doc.
    pub cache_write: f64,
    /// Generated tokens, `outputCost` (`:403-404`).
    pub output: f64,
}

impl Pricing {
    /// The base-class default: three zeros (`model-class.ts:398-409`), i.e. an unpriced model whose
    /// cost lines read `$0.0000`.
    #[must_use]
    pub const fn free() -> Self {
        Self {
            input: 0.0,
            cache_read: 0.0,
            cache_write: 0.0,
            output: 0.0,
        }
    }

    /// A published three-rate list, the way every concrete client pins one
    /// (`makora-glm-5-3.ts:32-39`). `cache_write` starts at zero because no TypeScript row sets it.
    #[must_use]
    pub const fn per_million(input: f64, cache_read: f64, output: f64) -> Self {
        Self {
            input,
            cache_read,
            cache_write: 0.0,
            output,
        }
    }

    /// Whether every rate is a usable money value: finite and not negative.
    ///
    /// `is_finite` is the whole defence against a typo like `0.72e999` (which parses to infinity and
    /// would otherwise print `inf` dollars on the dashboard forever), and the sign check is the
    /// defence against a transcription minus-sign. `NaN.is_finite()` is `false`, so one test covers
    /// both.
    #[must_use]
    pub fn is_sane(&self) -> bool {
        [self.input, self.cache_read, self.cache_write, self.output]
            .iter()
            .all(|rate| rate.is_finite() && *rate >= 0.0)
    }

    /// The input-side dollars: fresh input at `input` plus the replayed prefix at `cache_read`
    /// (`costInputTotal`, `model-class.ts:534-539`, reproduced term for term).
    #[must_use]
    pub fn cost_input(&self, input_tokens: u64, cache_tokens: u64) -> f64 {
        (as_dollars(input_tokens) * self.input + as_dollars(cache_tokens) * self.cache_read)
            / TOKENS_PER_DOLLAR
    }

    /// The output-side dollars (`costOutputTotal`, `model-class.ts:542-544`).
    #[must_use]
    pub fn cost_output(&self, output_tokens: u64) -> f64 {
        as_dollars(output_tokens) * self.output / TOKENS_PER_DOLLAR
    }

    /// The four figures one client's lifetime totals add up to.
    ///
    /// `total` is the SUM OF THE THREE BRACKETS, not an independent product — the property
    /// `costTotal` relies on (`model-class.ts:547-550`) and the reason the dashboard's three cost
    /// lines always add up to its total line (`private-stats.ts:255-258`).
    #[must_use]
    pub fn cost(&self, input_tokens: u64, cache_tokens: u64, output_tokens: u64) -> Cost {
        let input = self.cost_input(input_tokens, cache_tokens);
        let cache = as_dollars(cache_tokens) * self.cache_read / TOKENS_PER_DOLLAR;
        let output = self.cost_output(output_tokens);

        Cost {
            input: input - cache,
            cache,
            output,
            total: input + output,
        }
    }
}

/// The dollar brackets of one client's lifetime totals, split the way the dashboard splits them
/// (`private-stats.ts:237-258`: `inputCost` / `cacheCost` / `outputCost` / `totalCost`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Cost {
    /// Fresh input only — the cached replay is broken out so the UI can show what the prompt cache
    /// charged.
    pub input: f64,
    /// The replayed-prefix dollars.
    pub cache: f64,
    /// Generated-token dollars.
    pub output: f64,
    /// The three above added together, so the card is self-auditing.
    pub total: f64,
}

/// Token counts are integers the way the accounting layer produces them; money is `f64`. This is the
/// only widening conversion in the money path, and it is exact well past any lifetime token total a
/// process-resident counter can reach (`f64` integers are exact to 2^53).
fn as_dollars(tokens: u64) -> f64 {
    // `as` is deliberate and lossless in this range; `unwrap_or(f64::MAX)` on a conversion that
    // cannot fail would only add a branch to a hot path.
    tokens as f64
}
