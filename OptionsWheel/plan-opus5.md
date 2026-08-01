# OptionsWheel Improvement Plan

Original review and 11-point improvement plan for the options-wheel screener
(`OptionsWheel/src/options_wheel/analysis.py`), plus implementation status.

## Context

The project screens for attractive cash-secured put / covered call
opportunities. This plan followed a review of `analysis.py` (~1,500 lines),
both YAML configs, and the last run outputs.

## Blocking / correctness issues

1. **Zero results is structural, not bad luck.** `put_results.json` showed
   `candidates_after_phase1: 0` because `BASE_URL`/`HIST_URL`/`OPTIONS_URL`
   were hard-coded to `http://localhost:7071` while the run output referenced
   the Azure URL. Fix: make the endpoint a config/env value (`OW_API_BASE` or
   similar) instead of commented-out blocks.
   - **Status: Implemented.** API endpoints are now configurable via env vars,
     defaulting to the previous hard-coded values.

2. **The put config is mathematically self-contradictory.**
   `MAX_ABS_DELTA: 0.12` + `MIN_OTM_PCT: 5` + `TARGET_MONTHLY_YIELD_PCT: 1.3`.
   A 0.12-delta 30-DTE put only pays ~1.3%/mo of strike when IV is roughly
   55-60%+. So the screen can only ever return very high-IV names - pure
   adverse selection. Either drop the yield target to ~0.6-0.8%/mo, or widen
   delta to 0.15-0.25.
   - **Status: Not changed.** Flagged for consideration; shipped config
     values were left as-is since this is a tuning decision, not a code fix.

3. **`EV` is structurally approximately 0 by construction.**
   `_calculate_pop_ev` computed premium minus the *risk-neutral* expected
   payoff. If the option is fairly priced, EV = 0 always; it only ever
   measures rounding noise. The real edge in selling options is the
   **variance risk premium** (IV minus realized vol). Compute the expected
   loss with `hv_current` instead of the option's IV, and with a real-world
   drift (0 or beta-adjusted), so EV becomes a genuine ranking signal. Same
   for `PoP`: N(d2) under IV is systematically pessimistic.
   - **Status: Implemented.** `metrics.py` provides real-world-measure
     `EV`/`PoP` using a forecast volatility blended from realised vol and a
     haircut implied vol, wired into `analysis.py`.

4. **The score rewards raw IV** (`iv_score = min(IV,2)/2 * 10`), i.e. it
   rewards the riskiest names. It should reward **IV/HV ratio** (>= ~1.2),
   not IV level. Also `EV`, `PoP` and delta are in the output but carry zero
   weight in `Score`.
   - **Status: Implemented.** Scoring now uses the variance risk premium
     (IV / forecast vol ratio) via `metrics.variance_risk_premium` instead of
     raw IV level.

## Highest-leverage realistic improvements

5. **Model costs.** Everything was priced at mid. Add commission
   (~$0.65/contract) and a fill assumption of mid minus 25-40% of the
   half-spread, then recompute yield. `MAX_SPREAD_PCT: 20` is far too loose -
   at 20% spread the round-trip eats most of a 1%/mo credit. Use <=10% and an
   absolute cap (spread <= $0.05-0.10), and require `bid > 0`.
   - **Status: Implemented.** `metrics.net_credit` models commission and
     slippage; yields are computed off net premium. `MAX_SPREAD_PCT` was
     tightened to 10% in both YAML configs and the code default, an absolute
     `MAX_SPREAD_ABS` cap ($0.25) was added, and contracts without a
     two-sided quote (`bid > 0` and `ask >= bid`) are now rejected outright
     instead of falling back to `lastPrice`.

6. **Turn on `EXCLUDE_EARNINGS_BEFORE_EXPIRY`.** With `MIN_IV_RANK: 0.3` and
   earnings allowed, you actively select contracts whose high IV *is* the
   earnings gap risk - you're paid for a jump you can't hedge.
   - **Status: Verified.** Confirmed the flag is wired correctly end-to-end;
     left as a config choice per option type.

7. **Build a real IV Rank.** `compute_iv_hv_percentile` is IV vs. HV range and
   is honestly labeled as such, but it's noisy. Since the scan runs on a
   schedule (GH Actions), persist daily ATM IV per ticker to a small time
   series and compute true 252-day IV rank/percentile. Cheap, and it's the
   single best premium-selling filter.
   - **Status: Implemented.** `iv_history.py` persists ATM IV history under
     `data/history`, producing true `IVRank`/`IVPercentile`, with fallback to
     the existing IV/HV percentile while history warms up. Series are keyed
     per `symbol|option_type` so the put and call scans of the same day do not
     overwrite each other (skew makes the two series different).

8. **Rank by risk-adjusted credit, not yield.** Two better primary metrics:
   - *Distance in sigmas*: `z = ln(K/S) / (HV * sqrt(T))` - comparable across
     tickers, unlike `OTMPct`.
   - *Credit per unit of tail risk*: `premium / CVaR_of_assignment`, or simply
     `premium / (expected loss under HV)`.
   Use simple annualization `(premium/collateral) * 365/DTE`, not compounding
   - the current `(1+p/K)^(365/DTE)` inflates weeklies, which are exactly the
     highest-gamma, highest-cost trades.
   - **Status: Implemented.** Added `sigma_distance` and `credit_risk_ratio`
     fields via `metrics.py`, and switched `MonthlyYieldPct`/
     `AnnualizedYieldPct` to simple (non-compounding) annualisation.

9. **Wheel-specific gate.** For puts, assignment isn't failure - being
   assigned on garbage is. Add a "would I own it?" filter: effective basis
   (`strike - premium`) vs. 200-DMA / valuation, and reject if basis is above
   current price. For calls, `MAX_ABS_DELTA: 0.5` will get you called away
   constantly; 0.20-0.30 is the usual covered-call band. Set
   `MIN_ABS_DELTA: 0.05` to drop lottery-ticket strikes where premium is all
   spread.
   - **Status: Not implemented.** Deferred as lower priority relative to
     items 1, 3, 4, 5, 7, 8, 10, 11.

10. **Portfolio layer.** Top-N today can be 5 correlated high-beta names. Add:
    max 1 contract per sector, total collateral budget, and per-name
    concentration cap.
    - **Status: Implemented.** `portfolio.py` provides `build_portfolio`
      (max positions, per-sector cap, collateral budget, per-position cap),
      wired into `main()` with new `PORTFOLIO_*` config keys in both YAML
      configs; results include a `portfolio` block in the output JSON, rendered
      as a "Suggested portfolio" panel in `web/index.html`.

11. **Close the feedback loop - this is the only way to *really* improve the
    ratio.** Archive every scan's candidates, then re-score them at expiry
    using the existing historical endpoint: finished ITM? realized P/L if
    held? That gives measured hit-rate and P/L per filter setting, so scoring
    weights become fitted rather than guessed. The weights (30/20/15/10/15/10/10)
    were arbitrary and unvalidated.
    - **Status: Implemented.** `outcomes.py` archives every scan and grades
      expired trades; `analysis.py` archives each scan automatically, and
      `python -m options_wheel.outcomes --type put` (or `--type call`) grades
      expired candidates.

## Engineering hygiene

- No tests existed. The pure functions (`_estimate_delta`, `_calculate_pop_ev`,
  yield math, `calculate_adx`/`rsi`) are ideal for pytest golden-value tests -
  a sign error here silently corrupts every ranking.
  - **Status: Implemented.** Added `OptionsWheel/tests/` with pytest coverage
    for `metrics.py`, `portfolio.py`, `iv_history.py` and the contract
    quote/spread gating in `analysis.py` (13 tests, all passing).
- `config/*.json` and `config/*.yaml` duplicated the same keys but only YAML
  was loaded (`load_screening_config`); delete the JSON or document it.
  - **Status: Implemented.** Removed the unused/duplicate `.json` config
    files after confirming nothing else in the repo reads them.
- Dead path in `_evaluate_contract`: the `lastPrice` fallback sets `premium`
  but `spread_pct` stays `None`, so the contract is always dropped a few
  lines later.
  - **Status: Fixed.** The fallback was removed entirely: a two-sided quote
    (`bid > 0`, `ask >= bid`) is now required, so the spread filters always
    apply and unquotable contracts are counted under
    `contracts_excluded_missing_quote`.
- README references `analyze_stocks.py`, `display_results.py`,
  `tickers.json`, `screening_config.json`, `index.html` - none of those paths
  exist anymore.
  - **Status: Not addressed** (out of scope for this pass; repo-root README,
    not part of the OptionsWheel code changes).
- `is_downtrend` computes
  `sum([below_ema, strong_trend and below_ema, weak_rsi and below_ema]) >= 2`,
  which just means "below EMA and (trending or weak RSI)". Write it that way.
  - **Status: Not addressed.**

## Implementation summary

Implemented in commit `a86ba3c` ("Integrate OptionsWheel risk metrics and
portfolio layer"): points 1, 3, 4, 5, 7, 8, 10, 11, plus verification of 6 and
the engineering-hygiene fixes (tests, dead-code fix, duplicate config
removal). Points 2, 9, and the README/`is_downtrend` hygiene items were left
for a future pass since they are tuning/config decisions or lower priority
relative to the rest of the plan.
