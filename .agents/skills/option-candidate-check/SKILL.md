---
name: option-candidate-check
description: Use when the user wants to check whether a specific put or call option is a good or bad candidate to sell for a given stock (cash-secured put / covered call / options wheel). Walks the user through ticker, option type, expiration and strike, then queries the OptionsWheel API, computes delta, PoP, EV, yield and event risk, and explains the verdict. Trigger keywords: "check this option", "is this put/call a good candidate", "should I sell this option", "options wheel", "cash-secured put", "covered call".
---

# Option Candidate Check (OptionsWheel)

Evaluate a single option the user is considering **selling** and explain, with
numbers, why it is a good or bad candidate. This skill drives a short
question-and-answer flow, fetches live data from the OptionsWheel API, computes
the same risk metrics the screener uses, then renders a clear verdict.

## Files

- `check_option.py` — helper script. Run it with `python` (see below). It
  locates the `options_wheel` module automatically and needs `requests`.
- Reference for the underlying logic: `OptionsWheel/src/options_wheel/analysis.py`
  and `OptionsWheel/src/options_wheel/metrics.py`.
- Screen thresholds live in `OptionsWheel/config/screening_config_puts.yaml`
  (puts) and `OptionsWheel/config/screening_config_calls.yaml` (calls).

## API

Base URL comes from the `OW_API_BASE` env var, defaulting to
`http://localhost:7071/api`. If the helper fails to connect, ask the user to
start the API (or set `OW_API_BASE`).

- Quote: `GET {base}/yahoo-finance?symbols=TICKER&fields=symbol,shortName,regularMarketPrice,trailingPE,averageDailyVolume3Month,marketCap,trailingAnnualDividendYield,fiftyDayAverage,earningsTimestamp,earningsTimestampStart,earningsTimestampEnd,dividendDate,trailingAnnualDividendRate`
  Returns a JSON array with one object per symbol.
- Options chain: `GET {base}/yahoo-finance-stock-options?ticker=TICKER&filter=puts&limit=50` (or `filter=calls`)
  - `filter` must be the **plural** `puts`/`calls`.
  - Returns `expirationDates` (all expiries), `strikes`, `quote` (spot), and
    `options[0]` (the nearest expiry chain).
  - To fetch a specific expiry add `&expirationDate=YYYY-MM-DD` (ISO date only;
    any other format returns HTTP 400).
- Historical: `GET {base}/yahoo-finance-historical?ticker=TICKER&from=YYYY-MM-DD&to=YYYY-MM-DD&interval=1d`
  Returns `{"meta":..., "quotes":[{date,open,high,low,close,volume}]}`.

The helper handles all three calls and computes every metric. You normally only
need to run the helper, not the raw API.

## Workflow

Run in the current working directory (the skill folder is
`.agents/skills/option-candidate-check`):

```bash
python .agents/skills/option-candidate-check/check_option.py --ticker AAPL --type put --list
```

### Step 1 — Ticker
Ask the user for the ticker symbol (e.g. "AAPL"). Free-text input.

### Step 2 — Put or call
Ask whether they are considering a **put** (cash-secured put) or a **call**
(covered call). Two clear options; the helper uses singular `put`/`call` for
`--type`.

### Step 3 — Expiration (next 10)
Run the `--list` command above. It prints the **next 10 expirations** (date +
DTE) and suggested strikes around the current price.

Present the 10 expirations to the user as choices (date and DTE in each label)
and let them pick one. Allow free-text input in case they want a date further
out.

### Step 4 — Strike
Present the `suggested_strikes.strikes` from the `--list` output (a window
around the ATM strike) as choices, but make clear they can type any strike.
For a **put** the interesting strikes are **below** spot (OTM); for a **call**
they are **above** spot.

### Step 5 — Fetch the full analysis
```bash
python .agents/skills/option-candidate-check/check_option.py --ticker AAPL --type put --expiration 2026-09-11 --strike 295
```
If the strike is not available for that expiry the helper prints the available
strikes — go back to Step 4 and pick one of those.

### Step 6 — Research events
Beyond the earnings / ex-dividend data already in the report, do a web search
for the ticker ("<TICKER> earnings date", "<TICKER> stock news", "<TICKER>
upcoming events") and note any recent news, product launches, splits,
lawsuits, or macro catalysts that could move the stock before expiry.

### Step 7 — Interpret and give a verdict
Use the screen thresholds from the config YAMLs as reference:

| Check | Put | Call |
|---|---|---|
| |Delta| band | 0.00–0.12 | 0.00–0.50 |
| OTM | >= 5% | >= 5% |
| DTE window | 4–60 | 7–60 |
| Open interest | >= 50 | >= 100 |
| Daily volume | >= 5 | >= 5 |
| Spread | <= 10% and <= $0.25 | <= 10% and <= $0.25 |
| Monthly yield target | >= 1.3% | >= 1.3% |
| IV rank | >= 0.30 | >= 0.30 |

Read each metric in the report:

- `delta_bs` — probability-like sensitivity. Lower |delta| = further OTM =
  safer but smaller premium. Too close to ATM (|delta| > 0.35–0.4) = high
  assignment risk.
- `probability_of_profit_pct` — chance the short expires profitable (real-world
  measure). Higher is better; < 60% is weak for a selling setup.
- `ev_per_share` / `expected_loss_per_share` — net premium minus expected
  assignment loss. Positive EV (with `credit_risk_ratio` > 1) is the core edge.
- `vrp_ratio` — IV vs forecast (realised-based) vol. **Above 1** means the
  option is priced above where the stock actually moves: the seller's edge.
  Below 1 you are underpaid for the risk.
- `sigma_distance` — how many standard deviations the strike is from spot.
  Higher = safer. 0.3–0.5 is moderate; > 0.5 is far.
- `monthly_yield_pct` / `annualized_yield_pct` — return on collateral.
- `spread_pct` / `spread_abs` — wide spreads erode the credit; > 10% is poor.
- `iv_hv_percentile` / `iv_rank` — current IV relative to its own range. High
  (>= 0.5) means premiums are rich.
- `events.earnings_before_expiry` — if **true**, warn hard: the earnings jump
  is the reason IV is high, and selling into it is paid-for-unhedgeable-gap
  risk. Strong reason to reject unless the user wants earnings risk.
- `events.ex_div_risk` — for calls, `HIGH`/`MEDIUM` means an ex-dividend date
  falls inside the contract life (early-assignment risk on ITM calls); for
  puts, `DIVIDEND_DROP` just means the stock will gap down by the dividend.
- `technicals` — RSI/ADX/MACD/EMA50 context: for puts a strong downtrend
  (price well below EMA50, RSI < 35) is a red flag; for calls an explosive
  uptrend (price well above EMA50, RSI > 70) means you get assigned/stocks get
  called away.

Deliver a short structured verdict:

- **GOOD CANDIDATE** — passes most thresholds, positive EV, VRP > 1, decent
  PoP, no earnings before expiry, acceptable liquidity.
- **MIXED** — passes some checks but has notable issues (e.g. good yield but
  wide spread, or VRP slightly under 1). List what is good and what is not.
- **BAD CANDIDATE** — fails multiple checks, negative EV, VRP < 1, earnings
  before expiry, terrible liquidity, or a strong opposing trend.

Always quote the actual numbers (delta, PoP, EV, VRP, yield, DTE) and finish
with a one-line bottom line: "Overall: <verdict>". Never give guarantees — note
this is an evaluation, not financial advice.
