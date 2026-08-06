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
- Summary: `GET {base}/yahoo-finance-summary?ticker=TICKER&modules=financialData,defaultKeyStatistics,recommendationTrend`
  Returns the Yahoo `quoteSummary` modules: analyst price targets and rating
  (`financialData`), short interest / beta / valuation (`defaultKeyStatistics`),
  and rating breakdown over time (`recommendationTrend`). `modules` defaults to
  those three if omitted. Any module from Yahoo's quoteSummary is allowed
  (max 20).

The helper handles all of the calls and computes every metric. You normally only
need to run the helper, not the raw API.

### External sources used by `--news`

- **OptionsWheel API `yahoo-finance-summary`** — primary source for analyst
  consensus (price targets, rating breakdown, short interest, valuation).
- **Google News RSS** — `https://news.google.com/rss/search?q=TICKER+stock&hl=en-US&gl=US&ceid=US:en`
  returns recent headlines.
- **SEC EDGAR** — ticker-to-CIK map (`https://www.sec.gov/files/company_tickers.json`,
  cached 24h in the OS temp dir) then `https://data.sec.gov/submissions/CIK<10-digit>.json`
  for recent 8-K/10-Q/10-K/S-4/13D/13G/144 filings. SEC requires a proper
  User-Agent; set `OW_SEC_USER_AGENT` (default `StockQuotesStrategy research@stockquotes.example.com`)
  if you hit a 403.
- **stockanalysis.com** — `https://stockanalysis.com/stocks/TICKER/forecast/`
  fallback for analyst price-target/rating consensus if the API summary
  endpoint is unavailable.

### Macro-event calendar (`events.macro_events`)

The full analysis (Step 5) also lists scheduled **macroeconomic events that fall
inside the contract window** (today → expiration), each with
`days_before_expiry`. Sources (both cached 24h in the OS temp dir):

- **Federal Reserve (FOMC / rate decision)** —
  `https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm`
  (official calendar; covers the current and next calendar year). Uses the
  last day of each meeting, i.e. the statement/rate-decision day.
- **BLS** — `https://www.bls.gov/schedule/{year}/home.htm`
  for CPI, Employment Situation (Nonfarm Payrolls) and Producer Price Index
  release dates (full current year).
- **tradingeconomics.com** —
  `https://tradingeconomics.com/calendar` is the fallback for CPI / Nonfarm
  Payrolls / PPI when BLS is unreachable (BLS applies burst rate-limiting and
  can return HTTP 403; it works on the first daily request). The TE calendar
  only covers the next ~2 weeks.

If both fail, the `errors` map says so — fall back to a manual web search
("<TICKER> economic calendar", "FOMC meeting dates 2026") and note it in the
verdict.

## Workflow

Run in the current working directory (the skill folder is
`.agents/skills/option-candidate-check`):

```bash
python .agents/skills/option-candidate-check/check_option.py --ticker AAPL --type put --list
```

### Step 0 — Research mode (news, SEC filings, analyst consensus)
The helper has a dedicated research mode that pulls analyst price-target
consensus from the quote, recent headlines from Google News RSS, and material
SEC filings (8-K, 10-Q/10-K, S-4, 13D/13G, 144) from EDGAR:

```bash
python .agents/skills/option-candidate-check/check_option.py --ticker AAPL --news
```

Run it alongside the full analysis (Step 5) and fold the results into the
verdict.

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
Run the `--news` mode above (Step 0) and read its four sections:

- `quote_metrics` — current price, beta, 52-week high/low,
  `distance_from_52w_high_pct` (negative = below the high), shares outstanding,
  trailing P/E, market cap, avg volume. A stock far below its 52w high or with
  a high beta is more volatile; a very high P/E can flag sentiment-heavy pricing.
- `analyst_consensus` — `price_target.avg/median/low/high/num_analysts`,
  `rating.consensus/score/count` plus the strong-buy/buy/hold/sell/strong-sell
  split, `short_interest` (shares short, `short_ratio`, `short_percent_of_float`)
  and `valuation` (revenue/gross/profit margins, forward P/E, PEG, P/B,
  institutional/insider ownership, beta). Compare `price_target.avg` vs current
  price for implied upside; a `sell`-heavy split, `short_percent_of_float` > 10%,
  or a high `short_ratio` are caution flags for puts and calls alike.
- `sec_filings` — recent material filings from EDGAR. An **8-K** in the last 180
  days can be a merger/acquisition, lawsuit, or guidance cut; **S-4** = merger
  proxy (deal pending); **13D/13G** = activist/institutional stake (potential
  bid or pressure); **144** = insider selling ahead of expiry. Any of these is
  event risk beyond the calendar.
- `news` — latest headlines with date/source. Scan for catalysts (product
  launches, earnings pre-announcements, splits, regulatory action, macro).

If a section fails (network, unknown ticker, no SEC match, blocked page), fall
back to a manual web search ("<TICKER> stock news", "<TICKER> earnings date",
"<TICKER> lawsuit") and note it in the verdict.

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
  (>= 0.5) means premiums are rich. If `iv_rank_fallback` is **true**, the
  `iv_rank` shown is a realised-HV-based proxy (the true IV-rank store is still
  building history), so treat the value as approximate.
- `backtest` — realized cross-check over the trailing year: `expiry_itm_rate_pct`
  (fraction of same-tenor windows where the option was ITM at expiry),
  `breach_rate_pct` (fraction where the strike was hit intraday at any point),
  and `breakeven_win_rate_pct` (fraction profitable at expiry including
  premium — the closest analogue to `probability_of_profit_pct`). Compare it
  with the model PoP: a large gap means the trailing year's actual price path
  disagreed with the model — investigate whether that is regime (stock far from
  where it traded) or a genuinely poor setup. Treat as a sanity cross-check,
  not gospel: it ignores today's vol/premium and reflects one specific past path.
- `events.earnings_before_expiry` — if **true**, warn hard: the earnings jump
  is the reason IV is high, and selling into it is paid-for-unhedgeable-gap
  risk. Quantify it with `events.earnings_gap`: `avg_abs_move_pct` /
  `max_abs_move_pct` is how much the stock actually moved around recent
  earnings, `avg_signed_move_pct` the direction bias, `avg_surprise_pct` the
  beat/miss magnitude. Strong reason to reject unless the user wants earnings risk.
- `events.ex_div_risk` — for calls, `HIGH`/`MEDIUM` means an ex-dividend date
  falls inside the contract life (early-assignment risk on ITM calls); for
  puts, `DIVIDEND_DROP` just means the stock will gap down by the dividend.
- `events.macro_events` — scheduled **macroeconomic events before expiry**:
  `FOMC / Fed rate decision`, `CPI`, `Nonfarm Payrolls / Employment Situation`,
  `PPI`, each with `date` and `days_before_expiry`. A **Fed rate decision
  inside the window** is the macro analogue of an earnings report: implied vol
  is elevated around it and the outcome (25/50bp move, hawkish/dovish dot
  plot) can gap the whole market — treat it like event risk when pricing a
  short. CPI / jobs-report prints matter most for rate-sensitive, high-beta
  names; check whether the release is before expiry (it is, by construction)
  and how close. If the `errors` map is non-empty the calendar was partially
  unavailable — note the fallback/limitation in the verdict.
- `consensus` — `beta`, 52-week high/low, `runway_from_52w_high_pct` (upside
  left to the 52w high) and shares outstanding. For analyst price targets and
  rating splits, use the `--news` mode (`analyst_consensus`).
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
