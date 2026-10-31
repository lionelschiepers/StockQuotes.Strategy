# StockQuotes.Strategy - OptionsWheel

**OptionsWheel** screens the market for options to **sell**: cash-secured puts
and covered calls (the "wheel"). It pulls quotes and option chains, filters
out illiquid or badly priced contracts, estimates each contract's real-world
probability of profit and expected value, and ranks the survivors in a
terminal table and a web dashboard.

Every scan is archived and graded once the contracts expire, so you can check
whether the screener's picks actually made money.

## Quick start

```bash
cd OptionsWheel
pip install -r requirements.txt
python analyze_puts.py          # cash-secured puts  (add --top 50 for a quick run)
python analyze_calls.py         # covered calls
python -m http.server 8000      # then open http://localhost:8000/web/
```

On Windows `OptionsWheel\analyze.cmd` does all of the above.

To print saved results in the terminal: `python -m options_wheel.display --type put`.

## How a contract is evaluated

1. **Stock filter**: price, average volume, market cap
   (`data/input/tickers_put.json` / `tickers_call.json` list the universe).
2. **Contract filters**: DTE window, |delta| band, OTM distance, minimum
   premium, open interest and volume, and a two-sided quote with a bounded
   spread. Premiums are taken **net** of commission and slippage.
3. **Event risk**: puts whose life spans an earnings report are excluded
   (graded scans show they lost money on average). For calls the stock's own
   typical earnings jump (last four reports) is added to the volatility
   forecast instead.
4. **Risk model**: a forecast volatility blends realised volatility with a
   haircut implied volatility. PoP and EV are then computed under that
   forecast and a constant real-world drift (`REAL_WORLD_DRIFT`). Also
   reported: the strike distance in forecast standard deviations (`SigmaDist`),
   the variance risk premium (ATM IV / realised vol), and IV Rank built from
   the scans' own ATM IV history.
5. **Score (0-100)**: a weighted blend of safety (sigma distance), volatility
   (penalises extreme IV), yield (capped at the target), DTE fit, VRP,
   liquidity and spread. The weights live in the config.
6. **Portfolio**: a suggested basket with per-sector, per-position and total
   collateral caps.

## Configuration

`OptionsWheel/config/screening_config_puts.yaml` and
`screening_config_calls.yaml` hold every threshold, cost assumption, model
parameter and score weight, each with a comment. Any key can be overridden
with an environment variable prefixed `OW_`, e.g. `OW_TARGET_MONTHLY_YIELD_PCT=1.0`.
Unknown keys in the YAML produce a warning.

The API base URL defaults to the hosted Azure API; set `OW_API_BASE`
(e.g. `http://localhost:7071/api`) to use another one.

## Feedback loop: grading past scans

```bash
python -m options_wheel.outcomes --type put     # or --type call
python -m options_wheel.outcomes --type put --regrade --profit-target 0.5 --stop-loss 2
```

This grades every archived candidate whose expiry has passed and writes
`data/output/outcomes_<type>.json`:

- **held**: P/L if held to expiry (split-adjusted).
- **managed**: P/L with a profit target and a stop, repriced daily at the scan's IV.
- Win rate vs model PoP and realised vs model EV, bucketed by status, score,
  delta, IV rank, sigma distance and earnings.

Contracts re-listed by several daily scans are counted once (earliest scan).

## Automation and the `ow-data` branch

- `.github/workflows/run-analysis.yml` runs each weekday mid-session. It runs
  the tests, scans calls and puts, grades expired scans, and deploys the
  dashboard to GitHub Pages.
- Scan archives, the IV history and the latest outputs are committed to the
  **`ow-data`** branch, not `main`. The branch is created on the first run.
- `deploy-pages.yml` republishes the dashboard when `web/` or `config/`
  change, using the latest outputs from `ow-data`.
- `tests.yml` runs the test suite on pushes and pull requests.

To bring the CI history into a local checkout (merges IV history by date and
copies scans without overwriting local files):

```bash
git fetch origin ow-data
git worktree add ../ow-data ow-data        # once; later: git -C ../ow-data pull
cd OptionsWheel
python -m options_wheel.datasync pull ../../ow-data
```

## Checking a single contract

The `option-candidate-check` agent skill
(`.agents/skills/option-candidate-check`) evaluates one contract with the same
config and risk model as the screener and adds news, SEC filings, analyst
consensus and the macro calendar:

```bash
python .agents/skills/option-candidate-check/check_option.py --ticker AAPL --type put --list
python .agents/skills/option-candidate-check/check_option.py --ticker AAPL --type put --expiration 2026-12-18 --strike 200
```

## Project layout

```
OptionsWheel/
  analyze_puts.py, analyze_calls.py   entry points
  config/                             screening configs (YAML)
  data/input/                         ticker universes
  data/history/                       IV history (iv/YYYY-MM-DD.json) and scan archive; CI copy on ow-data, not tracked on main
  data/output/                        latest results and graded outcomes (generated)
  src/options_wheel/
    analysis.py     screener pipeline
    metrics.py      pricing, risk and score math (pure)
    earnings.py     historical earnings moves (pure)
    iv_history.py   IV Rank store
    portfolio.py    suggested portfolio
    outcomes.py     scan archive and grading
    datasync.py     sync with the ow-data branch
  tests/                              pytest suite (python -m pytest -q)
  web/                                dashboard
```

## Important

The model is calibrated on a short history (a few months of scans in a single
market regime); treat its PoP and EV as estimates, not promises. This tool
helps you find and evaluate setups faster; it does not guarantee profits.
Options trading carries risk, including assignment and large gaps. Size
positions conservatively and make your own final decision.
