# StockQuotes.Strategy - OptionsWheel

Want to **make money with your money** without spending hours manually checking stocks and options?

**OptionsWheel** is built for beginners and busy people who want a simple, practical way to find cash-secured put opportunities.

It scans the market for you, filters weak setups, and shows the best candidates in a clean ranked list.

## Why Beginners Like It

- Simple workflow: run one command, get ranked ideas.
- Clear filters: yield, strike distance, option liquidity, and spread quality.
- Beginner-friendly output: terminal table plus web dashboard.
- Safer structure: focuses on cash-secured puts (wheel-style income approach).
- Saves time: no need to manually open dozens of option chains.

## What It Does

- Screens a list of stocks from `OptionsWheel/tickers.json`.
- Filters stocks by practical checks (price, liquidity, market cap, and more).
- Analyzes put options with rules like:
  - target monthly yield
  - days to expiration (DTE)
  - out-of-the-money distance (OTM)
  - open interest and volume
  - bid/ask spread quality
  - optional delta and IV-rank filters
- Adds technical context (EMA50, RSI, ADX, RVI, MACD).
- Exports all results to JSON and displays them in a dashboard.

## 2-Minute Quick Start

1. Install dependencies:

```bash
pip install -r OptionsWheel/requirements.txt
```

2. Run the analysis:

```bash
python OptionsWheel/analyze_stocks.py
```

3. View results in terminal:

```bash
python OptionsWheel/display_results.py
```

4. Open `OptionsWheel/index.html` in your browser for the visual dashboard.

## Main Files

- `OptionsWheel/analyze_stocks.py` - runs the full scan and ranking.
- `OptionsWheel/display_results.py` - prints easy-to-read results.
- `OptionsWheel/screening_config.json` - your strategy settings.
- `OptionsWheel/analysis_results.json` - generated output.
- `OptionsWheel/index.html` - interactive results page.

## Customize Your Strategy

- Edit defaults in `OptionsWheel/screening_config.json`.
- Override settings with environment variables starting with `OW_`.
- Example knobs you can tune:
  - `OW_TARGET_MONTHLY_YIELD_PCT`
  - `OW_MIN_DTE`
  - `OW_MAX_SPREAD_PCT`
  - `OW_MIN_IV_RANK`

## Output You Get

`OptionsWheel/analysis_results.json` includes:

- run timestamp and summary numbers
- PASS and NEAR candidate contracts
- premium, monthly yield, DTE, OTM, spread, delta, IV, IV rank, and score

## Important

This tool helps you find better setups faster, but it does not guarantee profits.

Options trading has risk. Start small, use position sizing, and always do your own final decision before entering any trade.
