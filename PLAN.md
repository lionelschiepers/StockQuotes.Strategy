# Stock Analysis Plan: Momentum & Trend Filter

This plan outlines the process for filtering a list of stock tickers based on price and technical indicators using the `stock-quotes` MCP.

## 1. Prerequisites & Environment
- **MCP Toolset:** Ensure `stock-quotes` is active and accessible.
- **Python Dependencies:** `pandas` and `numpy` for technical indicator calculations.
- **Rate Limit Management:** Only sequential calls are allowed. Implement a small sleep (e.g., 0.1s) between historical data calls if the API starts rejecting requests.

## 2. Execution Phases

### Phase 1: Batch Screening (Price Filter)
- **Goal:** Quickly eliminate stocks > $100.
- **Method:** Use `get_stock_quotes` in batches of 50 tickers. required fields to load are: symbol, regularMarketPrice, shortName.
- **Logic:** Only pass tickers where `regularMarketPrice < 100` to Phase 2. Keep the top 10 in term of volume.
- **Output:** A list of "Candidate Tickers".

### Phase 2: Sequential Deep Analysis
- **Goal:** Calculate technical indicators for candidate stocks.
- **Method:** For each ticker in the Candidate List:
    1. Fetch historical data for the last 90 days using the custom API: `https://stockquote.lionelschiepers.synology.me/api/yahoo-finance-historical?ticker={symbol}&from={start_date}&to={end_date}`.
    2. This URL is preferred as it is easily consumed by Python scripts and returns data in a structured format (use the `quotes` field from the response).
    3. Calculate **EMA50**, **ADX(14)**, **RSI(14)**, and **MACD**.
    4. Retrieve `RSI_today` and `RSI_3_days_ago`.
- **Validation:** Ensure the ticker has at least 60 days of historical data to allow for stable indicator calculation.

### Phase 3: Criteria Filtering
Apply the following strict filters to each candidate:
1. **Price Check:** `Price < 100`.
2. **EMA Trend:** `Price > EMA50`.
3. **ADX Strength:** `ADX < 30` (Indicating a non-trending or early-trend state).
4. **RSI Range:** `30 <= RSI <= 50`.
5. **RSI Momentum:** `RSI_today > RSI_3_days_ago`.

### Phase 4: Aggregation & Sorting
- **Calculation:** `DiffPct = ((Price - EMA50) / EMA50) * 100`.
- **Sorting:** Order result list **Ascending** by `DiffPct`.
- **Reporting:** Generate the final summary table.

## 3. Detailed Technical Specifications

| Indicator | Period | Logic / Requirement |
| :--- | :--- | :--- |
| **EMA** | 50 | Exponential Moving Average of closing prices. |
| **ADX** | 14 | Average Directional Index; filter out strong trends (>30). |
| **RSI** | 14 | Relative Strength Index; ensure price isn't overbought/oversold. |
| **RSI Trend**| 3d | Compare current bar RSI vs the bar from 3 trading sessions prior. |
| **MACD** | 12,26,9| Moving Average Convergence Divergence with Signal line. |

### RSI Calculation (Wilder's Smoothing)
Use Wilder's smoothing method (not simple SMA)

## 4. Error Handling & Edge Cases
- **Missing Data:** If a ticker returns empty historical data, skip it and log a warning.
- **Insufficient History:** If a ticker has < 50 days of data, it cannot accurately calculate EMA50; exclude it.
- **API Timeout:** If a sequential call fails, retry once before skipping the ticker.

## 5. Ticker List
- @tickers.json contains an array of tickers.