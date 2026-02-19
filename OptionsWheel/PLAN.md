# Stock Analysis Plan: Momentum & Trend Filter

This plan outlines the process for filtering a list of stock tickers based on price and technical indicators using Python scripts and custom APIs.

## 1. Prerequisites & Environment
- **Environment:** Python 3.x.
- **Python Dependencies:** `requests`, `pandas`, and `numpy`.
- **API Access:** Use the provided bulk and historical quote APIs.
- **Rate Limit Management:** Implement a small sleep (e.g., 0.1s) between API calls to avoid rate limiting.

## 2. Execution Phases

### Phase 1: Batch Screening (Price Filter)
- **Goal:** Quickly eliminate stocks > $100.
- **Method:** Use a Python script to fetch data from `https://stockquote.lionelschiepers.synology.me/api/yahoo-finance?symbols={symbols}&fields=symbol,shortName,regularMarketPrice` in batches of 50 tickers.
- **Logic:** Only pass tickers where `regularMarketPrice < 100` to Phase 2.
- **Output:** A list of "Candidate Tickers".

### Phase 2: Sequential Deep Analysis
- **Goal:** Calculate technical indicators for candidate stocks.
- **Method:** For each ticker in the Candidate List:
    1. Display progression status (e.g., "Analyzing 10/150: AAPL...").
    2. Fetch historical data for the last 90 days using the custom API: `https://stockquote.lionelschiepers.synology.me/api/yahoo-finance-historical?ticker={symbol}&from={start_date}&to={end_date}`.
    3. This URL is preferred as it is easily consumed by Python scripts and returns data in a structured format (use the `quotes` field from the response).
    4. Calculate **EMA50**, **ADX(14)**, **RSI(14)**, **MACD(12,26,9)**, and **RVI(10,14)**.
    5. Retrieve `RSI_today` and `RSI_3_days_ago`.
- **Validation:** Ensure the ticker has at least 60 days of historical data to allow for stable indicator calculation.

### Phase 3: Criteria Filtering
Apply the following strict filters to each candidate:
1. **Price Check:** `Price < 100`.
2. **EMA Trend:** `Price < EMA50`.
3. **ADX Strength:** `ADX < 30` (Indicating a non-trending or early-trend state).
4. **RSI Range:** `15 <= RSI <= 35`.
5. **RSI Momentum:** `RSI_today > RSI_3_days_ago`.

### Phase 4: Aggregation & Sorting
- **Calculation:** `DiffPct = ((Price - EMA50) / EMA50) * 100`.
- **Sorting:** Order result list **Ascending** by `DiffPct`.
- **Reporting:** 
    - Generate a **Full Summary Table** combining both candidates meeting all criteria and "Near Misses" (failing exactly one criterion).
    - Include a `Status` column (PASS/NEAR) and a `Failed Criterion` column (where applicable).
    - Sorting: Order the combined list **Ascending** by `DiffPct`.
    - Ensure columns like `RVI` and `MACD` are included in the final report.

### Phase 5: Web Reporting
- **Goal:** Provide a user-friendly, interactive interface to view analysis results.
- **Method:** Create an `index.html` page that loads `analysis_results.json`.
- **Features:**
    - Summary dashboard showing totals (Analyzed, Candidates, Passed, Near Misses).
    - Sortable data table containing all technical indicators.
    - Visual indicators for Status (PASS/NEAR) and price vs. EMA trends.
    - Search and pagination for easy navigation.
    - **Put Options Column:** For each ticker, fetches put options from the API and checks if a valid put exists with:
        - Strike >= 8% below regular price (strike >= price * 0.92)
        - Last price >= 0.5% of regular price (lastPrice >= price * 0.005)
    - Displays "Yes" (green) or "No" (red) badge based on availability of qualifying puts.
    - API: `https://stockquote.lionelschiepers.synology.me/api/yahoo-finance-stock-options?ticker={TICKER}&filter=puts&limit=8`

## 3. Detailed Technical Specifications

| Indicator | Period | Logic / Requirement |
| :--- | :--- | :--- |
| **EMA** | 50 | Exponential Moving Average of closing prices. |
| **ADX** | 14 | Average Directional Index; filter out strong trends (>30). |
| **RSI** | 14 | Relative Strength Index; ensure price isn't overbought/oversold. |
| **RSI Trend**| 3d | Compare current bar RSI vs the bar from 3 trading sessions prior. |
| **MACD** | 12,26,9| Moving Average Convergence Divergence with Signal line. |
| **RVI** | 10,14 | Relative Volatility Index (Dorsey); 10-period StdDev, 14-period smoothing. |

### RSI Calculation (Wilder's Smoothing)
Use Wilder's smoothing method (not simple SMA)

## 4. Error Handling & Edge Cases
- **Missing Data:** If a ticker returns empty historical data, skip it and log a warning.
- **Insufficient History:** If a ticker has < 50 days of data, it cannot accurately calculate EMA50; exclude it.
- **API Timeout:** If a sequential call fails, retry once before skipping the ticker.

## 5. Ticker List
- @tickers.json contains an array of tickers.