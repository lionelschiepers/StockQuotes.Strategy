import requests
from requests.adapters import HTTPAdapter
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import threading
import random


def convert_numpy_types(obj):
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


class NumpyEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        if isinstance(o, np.floating):
            return float(o) if not (math.isnan(o) or math.isinf(o)) else None
        if isinstance(o, np.integer):
            return int(o)
        return super().default(o)


# Configuration
BATCH_SIZE = 50
PRICE_LIMIT = 500
HIST_DAYS = 120
# BASE_URL = "http://localhost:7071/api/yahoo-finance"
# HIST_URL = "http://localhost:7071/api/yahoo-finance-historical"
BASE_URL = "https://stockquote.lionelschiepers.synology.me/api/yahoo-finance"
HIST_URL = "https://stockquote.lionelschiepers.synology.me/api/yahoo-finance-historical"
SLEEP_TIME = 0.0
MAX_WORKERS = 4
REQUEST_TIMEOUT = 15
MAX_RETRIES = 6

_thread_local = threading.local()
_rate_limit_lock = threading.Lock()
_next_request_ts = 0.0
_next_slot_ts = 0.0
_min_request_interval = 0.35


def get_tickers():
    with open("tickers.json", "r") as f:
        return json.load(f)


def get_session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=MAX_WORKERS * 2, pool_maxsize=MAX_WORKERS * 4
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session
    return session


def _wait_if_rate_limited():
    global _next_request_ts
    while True:
        with _rate_limit_lock:
            wait_for = _next_request_ts - time.time()
        if wait_for <= 0:
            return
        time.sleep(min(wait_for, 0.25))


def _acquire_request_slot():
    global _next_slot_ts
    with _rate_limit_lock:
        now = time.time()
        slot_ts = max(now, _next_slot_ts)
        _next_slot_ts = slot_ts + _min_request_interval
    wait_for = slot_ts - time.time()
    if wait_for > 0:
        time.sleep(wait_for)


def _on_success():
    global _min_request_interval
    with _rate_limit_lock:
        _min_request_interval = max(0.12, _min_request_interval * 0.98)


def _on_rate_limited(retry_after):
    global _min_request_interval
    with _rate_limit_lock:
        grown_interval = _min_request_interval * 1.4
        floor_from_retry = max(0.25, retry_after)
        _min_request_interval = min(5.0, max(grown_interval, floor_from_retry))


def _set_global_cooldown(seconds):
    global _next_request_ts, _next_slot_ts, _min_request_interval
    seconds = max(0.0, float(seconds))
    with _rate_limit_lock:
        # Spread requests after cooldown to avoid herd retries.
        _min_request_interval = max(_min_request_interval, min(5.0, seconds))
        _next_request_ts = max(_next_request_ts, time.time() + seconds)
        _next_slot_ts = max(_next_slot_ts, _next_request_ts)


def safe_get(url):
    for attempt in range(MAX_RETRIES):
        _wait_if_rate_limited()
        _acquire_request_slot()
        try:
            response = get_session().get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code == 429:
                retry_after_raw = response.headers.get("Retry-After", "1")
                try:
                    retry_after = float(retry_after_raw)
                except ValueError:
                    retry_after = 1.0
                retry_after += random.uniform(0.05, 0.25)
                _on_rate_limited(retry_after)
                print(f"Rate limited. Waiting {retry_after:.2f} seconds...")
                _set_global_cooldown(retry_after)
                continue

            if response.status_code >= 500:
                backoff = min(0.25 * (2**attempt), 5.0)
                time.sleep(backoff)
                continue

            response.raise_for_status()
            _on_success()
            return response.json()
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                print(f"Error fetching {url}: {e}")
                return None
            backoff = min(0.25 * (2**attempt), 5.0)
            time.sleep(backoff)

    print(f"Error fetching {url}: max retries exceeded")
    return None


def batch_price_filter(tickers):
    candidates = []
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        symbols = ",".join(batch)
        url = f"{BASE_URL}?symbols={symbols}&fields=symbol,shortName,regularMarketPrice,trailingPE"
        data = safe_get(url)
        if data:
            for item in data:
                price = item.get("regularMarketPrice")
                pe = item.get("trailingPE")
                if (
                    price is not None
                    and price < PRICE_LIMIT
                    and pe is not None
                    and pe <= 100
                ):
                    candidates.append(
                        {
                            "symbol": item["symbol"],
                            "price": price,
                            "name": item.get("shortName", ""),
                        }
                    )
        if SLEEP_TIME > 0:
            time.sleep(SLEEP_TIME)
    return candidates


def calculate_ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # Wilder's Smoothing
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_adx(df, period=14):
    df = df.copy()
    df["up_move"] = df["high"].diff()
    df["down_move"] = -df["low"].diff()

    df["plus_dm"] = np.where(
        (df["up_move"] > df["down_move"]) & (df["up_move"] > 0), df["up_move"], 0
    )
    df["minus_dm"] = np.where(
        (df["down_move"] > df["up_move"]) & (df["down_move"] > 0), df["down_move"], 0
    )

    df["tr"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift(1)),
            abs(df["low"] - df["close"].shift(1)),
        ),
    )

    atr = df["tr"].ewm(alpha=1 / period, adjust=False).mean()
    plus_di = 100 * (df["plus_dm"].ewm(alpha=1 / period, adjust=False).mean() / atr)
    minus_di = 100 * (df["minus_dm"].ewm(alpha=1 / period, adjust=False).mean() / atr)

    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.ewm(alpha=1 / period, adjust=False).mean()
    return adx


def calculate_macd(series):
    ema12 = calculate_ema(series, 12)
    ema26 = calculate_ema(series, 26)
    macd = ema12 - ema26
    signal = calculate_ema(macd, 9)
    return macd, signal


def calculate_rvi(series, std_period=10, smooth_period=14):
    std = series.rolling(window=std_period).std()
    diff = series.diff()

    up = np.where(diff > 0, std, 0)
    down = np.where(diff < 0, std, 0)

    up_series = pd.Series(up, index=series.index)
    down_series = pd.Series(down, index=series.index)

    # Wilder's Smoothing
    avg_up = up_series.ewm(alpha=1 / smooth_period, adjust=False).mean()
    avg_down = down_series.ewm(alpha=1 / smooth_period, adjust=False).mean()

    rvi = 100 * (avg_up / (avg_up + avg_down))
    return rvi


def format_eta(seconds):
    if seconds < 0:
        return "--:--"
    mins, secs = divmod(int(seconds), 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs:d}:{mins:02d}:{secs:02d}"
    return f"{mins:d}:{secs:02d}"


def analyze_single_candidate(c, start_str, end_str):
    symbol = c["symbol"]
    url = f"{HIST_URL}?ticker={symbol}&from={start_str}&to={end_str}"
    data = safe_get(url)
    if not data:
        return None

    quotes = data.get("quotes", [])
    if len(quotes) < 60:
        return None

    df = pd.DataFrame(quotes)
    if df.empty:
        return None

    df = df.dropna(subset=["close"])
    if len(df) < 60:
        return None

    if isinstance(df["date"].iloc[0], str):
        df["date"] = pd.to_datetime(df["date"])
    else:
        df["date"] = pd.to_datetime(df["date"], unit="s")
    df = df.sort_values("date")

    close = df["close"]
    price = close.iloc[-1]

    ema50 = calculate_ema(close, 50).iloc[-1]
    rsi_series = calculate_rsi(close, 14)
    rsi_today = rsi_series.iloc[-1]
    rsi_3d_ago = rsi_series.iloc[-4] if len(rsi_series) >= 4 else None

    adx = calculate_adx(df, 14).iloc[-1]
    macd, macd_signal = calculate_macd(close)
    rvi = calculate_rvi(close, 10, 14).iloc[-1]

    conds = {
        "Price < EMA50": price < ema50,
        "ADX < 30": adx < 30,
        "15 <= RSI <= 35": 15 <= rsi_today <= 35,
        "RSI Rising (3d)": rsi_3d_ago is not None and rsi_today > rsi_3d_ago,
    }

    failed = [name for name, val in conds.items() if not val]

    res_data = {
        "Symbol": symbol,
        "Name": c["name"],
        "Price": round(price, 2),
        "EMA50": round(ema50, 2),
        "ADX": round(adx, 2),
        "RSI": round(rsi_today, 2),
        "RVI": round(rvi, 2),
        "MACD": round(macd.iloc[-1], 2),
        "Signal": round(macd_signal.iloc[-1], 2),
        "DiffPct": round(((price - ema50) / ema50) * 100, 2),
        "Status": "PASS" if len(failed) == 0 else "NEAR",
        "Failed Criterion": failed[0] if len(failed) == 1 else "",
    }

    return res_data, failed


def deep_analysis(candidates):
    results = []
    near_misses = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=HIST_DAYS)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    total = len(candidates)
    print(f"Analyzing {total} candidates from {start_str} to {end_str}")
    analysis_start = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(analyze_single_candidate, c, start_str, end_str): idx
            for idx, c in enumerate(candidates, 1)
        }

        for future in as_completed(futures):
            idx = futures[future]
            elapsed = time.time() - analysis_start
            if idx > 1:
                avg_time = elapsed / (idx - 1)
                eta_seconds = avg_time * (total - idx + 1)
                eta_str = format_eta(eta_seconds)
            else:
                eta_str = "--:--"
            symbol = candidates[idx - 1]["symbol"]
            msg = f"[{idx}/{total}] Analyzing {symbol:<10} ETA: {eta_str}"
            print(f"{msg:<60}", end="\r", flush=True)

            try:
                result = future.result()
                if result:
                    res_data, failed = result
                    if len(failed) == 0:
                        results.append(res_data)
                    elif len(failed) == 1:
                        near_misses.append(res_data)
            except Exception as e:
                print(f"\nError analyzing {symbol}: {e}")

    return results, near_misses


def main():
    print("Fetching tickers...")
    tickers = get_tickers()

    print(f"Phase 1: Screening {len(tickers)} tickers for price < ${PRICE_LIMIT}...")
    candidates = batch_price_filter(tickers)
    print(f"Found {len(candidates)} candidates.")

    print("Phase 2 & 3: Deep analysis and filtering...")
    final_results, near_misses = deep_analysis(candidates)

    print("Phase 4: Sorting and Reporting...")
    combined_results = final_results + near_misses
    combined_results.sort(key=lambda x: x["DiffPct"])

    if combined_results:
        print("\nFull Summary Table (Sorted by DiffPct):")
        df = pd.DataFrame(combined_results)
        # Reorder columns for better display
        cols = [
            "Symbol",
            "Name",
            "Status",
            "Price",
            "EMA50",
            "DiffPct",
            "ADX",
            "RSI",
            "RVI",
            "MACD",
            "Failed Criterion",
        ]
        print(df[cols].to_string(index=False))
    else:
        print("\nNo stocks matched the criteria or were near misses.")

    # Save results to JSON
    output = {
        "timestamp": datetime.now().isoformat(),
        "total_tickers_analyzed": len(tickers),
        "candidates_after_phase1": len(candidates),
        "passed_all_criteria": len(final_results),
        "near_misses": len(near_misses),
        "results": combined_results,
    }
    output = convert_numpy_types(output)

    with open("analysis_results.json", "w") as f:
        json.dump(output, f, indent=2, cls=NumpyEncoder)
    print(f"\nResults saved to analysis_results.json")


if __name__ == "__main__":
    main()
