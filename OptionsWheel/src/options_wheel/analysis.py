import requests
from requests.adapters import HTTPAdapter
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import threading
import random
import argparse
import traceback

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(MODULE_DIR, "..", ".."))
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
DATA_INPUT_DIR = os.path.join(PROJECT_ROOT, "data", "input")
DATA_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "output")


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
HIST_DAYS = 120
#BASE_URL = "http://localhost:7071/api/yahoo-finance"
#HIST_URL = "http://localhost:7071/api/yahoo-finance-historical"
#OPTIONS_URL = "http://localhost:7071/api/yahoo-finance-stock-options"
BASE_URL = "https://stockquote.lionelschiepers.synology.me/api/yahoo-finance"
HIST_URL = "https://stockquote.lionelschiepers.synology.me/api/yahoo-finance-historical"
OPTIONS_URL = (
   "https://stockquote.lionelschiepers.synology.me/api/yahoo-finance-stock-options"
)
SLEEP_TIME = 0.0
MAX_WORKERS = 4
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
OPTIONS_REQUEST_TIMEOUT = 25
OPTIONS_MAX_RETRIES = 3
DEBUG = False

# Caching
CACHE_DIR = os.environ.get("OPTIONS_CACHE_DIR", os.path.join(PROJECT_ROOT, ".cache"))
CACHE_TTL_SECONDS = float(os.environ.get("OPTIONS_CACHE_TTL", 3600))  # 1 hours default


def _get_cache_path(url):
    import hashlib

    safe = hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]
    return os.path.join(CACHE_DIR, f"{safe}.json")


def _read_cache(url):
    if CACHE_TTL_SECONDS <= 0:
        return None
    path = _get_cache_path(url)
    try:
        if not os.path.exists(path):
            return None
        mtime = os.path.getmtime(path)
        if time.time() - mtime > CACHE_TTL_SECONDS:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_cache(url, data):
    if CACHE_TTL_SECONDS <= 0:
        return
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        path = _get_cache_path(url)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass

DEFAULT_SCREENING_CONFIG = {
    "MAX_PRICE": 120.0,
    "MIN_STOCK_AVG_VOLUME": 500000,
    "MIN_MARKET_CAP": 500000000,
    "EXCLUDE_EARNINGS_BEFORE_EXPIRY": True,
    "TARGET_MONTHLY_YIELD_PCT": 1.0,
    "MIN_PREMIUM": 0.15,
    "MIN_DTE": 20,
    "MAX_DTE": 60,
    "MIN_OTM_PCT": 5.0,
    "MIN_OPEN_INTEREST": 100,
    "MIN_VOLUME": 10,
    "MAX_SPREAD_PCT": 20.0,
    "MIN_ABS_DELTA": 0.0,
    "MAX_ABS_DELTA": 0.11,
    "MAX_EXPIRATIONS_PER_SYMBOL": 3,
    "MAX_CONTRACTS_PER_SYMBOL": 3,
    "OPTIONS_REQUEST_TIMEOUT": 25,
    "OPTIONS_MAX_RETRIES": 8,
    "RISK_FREE_RATE": 0.0,
    "DIVIDEND_YIELD": 0.0,
    "MIN_IV_RANK": 0.0,
    "FILTER_DOWNTRENDS": True,
}


def _coerce_config_value(current_value, raw_value):
    if isinstance(current_value, bool):
        return str(raw_value).strip().lower() in ("1", "true", "yes", "on")
    if isinstance(current_value, int) and not isinstance(current_value, bool):
        return int(float(raw_value))
    if isinstance(current_value, float):
        return float(raw_value)
    return raw_value


def debug_log(message):
    if DEBUG:
        print(f"[DEBUG] {message}")


def _shorten_response_text(text, max_len=240):
    if not text:
        return ""
    compact = " ".join(str(text).split())
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 3] + "..."


def load_screening_config(defaults, option_type="put"):
    cfg = dict(defaults)
    default_calls_config = os.path.join(CONFIG_DIR, "screening_config_calls.json")
    default_puts_config = os.path.join(CONFIG_DIR, "screening_config_puts.json")
    if option_type == "call":
        config_path = os.environ.get("OPTIONS_SCREEN_CONFIG_CALLS", default_calls_config)
        if not os.path.exists(config_path):
            config_path = os.environ.get("OPTIONS_SCREEN_CONFIG", default_puts_config)
    else:
        config_path = os.environ.get("OPTIONS_SCREEN_CONFIG", default_puts_config)

    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                file_cfg = json.load(f)
            if isinstance(file_cfg, dict):
                for key, value in file_cfg.items():
                    if key in cfg:
                        cfg[key] = _coerce_config_value(cfg[key], value)
        except Exception as e:
            print(f"Warning: could not load {config_path}: {e}")

    for key in cfg:
        env_key = f"OW_{key}"
        raw_env = os.environ.get(env_key)
        if raw_env is None:
            continue
        try:
            cfg[key] = _coerce_config_value(cfg[key], raw_env)
        except Exception as e:
            print(f"Warning: invalid env value for {env_key}: {e}")

    return cfg


def validate_screening_config(cfg):
    errors = []

    if cfg["TARGET_MONTHLY_YIELD_PCT"] <= 0 or cfg["TARGET_MONTHLY_YIELD_PCT"] > 20:
        errors.append("TARGET_MONTHLY_YIELD_PCT must be in (0, 20].")

    if cfg["MAX_PRICE"] <= 0 or cfg["MAX_PRICE"] > 10000:
        errors.append("MAX_PRICE must be in (0, 10000].")

    if cfg["MIN_STOCK_AVG_VOLUME"] < 0 or cfg["MIN_STOCK_AVG_VOLUME"] > 1_000_000_000:
        errors.append("MIN_STOCK_AVG_VOLUME must be in [0, 1000000000].")

    if cfg["MIN_MARKET_CAP"] < 0 or cfg["MIN_MARKET_CAP"] > 10_000_000_000_000:
        errors.append("MIN_MARKET_CAP must be in [0, 10000000000000].")

    if cfg["MIN_PREMIUM"] <= 0 or cfg["MIN_PREMIUM"] > 100:
        errors.append("MIN_PREMIUM must be in (0, 100].")

    if cfg["MIN_DTE"] < 1:
        errors.append("MIN_DTE must be >= 1.")
    if cfg["MAX_DTE"] < cfg["MIN_DTE"]:
        errors.append("MAX_DTE must be >= MIN_DTE.")
    if cfg["MAX_DTE"] > 3650:
        errors.append("MAX_DTE must be <= 3650.")

    if cfg["MIN_OTM_PCT"] < 0 or cfg["MIN_OTM_PCT"] > 50:
        errors.append("MIN_OTM_PCT must be in [0, 50].")

    if cfg["MIN_OPEN_INTEREST"] < 0:
        errors.append("MIN_OPEN_INTEREST must be >= 0.")
    if cfg["MIN_VOLUME"] < 0:
        errors.append("MIN_VOLUME must be >= 0.")

    if cfg["MAX_SPREAD_PCT"] <= 0 or cfg["MAX_SPREAD_PCT"] > 100:
        errors.append("MAX_SPREAD_PCT must be in (0, 100].")

    if cfg["MIN_ABS_DELTA"] < 0 or cfg["MIN_ABS_DELTA"] > 1:
        errors.append("MIN_ABS_DELTA must be in [0, 1].")
    if cfg["MAX_ABS_DELTA"] < 0 or cfg["MAX_ABS_DELTA"] > 1:
        errors.append("MAX_ABS_DELTA must be in [0, 1].")
    if cfg["MAX_ABS_DELTA"] < cfg["MIN_ABS_DELTA"]:
        errors.append("MAX_ABS_DELTA must be >= MIN_ABS_DELTA.")

    if cfg["MAX_EXPIRATIONS_PER_SYMBOL"] < 1 or cfg["MAX_EXPIRATIONS_PER_SYMBOL"] > 24:
        errors.append("MAX_EXPIRATIONS_PER_SYMBOL must be in [1, 24].")
    if cfg["MAX_CONTRACTS_PER_SYMBOL"] < 1 or cfg["MAX_CONTRACTS_PER_SYMBOL"] > 50:
        errors.append("MAX_CONTRACTS_PER_SYMBOL must be in [1, 50].")
    if cfg["OPTIONS_REQUEST_TIMEOUT"] < 3 or cfg["OPTIONS_REQUEST_TIMEOUT"] > 120:
        errors.append("OPTIONS_REQUEST_TIMEOUT must be in [3, 120].")
    if cfg["OPTIONS_MAX_RETRIES"] < 1 or cfg["OPTIONS_MAX_RETRIES"] > 20:
        errors.append("OPTIONS_MAX_RETRIES must be in [1, 20].")
    if cfg["RISK_FREE_RATE"] < -0.05 or cfg["RISK_FREE_RATE"] > 0.25:
        errors.append("RISK_FREE_RATE must be in [-0.05, 0.25].")
    if cfg["DIVIDEND_YIELD"] < 0 or cfg["DIVIDEND_YIELD"] > 0.25:
        errors.append("DIVIDEND_YIELD must be in [0, 0.25].")

    if cfg["MIN_IV_RANK"] < 0 or cfg["MIN_IV_RANK"] > 1.0:
        errors.append("MIN_IV_RANK must be in [0, 1.0].")

    if not isinstance(cfg["FILTER_DOWNTRENDS"], bool):
        errors.append("FILTER_DOWNTRENDS must be true/false.")

    if not isinstance(cfg["EXCLUDE_EARNINGS_BEFORE_EXPIRY"], bool):
        errors.append("EXCLUDE_EARNINGS_BEFORE_EXPIRY must be true/false.")

    if errors:
        raise ValueError("Invalid screening configuration:\n- " + "\n- ".join(errors))

    return cfg


def init_screening_config(option_type="put"):
    """Initialize screening config globals. Call once after parsing args."""
    global SCREENING_CONFIG
    global TARGET_MONTHLY_YIELD_PCT, PRICE_LIMIT, MIN_STOCK_AVG_VOLUME, MIN_MARKET_CAP
    global EXCLUDE_EARNINGS_BEFORE_EXPIRY, MIN_PREMIUM, MIN_DTE, MAX_DTE, MIN_OTM_PCT
    global MIN_OPEN_INTEREST, MIN_VOLUME, MAX_SPREAD_PCT, MIN_ABS_DELTA, MAX_ABS_DELTA
    global MAX_EXPIRATIONS_PER_SYMBOL, MAX_CONTRACTS_PER_SYMBOL
    global OPTIONS_REQUEST_TIMEOUT, OPTIONS_MAX_RETRIES
    global RISK_FREE_RATE, DIVIDEND_YIELD, MIN_IV_RANK, FILTER_DOWNTRENDS

    SCREENING_CONFIG = validate_screening_config(
        load_screening_config(DEFAULT_SCREENING_CONFIG, option_type=option_type)
    )

    TARGET_MONTHLY_YIELD_PCT = SCREENING_CONFIG["TARGET_MONTHLY_YIELD_PCT"]
    PRICE_LIMIT = SCREENING_CONFIG["MAX_PRICE"]
    MIN_STOCK_AVG_VOLUME = SCREENING_CONFIG["MIN_STOCK_AVG_VOLUME"]
    MIN_MARKET_CAP = SCREENING_CONFIG["MIN_MARKET_CAP"]
    EXCLUDE_EARNINGS_BEFORE_EXPIRY = SCREENING_CONFIG["EXCLUDE_EARNINGS_BEFORE_EXPIRY"]
    MIN_PREMIUM = SCREENING_CONFIG["MIN_PREMIUM"]
    MIN_DTE = SCREENING_CONFIG["MIN_DTE"]
    MAX_DTE = SCREENING_CONFIG["MAX_DTE"]
    MIN_OTM_PCT = SCREENING_CONFIG["MIN_OTM_PCT"]
    MIN_OPEN_INTEREST = SCREENING_CONFIG["MIN_OPEN_INTEREST"]
    MIN_VOLUME = SCREENING_CONFIG["MIN_VOLUME"]
    MAX_SPREAD_PCT = SCREENING_CONFIG["MAX_SPREAD_PCT"]
    MIN_ABS_DELTA = SCREENING_CONFIG["MIN_ABS_DELTA"]
    MAX_ABS_DELTA = SCREENING_CONFIG["MAX_ABS_DELTA"]
    MAX_EXPIRATIONS_PER_SYMBOL = SCREENING_CONFIG["MAX_EXPIRATIONS_PER_SYMBOL"]
    MAX_CONTRACTS_PER_SYMBOL = SCREENING_CONFIG["MAX_CONTRACTS_PER_SYMBOL"]
    OPTIONS_REQUEST_TIMEOUT = SCREENING_CONFIG["OPTIONS_REQUEST_TIMEOUT"]
    OPTIONS_MAX_RETRIES = SCREENING_CONFIG["OPTIONS_MAX_RETRIES"]
    RISK_FREE_RATE = SCREENING_CONFIG["RISK_FREE_RATE"]
    DIVIDEND_YIELD = SCREENING_CONFIG["DIVIDEND_YIELD"]
    MIN_IV_RANK = SCREENING_CONFIG["MIN_IV_RANK"]
    FILTER_DOWNTRENDS = SCREENING_CONFIG["FILTER_DOWNTRENDS"]


# Default initialization (for backward compatibility when imported)
init_screening_config("put")

_thread_local = threading.local()
_rate_limit_lock = threading.Lock()
_next_request_ts = 0.0
_next_slot_ts = 0.0
_min_request_interval = 0.35
_error_stats_lock = threading.Lock()
_error_stats = {
    "rate_limited_429": 0,
    "http_4xx": 0,
    "http_5xx": 0,
    "request_exceptions": 0,
    "max_retries_exceeded": 0,
    "empty_payloads": 0,
    "empty_contract_sets": 0,
    "symbol_analysis_exceptions": 0,
    "contracts_excluded_earnings": 0,
    "contracts_excluded_missing_spread": 0,
}


def get_tickers(option_type="put"):
    filename = "tickers_call.json" if option_type == "call" else "tickers_put.json"
    file_path = os.path.join(DATA_INPUT_DIR, filename)
    with open(file_path, "r") as f:
        tickers = json.load(f)
    return [
        ticker for ticker in tickers if isinstance(ticker, str) and "." not in ticker
    ]


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


def _bump_error_stat(key, amount=1):
    with _error_stats_lock:
        _error_stats[key] = _error_stats.get(key, 0) + amount


def _snapshot_error_stats():
    with _error_stats_lock:
        return dict(_error_stats)


def print_error_summary():
    stats = _snapshot_error_stats()
    ordered_items = [
        ("429 rate-limited responses", stats["rate_limited_429"]),
        ("HTTP 5xx responses", stats["http_5xx"]),
        ("HTTP 4xx responses", stats["http_4xx"]),
        ("Request exceptions", stats["request_exceptions"]),
        ("Max retries exceeded", stats["max_retries_exceeded"]),
        ("Symbols with empty payload", stats["empty_payloads"]),
        ("Symbols with no contracts", stats["empty_contract_sets"]),
        ("Contracts excluded for earnings", stats["contracts_excluded_earnings"]),
        (
            "Contracts excluded for missing spread",
            stats["contracts_excluded_missing_spread"],
        ),
        ("Worker analysis exceptions", stats["symbol_analysis_exceptions"]),
    ]
    non_zero_items = [(label, count) for label, count in ordered_items if count > 0]

    print("\nError/Retry Summary:")
    if not non_zero_items:
        print("- None")
        return

    for label, count in non_zero_items:
        print(f"- {label}: {count}")


def safe_get(url, timeout=REQUEST_TIMEOUT, max_retries=MAX_RETRIES):
    cached = _read_cache(url)
    if cached is not None:
        debug_log(f"CACHE HIT: {url}")
        return cached

    last_error = None
    for attempt in range(max_retries):
        debug_log(f"GET attempt {attempt + 1}/{max_retries}: {url}")
        _wait_if_rate_limited()
        _acquire_request_slot()
        try:
            response = get_session().get(url, timeout=timeout)
            if response.status_code == 429:
                _bump_error_stat("rate_limited_429")
                retry_after_raw = response.headers.get("Retry-After", "1")
                try:
                    retry_after = float(retry_after_raw)
                except ValueError:
                    retry_after = 1.0
                retry_after += random.uniform(0.05, 0.25)
                _on_rate_limited(retry_after)
                print(f"Rate limited. Waiting {retry_after:.2f} seconds...")
                debug_log(
                    f"HTTP 429 on attempt {attempt + 1}/{max_retries}; "
                    f"retry-after={retry_after:.2f}s"
                )
                _set_global_cooldown(retry_after)
                last_error = f"HTTP 429 (retry-after: {retry_after:.2f}s)"
                continue

            if response.status_code >= 500:
                _bump_error_stat("http_5xx")
                backoff = min(0.52 * (2**attempt), 5.0)
                debug_log(
                    f"HTTP {response.status_code} on attempt {attempt + 1}/{max_retries}; "
                    f"backoff={backoff:.2f}s; body={_shorten_response_text(response.text)}"
                )
                _set_global_cooldown(backoff)
                last_error = f"HTTP {response.status_code}"
                time.sleep(backoff)
                continue

            # Do not retry most client errors (invalid/unsupported symbol, bad request, etc.)
            if response.status_code >= 400:
                _bump_error_stat("http_4xx")
                print(f"Skipping {url}: HTTP {response.status_code}")
                debug_log(
                    f"HTTP {response.status_code} body: "
                    f"{_shorten_response_text(response.text)}"
                )
                return None

            response.raise_for_status()
            _on_success()
            data = response.json()
            _write_cache(url, data)
            return data
        except requests.RequestException as e:
            _bump_error_stat("request_exceptions")
            last_error = str(e)
            if attempt == max_retries - 1:
                print(f"Error fetching {url}: {e}")
                return None
            backoff = min(0.35 * (2**attempt), 8.0) + random.uniform(0.05, 0.4)
            debug_log(
                f"Request exception on attempt {attempt + 1}/{max_retries}: {e}; "
                f"backoff={backoff:.2f}s"
            )
            _set_global_cooldown(backoff)
            time.sleep(backoff)

    _bump_error_stat("max_retries_exceeded")
    print(f"Error fetching {url}: max retries exceeded ({last_error})")
    return None


def batch_price_filter(tickers):
    candidates = []
    now_dt = datetime.now(timezone.utc)
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        symbols = ",".join(batch)
        url = (
            f"{BASE_URL}?symbols={symbols}&fields="
            "symbol,shortName,regularMarketPrice,trailingPE,"
            "averageDailyVolume3Month,marketCap,"
            "earningsTimestamp,earningsTimestampStart,earningsTimestampEnd"
        )
        data = safe_get(url)
        if data:
            for item in data:
                price = item.get("regularMarketPrice")
                avg_volume_3m = int(
                    _to_float(item.get("averageDailyVolume3Month")) or 0
                )
                market_cap = int(_to_float(item.get("marketCap")) or 0)
                next_earnings_dt = _extract_next_earnings_dt(item, now_dt)
                if (
                    price is not None
                    and price < PRICE_LIMIT
                    and avg_volume_3m >= MIN_STOCK_AVG_VOLUME
                    and market_cap >= MIN_MARKET_CAP
                ):
                    candidates.append(
                        {
                            "symbol": item["symbol"],
                            "price": price,
                            "name": item.get("shortName", ""),
                            "averageDailyVolume3Month": avg_volume_3m,
                            "marketCap": market_cap,
                            "next_earnings_dt": next_earnings_dt,
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
    rsi = 100 - (100 / (1 + rs))
    # Handle division by zero: if avg_loss is 0, RSI should be 100
    rsi = rsi.where(avg_loss != 0, 100)
    return rsi


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
    # Avoid division by zero: if atr is 0, DI values should be 0
    plus_di_raw = df["plus_dm"].ewm(alpha=1 / period, adjust=False).mean()
    minus_di_raw = df["minus_dm"].ewm(alpha=1 / period, adjust=False).mean()
    plus_di = 100 * (plus_di_raw / atr.replace(0, np.nan))
    minus_di = 100 * (minus_di_raw / atr.replace(0, np.nan))
    plus_di = plus_di.fillna(0)
    minus_di = minus_di.fillna(0)

    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    dx = dx.fillna(0)
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


def fetch_historical_indicators(symbol):
    """Fetch historical prices and compute EMA50, RSI, ADX, RVI, MACD, and HV-based IV rank data."""
    from datetime import datetime, timedelta

    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=HIST_DAYS)).strftime("%Y-%m-%d")
    url = f"{HIST_URL}?ticker={symbol}&from={from_date}&to={to_date}&interval=1d"
    data = safe_get(url, timeout=REQUEST_TIMEOUT, max_retries=MAX_RETRIES)
    if not data:
        return None
    # API returns {"meta": ..., "quotes": [...], ...} or a flat list
    if isinstance(data, dict):
        data = data.get("quotes") or data.get("prices") or []
    if not isinstance(data, list) or len(data) < 50:
        return None

    df = pd.DataFrame(data)
    # Normalize column names (API may return various casings)
    df.columns = [c.lower() for c in df.columns]

    required = {"close", "high", "low"}
    if not required.issubset(set(df.columns)):
        return None

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df = df.dropna(subset=["close", "high", "low"]).reset_index(drop=True)

    if len(df) < 50:
        return None

    close = df["close"]

    ema50 = calculate_ema(close, 50).iloc[-1]
    rsi = calculate_rsi(close, 14).iloc[-1]
    adx = calculate_adx(df, 14).iloc[-1]
    rvi = calculate_rvi(close).iloc[-1]
    macd_line, signal_line = calculate_macd(close)
    macd_val = macd_line.iloc[-1]
    signal_val = signal_line.iloc[-1]

    # Compute historical volatility (annualized) for IV rank comparison
    log_returns = np.log(close / close.shift(1)).dropna()
    hv_current = log_returns[-20:].std() * np.sqrt(252)  # 20-day HV
    hv_high = log_returns.rolling(20).std().max() * np.sqrt(
        252
    )  # max 20-day HV in period
    hv_low = log_returns.rolling(20).std().min() * np.sqrt(
        252
    )  # min 20-day HV in period

    return {
        "ema50": float(ema50) if not np.isnan(ema50) else None,
        "rsi": float(rsi) if not np.isnan(rsi) else None,
        "adx": float(adx) if not np.isnan(adx) else None,
        "rvi": float(rvi) if not np.isnan(rvi) else None,
        "macd": float(macd_val) if not np.isnan(macd_val) else None,
        "signal": float(signal_val) if not np.isnan(signal_val) else None,
        "price": float(close.iloc[-1]),
        "hv_current": float(hv_current) if not np.isnan(hv_current) else None,
        "hv_high": float(hv_high) if not np.isnan(hv_high) else None,
        "hv_low": float(hv_low) if not np.isnan(hv_low) else None,
    }


def compute_iv_hv_percentile(option_iv, hv_low, hv_high):
    """Compute IV/HV percentile: how current option IV compares to the stock's historical volatility range.

    Note: this is NOT true IV Rank (which compares current IV to its own 52-week high/low).
    It compares implied volatility to realized historical volatility over the lookback period.
    """
    if option_iv is None or hv_low is None or hv_high is None:
        return None
    if hv_high <= hv_low:
        return None
    rank = (option_iv - hv_low) / (hv_high - hv_low)
    return max(0.0, min(1.0, rank))


def is_downtrend(indicators):
    """Return True if the stock appears to be in a strong downtrend (bad for selling puts)."""
    if indicators is None:
        return False  # Can't determine, don't filter

    price = indicators.get("price")
    ema50 = indicators.get("ema50")
    rsi = indicators.get("rsi")
    adx = indicators.get("adx")

    if price is None or ema50 is None:
        return False

    # Strong downtrend: price well below EMA50, high ADX (trending), and oversold RSI
    below_ema = price < ema50 * 0.95  # More than 5% below EMA50
    strong_trend = adx is not None and adx > 25
    weak_rsi = rsi is not None and rsi < 35

    # Need at least 2 of 3 signals to flag as downtrend
    signals = sum([below_ema, strong_trend and below_ema, weak_rsi and below_ema])
    return signals >= 2


def format_eta(seconds):
    if seconds < 0:
        return "--:--"
    mins, secs = divmod(int(seconds), 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs:d}:{mins:02d}:{secs:02d}"
    return f"{mins:d}:{secs:02d}"


def _to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_round(value, digits=2):
    numeric = _to_float(value)
    return round(numeric, digits) if numeric is not None else None


def _parse_expiration(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        ts = int(value)
        if ts > 10_000_000_000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            debug_log(f"Could not parse expiration date: {value}")
            return None

    return None


def _extract_next_earnings_dt(quote_item, now_dt):
    if not isinstance(quote_item, dict):
        return None

    candidates = []
    for key in ("earningsTimestamp", "earningsTimestampStart", "earningsTimestampEnd"):
        parsed = _parse_expiration(quote_item.get(key))
        if parsed is not None:
            candidates.append(parsed)

    if not candidates:
        return None

    future_candidates = [dt for dt in candidates if dt >= now_dt]
    if future_candidates:
        return min(future_candidates)
    # If all earnings dates are in the past, there is no upcoming earnings known
    return None


def _dte_from_expiration(expiration_dt, now_dt):
    if expiration_dt is None:
        return None

    if expiration_dt.tzinfo is None:
        expiration_dt = expiration_dt.replace(tzinfo=timezone.utc)
    else:
        expiration_dt = expiration_dt.astimezone(timezone.utc)

    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    else:
        now_dt = now_dt.astimezone(timezone.utc)

    return max(0, int((expiration_dt.date() - now_dt.date()).days))


def _normal_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _years_to_expiration(expiration_dt, now_dt):
    if expiration_dt is None:
        return None

    if expiration_dt.tzinfo is None:
        expiration_dt = expiration_dt.replace(tzinfo=timezone.utc)
    else:
        expiration_dt = expiration_dt.astimezone(timezone.utc)

    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    else:
        now_dt = now_dt.astimezone(timezone.utc)

    expiry_close = datetime(
        expiration_dt.year,
        expiration_dt.month,
        expiration_dt.day,
        20,
        0,
        0,
        tzinfo=timezone.utc,
    )
    seconds = (expiry_close - now_dt).total_seconds()
    if seconds <= 0:
        return None
    return max(seconds / (365.0 * 24.0 * 60.0 * 60.0), 1.0 / (365.0 * 24.0 * 60.0))


def _estimate_delta(
    price,
    strike,
    implied_volatility,
    expiration_dt,
    now_dt,
    risk_free_rate,
    dividend_yield,
    option_type="put",
):
    if (
        price is None
        or strike is None
        or implied_volatility is None
        or price <= 0
        or strike <= 0
        or implied_volatility <= 0
    ):
        return None

    t_years = _years_to_expiration(expiration_dt, now_dt)
    if t_years is None:
        return None

    sigma_sqrt_t = implied_volatility * math.sqrt(t_years)
    if sigma_sqrt_t <= 0:
        return None

    r = risk_free_rate
    q = dividend_yield
    d1 = (
        math.log(price / strike) + (r - q + 0.5 * (implied_volatility**2)) * t_years
    ) / sigma_sqrt_t

    if option_type == "call":
        return math.exp(-q * t_years) * _normal_cdf(d1)
    return -math.exp(-q * t_years) * _normal_cdf(-d1)


def _extract_contracts(options_payload, option_type="put"):
    options = options_payload.get("options", [])
    if not isinstance(options, list):
        return []

    chain_key = "calls" if option_type == "call" else "puts"
    contracts = []
    for chain in options[:MAX_EXPIRATIONS_PER_SYMBOL]:
        if not isinstance(chain, dict):
            continue

        expiration_dt = _parse_expiration(chain.get("expirationDate"))
        items = chain.get(chain_key, [])
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            contracts.append((expiration_dt, item))

    return contracts


def _evaluate_contract(symbol_data, expiration_dt, option, now_dt, option_type="put"):
    symbol = symbol_data["symbol"]
    price = _to_float(symbol_data.get("price"))
    if price is None or price <= 0:
        return None

    strike = _to_float(option.get("strike"))
    bid = _to_float(option.get("bid"))
    ask = _to_float(option.get("ask"))
    last_price = _to_float(option.get("lastPrice"))
    implied_volatility = _to_float(option.get("impliedVolatility"))
    delta = _to_float(option.get("delta"))
    open_interest = int(_to_float(option.get("openInterest")) or 0)
    volume = int(_to_float(option.get("volume")) or 0)

    if strike is None or strike <= 0:
        return None

    premium = None
    spread_pct = None
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        premium = (bid + ask) / 2
        spread = ask - bid
        if premium > 0:
            spread_pct = (spread / premium) * 100
    elif last_price is not None and last_price > 0:
        premium = last_price

    if premium is None or premium <= 0:
        return None

    if spread_pct is None:
        _bump_error_stat("contracts_excluded_missing_spread")
        return None

    expiration = expiration_dt or _parse_expiration(option.get("expiration"))
    dte = _dte_from_expiration(expiration, now_dt)
    if dte is None or dte <= 0:
        return None

    next_earnings_dt = symbol_data.get("next_earnings_dt")
    earnings_before_expiry = (
        isinstance(next_earnings_dt, datetime)
        and next_earnings_dt >= now_dt
        and expiration is not None
        and next_earnings_dt.date() <= expiration.date()
    )
    if EXCLUDE_EARNINGS_BEFORE_EXPIRY and earnings_before_expiry:
        _bump_error_stat("contracts_excluded_earnings")
        return None

    if delta is None:
        delta = _estimate_delta(
            price,
            strike,
            implied_volatility,
            expiration,
            now_dt,
            RISK_FREE_RATE,
            DIVIDEND_YIELD,
            option_type=option_type,
        )

    # OTM% calculation differs for puts vs calls
    if option_type == "call":
        otm_pct = ((strike - price) / price) * 100
    else:
        otm_pct = ((price - strike) / price) * 100

    monthly_yield_pct = (premium / strike) * (30 / dte) * 100
    annualized_yield_pct = monthly_yield_pct * 12

    checks = {
        "Yield >= 1%/month": monthly_yield_pct >= TARGET_MONTHLY_YIELD_PCT,
        f"Premium > {MIN_PREMIUM}": premium > MIN_PREMIUM,
        f"{MIN_DTE} <= DTE <= {MAX_DTE}": MIN_DTE <= dte <= MAX_DTE,
        f"OTM >= {MIN_OTM_PCT}%": (strike > price if option_type == "call" else strike < price) and otm_pct >= MIN_OTM_PCT,
        f"OI >= {MIN_OPEN_INTEREST}": open_interest >= MIN_OPEN_INTEREST,
        f"Volume >= {MIN_VOLUME}": volume >= MIN_VOLUME,
        f"Spread <= {MAX_SPREAD_PCT}%": spread_pct is not None
        and spread_pct <= MAX_SPREAD_PCT,
    }

    if delta is not None:
        abs_delta = abs(delta)
        checks[f"{MIN_ABS_DELTA:.2f} <= |Delta| <= {MAX_ABS_DELTA:.2f}"] = (
            MIN_ABS_DELTA <= abs_delta <= MAX_ABS_DELTA
        )

    failed = [name for name, passed in checks.items() if not passed]

    liquidity_score = min(open_interest / 500, 1.0) * 15 + min(volume / 50, 1.0) * 10
    yield_score = min(monthly_yield_pct / TARGET_MONTHLY_YIELD_PCT, 2.0) * 30
    otm_score = min(max(otm_pct, 0) / 10, 1.0) * 20
    spread_score = 0
    if spread_pct is not None:
        spread_score = max(0.0, 1.0 - (spread_pct / MAX_SPREAD_PCT)) * 15
    dte_mid = (MIN_DTE + MAX_DTE) / 2
    dte_score = max(0.0, 1.0 - abs(dte - dte_mid) / dte_mid) * 10
    iv_score = min(max(implied_volatility or 0, 0), 2.0) / 2.0 * 10
    score = (
        yield_score + otm_score + liquidity_score + spread_score + dte_score + iv_score
    )

    return {
        "Symbol": symbol,
        "Name": symbol_data["name"],
        "Price": round(price, 2),
        "EMA50": None,
        "ADX": None,
        "RSI": None,
        "RVI": None,
        "MACD": None,
        "Signal": None,
        "DiffPct": None,
        "Status": "PASS" if len(failed) == 0 else "NEAR",
        "Failed Criterion": failed[0] if len(failed) == 1 else "",
        "Strike": round(strike, 2),
        "Expiration": expiration.strftime("%Y-%m-%d") if expiration else None,
        "NextEarnings": next_earnings_dt.strftime("%Y-%m-%d")
        if isinstance(next_earnings_dt, datetime)
        else None,
        "EarningsBeforeExpiry": earnings_before_expiry,
        "DTE": dte,
        "Premium": round(premium, 2),
        "Bid": _safe_round(bid),
        "Ask": _safe_round(ask),
        "SpreadPct": _safe_round(spread_pct),
        "MonthlyYieldPct": round(monthly_yield_pct, 2),
        "AnnualizedYieldPct": round(annualized_yield_pct, 2),
        "OTMPct": round(otm_pct, 2),
        "OpenInterest": open_interest,
        "Volume": volume,
        "Delta": _safe_round(delta, 3),
        "ImpliedVolatility": _safe_round(implied_volatility, 3),
        "Score": round(score, 2),
    }, failed


def analyze_single_symbol_options(symbol_data, option_type="put"):
    symbol = symbol_data["symbol"]

    # Fetch historical indicators for trend/IV analysis
    indicators = fetch_historical_indicators(symbol)

    # Filter out strong downtrends (only for puts)
    if option_type == "put" and FILTER_DOWNTRENDS and is_downtrend(indicators):
        debug_log(f"Skipping {symbol}: strong downtrend detected")
        return [], []

    api_filter = "calls" if option_type == "call" else "puts"
    url = (
        f"{OPTIONS_URL}?ticker={symbol}&filter={api_filter}&limit=25"
        f"&expirationDatesCount={MAX_EXPIRATIONS_PER_SYMBOL}"
    )
    data = safe_get(
        url,
        timeout=OPTIONS_REQUEST_TIMEOUT,
        max_retries=OPTIONS_MAX_RETRIES,
    )
    if not data:
        _bump_error_stat("empty_payloads")
        debug_log(f"No options payload for {symbol}: {url}")
        return [], []

    now_dt = datetime.now(timezone.utc)
    contracts = _extract_contracts(data, option_type=option_type)
    if len(contracts) == 0:
        _bump_error_stat("empty_contract_sets")
        debug_log(f"No {option_type} contracts extracted for {symbol}")
    passed_contracts = []
    near_contracts = []

    for expiration_dt, option in contracts:
        evaluated = _evaluate_contract(symbol_data, expiration_dt, option, now_dt, option_type=option_type)
        if not evaluated:
            continue

        contract_data, failed = evaluated

        # Enrich with technical indicators
        if indicators:
            contract_data["EMA50"] = _safe_round(indicators.get("ema50"))
            contract_data["ADX"] = _safe_round(indicators.get("adx"))
            contract_data["RSI"] = _safe_round(indicators.get("rsi"))
            contract_data["RVI"] = _safe_round(indicators.get("rvi"))
            contract_data["MACD"] = _safe_round(indicators.get("macd"), 3)
            contract_data["Signal"] = _safe_round(indicators.get("signal"), 3)
            if indicators.get("ema50") and indicators.get("price"):
                diff_pct = (
                    (indicators["price"] - indicators["ema50"]) / indicators["ema50"]
                ) * 100
                contract_data["DiffPct"] = _safe_round(diff_pct)

        # IV/HV percentile filter (not true IV Rank)
        iv = contract_data.get("ImpliedVolatility")
        if indicators and iv is not None:
            iv_rank = compute_iv_hv_percentile(
                iv, indicators.get("hv_low"), indicators.get("hv_high")
            )
            contract_data["IVRank"] = _safe_round(iv_rank)
            if iv_rank is not None and iv_rank < MIN_IV_RANK:
                failed.append(f"IVRank >= {MIN_IV_RANK:.0%}")
                contract_data["Status"] = (
                    "NEAR" if len(failed) == 1 else contract_data["Status"]
                )
                contract_data["Failed Criterion"] = (
                    failed[0]
                    if len(failed) == 1
                    else contract_data.get("Failed Criterion", "")
                )
        else:
            contract_data["IVRank"] = None

        if len(failed) == 0:
            passed_contracts.append(contract_data)
        elif len(failed) == 1:
            near_contracts.append(contract_data)

    passed_contracts.sort(
        key=lambda row: (row["Score"], row["MonthlyYieldPct"], row["OpenInterest"]),
        reverse=True,
    )
    near_contracts.sort(
        key=lambda row: (row["Score"], row["MonthlyYieldPct"], row["OpenInterest"]),
        reverse=True,
    )

    return passed_contracts[:MAX_CONTRACTS_PER_SYMBOL], near_contracts[
        :MAX_CONTRACTS_PER_SYMBOL
    ]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-top",
        "--top",
        dest="top",
        type=int,
        default=None,
        help="Limit analysis to the first N tickers.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging for retries and API errors.",
    )
    parser.add_argument(
        "--type",
        dest="option_type",
        choices=["put", "call"],
        default="put",
        help="Type of options to analyze: 'put' (default) or 'call'.",
    )
    args = parser.parse_args()
    if args.top is not None and args.top <= 0:
        parser.error("-top/--top must be greater than 0")
    return args


def deep_analysis(candidates, option_type="put"):
    results = []
    near_misses = []

    total = len(candidates)
    type_label = option_type.upper()
    print(f"Analyzing {type_label} options for {total} candidates")
    analysis_start = time.time()
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(analyze_single_symbol_options, c, option_type): idx
            for idx, c in enumerate(candidates, 1)
        }

        for future in as_completed(futures):
            idx = futures[future]
            completed += 1
            elapsed = time.time() - analysis_start
            if completed > 0:
                avg_time = elapsed / completed
                eta_seconds = avg_time * (total - completed)
                eta_str = format_eta(eta_seconds)
            else:
                eta_str = "--:--"
            symbol = candidates[idx - 1]["symbol"]
            msg = f"[{completed}/{total}] Options scan {symbol:<10} ETA: {eta_str}"
            print(f"{msg:<60}", end="\r", flush=True)

            try:
                passed_contracts, near_contracts = future.result()
                results.extend(passed_contracts)
                near_misses.extend(near_contracts)
            except Exception as e:
                _bump_error_stat("symbol_analysis_exceptions")
                print(f"\nError analyzing {symbol}: {e}")
                if DEBUG:
                    traceback.print_exc()

    return results, near_misses


def main():
    global DEBUG
    args = parse_args()
    DEBUG = args.debug
    option_type = args.option_type
    type_label = option_type.upper()

    # Re-initialize config for the chosen option type
    init_screening_config(option_type)

    if DEBUG:
        print("Debug logging enabled.")

    print(f"Fetching {type_label} tickers...")
    tickers = get_tickers(option_type)
    if args.top is not None:
        tickers = tickers[: args.top]
        print(f"Limiting analysis to first {len(tickers)} tickers (-top={args.top}).")

    print(
        f"Screen config ({type_label}): "
        f"avgVol3m>={MIN_STOCK_AVG_VOLUME:,}, "
        f"marketCap>={MIN_MARKET_CAP:,}, "
        f"excludeEarningsBeforeExpiry={EXCLUDE_EARNINGS_BEFORE_EXPIRY}, "
        f"yield>={TARGET_MONTHLY_YIELD_PCT:.2f}%/mo, "
        f"DTE={MIN_DTE}-{MAX_DTE}, "
        f"|delta|={MIN_ABS_DELTA:.2f}-{MAX_ABS_DELTA:.2f}, "
        f"spread<={MAX_SPREAD_PCT:.2f}%, "
        f"ivRank>={MIN_IV_RANK:.0%}, "
        f"filterDowntrends={FILTER_DOWNTRENDS}"
    )

    print(f"Phase 1: Screening {len(tickers)} tickers for price < ${PRICE_LIMIT}...")
    candidates = batch_price_filter(tickers)
    print(f"Found {len(candidates)} candidates.")

    print(f"Phase 2 & 3: {type_label} options-first analysis and filtering...")
    final_results, near_misses = deep_analysis(candidates, option_type=option_type)

    print("Phase 4: Sorting and Reporting...")
    combined_results = final_results + near_misses
    combined_results.sort(
        key=lambda x: (x["Status"] != "PASS", -x["Score"], -x["MonthlyYieldPct"])
    )

    if combined_results:
        print(f"\nFull {type_label} Summary Table (Options-first ranking):")
        df = pd.DataFrame(combined_results)
        # Reorder columns for better display
        cols = [
            "Symbol",
            "Name",
            "Status",
            "Price",
            "Strike",
            "Expiration",
            "NextEarnings",
            "EarningsBeforeExpiry",
            "DTE",
            "Premium",
            "MonthlyYieldPct",
            "AnnualizedYieldPct",
            "OTMPct",
            "OpenInterest",
            "Volume",
            "SpreadPct",
            "Delta",
            "ImpliedVolatility",
            "IVRank",
            "RSI",
            "ADX",
            "DiffPct",
            "Score",
            "Failed Criterion",
        ]
        available_cols = [c for c in cols if c in df.columns]
        print(df[available_cols].to_string(index=False))
    else:
        print(f"\nNo {type_label} stocks matched the criteria or were near misses.")

    print_error_summary()

    # Save results to JSON
    output_file = (
        os.path.join(DATA_OUTPUT_DIR, "call_results.json")
        if option_type == "call"
        else os.path.join(DATA_OUTPUT_DIR, "put_results.json")
    )
    output = {
        "timestamp": datetime.now().isoformat(),
        "option_type": option_type,
        "total_tickers_analyzed": len(tickers),
        "candidates_after_phase1": len(candidates),
        "passed_all_criteria": len(final_results),
        "near_misses": len(near_misses),
        "screening_mode": "options-first",
        "target_monthly_yield_pct": TARGET_MONTHLY_YIELD_PCT,
        "screening_config": SCREENING_CONFIG,
        "results": combined_results,
    }
    output = convert_numpy_types(output)

    os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2, cls=NumpyEncoder)
    print(f"\nResults saved to {output_file}")


if __name__ == "__main__":
    main()
