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
# BASE_URL = "http://localhost:7071/api/yahoo-finance"
# HIST_URL = "http://localhost:7071/api/yahoo-finance-historical"
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

DEFAULT_SCREENING_CONFIG = {
    "MAX_PRICE": 80.0,
    "TARGET_MONTHLY_YIELD_PCT": 1.0,
    "MIN_PREMIUM": 0.15,
    "MIN_DTE": 20,
    "MAX_DTE": 60,
    "MIN_OTM_PCT": 5.0,
    "MIN_OPEN_INTEREST": 100,
    "MIN_VOLUME": 10,
    "MAX_SPREAD_PCT": 20.0,
    "MIN_ABS_DELTA": 0.10,
    "MAX_ABS_DELTA": 0.35,
    "MAX_EXPIRATIONS_PER_SYMBOL": 3,
    "MAX_CONTRACTS_PER_SYMBOL": 3,
    "OPTIONS_REQUEST_TIMEOUT": 25,
    "OPTIONS_MAX_RETRIES": 8,
    "RISK_FREE_RATE": 0.0,
    "DIVIDEND_YIELD": 0.0,
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


def load_screening_config(defaults):
    cfg = dict(defaults)
    config_path = os.environ.get("OPTIONS_SCREEN_CONFIG", "screening_config.json")

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

    if cfg["MAX_EXPIRATIONS_PER_SYMBOL"] < 1 or cfg["MAX_EXPIRATIONS_PER_SYMBOL"] > 12:
        errors.append("MAX_EXPIRATIONS_PER_SYMBOL must be in [1, 12].")
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

    if errors:
        raise ValueError("Invalid screening configuration:\n- " + "\n- ".join(errors))

    return cfg


SCREENING_CONFIG = validate_screening_config(
    load_screening_config(DEFAULT_SCREENING_CONFIG)
)

# Options-first screening parameters
TARGET_MONTHLY_YIELD_PCT = SCREENING_CONFIG["TARGET_MONTHLY_YIELD_PCT"]
PRICE_LIMIT = SCREENING_CONFIG["MAX_PRICE"]
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
}


def get_tickers():
    with open("tickers.json", "r") as f:
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
            return response.json()
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


def _estimate_put_delta(
    price,
    strike,
    implied_volatility,
    expiration_dt,
    now_dt,
    risk_free_rate,
    dividend_yield,
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
    return -math.exp(-q * t_years) * _normal_cdf(-d1)


def _extract_contracts(options_payload):
    options = options_payload.get("options", [])
    if not isinstance(options, list):
        return []

    contracts = []
    for chain in options[:MAX_EXPIRATIONS_PER_SYMBOL]:
        if not isinstance(chain, dict):
            continue

        expiration_dt = _parse_expiration(chain.get("expirationDate"))
        puts = chain.get("puts", [])
        if not isinstance(puts, list):
            continue

        for put in puts:
            if not isinstance(put, dict):
                continue
            contracts.append((expiration_dt, put))

    return contracts


def _evaluate_put_contract(symbol_data, expiration_dt, option, now_dt):
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

    expiration = expiration_dt or _parse_expiration(option.get("expiration"))
    dte = _dte_from_expiration(expiration, now_dt)
    if dte is None or dte <= 0:
        return None

    if delta is None:
        delta = _estimate_put_delta(
            price,
            strike,
            implied_volatility,
            expiration,
            now_dt,
            RISK_FREE_RATE,
            DIVIDEND_YIELD,
        )

    otm_pct = ((price - strike) / price) * 100
    monthly_yield_pct = (premium / strike) * (30 / dte) * 100
    annualized_yield_pct = monthly_yield_pct * 12

    checks = {
        "Yield >= 1%/month": monthly_yield_pct >= TARGET_MONTHLY_YIELD_PCT,
        f"Premium > {MIN_PREMIUM}": premium > MIN_PREMIUM,
        f"{MIN_DTE} <= DTE <= {MAX_DTE}": MIN_DTE <= dte <= MAX_DTE,
        f"OTM >= {MIN_OTM_PCT}%": strike < price and otm_pct >= MIN_OTM_PCT,
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


def analyze_single_symbol_options(symbol_data):
    symbol = symbol_data["symbol"]
    url = (
        f"{OPTIONS_URL}?ticker={symbol}&filter=puts&limit=25"
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
    contracts = _extract_contracts(data)
    if len(contracts) == 0:
        _bump_error_stat("empty_contract_sets")
        debug_log(f"No put contracts extracted for {symbol}")
    passed_contracts = []
    near_contracts = []

    for expiration_dt, option in contracts:
        evaluated = _evaluate_put_contract(symbol_data, expiration_dt, option, now_dt)
        if not evaluated:
            continue

        contract_data, failed = evaluated
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
    args = parser.parse_args()
    if args.top is not None and args.top <= 0:
        parser.error("-top/--top must be greater than 0")
    return args


def deep_analysis(candidates):
    results = []
    near_misses = []

    total = len(candidates)
    print(f"Analyzing options for {total} candidates")
    analysis_start = time.time()
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(analyze_single_symbol_options, c): idx
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

    if DEBUG:
        print("Debug logging enabled.")

    print("Fetching tickers...")
    tickers = get_tickers()
    if args.top is not None:
        tickers = tickers[: args.top]
        print(f"Limiting analysis to first {len(tickers)} tickers (-top={args.top}).")

    print(
        "Screen config: "
        f"yield>={TARGET_MONTHLY_YIELD_PCT:.2f}%/mo, "
        f"DTE={MIN_DTE}-{MAX_DTE}, "
        f"|delta|={MIN_ABS_DELTA:.2f}-{MAX_ABS_DELTA:.2f}, "
        f"spread<={MAX_SPREAD_PCT:.2f}%"
    )

    print(f"Phase 1: Screening {len(tickers)} tickers for price < ${PRICE_LIMIT}...")
    candidates = batch_price_filter(tickers)
    print(f"Found {len(candidates)} candidates.")

    print("Phase 2 & 3: Options-first analysis and filtering...")
    final_results, near_misses = deep_analysis(candidates)

    print("Phase 4: Sorting and Reporting...")
    combined_results = final_results + near_misses
    combined_results.sort(
        key=lambda x: (x["Status"] != "PASS", -x["Score"], -x["MonthlyYieldPct"])
    )

    if combined_results:
        print("\nFull Summary Table (Options-first ranking):")
        df = pd.DataFrame(combined_results)
        # Reorder columns for better display
        cols = [
            "Symbol",
            "Name",
            "Status",
            "Price",
            "Strike",
            "Expiration",
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
            "Score",
            "Failed Criterion",
        ]
        print(df[cols].to_string(index=False))
    else:
        print("\nNo stocks matched the criteria or were near misses.")

    print_error_summary()

    # Save results to JSON
    output = {
        "timestamp": datetime.now().isoformat(),
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

    with open("analysis_results.json", "w") as f:
        json.dump(output, f, indent=2, cls=NumpyEncoder)
    print(f"\nResults saved to analysis_results.json")


if __name__ == "__main__":
    main()
