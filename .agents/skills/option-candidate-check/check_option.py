"""One-off option-candidate checker for the OptionsWheel skill.

Fetches live quotes, the options chain and historical prices from the
OptionsWheel API, computes the same metrics as OptionsWheel/src/options_wheel,
and prints a JSON report used to explain whether an option is a good or bad
candidate to sell.

Usage:
  python check_option.py --ticker AAPL --type put --list
  python check_option.py --ticker AAPL --type put --expiration 2026-09-18 --strike 290
  python check_option.py --ticker AAPL --type call --expiration 2026-09-18 --strike 315

Environment:
  OW_API_BASE   base URL of the OptionsWheel API (default http://localhost:7071/api)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def find_options_wheel_src(start=SCRIPT_DIR):
    """Locate OptionsWheel/src by walking up the directory tree."""
    cur = os.path.abspath(start)
    while True:
        candidate = os.path.join(cur, "OptionsWheel", "src")
        if os.path.isdir(os.path.join(candidate, "options_wheel")):
            return candidate
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


def api_base():
    return os.environ.get("OW_API_BASE", "http://localhost:7071/api").rstrip("/")


def http_get_json(url, timeout=30, retries=3):
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(0.6 * (attempt + 1))
    raise RuntimeError(f"API request failed: {url} ({last_err})")


def fetch_quote(symbol):
    base = api_base()
    fields = (
        "symbol,shortName,regularMarketPrice,trailingPE,"
        "averageDailyVolume3Month,marketCap,trailingAnnualDividendYield,"
        "fiftyDayAverage,earningsTimestamp,earningsTimestampStart,"
        "earningsTimestampEnd,dividendDate,trailingAnnualDividendRate"
    )
    url = f"{base}/yahoo-finance?symbols={symbol}&fields={fields}"
    data = http_get_json(url)
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"No quote found for symbol {symbol}")
    item = next((x for x in data if str(x.get("symbol", "")).upper() == symbol.upper()), data[0])
    return item


def fetch_chain(symbol, option_type, expiration_date=None):
    base = api_base()
    api_filter = "calls" if option_type == "call" else "puts"
    url = f"{base}/yahoo-finance-stock-options?ticker={symbol}&filter={api_filter}&limit=50"
    if expiration_date:
        url += f"&expirationDate={expiration_date}"
    return http_get_json(url, timeout=45)


def fetch_history(symbol):
    base = api_base()
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    url = f"{base}/yahoo-finance-historical?ticker={symbol}&from={from_date}&to={to_date}&interval=1d"
    data = http_get_json(url, timeout=45)
    if isinstance(data, dict):
        quotes = data.get("quotes") or data.get("prices") or []
    else:
        quotes = data or []
    if len(quotes) < 50:
        raise RuntimeError(f"Insufficient historical data for {symbol}: {len(quotes)} bars")
    return quotes


def _to_float(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _safe_round(v, digits=2):
    f = _to_float(v)
    return round(f, digits) if f is not None and math.isfinite(f) else None


def _parse_iso_dt(value):
    from options_wheel.analysis import _parse_expiration
    return _parse_expiration(value)


def list_expirations_and_strikes(symbol, option_type):
    chain = fetch_chain(symbol, option_type)
    spot = _to_float((chain.get("quote") or {}).get("regularMarketPrice"))
    expiration_dates = chain.get("expirationDates") or []
    strikes = chain.get("strikes") or []
    now = datetime.now(timezone.utc)
    future = []
    for raw in expiration_dates:
        dt = _parse_iso_dt(raw)
        if dt is None:
            continue
        dte = max(0, (dt.date() - now.date()).days)
        if dte >= 0:
            future.append({"date": dt.strftime("%Y-%m-%d"), "dte": dte})
    future.sort(key=lambda x: (x["dte"], x["date"]))
    next_ten = future[:10]

    suggestion = {"atm": _safe_round(spot), "strikes": []}
    if spot and strikes:
        ordered = sorted(strikes)
        atm = min(ordered, key=lambda s: abs(s - spot))
        idx = ordered.index(atm)
        lo = max(0, idx - 5)
        hi = min(len(ordered), idx + 6)
        suggestion["strikes"] = [float(s) for s in ordered[lo:hi]]

    return {
        "symbol": symbol,
        "option_type": option_type,
        "spot": _safe_round(spot),
        "expirations": next_ten,
        "suggested_strikes": suggestion,
    }


def analyze_contract(symbol, option_type, expiration_date, strike):
    from options_wheel.analysis import (
        IV_HISTORY_STORE,
        _dte_from_expiration,
        _extract_next_earnings_dt,
        compute_iv_hv_percentile,
        extract_atm_iv,
        fetch_historical_indicators,
    )
    from options_wheel.metrics import (
        collateral_per_share,
        credit_risk_ratio,
        expected_itm_payoff,
        forecast_volatility,
        net_credit,
        option_delta,
        probability_of_profit,
        sigma_distance,
        simple_yields,
        variance_risk_premium,
        years_to_expiration,
    )

    now_dt = datetime.now(timezone.utc)
    expiration_dt = _parse_iso_dt(expiration_date)
    if expiration_dt is None:
        raise RuntimeError(f"Cannot parse expiration date: {expiration_date}")
    expiration_dt = expiration_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    quote = fetch_quote(symbol)
    chain = fetch_chain(symbol, option_type, expiration_date=expiration_date)
    spot = _to_float(quote.get("regularMarketPrice"))
    if not spot:
        spot = _to_float((chain.get("quote") or {}).get("regularMarketPrice"))
    if not spot:
        raise RuntimeError(f"No spot price for {symbol}")

    strike = float(strike)
    chain_list = chain.get("options") or []
    contracts = []
    if chain_list:
        key = "calls" if option_type == "call" else "puts"
        items = chain_list[0].get(key) or []
        for item in items:
            if _to_float(item.get("strike")) == strike:
                contracts.append((expiration_dt, item))
    if not contracts:
        avail = sorted({_to_float(i.get("strike")) for c in chain_list for i in (c.get("calls") or []) + (c.get("puts") or []) if _to_float(i.get("strike"))})
        raise RuntimeError(
            f"Strike {strike} not found for {symbol} {option_type} on {expiration_date}. "
            f"Available strikes: {avail[:30]}"
        )

    _, option = contracts[0]
    bid = _to_float(option.get("bid"))
    ask = _to_float(option.get("ask"))
    implied_vol = _to_float(option.get("impliedVolatility"))
    last_price = _to_float(option.get("lastPrice"))
    open_interest = int(_to_float(option.get("openInterest")) or 0)
    volume = int(_to_float(option.get("volume")) or 0)
    api_delta = _to_float(option.get("delta"))

    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        premium = last_price
        net_premium = last_price
        spread_abs = None
        spread_pct = None
    else:
        premium, net_premium, spread_pct = net_credit(
            bid, ask, commission_per_contract=0.65, slippage_pct_of_spread=30.0, round_trip=False
        )
        spread_abs = ask - bid

    dividend_yield = _to_float(quote.get("trailingAnnualDividendYield")) or 0.0
    trailing_div_rate = _to_float(quote.get("trailingAnnualDividendRate")) or 0.0
    risk_free = 0.045

    dte = _dte_from_expiration(expiration_dt, now_dt)
    t_years = years_to_expiration(expiration_dt, now_dt)

    indicators = fetch_historical_indicators(symbol)

    hv_current = indicators.get("hv_current") if indicators else None
    hv_long = indicators.get("hv_long") if indicators else None
    hv_high = indicators.get("hv_high") if indicators else None
    hv_low = indicators.get("hv_low") if indicators else None
    drift_raw = indicators.get("realized_drift") if indicators else None
    drift = max(-0.25, min(0.25, drift_raw)) if drift_raw is not None else 0.0

    if api_delta is None:
        delta = option_delta(spot, strike, implied_vol, t_years, risk_free, dividend_yield, option_type=option_type)
    else:
        delta = api_delta

    forecast_vol = forecast_volatility(implied_vol, hv_current, hv_long, hv_weight=0.5, iv_haircut=0.85)
    vrp_ratio = variance_risk_premium(implied_vol, forecast_vol)
    expected_loss = expected_itm_payoff(spot, strike, forecast_vol, t_years, drift, option_type=option_type)
    pop = probability_of_profit(spot, strike, forecast_vol, t_years, drift, net_premium, option_type=option_type)
    sigma_dist = sigma_distance(spot, strike, forecast_vol, t_years, option_type=option_type)
    risk_ratio = credit_risk_ratio(net_premium, expected_loss)
    ev = None
    if net_premium is not None and expected_loss is not None:
        ev = net_premium - expected_loss
    collateral = collateral_per_share(strike, spot, net_premium, option_type=option_type)
    monthly_yield, annualized_yield = simple_yields(net_premium, collateral, dte)

    if option_type == "call":
        otm_pct = ((strike - spot) / spot) * 100
    else:
        otm_pct = ((spot - strike) / spot) * 100

    # Events ---------------------------------------------------------------
    next_earnings_dt = _extract_next_earnings_dt(quote, now_dt)
    earnings_before_expiry = (
        next_earnings_dt is not None
        and next_earnings_dt.date() <= expiration_dt.date()
        and next_earnings_dt >= now_dt
    )
    ex_div_dt = _parse_iso_dt(quote.get("dividendDate"))
    ex_div_risk = "NONE"
    if ex_div_dt is not None:
        ex_div_dt = ex_div_dt.astimezone(timezone.utc)
        if now_dt.date() <= ex_div_dt.date() <= expiration_dt.date():
            dividend_amount = trailing_div_rate / 4.0 if trailing_div_rate > 0 else 0.0
            if option_type == "call":
                extrinsic = (premium or 0.0) - max(spot - strike, 0.0)
                ex_div_risk = "HIGH" if dividend_amount > 0 and extrinsic < dividend_amount else "MEDIUM"
            else:
                ex_div_risk = "DIVIDEND_DROP"

    # IV rank / IV-HV percentile -------------------------------------------
    iv_rank = iv_percentile = iv_obs = None
    iv_hv_percentile = None
    atm_iv = None
    try:
        payload = {"options": [chain_list[0]]} if chain_list else {"options": []}
        contracts_all = []
        for c in (payload.get("options") or []):
            for item in (c.get("puts") or []) + (c.get("calls") or []):
                contracts_all.append((expiration_dt, item))
        atm_iv = extract_atm_iv(contracts_all, spot, now_dt=now_dt, dte_fn=_dte_from_expiration)
        key = f"{symbol}|{option_type}"
        iv_rank, iv_percentile, iv_obs = IV_HISTORY_STORE.rank(key, atm_iv)
    except Exception:
        pass
    if implied_vol is not None and hv_high is not None and hv_low is not None:
        iv_hv_percentile = compute_iv_hv_percentile(implied_vol, hv_low, hv_high)

    report = {
        "symbol": symbol,
        "name": quote.get("shortName", ""),
        "option_type": option_type,
        "spot": _safe_round(spot),
        "strike": strike,
        "expiration": expiration_dt.strftime("%Y-%m-%d"),
        "dte": dte,
        "quote": {
            "bid": _safe_round(bid),
            "ask": _safe_round(ask),
            "last_price": _safe_round(last_price),
            "premium_mid": _safe_round(premium),
            "net_premium": _safe_round(net_premium),
            "spread_pct": _safe_round(spread_pct),
            "spread_abs": _safe_round(spread_abs),
            "open_interest": open_interest,
            "volume": volume,
            "implied_volatility": _safe_round(implied_vol, 4),
            "api_delta": _safe_round(api_delta, 3),
            "delta_bs": _safe_round(delta, 3),
            "moneyness_otm_pct": _safe_round(otm_pct),
        },
        "events": {
            "next_earnings": next_earnings_dt.strftime("%Y-%m-%d") if next_earnings_dt else None,
            "earnings_before_expiry": earnings_before_expiry,
            "ex_dividend_date": ex_div_dt.strftime("%Y-%m-%d") if ex_div_dt else None,
            "ex_div_risk": ex_div_risk,
            "dividend_yield": _safe_round(dividend_yield, 4),
        },
        "technicals": {
            key: _safe_round(indicators.get(key)) if indicators else None
            for key in ("ema50", "rsi", "adx", "rvi", "macd", "signal", "price")
        },
        "volatility": {
            "hv_current": _safe_round(hv_current, 4),
            "hv_long": _safe_round(hv_long, 4),
            "hv_high": _safe_round(hv_high, 4),
            "hv_low": _safe_round(hv_low, 4),
            "forecast_vol": _safe_round(forecast_vol, 4),
            "vrp_ratio": _safe_round(vrp_ratio, 3),
            "atm_iv": _safe_round(atm_iv, 4),
            "iv_rank": _safe_round(iv_rank, 3),
            "iv_percentile": _safe_round(iv_percentile, 3),
            "iv_hv_percentile": _safe_round(iv_hv_percentile, 3),
            "realized_drift": _safe_round(drift_raw, 4),
        },
        "returns_risk": {
            "collateral_per_share": _safe_round(collateral),
            "monthly_yield_pct": _safe_round(monthly_yield),
            "annualized_yield_pct": _safe_round(annualized_yield),
            "probability_of_profit_pct": _safe_round(pop * 100 if pop is not None else None, 1),
            "expected_loss_per_share": _safe_round(expected_loss),
            "ev_per_share": _safe_round(ev),
            "sigma_distance": _safe_round(sigma_dist, 3),
            "credit_risk_ratio": _safe_round(risk_ratio, 3),
        },
    }
    return report


def main():
    parser = argparse.ArgumentParser(description="OptionsWheel candidate checker")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--type", choices=["put", "call"], default="put")
    parser.add_argument("--list", action="store_true", help="List next expirations and suggested strikes")
    parser.add_argument("--expiration", default=None, help="Expiration date YYYY-MM-DD")
    parser.add_argument("--strike", type=float, default=None)
    args = parser.parse_args()

    src = find_options_wheel_src()
    if src is None:
        sys.stderr.write("ERROR: cannot locate OptionsWheel/src (options_wheel module).\n")
        sys.exit(2)
    if src not in sys.path:
        sys.path.insert(0, src)

    if args.list:
        result = list_expirations_and_strikes(args.ticker, args.type)
    else:
        if not args.expiration or args.strike is None:
            sys.stderr.write("ERROR: --expiration and --strike are required unless --list is used.\n")
            sys.exit(2)
        result = analyze_contract(args.ticker, args.type, args.expiration, args.strike)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
