"""Feedback loop: archive every scan and grade it once the contracts expire.

Without this the screener is unfalsifiable - the scoring weights are guesses and
nothing ever tells you whether a filter helped or hurt. Each run archives the
ranked candidates; ``evaluate`` then replays the archive against actual price
history and reports the realised hit rate and P/L per contract, bucketed by the
features the screener ranks on (score, delta, IV rank, sigma distance). Those
buckets are what you calibrate the config against.

Two P/L figures are reported per trade:

* **held** - the option is held to expiry and settled at intrinsic value.
* **managed** - a simple, common management rule: buy back once a share of the
  credit is captured (profit target) or once the loss reaches a multiple of the
  credit (stop). The option is repriced each day with Black-Scholes at the
  implied volatility recorded at scan time. Constant IV is a simplification:
  real IV rises when the stock falls (stops would trigger earlier and cost
  more). It also misses the event variance that holds an option's price up
  until an earnings report, so trades spanning earnings are only managed after
  the report date, and left unmodeled when that date was not archived.

Yahoo returns split-adjusted closes for the whole history, so prices are
rescaled into the share basis of the scan date before strikes are compared.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime, timezone

from .metrics import black_scholes_price

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(MODULE_DIR, "..", ".."))
DATA_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "output")
HISTORY_DIR = os.path.join(PROJECT_ROOT, "data", "history")
SCAN_ARCHIVE_DIR = os.path.join(HISTORY_DIR, "scans")

# Bump when grading logic changes so stale cached grades are recomputed.
GRADER_VERSION = 2
DEFAULT_PROFIT_TARGET = 0.5
DEFAULT_STOP_LOSS = 2.0
DEFAULT_EXIT_COMMISSION = 0.65
DEFAULT_RISK_FREE_RATE = 0.045

ARCHIVED_FIELDS = (
    "Symbol",
    "Status",
    "Price",
    "Strike",
    "Expiration",
    "DTE",
    "Premium",
    "NetPremium",
    "Delta",
    "OTMPct",
    "OpenInterest",
    "Volume",
    "SpreadPct",
    "ImpliedVolatility",
    "ATMImpliedVolatility",
    "ForecastVol",
    "IVRank",
    "IVHVPercentile",
    "VRPRatio",
    "SigmaDistance",
    "MonthlyYieldPct",
    "PoP",
    "EV",
    "Score",
    "EarningsBeforeExpiry",
    "NextEarnings",
    "EarningsMovePct",
)


def checked_option_type(option_type):
    """Return ``option_type`` as a known literal; it names files and folders."""
    if option_type == "put":
        return "put"
    if option_type == "call":
        return "call"
    raise ValueError(f"Unknown option type: {option_type!r}")


def archive_scan(rows, option_type="put", scan_date=None, archive_dir=SCAN_ARCHIVE_DIR):
    """Persist a compact snapshot of one scan for later grading."""
    option_type = checked_option_type(option_type)
    scan_date = scan_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    directory = os.path.join(archive_dir, option_type)
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{scan_date}.json")

    payload = {
        "scan_date": scan_date,
        "option_type": option_type,
        "candidates": [
            {field: row.get(field) for field in ARCHIVED_FIELDS if field in row}
            for row in rows
        ],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    return path


def _trade_key(scan_date, row):
    return "|".join(
        str(x)
        for x in (
            scan_date,
            row.get("Symbol"),
            row.get("Strike"),
            row.get("Expiration"),
        )
    )


def _contract_key(trade):
    return (trade.get("Symbol"), trade.get("Strike"), trade.get("Expiration"))


def normalize_bars(quotes):
    """``[(YYYY-MM-DD, close), ...]`` sorted by date from raw API quotes."""
    bars = {}
    for quote in quotes or []:
        raw_date = quote.get("date") or quote.get("Date")
        close = quote.get("close") if quote.get("close") is not None else quote.get("Close")
        if not raw_date or close is None:
            continue
        bars[str(raw_date)[:10]] = float(close)
    return sorted(bars.items())


def parse_splits(events):
    """``[(YYYY-MM-DD, ratio), ...]`` from the history API ``events.splits``."""
    splits = []
    raw = (events or {}).get("splits") if isinstance(events, dict) else None
    if isinstance(raw, dict):
        raw = list(raw.values())
    for item in raw or []:
        if not isinstance(item, dict) or not item.get("date"):
            continue
        try:
            ratio = float(item.get("numerator")) / float(item.get("denominator"))
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        if ratio > 0 and not math.isclose(ratio, 1.0):
            splits.append((str(item["date"])[:10], ratio))
    return splits


def split_factor(splits, scan_date):
    """Multiplier that converts today's split-adjusted prices into scan-date shares."""
    factor = 1.0
    for split_date, ratio in splits or []:
        if split_date > scan_date:
            factor *= ratio
    return factor


def _days_between(start, end):
    return (datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")).days


def _exit_signal(price, credit, profit_target, stop_loss):
    if price is None:
        return None
    if profit_target and price <= credit * (1.0 - profit_target):
        return "target"
    if stop_loss and price >= credit * (1.0 + stop_loss):
        return "stop"
    return None


def _managed_exit(
    path,
    strike,
    credit,
    implied_vol,
    expiration,
    option_type,
    profit_target,
    stop_loss,
    risk_free_rate,
    not_before=None,
):
    """First day the repriced option hits the profit target or the stop."""
    if not implied_vol or implied_vol <= 0 or not (profit_target or stop_loss):
        return None
    for day, spot in path:
        if day >= expiration:
            break
        if not_before and day <= not_before:
            continue
        t_years = _days_between(day, expiration) / 365.0
        price = black_scholes_price(
            spot, strike, implied_vol, t_years, risk_free_rate, option_type=option_type
        )
        reason = _exit_signal(price, credit, profit_target, stop_loss)
        if reason:
            return reason, day, price
    return None


def _settlement(option_type, strike, settle_price, path):
    """``(intrinsic value at expiry, strike touched before expiry)``."""
    if option_type == "call":
        worst = max((close for _, close in path), default=None)
        return max(settle_price - strike, 0.0), worst is not None and worst > strike
    worst = min((close for _, close in path), default=None)
    return max(strike - settle_price, 0.0), worst is not None and worst < strike


def _round_per_contract(value):
    return round(value * 100.0, 2) if value is not None else None


def _collateral_per_share(row, strike, net_premium, option_type):
    collateral_base = strike if option_type != "call" else row.get("Price") or strike
    return max(collateral_base - net_premium, 0.01)


def grade_trade(
    row,
    scan_date,
    bars,
    option_type="put",
    splits=None,
    profit_target=DEFAULT_PROFIT_TARGET,
    stop_loss=DEFAULT_STOP_LOSS,
    exit_commission=DEFAULT_EXIT_COMMISSION,
    risk_free_rate=DEFAULT_RISK_FREE_RATE,
):
    """Compute the realised outcome of one archived candidate.

    ``bars`` is the output of :func:`normalize_bars` (split-adjusted closes as
    returned today) and ``splits`` the split events after ``scan_date``.
    """
    expiration = row.get("Expiration")
    strike = row.get("Strike")
    net_premium = row.get("NetPremium") or row.get("Premium")
    if not expiration or not strike or not net_premium:
        return None

    factor = split_factor(splits, scan_date)
    path = [(day, close * factor) for day, close in bars if scan_date < day <= expiration]
    settled = [(day, close) for day, close in bars if day <= expiration]
    if not settled:
        return None
    settle_date, settle_price = settled[-1][0], settled[-1][1] * factor
    intrinsic, breached = _settlement(option_type, strike, settle_price, path)

    pnl_per_share = net_premium - intrinsic
    exit_reason, exit_date, managed_pnl = "expiry", settle_date, pnl_per_share
    # Constant IV drops the event variance that props the option up until the
    # report, so pre-earnings exits would be fictional: only manage after it.
    earnings_date = row.get("NextEarnings")
    if row.get("EarningsBeforeExpiry") and not earnings_date:
        exit_reason, exit_date, managed_pnl = "unmodeled", None, None
    else:
        managed = _managed_exit(
            path,
            strike,
            net_premium,
            row.get("ImpliedVolatility"),
            expiration,
            option_type,
            profit_target,
            stop_loss,
            risk_free_rate,
            not_before=earnings_date if row.get("EarningsBeforeExpiry") else None,
        )
        if managed:
            exit_reason, exit_date, buyback = managed
            managed_pnl = net_premium - buyback - exit_commission / 100.0

    collateral = _collateral_per_share(row, strike, net_premium, option_type)
    dte = row.get("DTE") or 1
    ev = row.get("EV")

    return {
        "ScanDate": scan_date,
        "Symbol": row.get("Symbol"),
        "Status": row.get("Status"),
        "Strike": strike,
        "Expiration": expiration,
        "SettleDate": settle_date,
        "SettlePrice": round(settle_price, 2),
        "SplitFactor": factor,
        "NetPremium": net_premium,
        "Assigned": intrinsic > 0,
        "TouchedStrike": bool(breached),
        "PnLPerContract": round(pnl_per_share * 100.0, 2),
        "ReturnOnCollateralPct": round(pnl_per_share / collateral * 100.0, 3),
        "AnnualizedReturnPct": round(pnl_per_share / collateral * (365.0 / dte) * 100.0, 2),
        "ManagedExit": exit_reason,
        "ManagedExitDate": exit_date,
        "ManagedPnLPerContract": _round_per_contract(managed_pnl),
        "Score": row.get("Score"),
        "Delta": row.get("Delta"),
        "IVRank": row.get("IVRank"),
        "IVHVPercentile": row.get("IVHVPercentile"),
        "SigmaDistance": row.get("SigmaDistance"),
        "PoP": row.get("PoP"),
        "EV": ev,
        "ModelEVPerContract": _round_per_contract(ev),
        "EarningsBeforeExpiry": row.get("EarningsBeforeExpiry"),
        "NextEarnings": row.get("NextEarnings"),
        "EarningsMovePct": row.get("EarningsMovePct"),
    }


def _bucket(value, edges, labels):
    if value is None:
        return "unknown"
    for edge, label in zip(edges, labels):
        if value < edge:
            return label
    return labels[-1]


def _mean(values):
    values = [v for v in values if v is not None]
    return round(sum(values) / len(values), 2) if values else None


def unique_contracts(trades):
    """Keep the earliest scan of each contract.

    Daily scans re-list the same contract; counting every sighting would weight
    long-lived contracts many times and overstate sample sizes.
    """
    first = {}
    for trade in sorted(trades, key=lambda t: t["ScanDate"]):
        first.setdefault(_contract_key(trade), trade)
    return list(first.values())


def summarize(trades):
    """Aggregate unique graded contracts overall and by the screener's features."""
    if not trades:
        return {"trade_count": 0}
    unique = unique_contracts(trades)

    def agg(subset):
        if not subset:
            return None
        n = len(subset)
        managed = [t for t in subset if t.get("ManagedPnLPerContract") is not None]
        exits = {}
        for t in managed:
            exits[t.get("ManagedExit")] = exits.get(t.get("ManagedExit"), 0) + 1
        return {
            "trades": n,
            "win_rate_pct": round(sum(1 for t in subset if t["PnLPerContract"] > 0) / n * 100.0, 1),
            "model_pop_pct": _mean([t.get("PoP") for t in subset]),
            "assignment_rate_pct": round(sum(1 for t in subset if t["Assigned"]) / n * 100.0, 1),
            "avg_pnl_per_contract": _mean([t["PnLPerContract"] for t in subset]),
            "model_ev_per_contract": _mean([t.get("ModelEVPerContract") for t in subset]),
            "avg_return_on_collateral_pct": round(
                sum(t["ReturnOnCollateralPct"] for t in subset) / n, 3
            ),
            "avg_annualized_return_pct": _mean([t["AnnualizedReturnPct"] for t in subset]),
            "managed_trades": len(managed),
            "managed_win_rate_pct": round(
                sum(1 for t in managed if t["ManagedPnLPerContract"] > 0) / len(managed) * 100.0, 1
            )
            if managed
            else None,
            "avg_managed_pnl_per_contract": _mean([t["ManagedPnLPerContract"] for t in managed]),
            "managed_exits": exits,
        }

    def group(key_fn):
        groups = {}
        for trade in unique:
            groups.setdefault(key_fn(trade), []).append(trade)
        return {k: agg(v) for k, v in sorted(groups.items(), key=lambda kv: str(kv[0]))}

    return {
        "trade_count": len(trades),
        "unique_contracts": len(unique),
        "overall": agg(unique),
        "by_status": group(lambda t: t.get("Status") or "unknown"),
        "by_score_bucket": group(
            lambda t: _bucket(t.get("Score"), [40, 55, 70], ["<40", "40-55", "55-70", ">=70"])
        ),
        "by_abs_delta_bucket": group(
            lambda t: _bucket(
                abs(t["Delta"]) if t.get("Delta") is not None else None,
                [0.10, 0.20, 0.30],
                ["<0.10", "0.10-0.20", "0.20-0.30", ">=0.30"],
            )
        ),
        "by_iv_rank_bucket": group(
            lambda t: _bucket(
                t.get("IVRank") if t.get("IVRank") is not None else t.get("IVHVPercentile"),
                [0.3, 0.5, 0.7],
                ["<0.3", "0.3-0.5", "0.5-0.7", ">=0.7"],
            )
        ),
        "by_sigma_distance_bucket": group(
            lambda t: _bucket(
                t.get("SigmaDistance"), [1.0, 1.5, 2.0], ["<1.0", "1.0-1.5", "1.5-2.0", ">=2.0"]
            )
        ),
        "by_earnings_before_expiry": group(
            lambda t: "earnings" if t.get("EarningsBeforeExpiry") else "no_earnings"
        ),
    }


def _fetch_history(symbol, from_date, to_date):
    """Return ``(bars, splits)``; ``to_date`` must be today to see every split."""
    from .analysis import HIST_URL, safe_get  # local import avoids a cycle

    url = f"{HIST_URL}?ticker={symbol}&from={from_date}&to={to_date}&interval=1d"
    data = safe_get(url)
    events = None
    if isinstance(data, dict):
        events = data.get("events")
        data = data.get("quotes") or data.get("prices") or []
    quotes = data if isinstance(data, list) else []
    return normalize_bars(quotes), parse_splits(events)


def _load_previous(output_path):
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _previous_grades(output_path, regrade):
    previous = {} if regrade else _load_previous(output_path)
    if previous and previous.get("grader_version") != GRADER_VERSION:
        print("Grading logic changed since the last run; regrading every trade.")
        previous = {}
    return previous


def _is_expired(row, today):
    expiration = row.get("Expiration")
    if not expiration:
        return False
    try:
        expiry_date = datetime.strptime(expiration, "%Y-%m-%d").date()
    except ValueError:
        return False
    return expiry_date < today


def _pending_trades(directory, known_keys, today):
    """``[(scan_date, row), ...]`` of archived contracts expired but not graded."""
    pending = []
    for filename in sorted(os.listdir(directory)):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(directory, filename), "r", encoding="utf-8") as f:
            scan = json.load(f)
        scan_date = scan.get("scan_date") or filename[:-5]
        pending.extend(
            (scan_date, row)
            for row in scan.get("candidates", [])
            if _is_expired(row, today) and _trade_key(scan_date, row) not in known_keys
        )
    return pending


def _grade_pending(pending, today, option_type, fetch_history, profit_target, stop_loss):
    by_symbol = {}
    for scan_date, row in pending:
        by_symbol.setdefault(row.get("Symbol"), []).append((scan_date, row))

    graded = []
    to_date = today.strftime("%Y-%m-%d")
    for index, (symbol, items) in enumerate(sorted(by_symbol.items()), 1):
        from_date = min(scan_date for scan_date, _ in items)
        print(f"[{index}/{len(by_symbol)}] Grading {symbol:<10}", end="\r", flush=True)
        bars, splits = fetch_history(symbol, from_date, to_date)
        if not bars:
            continue
        for scan_date, row in items:
            trade = grade_trade(
                row,
                scan_date,
                bars,
                option_type,
                splits=splits,
                profit_target=profit_target,
                stop_loss=stop_loss,
            )
            if trade:
                graded.append(trade)
    return graded


def _print_overall(overall):
    print(
        f"{overall['trades']} unique contracts | win rate {overall['win_rate_pct']}% "
        f"(model PoP {overall['model_pop_pct']}%) | held P/L "
        f"${overall['avg_pnl_per_contract']}/contract (model EV ${overall['model_ev_per_contract']}) | "
        f"managed P/L ${overall['avg_managed_pnl_per_contract']}/contract "
        f"({overall['managed_trades']} modeled)"
    )


def evaluate(
    option_type="put",
    archive_dir=SCAN_ARCHIVE_DIR,
    output_dir=DATA_OUTPUT_DIR,
    regrade=False,
    profit_target=DEFAULT_PROFIT_TARGET,
    stop_loss=DEFAULT_STOP_LOSS,
    fetch_history=None,
):
    """Grade every archived candidate whose expiry has passed."""
    option_type = checked_option_type(option_type)
    fetch_history = fetch_history or _fetch_history
    directory = os.path.join(archive_dir, option_type)
    if not os.path.isdir(directory):
        print(f"No scan archive found at {directory}; nothing to evaluate yet.")
        return None

    output_path = os.path.join(output_dir, f"outcomes_{option_type}.json")
    graded = _previous_grades(output_path, regrade).get("trades", [])
    known_keys = {_trade_key(t["ScanDate"], t) for t in graded}

    today = datetime.now(timezone.utc).date()
    pending = _pending_trades(directory, known_keys, today)
    if not pending:
        print(f"No newly expired {option_type} candidates to grade.")
    else:
        graded.extend(
            _grade_pending(pending, today, option_type, fetch_history, profit_target, stop_loss)
        )

    graded.sort(key=lambda t: (t["Expiration"], t["Symbol"], t["ScanDate"]))
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "grader_version": GRADER_VERSION,
        "option_type": option_type,
        "management": {"profit_target": profit_target, "stop_loss": stop_loss},
        "summary": summarize(graded),
        "trades": graded,
    }
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"\nGraded {len(graded)} {option_type} trades -> {output_path}")

    overall = payload["summary"].get("overall")
    if overall:
        _print_overall(overall)
    return payload


def main():
    parser = argparse.ArgumentParser(description="Grade archived option scans.")
    parser.add_argument("--type", dest="option_type", choices=["put", "call"], default="put")
    parser.add_argument(
        "--regrade", action="store_true", help="Ignore cached grades and regrade every trade."
    )
    parser.add_argument(
        "--profit-target",
        type=float,
        default=DEFAULT_PROFIT_TARGET,
        help="Managed exit: share of the credit captured before buying back (0 disables).",
    )
    parser.add_argument(
        "--stop-loss",
        type=float,
        default=DEFAULT_STOP_LOSS,
        help="Managed exit: loss as a multiple of the credit that triggers a stop (0 disables).",
    )
    args = parser.parse_args()
    evaluate(
        args.option_type,
        regrade=args.regrade,
        profit_target=args.profit_target,
        stop_loss=args.stop_loss,
    )


if __name__ == "__main__":
    main()
