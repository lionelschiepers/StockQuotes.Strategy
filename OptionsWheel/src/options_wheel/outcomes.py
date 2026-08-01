"""Feedback loop: archive every scan and grade it once the contracts expire.

Without this the screener is unfalsifiable - the scoring weights are guesses and
nothing ever tells you whether a filter helped or hurt. Each run archives the
ranked candidates; ``evaluate`` then replays the archive against actual price
history and reports the realised hit rate and P/L per contract, bucketed by the
features the screener ranks on (score, delta, IV rank, sigma distance). Those
buckets are what you calibrate the config against.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(MODULE_DIR, "..", ".."))
DATA_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "output")
HISTORY_DIR = os.path.join(PROJECT_ROOT, "data", "history")
SCAN_ARCHIVE_DIR = os.path.join(HISTORY_DIR, "scans")

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
    "ImpliedVolatility",
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
)


def archive_scan(rows, option_type="put", scan_date=None, archive_dir=SCAN_ARCHIVE_DIR):
    """Persist a compact snapshot of one scan for later grading."""
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


def _close_on_or_before(quotes, target_date):
    best = None
    for quote in quotes:
        raw_date = quote.get("date") or quote.get("Date")
        close = quote.get("close") or quote.get("Close")
        if not raw_date or close is None:
            continue
        day = str(raw_date)[:10]
        if day <= target_date and (best is None or day > best[0]):
            best = (day, float(close))
    return best


def _extreme_close(quotes, start_date, end_date, kind="min"):
    values = []
    for quote in quotes:
        raw_date = quote.get("date") or quote.get("Date")
        close = quote.get("close") or quote.get("Close")
        if not raw_date or close is None:
            continue
        day = str(raw_date)[:10]
        if start_date <= day <= end_date:
            values.append(float(close))
    if not values:
        return None
    return min(values) if kind == "min" else max(values)


def _fetch_history(symbol, from_date, to_date):
    from .analysis import HIST_URL, safe_get  # local import avoids a cycle

    url = f"{HIST_URL}?ticker={symbol}&from={from_date}&to={to_date}&interval=1d"
    data = safe_get(url)
    if isinstance(data, dict):
        data = data.get("quotes") or data.get("prices") or []
    return data if isinstance(data, list) else []


def grade_trade(row, scan_date, quotes, option_type="put"):
    """Compute the realised outcome of one archived candidate."""
    expiration = row.get("Expiration")
    strike = row.get("Strike")
    net_premium = row.get("NetPremium") or row.get("Premium")
    if not expiration or not strike or not net_premium:
        return None

    settle = _close_on_or_before(quotes, expiration)
    if settle is None:
        return None
    settle_date, settle_price = settle

    if option_type == "call":
        intrinsic = max(settle_price - strike, 0.0)
        worst = _extreme_close(quotes, scan_date, expiration, kind="max")
        breached = worst is not None and worst > strike
    else:
        intrinsic = max(strike - settle_price, 0.0)
        worst = _extreme_close(quotes, scan_date, expiration, kind="min")
        breached = worst is not None and worst < strike

    pnl_per_share = net_premium - intrinsic
    collateral = max(
        (strike if option_type != "call" else row.get("Price") or strike) - net_premium,
        0.01,
    )
    dte = row.get("DTE") or 1

    return {
        "ScanDate": scan_date,
        "Symbol": row.get("Symbol"),
        "Status": row.get("Status"),
        "Strike": strike,
        "Expiration": expiration,
        "SettleDate": settle_date,
        "SettlePrice": round(settle_price, 2),
        "NetPremium": net_premium,
        "Assigned": intrinsic > 0,
        "TouchedStrike": bool(breached),
        "PnLPerContract": round(pnl_per_share * 100.0, 2),
        "ReturnOnCollateralPct": round(pnl_per_share / collateral * 100.0, 3),
        "AnnualizedReturnPct": round(pnl_per_share / collateral * (365.0 / dte) * 100.0, 2),
        "Score": row.get("Score"),
        "Delta": row.get("Delta"),
        "IVRank": row.get("IVRank"),
        "IVHVPercentile": row.get("IVHVPercentile"),
        "SigmaDistance": row.get("SigmaDistance"),
        "PoP": row.get("PoP"),
        "EV": row.get("EV"),
        "EarningsBeforeExpiry": row.get("EarningsBeforeExpiry"),
    }


def _bucket(value, edges, labels):
    if value is None:
        return "unknown"
    for edge, label in zip(edges, labels):
        if value < edge:
            return label
    return labels[-1]


def summarize(trades):
    """Aggregate graded trades overall and by the screener's ranking features."""
    if not trades:
        return {"trade_count": 0}

    def agg(subset):
        if not subset:
            return None
        wins = sum(1 for t in subset if t["PnLPerContract"] > 0)
        return {
            "trades": len(subset),
            "win_rate_pct": round(wins / len(subset) * 100.0, 1),
            "assignment_rate_pct": round(
                sum(1 for t in subset if t["Assigned"]) / len(subset) * 100.0, 1
            ),
            "avg_pnl_per_contract": round(
                sum(t["PnLPerContract"] for t in subset) / len(subset), 2
            ),
            "avg_return_on_collateral_pct": round(
                sum(t["ReturnOnCollateralPct"] for t in subset) / len(subset), 3
            ),
            "avg_annualized_return_pct": round(
                sum(t["AnnualizedReturnPct"] for t in subset) / len(subset), 2
            ),
        }

    def group(key_fn):
        groups = {}
        for trade in trades:
            groups.setdefault(key_fn(trade), []).append(trade)
        return {k: agg(v) for k, v in sorted(groups.items(), key=lambda kv: str(kv[0]))}

    return {
        "trade_count": len(trades),
        "overall": agg(trades),
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


def evaluate(option_type="put", archive_dir=SCAN_ARCHIVE_DIR, output_dir=DATA_OUTPUT_DIR):
    """Grade every archived candidate whose expiry has passed."""
    directory = os.path.join(archive_dir, option_type)
    if not os.path.isdir(directory):
        print(f"No scan archive found at {directory}; nothing to evaluate yet.")
        return None

    output_path = os.path.join(output_dir, f"outcomes_{option_type}.json")
    graded = []
    known_keys = set()
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            previous = json.load(f)
        graded = previous.get("trades", [])
        known_keys = {_trade_key(t["ScanDate"], t) for t in graded}
    except (OSError, ValueError, KeyError):
        graded = []

    today = datetime.now(timezone.utc).date()
    pending = []
    for filename in sorted(os.listdir(directory)):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(directory, filename), "r", encoding="utf-8") as f:
            scan = json.load(f)
        scan_date = scan.get("scan_date") or filename[:-5]
        for row in scan.get("candidates", []):
            expiration = row.get("Expiration")
            if not expiration:
                continue
            try:
                expiry_date = datetime.strptime(expiration, "%Y-%m-%d").date()
            except ValueError:
                continue
            if expiry_date >= today:
                continue
            if _trade_key(scan_date, row) in known_keys:
                continue
            pending.append((scan_date, row))

    if not pending:
        print(f"No newly expired {option_type} candidates to grade.")
    else:
        by_symbol = {}
        for scan_date, row in pending:
            by_symbol.setdefault(row.get("Symbol"), []).append((scan_date, row))

        for index, (symbol, items) in enumerate(sorted(by_symbol.items()), 1):
            from_date = min(scan_date for scan_date, _ in items)
            to_date = max(row.get("Expiration") for _, row in items)
            to_date = (
                datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=5)
            ).strftime("%Y-%m-%d")
            print(f"[{index}/{len(by_symbol)}] Grading {symbol}", end="\r", flush=True)
            quotes = _fetch_history(symbol, from_date, to_date)
            if not quotes:
                continue
            for scan_date, row in items:
                trade = grade_trade(row, scan_date, quotes, option_type)
                if trade:
                    graded.append(trade)

    graded.sort(key=lambda t: (t["Expiration"], t["Symbol"]))
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "option_type": option_type,
        "summary": summarize(graded),
        "trades": graded,
    }
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"\nGraded {len(graded)} {option_type} trades -> {output_path}")

    overall = payload["summary"].get("overall")
    if overall:
        print(
            f"Win rate {overall['win_rate_pct']}% | "
            f"avg P/L ${overall['avg_pnl_per_contract']}/contract | "
            f"avg annualized ROC {overall['avg_annualized_return_pct']}%"
        )
    return payload


def main():
    parser = argparse.ArgumentParser(description="Grade archived option scans.")
    parser.add_argument("--type", dest="option_type", choices=["put", "call"], default="put")
    args = parser.parse_args()
    evaluate(args.option_type)


if __name__ == "__main__":
    main()
