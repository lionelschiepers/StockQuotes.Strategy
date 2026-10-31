"""Historical earnings moves, used to price the jump risk of an upcoming report.

A short option whose life spans an earnings release carries a one-off gap risk
that trailing realised volatility does not contain. Back-testing the archived
scans showed those trades were the screener's worst: the model rated them as
its best (high premium) while they realised the largest losses. This module
turns the stock's own past earnings reactions into a typical jump size that
:func:`options_wheel.metrics.forecast_volatility` adds to the forecast.

Everything here is pure; fetching lives with the callers.
"""

from __future__ import annotations

import math
from datetime import date, datetime

DEFAULT_MAX_REPORTS = 4
MIN_REPORTS = 2
TRADING_DAYS = 252


def _to_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and len(value) >= 10:
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def report_dates_from_summary(payload):
    """Extract past report dates from a ``yahoo-finance-summary?modules=earnings`` payload."""
    if not isinstance(payload, dict):
        return []
    chart = (payload.get("earnings") or {}).get("earningsChart") or {}
    dates = {_to_date(item.get("reportedDate")) for item in chart.get("quarterly") or [] if isinstance(item, dict)}
    return sorted(d for d in dates if d is not None)


def earnings_moves(report_dates, closes, before=None, max_reports=DEFAULT_MAX_REPORTS):
    """Log return from the last close *before* each report to the first close *after* it.

    The two-session window captures the reaction whether the company reported
    before the open or after the close. ``closes`` is an iterable of
    ``(date, close)``; only reports strictly before ``before`` are used.
    """
    bars = sorted(
        (d, float(c)) for d, c in ((_to_date(d), c) for d, c in closes) if d is not None and c and float(c) > 0
    )
    cutoff = _to_date(before) if before is not None else None
    reports = [d for d in report_dates if cutoff is None or d < cutoff][-max_reports:]

    moves = []
    for report in reports:
        pre = next((c for d, c in reversed(bars) if d < report), None)
        post = next((c for d, c in bars if d > report), None)
        if pre and post:
            moves.append(math.log(post / pre))
    return moves


def jump_size(moves, annual_vol=None):
    """Typical earnings jump (log move) net of the normal two-day diffusion.

    ``sqrt(mean(move^2) - 2 * annual_vol^2 / 252)``. Returns ``None`` with
    fewer than two observations.
    """
    if len(moves) < MIN_REPORTS:
        return None
    mean_square = sum(m * m for m in moves) / len(moves)
    daily_var = (annual_vol**2 / TRADING_DAYS) if annual_vol else 0.0
    return math.sqrt(max(mean_square - 2.0 * daily_var, 0.0))


def move_stats(moves):
    """Summary of past reactions in percent, for reports."""
    if not moves:
        return None
    pct = [(math.exp(m) - 1.0) * 100.0 for m in moves]
    abs_pct = [abs(p) for p in pct]
    return {
        "count": len(pct),
        "avg_abs_move_pct": round(sum(abs_pct) / len(abs_pct), 2),
        "max_abs_move_pct": round(max(abs_pct), 2),
        "avg_signed_move_pct": round(sum(pct) / len(pct), 2),
        "rms_move_pct": round(math.sqrt(sum(p * p for p in pct) / len(pct)), 2),
    }
