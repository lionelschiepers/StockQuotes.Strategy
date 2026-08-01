"""Portfolio construction on top of the ranked contract list.

The ranked table answers "which contract is attractive?", not "which basket
should I actually sell?". Selling the five best-scoring contracts is usually
selling the same trade five times: same sector, same volatility regime, same
macro driver. This module applies the constraints that matter for the realised
risk-adjusted return of the account:

* one position per underlying,
* a cap per sector (optional, needs ``data/input/sectors.json``),
* a total collateral budget and a per-position share of it,
* a cap on the number of concurrent positions.
"""

from __future__ import annotations

import json
import os

CONTRACT_MULTIPLIER = 100.0


def load_sector_map(path):
    """Optional ``{"AAPL": "Technology", ...}`` mapping.

    Yahoo's quote endpoint does not return a sector, so the mapping is supplied
    by the user. When the file is absent the sector constraint is skipped.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k).upper(): str(v) for k, v in data.items() if v}


def contract_collateral(row, option_type="put"):
    """Capital required to hold one contract."""
    net_premium = row.get("NetPremium") or row.get("Premium") or 0.0
    if option_type == "call":
        base = row.get("Price")
    else:
        base = row.get("Strike")
    if not base:
        return None
    return max((base - net_premium) * CONTRACT_MULTIPLIER, CONTRACT_MULTIPLIER)


def build_portfolio(rows, config, option_type="put", sector_map=None):
    """Greedily pick the best-scoring contracts that satisfy the constraints.

    ``rows`` must be the PASS rows already sorted best first. Returns a dict with
    the selected positions and the aggregate exposure.
    """
    sector_map = sector_map or {}
    max_positions = int(config.get("PORTFOLIO_MAX_POSITIONS", 10) or 0)
    max_per_sector = int(config.get("PORTFOLIO_MAX_PER_SECTOR", 2) or 0)
    budget = float(config.get("PORTFOLIO_COLLATERAL_BUDGET", 0.0) or 0.0)
    max_pct = float(config.get("PORTFOLIO_MAX_PCT_PER_POSITION", 25.0) or 0.0)

    per_position_cap = budget * (max_pct / 100.0) if budget > 0 and max_pct > 0 else None

    selected = []
    used_symbols = set()
    sector_counts = {}
    total_collateral = 0.0
    total_credit = 0.0
    skipped = {"duplicate_symbol": 0, "sector_cap": 0, "position_cap": 0, "budget": 0}

    for row in rows:
        if max_positions and len(selected) >= max_positions:
            skipped["position_cap"] += 1
            continue

        symbol = row.get("Symbol")
        if symbol in used_symbols:
            skipped["duplicate_symbol"] += 1
            continue

        sector = sector_map.get(str(symbol).upper())
        if sector and max_per_sector and sector_counts.get(sector, 0) >= max_per_sector:
            skipped["sector_cap"] += 1
            continue

        collateral = contract_collateral(row, option_type)
        if collateral is None:
            continue
        if per_position_cap and collateral > per_position_cap:
            skipped["budget"] += 1
            continue
        if budget > 0 and total_collateral + collateral > budget:
            skipped["budget"] += 1
            continue

        credit = (row.get("NetPremium") or row.get("Premium") or 0.0) * CONTRACT_MULTIPLIER
        used_symbols.add(symbol)
        if sector:
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
        total_collateral += collateral
        total_credit += credit

        selected.append(
            {
                "Symbol": symbol,
                "Sector": sector,
                "Strike": row.get("Strike"),
                "Expiration": row.get("Expiration"),
                "DTE": row.get("DTE"),
                "NetPremium": row.get("NetPremium"),
                "Collateral": round(collateral, 2),
                "Credit": round(credit, 2),
                "MonthlyYieldPct": row.get("MonthlyYieldPct"),
                "PoP": row.get("PoP"),
                "EV": row.get("EV"),
                "Score": row.get("Score"),
            }
        )

    weighted_yield = None
    if total_collateral > 0:
        weighted_yield = sum(
            (p["MonthlyYieldPct"] or 0.0) * p["Collateral"] for p in selected
        ) / total_collateral

    return {
        "positions": selected,
        "position_count": len(selected),
        "total_collateral": round(total_collateral, 2),
        "total_credit": round(total_credit, 2),
        "weighted_monthly_yield_pct": round(weighted_yield, 3) if weighted_yield else None,
        "sector_map_available": bool(sector_map),
        "skipped": skipped,
        "constraints": {
            "PORTFOLIO_MAX_POSITIONS": max_positions,
            "PORTFOLIO_MAX_PER_SECTOR": max_per_sector,
            "PORTFOLIO_COLLATERAL_BUDGET": budget,
            "PORTFOLIO_MAX_PCT_PER_POSITION": max_pct,
        },
    }
