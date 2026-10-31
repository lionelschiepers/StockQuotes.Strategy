import json

import pytest

from options_wheel import outcomes
from options_wheel.outcomes import grade_trade, parse_splits, split_factor, unique_contracts

SCAN_DATE = "2026-01-02"
EXPIRATION = "2026-01-30"


def _put_row(**overrides):
    row = {
        "Symbol": "AAA",
        "Status": "PASS",
        "Strike": 90.0,
        "Expiration": EXPIRATION,
        "DTE": 28,
        "NetPremium": 1.0,
        "ImpliedVolatility": 0.30,
        "EV": 0.20,
        "EarningsBeforeExpiry": False,
    }
    row.update(overrides)
    return row


def test_split_is_converted_back_to_scan_date_shares():
    splits = parse_splits(
        {
            "splits": {
                "a": {"date": "2025-06-01", "numerator": 3, "denominator": 1},
                "b": {"date": "2026-01-15", "numerator": 2, "denominator": 1},
            }
        }
    )
    assert split_factor(splits, SCAN_DATE) == 2.0

    # Split-adjusted closes: 48 today is 96 in scan-date shares, above the 90 strike.
    bars = [(SCAN_DATE, 50.0), ("2026-01-20", 49.0), (EXPIRATION, 48.0)]
    trade = grade_trade(_put_row(), SCAN_DATE, bars, "put", splits=splits)
    assert trade["SplitFactor"] == 2.0
    assert trade["SettlePrice"] == 96.0
    assert trade["Assigned"] is False
    assert trade["PnLPerContract"] == 100.0


def test_managed_exit_takes_profit_target():
    bars = [(SCAN_DATE, 100.0), ("2026-01-05", 120.0), (EXPIRATION, 121.0)]
    trade = grade_trade(_put_row(), SCAN_DATE, bars, "put")
    assert trade["ManagedExit"] == "target"
    assert trade["ManagedExitDate"] == "2026-01-05"
    assert 99.0 < trade["ManagedPnLPerContract"] < trade["PnLPerContract"] == 100.0


def test_managed_exit_stops_out_even_if_trade_recovers():
    bars = [(SCAN_DATE, 100.0), ("2026-01-05", 80.0), (EXPIRATION, 95.0)]
    trade = grade_trade(_put_row(), SCAN_DATE, bars, "put")
    assert trade["PnLPerContract"] == 100.0
    assert trade["TouchedStrike"] is True
    assert trade["ManagedExit"] == "stop"
    assert trade["ManagedPnLPerContract"] < -200.0


def test_earnings_trades_are_only_managed_after_the_report():
    bars = [(SCAN_DATE, 100.0), ("2026-01-05", 120.0), ("2026-01-12", 120.0), (EXPIRATION, 121.0)]

    unknown = grade_trade(_put_row(EarningsBeforeExpiry=True), SCAN_DATE, bars, "put")
    assert unknown["ManagedExit"] == "unmodeled"
    assert unknown["ManagedPnLPerContract"] is None
    assert unknown["PnLPerContract"] == 100.0

    known = grade_trade(
        _put_row(EarningsBeforeExpiry=True, NextEarnings="2026-01-10"), SCAN_DATE, bars, "put"
    )
    assert known["ManagedExit"] == "target"
    assert known["ManagedExitDate"] == "2026-01-12"


def test_unique_contracts_keeps_earliest_scan():
    trades = [
        {"ScanDate": "2026-01-05", "Symbol": "AAA", "Strike": 90.0, "Expiration": EXPIRATION},
        {"ScanDate": "2026-01-02", "Symbol": "AAA", "Strike": 90.0, "Expiration": EXPIRATION},
        {"ScanDate": "2026-01-05", "Symbol": "AAA", "Strike": 85.0, "Expiration": EXPIRATION},
    ]
    unique = unique_contracts(trades)
    assert len(unique) == 2
    assert {t["ScanDate"] for t in unique if t["Strike"] == 90.0} == {"2026-01-02"}


def test_evaluate_grades_expired_archive_and_skips_known_trades(tmp_path):
    archive_dir = tmp_path / "scans"
    output_dir = tmp_path / "output"
    (archive_dir / "put").mkdir(parents=True)
    for scan_date in (SCAN_DATE, "2026-01-05"):
        (archive_dir / "put" / f"{scan_date}.json").write_text(
            json.dumps({"scan_date": scan_date, "candidates": [_put_row(PoP=85.0)]}),
            encoding="utf-8",
        )

    calls = []

    def fake_history(symbol, from_date, to_date):
        calls.append((symbol, from_date))
        return [(SCAN_DATE, 100.0), ("2026-01-06", 101.0), (EXPIRATION, 102.0)], []

    payload = outcomes.evaluate("put", str(archive_dir), str(output_dir), fetch_history=fake_history)
    assert calls == [("AAA", SCAN_DATE)]
    assert len(payload["trades"]) == 2
    overall = payload["summary"]["overall"]
    assert overall["trades"] == 1
    assert overall["win_rate_pct"] == 100.0
    assert overall["model_pop_pct"] == pytest.approx(85.0)

    calls.clear()
    again = outcomes.evaluate("put", str(archive_dir), str(output_dir), fetch_history=fake_history)
    assert calls == []
    assert len(again["trades"]) == 2
