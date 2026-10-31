import math
from datetime import date

import pytest

from options_wheel.earnings import earnings_moves, jump_size, report_dates_from_summary


def test_report_dates_from_summary_parses_and_sorts():
    payload = {
        "earnings": {
            "earningsChart": {
                "quarterly": [
                    {"reportedDate": "2026-07-25T20:00:00Z"},
                    {"reportedDate": "2026-04-24"},
                    {"reportedDate": None},
                ]
            }
        }
    }
    assert report_dates_from_summary(payload) == [date(2026, 4, 24), date(2026, 7, 25)]
    assert report_dates_from_summary(None) == []


def test_earnings_moves_span_report_and_respect_cutoff():
    closes = [
        ("2026-04-22", 100.0),
        ("2026-04-23", 100.0),
        ("2026-04-24", 104.0),  # report day itself is excluded from the window
        ("2026-04-27", 110.0),
        ("2026-07-24", 120.0),
        ("2026-07-28", 108.0),
    ]
    reports = [date(2026, 4, 24), date(2026, 7, 25)]

    moves = earnings_moves(reports, closes)
    assert moves == pytest.approx([math.log(110 / 100), math.log(108 / 120)])

    only_first = earnings_moves(reports, closes, before="2026-07-25")
    assert only_first == pytest.approx([math.log(110 / 100)])


def test_jump_size_needs_two_reports_and_nets_out_diffusion():
    assert jump_size([0.10]) is None

    moves = [0.10, -0.10]
    assert jump_size(moves) == pytest.approx(0.10)

    annual_vol = 0.30
    expected = math.sqrt(0.01 - 2 * annual_vol**2 / 252)
    assert jump_size(moves, annual_vol) == pytest.approx(expected)

    assert jump_size([0.001, -0.001], annual_vol=0.50) == 0.0
