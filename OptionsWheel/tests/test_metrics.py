import math
from datetime import datetime, timezone

import pytest

from options_wheel.metrics import (
    credit_risk_ratio,
    expected_itm_payoff,
    forecast_volatility,
    net_credit,
    option_delta,
    probability_of_profit,
    probability_otm,
    sigma_distance,
    simple_yields,
    variance_risk_premium,
    years_to_expiration,
)


def test_option_delta_and_probability_sanity():
    now_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
    expiry_dt = datetime(2026, 2, 1, tzinfo=timezone.utc)
    t_years = years_to_expiration(expiry_dt, now_dt)

    put_delta = option_delta(100.0, 95.0, 0.25, t_years, 0.045, 0.0, option_type="put")
    call_delta = option_delta(100.0, 105.0, 0.25, t_years, 0.045, 0.0, option_type="call")

    assert put_delta is not None and -1.0 < put_delta < 0.0
    assert call_delta is not None and 0.0 < call_delta < 1.0

    prob_otm = probability_otm(100.0, 95.0, 0.20, t_years, 0.0, option_type="put")
    assert prob_otm is not None and 0.0 < prob_otm < 1.0


def test_expected_itm_payoff_matches_discounted_black_scholes_call_value():
    spot = 100.0
    strike = 105.0
    sigma = 0.20
    t_years = 30.0 / 365.0
    risk_free_rate = 0.05

    undiscounted_payoff = expected_itm_payoff(
        spot, strike, sigma, t_years, risk_free_rate, option_type="call"
    )
    discounted_value = undiscounted_payoff * math.exp(-risk_free_rate * t_years)

    assert discounted_value == pytest.approx(0.66, abs=0.08)


def test_probability_of_profit_improves_with_more_credit():
    t_years = 30.0 / 365.0

    low_credit = probability_of_profit(100.0, 95.0, 0.20, t_years, 0.0, 0.75, option_type="put")
    high_credit = probability_of_profit(100.0, 95.0, 0.20, t_years, 0.0, 1.50, option_type="put")

    assert low_credit is not None and high_credit is not None
    assert high_credit > low_credit


def test_net_credit_simple_yields_and_credit_risk_ratio():
    mid, net, spread_pct = net_credit(
        1.00,
        1.20,
        commission_per_contract=0.65,
        slippage_pct_of_spread=30.0,
    )

    assert mid == pytest.approx(1.10)
    assert net == pytest.approx(1.0635)
    assert spread_pct == pytest.approx(18.1818, rel=1e-3)

    monthly, annualized = simple_yields(net, 48.9365, 30)
    assert monthly == pytest.approx(net / 48.9365 * 100.0)
    assert annualized == pytest.approx(monthly * (365.0 / 30.0), rel=1e-9)

    assert credit_risk_ratio(net, 0.80) == pytest.approx(net / 0.80)
    assert credit_risk_ratio(net, 0.0) is None


def test_forecast_vol_vrp_and_sigma_distance():
    forecast = forecast_volatility(0.40, 0.22, 0.18, hv_weight=0.5, iv_haircut=0.85)
    assert forecast == pytest.approx((0.20 + 0.34) / 2.0)

    vrp = variance_risk_premium(0.40, forecast)
    assert vrp is not None and vrp > 1.0

    t_years = 45.0 / 365.0
    put_sigma = sigma_distance(100.0, 90.0, 0.25, t_years, option_type="put")
    call_sigma = sigma_distance(100.0, 110.0, 0.25, t_years, option_type="call")
    assert put_sigma is not None and put_sigma > 0
    assert call_sigma is not None and call_sigma > 0
