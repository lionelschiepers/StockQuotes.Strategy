import math
from datetime import datetime, timezone

import pytest

from options_wheel.metrics import (
    black_scholes_price,
    composite_score,
    credit_risk_ratio,
    expected_itm_payoff,
    forecast_volatility,
    jump_adjusted_vol,
    net_credit,
    option_delta,
    probability_of_profit,
    probability_otm,
    score_components,
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

    assert put_delta is not None
    assert -1.0 < put_delta < 0.0
    assert call_delta is not None
    assert 0.0 < call_delta < 1.0

    prob_otm = probability_otm(100.0, 95.0, 0.20, t_years, 0.0, option_type="put")
    assert prob_otm is not None
    assert 0.0 < prob_otm < 1.0


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

    assert low_credit is not None
    assert high_credit is not None
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
    assert vrp is not None
    assert vrp > 1.0

    t_years = 45.0 / 365.0
    put_sigma = sigma_distance(100.0, 90.0, 0.25, t_years, option_type="put")
    call_sigma = sigma_distance(100.0, 110.0, 0.25, t_years, option_type="call")
    assert put_sigma is not None
    assert put_sigma > 0
    assert call_sigma is not None
    assert call_sigma > 0


def test_earnings_jump_adds_variance_to_realised_leg_only():
    t_years = 30.0 / 365.0
    jump = 0.08
    assert jump_adjusted_vol(0.20, jump, t_years) == pytest.approx(math.sqrt(0.04 + jump**2 / t_years))
    assert jump_adjusted_vol(0.20, None, t_years) == 0.20

    base = forecast_volatility(0.40, 0.22, 0.18, hv_weight=0.5, iv_haircut=0.85)
    with_jump = forecast_volatility(
        0.40, 0.22, 0.18, hv_weight=0.5, iv_haircut=0.85, jump_move=jump, t_years=t_years
    )
    expected_hv = jump_adjusted_vol(0.20, jump, t_years)
    assert with_jump == pytest.approx(0.5 * expected_hv + 0.5 * 0.34)
    assert with_jump > base


def test_black_scholes_price_matches_parity_and_intrinsic_at_expiry():
    t_years = 60.0 / 365.0
    rate = 0.04
    put = black_scholes_price(100.0, 95.0, 0.30, t_years, rate, option_type="put")
    call = black_scholes_price(100.0, 95.0, 0.30, t_years, rate, option_type="call")
    assert call - put == pytest.approx(100.0 - 95.0 * math.exp(-rate * t_years), abs=1e-9)

    assert black_scholes_price(90.0, 95.0, 0.30, 0.0, option_type="put") == pytest.approx(5.0)
    assert black_scholes_price(90.0, 95.0, 0.30, 0.0, option_type="call") == 0.0


def test_score_components_are_bounded_and_yield_is_capped():
    components = score_components(
        sigma_dist=3.0,
        implied_vol=1.5,
        monthly_yield_pct=5.0,
        target_yield_pct=1.2,
        dte=30,
        min_dte=20,
        max_dte=40,
        vrp=0.8,
        open_interest=10_000,
        volume=None,
        spread_pct=5.0,
        max_spread_pct=20.0,
    )
    assert all(0.0 <= value <= 1.0 for value in components.values())
    assert components["safety"] == 1.0
    assert components["vol"] == 0.0
    assert components["yield"] == 1.0
    assert components["dte"] == 1.0
    assert components["vrp"] == 0.0
    assert components["oi"] == 1.0
    assert components["volume"] == 0.0
    assert components["spread"] == pytest.approx(0.75)

    weights = {"safety": 30, "yield": 10, "spread": 10}
    assert composite_score(components, weights) == pytest.approx(100.0 * (30 + 10 + 7.5) / 50)
    assert composite_score(components, {}) == 0.0
