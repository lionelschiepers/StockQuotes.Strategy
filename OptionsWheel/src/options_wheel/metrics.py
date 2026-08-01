"""Pure option maths and ranking metrics.

Every function here is deterministic and side-effect free so that it can be unit
tested (see ``tests/test_metrics.py``). Nothing in this module performs I/O.

Two probability measures are used on purpose:

* **risk neutral** (drift = r - q) reproduces the Black-Scholes price. Under this
  measure the expected value of writing a fairly priced option is exactly zero,
  so it is useless for ranking candidates.
* **real world** (drift = configurable, volatility = forecast from realised
  volatility) is what the screener uses for ``EV`` / ``PoP``. The edge of an
  option seller is the variance risk premium, i.e. implied volatility trading
  above the volatility that is actually realised, and that only shows up when
  the payoff is evaluated with a realistic volatility instead of the option's
  own implied volatility.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0
MIN_YEARS = 1.0 / (365.0 * 24.0 * 60.0)
CONTRACT_MULTIPLIER = 100.0


def normal_cdf(x):
    """Standard normal cumulative distribution function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def years_to_expiration(expiration_dt, now_dt):
    """Year fraction between ``now_dt`` and the 20:00 UTC close on expiry day."""
    if expiration_dt is None or now_dt is None:
        return None

    expiration_dt = _as_utc(expiration_dt)
    now_dt = _as_utc(now_dt)

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
    return max(seconds / SECONDS_PER_YEAR, MIN_YEARS)


def _as_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def d1_d2(spot, strike, sigma, t_years, drift):
    """Return the Black-Scholes ``d1``/``d2`` for a lognormal with ``drift``.

    ``drift`` is the continuously compounded expected growth rate of the
    underlying, i.e. ``E[S_T] = spot * exp(drift * t_years)``. Passing
    ``r - q`` gives the risk-neutral values.
    """
    if (
        spot is None
        or strike is None
        or sigma is None
        or t_years is None
        or spot <= 0
        or strike <= 0
        or sigma <= 0
        or t_years <= 0
    ):
        return None, None

    sigma_sqrt_t = sigma * math.sqrt(t_years)
    d2 = (math.log(spot / strike) + (drift - 0.5 * sigma**2) * t_years) / sigma_sqrt_t
    return d2 + sigma_sqrt_t, d2


def option_delta(spot, strike, sigma, t_years, risk_free_rate, dividend_yield, option_type="put"):
    """Black-Scholes delta (negative for puts)."""
    d1, _ = d1_d2(spot, strike, sigma, t_years, risk_free_rate - dividend_yield)
    if d1 is None:
        return None

    discount = math.exp(-dividend_yield * t_years)
    if option_type == "call":
        return discount * normal_cdf(d1)
    return -discount * normal_cdf(-d1)


def probability_otm(spot, strike, sigma, t_years, drift, option_type="put"):
    """Probability that the short option expires worthless (no assignment)."""
    _, d2 = d1_d2(spot, strike, sigma, t_years, drift)
    if d2 is None:
        return None
    if option_type == "call":
        return normal_cdf(-d2)
    return normal_cdf(d2)


def expected_itm_payoff(spot, strike, sigma, t_years, drift, option_type="put"):
    """Expected payoff ``E[(K - S_T)^+]`` (put) / ``E[(S_T - K)^+]`` (call).

    Undiscounted, per share, under a lognormal with the supplied ``drift``.
    """
    d1, d2 = d1_d2(spot, strike, sigma, t_years, drift)
    if d1 is None:
        return None

    forward = spot * math.exp(drift * t_years)
    if option_type == "call":
        return max(forward * normal_cdf(d1) - strike * normal_cdf(d2), 0.0)
    return max(strike * normal_cdf(-d2) - forward * normal_cdf(-d1), 0.0)


def probability_of_profit(spot, strike, sigma, t_years, drift, net_premium, option_type="put"):
    """Probability that the trade is profitable at expiry (breakeven based)."""
    if net_premium is None or net_premium <= 0:
        return None
    if option_type == "call":
        breakeven = strike + net_premium
        _, d2 = d1_d2(spot, breakeven, sigma, t_years, drift)
        return normal_cdf(-d2) if d2 is not None else None

    breakeven = strike - net_premium
    if breakeven <= 0:
        return 1.0
    _, d2 = d1_d2(spot, breakeven, sigma, t_years, drift)
    return normal_cdf(d2) if d2 is not None else None


def forecast_volatility(implied_vol, hv_short, hv_long, hv_weight=0.5, iv_haircut=0.85):
    """Blend realised volatility with a haircut implied volatility.

    Realised volatility is the best available proxy for what will actually be
    realised over the life of the contract; the haircut implied volatility keeps
    the estimate anchored when the history is missing or the regime just
    changed. Returns ``None`` when nothing usable is available.
    """
    hv_parts = [v for v in (hv_short, hv_long) if v is not None and v > 0]
    hv_blend = sum(hv_parts) / len(hv_parts) if hv_parts else None

    iv_part = implied_vol * iv_haircut if implied_vol and implied_vol > 0 else None

    if hv_blend is None:
        return iv_part
    if iv_part is None:
        return hv_blend

    weight = min(max(hv_weight, 0.0), 1.0)
    return weight * hv_blend + (1.0 - weight) * iv_part


def variance_risk_premium(implied_vol, forecast_vol):
    """``IV / forecast vol``. Above 1 means the option is priced above what the
    underlying has actually been moving - the seller's edge."""
    if not implied_vol or not forecast_vol or forecast_vol <= 0:
        return None
    return implied_vol / forecast_vol


def net_credit(
    bid,
    ask,
    commission_per_contract=0.0,
    slippage_pct_of_spread=0.0,
    round_trip=False,
    multiplier=CONTRACT_MULTIPLIER,
):
    """Credit actually expected per share after slippage and commissions.

    The screener quotes options at the mid price, which nobody gets filled at.
    We assume a fill at ``mid - slippage_pct_of_spread`` of the half spread and
    subtract broker commissions (doubled when the position is expected to be
    bought back rather than held to expiry).
    """
    if bid is None or ask is None or bid <= 0 or ask < bid:
        return None, None, None

    mid = (bid + ask) / 2.0
    half_spread = (ask - bid) / 2.0
    slippage = half_spread * (max(slippage_pct_of_spread, 0.0) / 100.0)
    fees = commission_per_contract * (2 if round_trip else 1) / multiplier
    net = mid - slippage - fees
    spread_pct = ((ask - bid) / mid * 100.0) if mid > 0 else None
    return mid, net, spread_pct


def collateral_per_share(strike, spot, net_premium, option_type="put"):
    """Capital tied up per share.

    Cash-secured put: strike minus the credit received. Covered call: the cost
    of the shares minus the credit received.
    """
    base = strike if option_type != "call" else spot
    if base is None or base <= 0:
        return None
    credit = net_premium or 0.0
    return max(base - credit, 0.01)


def simple_yields(net_premium, collateral, dte):
    """Return ``(monthly_pct, annualized_pct)`` using *simple* annualisation.

    Compounding a weekly credit to a yearly figure assumes every roll gets the
    same premium, which systematically flatters short-dated, high-gamma,
    commission-heavy contracts. Simple scaling keeps DTEs comparable.
    """
    if not net_premium or not collateral or not dte or collateral <= 0 or dte <= 0:
        return None, None
    period_return = net_premium / collateral
    return period_return * (30.0 / dte) * 100.0, period_return * (365.0 / dte) * 100.0


def sigma_distance(spot, strike, sigma, t_years, option_type="put"):
    """Distance of the strike from spot expressed in standard deviations.

    Unlike a raw OTM percentage this is comparable across tickers: 5% OTM on a
    utility is far safer than 5% OTM on a biotech.
    """
    if (
        not spot
        or not strike
        or not sigma
        or not t_years
        or spot <= 0
        or strike <= 0
        or sigma <= 0
        or t_years <= 0
    ):
        return None
    z = math.log(strike / spot) / (sigma * math.sqrt(t_years))
    return z if option_type == "call" else -z


def credit_risk_ratio(net_premium, expected_loss):
    """Credit collected per unit of expected assignment loss."""
    if net_premium is None or expected_loss is None:
        return None
    if expected_loss <= 1e-9:
        return None
    return net_premium / expected_loss


def max_monthly_yield_for_delta(abs_delta, dte, implied_vol, risk_free_rate=0.045):
    """Roughly the best monthly yield obtainable at a given delta and IV.

    Used to warn about configurations that ask for a premium level which simply
    does not exist at the configured delta cap.
    """
    if not (0 < abs_delta < 1) or dte <= 0 or implied_vol <= 0:
        return None

    t_years = dte / 365.0
    sigma_sqrt_t = implied_vol * math.sqrt(t_years)
    # Invert N(-d1) = delta for a put.
    d1 = -_inverse_normal_cdf(abs_delta)
    strike = 100.0 * math.exp(-d1 * sigma_sqrt_t + (risk_free_rate + 0.5 * implied_vol**2) * t_years)
    premium = expected_itm_payoff(100.0, strike, implied_vol, t_years, risk_free_rate, "put")
    premium *= math.exp(-risk_free_rate * t_years)
    monthly, _ = simple_yields(premium, strike, dte)
    return monthly


def _inverse_normal_cdf(p):
    """Acklam's rational approximation of the standard normal quantile."""
    if p <= 0.0 or p >= 1.0:
        raise ValueError("p must be in (0, 1)")

    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02,
         1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02,
         6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00,
         -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00,
         3.754408661907416e00]

    p_low, p_high = 0.02425, 1 - 0.02425
    if p < p_low:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1
        )
    if p > p_high:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1
        )
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (
        ((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1
    )
