from datetime import datetime, timedelta, timezone

from options_wheel import analysis


def _symbol_data():
    return {"symbol": "AAA", "name": "AAA Corp", "price": 100.0}


def _option(**overrides):
    option = {
        "strike": 90.0,
        "bid": 1.00,
        "ask": 1.04,
        "lastPrice": 1.02,
        "impliedVolatility": 0.45,
        "openInterest": 500,
        "volume": 100,
    }
    option.update(overrides)
    return option


def _evaluate(option):
    now_dt = datetime.now(timezone.utc)
    expiration_dt = now_dt + timedelta(days=30)
    return analysis._evaluate_contract(_symbol_data(), expiration_dt, option, now_dt)


def test_two_sided_quote_is_priced_at_mid():
    result = _evaluate(_option())
    assert result is not None
    contract_data, _ = result
    assert contract_data["PricingSource"] == "mid"
    assert contract_data["SpreadAbs"] is not None
    assert contract_data["NetPremium"] < contract_data["Premium"]


def test_contract_without_bid_is_rejected():
    assert _evaluate(_option(bid=0.0)) is None
    assert _evaluate(_option(bid=None)) is None


def test_contract_with_crossed_or_missing_ask_is_rejected():
    assert _evaluate(_option(ask=None)) is None
    assert _evaluate(_option(bid=1.5, ask=1.0)) is None


def test_spread_checks_are_always_applied():
    result = _evaluate(_option(bid=0.50, ask=1.50))
    assert result is not None
    _, failed = result
    assert any(name.startswith("Spread <= ") and name.endswith("%") for name in failed)
    assert any(name.startswith("Spread <= $") for name in failed)
