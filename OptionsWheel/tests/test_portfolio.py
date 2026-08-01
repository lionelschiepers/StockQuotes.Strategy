from options_wheel.portfolio import build_portfolio


def test_build_portfolio_enforces_max_positions_and_sector_caps():
    rows = [
        {"Symbol": "AAA", "Strike": 50.0, "Price": 52.0, "NetPremium": 1.0, "MonthlyYieldPct": 2.0, "Score": 90.0},
        {"Symbol": "BBB", "Strike": 45.0, "Price": 47.0, "NetPremium": 1.1, "MonthlyYieldPct": 1.8, "Score": 88.0},
        {"Symbol": "CCC", "Strike": 40.0, "Price": 41.0, "NetPremium": 0.9, "MonthlyYieldPct": 1.7, "Score": 87.0},
    ]
    sector_map = {"AAA": "Tech", "BBB": "Tech", "CCC": "Health"}
    config = {
        "PORTFOLIO_MAX_POSITIONS": 2,
        "PORTFOLIO_MAX_PER_SECTOR": 1,
        "PORTFOLIO_COLLATERAL_BUDGET": 0.0,
        "PORTFOLIO_MAX_PCT_PER_POSITION": 25.0,
    }

    portfolio = build_portfolio(rows, config, option_type="put", sector_map=sector_map)

    assert [p["Symbol"] for p in portfolio["positions"]] == ["AAA", "CCC"]
    assert portfolio["position_count"] == 2
    assert portfolio["skipped"]["sector_cap"] == 1


def test_build_portfolio_enforces_budget_and_position_cap():
    rows = [
        {"Symbol": "AAA", "Strike": 80.0, "Price": 82.0, "NetPremium": 1.0, "MonthlyYieldPct": 1.5, "Score": 95.0},
        {"Symbol": "BBB", "Strike": 35.0, "Price": 37.0, "NetPremium": 0.8, "MonthlyYieldPct": 1.4, "Score": 90.0},
        {"Symbol": "CCC", "Strike": 30.0, "Price": 31.0, "NetPremium": 0.7, "MonthlyYieldPct": 1.3, "Score": 85.0},
    ]
    config = {
        "PORTFOLIO_MAX_POSITIONS": 10,
        "PORTFOLIO_MAX_PER_SECTOR": 0,
        "PORTFOLIO_COLLATERAL_BUDGET": 7000.0,
        "PORTFOLIO_MAX_PCT_PER_POSITION": 60.0,
    }

    portfolio = build_portfolio(rows, config, option_type="put", sector_map={})

    assert [p["Symbol"] for p in portfolio["positions"]] == ["BBB", "CCC"]
    assert portfolio["total_collateral"] <= 7000.0
    assert portfolio["skipped"]["budget"] >= 1
