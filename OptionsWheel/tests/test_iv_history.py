import os

import pytest

from options_wheel.iv_history import IVHistoryStore, extract_atm_iv


def test_iv_history_store_rank_and_persistence():
    artifacts_dir = os.path.join(os.path.dirname(__file__), "_artifacts")
    os.makedirs(artifacts_dir, exist_ok=True)
    store_path = os.path.join(artifacts_dir, "iv_history_store_test.json")

    try:
        store = IVHistoryStore(store_path, min_observations=3).load()
        store.record("ABC", "2026-01-01", 0.20)
        store.record("ABC", "2026-01-02", 0.30)
        store.record("ABC", "2026-01-03", 0.40)
        store.save()

        loaded = IVHistoryStore(store_path, min_observations=3).load()
        iv_rank, iv_percentile, observation_count = loaded.rank("ABC", 0.35)

        assert observation_count == 3
        assert iv_rank == pytest.approx(0.75)
        assert iv_percentile == pytest.approx(2 / 3)
    finally:
        if os.path.exists(store_path):
            os.remove(store_path)


def test_extract_atm_iv_prefers_expiry_nearest_target_dte():
    from datetime import datetime, timezone

    now_dt = datetime(2026, 1, 20, tzinfo=timezone.utc)
    parsed_contracts = [
        (datetime(2026, 2, 20, tzinfo=timezone.utc), {"strike": 95.0, "impliedVolatility": 0.25}),
        (datetime(2026, 2, 20, tzinfo=timezone.utc), {"strike": 100.0, "impliedVolatility": 0.22}),
        (datetime(2026, 3, 20, tzinfo=timezone.utc), {"strike": 100.0, "impliedVolatility": 0.35}),
    ]

    atm_iv = extract_atm_iv(
        parsed_contracts,
        101.0,
        now_dt=now_dt,
        dte_fn=lambda expiry, now: (expiry.date() - now.date()).days,
    )

    assert atm_iv == 0.22
