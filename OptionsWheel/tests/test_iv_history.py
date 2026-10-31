import json

import pytest

from options_wheel.iv_history import IVHistoryStore, extract_atm_iv


def test_iv_history_store_rank_and_persistence(tmp_path):
    store = IVHistoryStore(str(tmp_path), min_observations=3).load()
    store.record("ABC", "2026-01-01", 0.20)
    store.record("ABC", "2026-01-02", 0.30)
    store.record("ABC", "2026-01-03", 0.40)
    store.save()

    loaded = IVHistoryStore(str(tmp_path), min_observations=3).load()
    iv_rank, iv_percentile, observation_count = loaded.rank("ABC", 0.35)

    assert observation_count == 3
    assert iv_rank == pytest.approx(0.75)
    assert iv_percentile == pytest.approx(2 / 3)


def test_iv_history_save_writes_only_changed_dates_and_prunes(tmp_path):
    store = IVHistoryStore(str(tmp_path), max_observations=2).load()
    store.record("AAA", "2026-01-01", 0.1)
    store.record("AAA", "2026-01-02", 0.2)
    store.save()
    day2 = tmp_path / "2026-01-02.json"
    day2.write_text('{"AAA":0.2,"untouched":1}', encoding="utf-8")

    store.record("AAA", "2026-01-03", 0.3)
    store.record("BBB", "2026-01-03", 0.4)
    store.save()

    assert sorted(p.name for p in tmp_path.iterdir()) == ["2026-01-02.json", "2026-01-03.json"]
    assert day2.read_text(encoding="utf-8") == '{"AAA":0.2,"untouched":1}'
    assert json.loads((tmp_path / "2026-01-03.json").read_text(encoding="utf-8")) == {
        "AAA": 0.3,
        "BBB": 0.4,
    }
    assert store.observations("AAA") == [0.2, 0.3]


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
