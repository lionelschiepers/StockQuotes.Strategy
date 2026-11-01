import json

import pytest

from options_wheel.datasync import resolve_data_dir, sync
from options_wheel.iv_history import IVHistoryStore


def _write(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_data_dir_must_stay_inside_the_workspace(tmp_path):
    root = tmp_path / "workspace"
    (root / "repo").mkdir(parents=True)
    assert resolve_data_dir(str(root / "repo" / ".." / "ow-data"), root=str(root)) == str(
        (root / "ow-data").resolve()
    )
    for escaping in (root / ".." / "elsewhere", root, tmp_path / "workspace-evil"):
        with pytest.raises(ValueError):
            resolve_data_dir(str(escaping), root=str(root))


def test_iv_history_merge_keeps_existing_dates(tmp_path):
    target = IVHistoryStore(str(tmp_path / "a"))
    target.record("AAA", "2026-01-01", 0.20)
    other = IVHistoryStore(str(tmp_path / "b"))
    other.record("AAA", "2026-01-01", 0.99)
    other.record("AAA", "2026-01-02", 0.30)
    other.record("BBB", "2026-01-02", 0.40)

    assert target.merge(other) == 2
    assert target.observations("AAA") == [0.20, 0.30]
    assert target.observations("BBB") == [0.40]
    assert target.merge(other) == 0


def test_push_then_pull_round_trip_never_clobbers_local_files(tmp_path):
    ci_history, ci_output = tmp_path / "ci" / "history", tmp_path / "ci" / "output"
    _write(ci_history / "iv" / "2026-01-01.json", {"AAA": 0.2})
    _write(ci_history / "scans" / "put" / "2026-01-02.json", {"scan_date": "2026-01-02"})
    _write(ci_output / "put_results.json", ["ci"])

    data_branch = tmp_path / "ow-data"
    stats = sync("push", str(data_branch), str(ci_history), str(ci_output))
    assert stats == {"iv_observations": 1, "scans": 1, "outputs": 1}
    assert (data_branch / "history" / "scans" / "put" / "2026-01-02.json").exists()

    local_history, local_output = tmp_path / "local" / "history", tmp_path / "local" / "output"
    _write(local_history / "iv" / "2026-01-05.json", {"AAA": 0.3})
    _write(local_output / "put_results.json", ["local"])

    stats = sync("pull", str(data_branch), str(local_history), str(local_output))
    assert stats == {"iv_observations": 1, "scans": 1, "outputs": 0}
    merged = IVHistoryStore(str(local_history / "iv")).load()
    assert merged.observations("AAA") == [0.2, 0.3]
    assert json.loads((local_output / "put_results.json").read_text(encoding="utf-8")) == ["local"]


def test_sync_converts_legacy_single_file(tmp_path):
    data_branch_history = tmp_path / "ow-data" / "history"
    _write(data_branch_history / "iv_history.json", {"AAA": {"2026-01-01": 0.2}})
    local_history = tmp_path / "local" / "history"
    _write(local_history / "iv" / "2026-01-02.json", {"AAA": 0.3})

    pulled = sync("pull", str(tmp_path / "ow-data"), str(local_history), str(tmp_path / "out"))
    assert pulled["iv_observations"] == 1
    assert (data_branch_history / "iv_history.json").exists()

    pushed = sync("push", str(tmp_path / "ow-data"), str(local_history), str(tmp_path / "out"))
    assert pushed["iv_observations"] == 1
    assert not (data_branch_history / "iv_history.json").exists()
    assert sorted(p.name for p in (data_branch_history / "iv").iterdir()) == [
        "2026-01-01.json",
        "2026-01-02.json",
    ]
