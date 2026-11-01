"""Persistent at-the-money implied volatility history and true IV Rank.

``compute_iv_hv_percentile`` in :mod:`options_wheel.analysis` compares an
option's implied volatility with the *realised* volatility range of the
underlying. That is a useful sanity check but it is not IV Rank: IV Rank
compares today's implied volatility with the implied volatility of the same
underlying over the past year, and it is the single most predictive filter for
premium selling.

Yahoo does not expose historical implied volatility, so we build it ourselves:
every scan records the ATM implied volatility of each analysed symbol, and once
enough observations have accumulated a real IV Rank / IV Percentile can be
computed. The store is a directory with one ``YYYY-MM-DD.json`` file per scan
date mapping ``symbol -> atm_iv``. Past days are never rewritten, so the
git-tracked copy CI keeps on the ``ow-data`` branch (see
:mod:`options_wheel.datasync`) only grows by one small file per scan.
"""

from __future__ import annotations

import glob
import json
import os
import re
import threading

DEFAULT_LOOKBACK_DAYS = 252
DEFAULT_MIN_OBSERVATIONS = 40
# Number of most recent scan dates kept on disk (~1.6 years of weekday scans).
DEFAULT_MAX_OBSERVATIONS = 400

# Store directory name inside ``data/history``.
IV_HISTORY_DIRNAME = "iv"
# Pre-split single-file format: ``{symbol: {date: atm_iv}}``.
LEGACY_FILENAME = "iv_history.json"

_DATE_FILE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _date_files(directory):
    """Return ``{date: path}`` for the per-date files in ``directory``."""
    files = {}
    for path in glob.glob(os.path.join(directory, "*.json")):
        date_str = os.path.splitext(os.path.basename(path))[0]
        if _DATE_FILE_RE.match(date_str):
            files[date_str] = path
    return files


class IVHistoryStore:
    """Thread-safe ``{symbol: {date: atm_iv}}`` store backed by per-date files."""

    def __init__(
        self,
        directory,
        lookback=DEFAULT_LOOKBACK_DAYS,
        min_observations=DEFAULT_MIN_OBSERVATIONS,
        max_observations=DEFAULT_MAX_OBSERVATIONS,
    ):
        self.directory = directory
        self.lookback = lookback
        self.min_observations = min_observations
        self.max_observations = max_observations
        self._lock = threading.Lock()
        self._data = {}
        self._dirty_dates = set()

    def load(self):
        data = {}
        for date_str, path in sorted(_date_files(self.directory).items()):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    day = json.load(f)
            except (OSError, ValueError):
                continue
            if not isinstance(day, dict):
                continue
            for symbol, iv in day.items():
                try:
                    data.setdefault(symbol, {})[date_str] = float(iv)
                except (TypeError, ValueError):
                    continue
        with self._lock:
            self._data = data
            self._dirty_dates = set()
        return self

    @classmethod
    def from_legacy_file(cls, path):
        """Read-only store holding the observations of a pre-split JSON file."""
        store = cls(None)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return store
        if isinstance(data, dict):
            store._data = {
                symbol: {str(k): float(v) for k, v in series.items()}
                for symbol, series in data.items()
                if isinstance(series, dict)
            }
        return store

    def save(self):
        """Write the files of dates changed since loading, then prune old dates."""
        with self._lock:
            if not self._dirty_dates:
                return False
            days = {date_str: {} for date_str in self._dirty_dates}
            for symbol, series in self._data.items():
                for date_str, iv in series.items():
                    if date_str in days:
                        days[date_str][symbol] = round(iv, 4)
            self._dirty_dates = set()

        os.makedirs(self.directory, exist_ok=True)
        for date_str, payload in days.items():
            path = os.path.join(self.directory, f"{date_str}.json")
            tmp_path = f"{path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
            os.replace(tmp_path, path)
        self._prune()
        return True

    def _prune(self):
        files = _date_files(self.directory)
        expired = sorted(files)[: -self.max_observations] if self.max_observations else []
        for date_str in expired:
            os.remove(files[date_str])
        if expired:
            expired = set(expired)
            with self._lock:
                for series in self._data.values():
                    for date_str in expired & series.keys():
                        del series[date_str]

    def record(self, symbol, date_str, atm_iv):
        """Record today's ATM implied volatility for ``symbol``."""
        if not symbol or atm_iv is None or atm_iv <= 0:
            return
        with self._lock:
            self._data.setdefault(symbol, {})[date_str] = float(atm_iv)
            self._dirty_dates.add(date_str)

    def merge(self, other):
        """Add observations from another store; dates already present are kept.

        Lets scans from several machines (local runs, CI) build one history.
        Returns the number of observations added.
        """
        added = 0
        with self._lock:
            for symbol, series in other._data.items():
                target = self._data.setdefault(symbol, {})
                for date_str, iv in series.items():
                    if date_str not in target:
                        target[date_str] = iv
                        self._dirty_dates.add(date_str)
                        added += 1
        return added

    def observations(self, symbol):
        with self._lock:
            series = self._data.get(symbol, {})
            values = [iv for _, iv in sorted(series.items())[-self.lookback :]]
        return values

    def rank(self, symbol, current_iv):
        """Return ``(iv_rank, iv_percentile, observation_count)``.

        ``iv_rank`` places the current IV inside its own 1-year high/low range;
        ``iv_percentile`` is the fraction of past observations below it. Both
        are ``None`` until ``min_observations`` history has accumulated.
        """
        if current_iv is None or current_iv <= 0:
            return None, None, 0

        values = self.observations(symbol)
        count = len(values)
        if count < self.min_observations:
            return None, None, count

        low = min(values)
        high = max(values)
        if high <= low:
            return None, None, count

        iv_rank = (current_iv - low) / (high - low)
        iv_rank = max(0.0, min(1.0, iv_rank))
        iv_percentile = sum(1 for v in values if v < current_iv) / count
        return iv_rank, iv_percentile, count


def _usable_contract(expiration_dt, option, now_dt, dte_fn):
    """``(expiration_dt, dte, strike, iv)`` for a priced contract, else ``None``."""
    iv = option.get("impliedVolatility")
    strike = option.get("strike")
    if expiration_dt is None or not iv or not strike:
        return None
    try:
        iv = float(iv)
        strike = float(strike)
    except (TypeError, ValueError):
        return None
    if iv <= 0 or strike <= 0:
        return None
    dte = dte_fn(expiration_dt, now_dt) if dte_fn else None
    return expiration_dt, dte, strike, iv


def _closest_expiry_contracts(usable, target_dte):
    """Contracts of the expiry closest to ``target_dte`` (nearest expiry without DTEs)."""
    if any(item[1] for item in usable):
        best_dte = min(
            (item[1] for item in usable if item[1]),
            key=lambda d: abs(d - target_dte),
        )
        return [item for item in usable if item[1] == best_dte]
    nearest_expiry = min(item[0] for item in usable)
    return [item for item in usable if item[0] == nearest_expiry]


def extract_atm_iv(contracts, spot, target_dte=30, now_dt=None, dte_fn=None):
    """Pick the implied volatility of the contract closest to at-the-money.

    ``contracts`` is the ``[(expiration_dt, option_dict), ...]`` list produced by
    the chain parser. The expiry closest to ``target_dte`` is used so the series
    stays comparable from day to day.
    """
    if not contracts or not spot or spot <= 0:
        return None

    usable = [
        item
        for item in (
            _usable_contract(expiration_dt, option, now_dt, dte_fn)
            for expiration_dt, option in contracts
        )
        if item is not None
    ]
    if not usable:
        return None

    usable = _closest_expiry_contracts(usable, target_dte)
    _, _, _, atm_iv = min(usable, key=lambda item: abs(item[2] - spot))
    return atm_iv
