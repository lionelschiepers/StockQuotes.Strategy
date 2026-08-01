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
computed. The store is a single JSON file so it can be cached between CI runs.
"""

from __future__ import annotations

import json
import os
import threading

DEFAULT_LOOKBACK_DAYS = 252
DEFAULT_MIN_OBSERVATIONS = 40
DEFAULT_MAX_OBSERVATIONS = 400


class IVHistoryStore:
    """Thread-safe ``{symbol: {date: atm_iv}}`` store backed by a JSON file."""

    def __init__(
        self,
        path,
        lookback=DEFAULT_LOOKBACK_DAYS,
        min_observations=DEFAULT_MIN_OBSERVATIONS,
        max_observations=DEFAULT_MAX_OBSERVATIONS,
    ):
        self.path = path
        self.lookback = lookback
        self.min_observations = min_observations
        self.max_observations = max_observations
        self._lock = threading.Lock()
        self._data = {}
        self._dirty = False

    def load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                self._data = {
                    symbol: {str(k): float(v) for k, v in series.items()}
                    for symbol, series in data.items()
                    if isinstance(series, dict)
                }
        except (OSError, ValueError):
            self._data = {}
        return self

    def save(self):
        if not self._dirty:
            return False
        with self._lock:
            trimmed = {}
            for symbol, series in self._data.items():
                if not series:
                    continue
                recent = sorted(series.items())[-self.max_observations :]
                trimmed[symbol] = {date: round(iv, 4) for date, iv in recent}
            payload = trimmed
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"), sort_keys=True)
        os.replace(tmp_path, self.path)
        self._dirty = False
        return True

    def record(self, symbol, date_str, atm_iv):
        """Record today's ATM implied volatility for ``symbol``."""
        if not symbol or atm_iv is None or atm_iv <= 0:
            return
        with self._lock:
            self._data.setdefault(symbol, {})[date_str] = float(atm_iv)
            self._dirty = True

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


def extract_atm_iv(contracts, spot, target_dte=30, now_dt=None, dte_fn=None):
    """Pick the implied volatility of the contract closest to at-the-money.

    ``contracts`` is the ``[(expiration_dt, option_dict), ...]`` list produced by
    the chain parser. The expiry closest to ``target_dte`` is used so the series
    stays comparable from day to day.
    """
    if not contracts or not spot or spot <= 0:
        return None

    usable = []
    for expiration_dt, option in contracts:
        iv = option.get("impliedVolatility")
        strike = option.get("strike")
        if expiration_dt is None or not iv or not strike:
            continue
        try:
            iv = float(iv)
            strike = float(strike)
        except (TypeError, ValueError):
            continue
        if iv <= 0 or strike <= 0:
            continue
        dte = dte_fn(expiration_dt, now_dt) if dte_fn else None
        usable.append((expiration_dt, dte, strike, iv))

    if not usable:
        return None

    if any(item[1] for item in usable):
        best_dte = min(
            (item[1] for item in usable if item[1]),
            key=lambda d: abs(d - target_dte),
        )
        usable = [item for item in usable if item[1] == best_dte]
    else:
        nearest_expiry = min(item[0] for item in usable)
        usable = [item for item in usable if item[0] == nearest_expiry]

    _, _, _, atm_iv = min(usable, key=lambda item: abs(item[2] - spot))
    return atm_iv
