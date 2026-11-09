"""Spike detection for one page, one 5-minute window at a time. Pure functions, no I/O.

Baseline: an exponentially weighted moving average (EWMA) and variance of the page's
edits per 5-minute window. Windows with no edits count as zero; they are applied
lazily (in closed form) when the page next appears, so quiet pages cost nothing.

    z = (edits - mean) / sqrt(var + var_floor)

`var_floor` stops pages with an almost-flat history (mean ~ 0, var ~ 0) from producing
huge z-scores on a single edit; with floor 1.0 a quiet page needs about 5 edits in 5
minutes to reach z = 4.

A trend OPENS when z >= z_start AND the window has >= min_edits edits from >= min_editors
distinct editors, with at most max_bot_share of them by bots, and less than
max_revert_share of them reverts. So one person (or one bot) can't create a trend, and a
vandalism-and-revert fight (the Edit War Detector's job, Phase 8) isn't reported as news.
It CLOSES after `end_after` consecutive windows with z < z_end.
See docs/DECISIONS.md D-029.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

WINDOW = timedelta(minutes=5)


@dataclass(frozen=True)
class Params:
    half_life_windows: float = 72.0  # 6 hours
    var_floor: float = 1.0
    z_start: float = 4.0
    z_end: float = 1.0
    min_edits: int = 5
    min_editors: int = 3
    max_bot_share: float = 0.5
    max_revert_share: float = 0.3  # added after the 2026-11-09 replay (D-032)
    end_after: int = 3  # quiet windows (15 minutes)
    max_decay_windows: int = 2016  # 7 days; beyond that the baseline is ~0 anyway

    @property
    def alpha(self) -> float:
        return 1 - 0.5 ** (1 / self.half_life_windows)


@dataclass(frozen=True)
class Baseline:
    mean: float = 0.0
    var: float = 0.0
    last_window: datetime | None = None
    windows_seen: int = 0


@dataclass(frozen=True)
class Obs:
    """What happened on a page in one window (a row of page_metrics_5min)."""

    edits: int
    distinct_editors: int
    bot_edits: int
    page_title: str
    reverts: int = 0


@dataclass(frozen=True)
class OpenTrend:
    trend_id: int | None
    page_title: str
    started_at: datetime
    peak_at: datetime
    peak_zscore: float
    peak_edits: int
    distinct_editors: int
    total_edits: int
    quiet_windows: int
    last_active_window: datetime


@dataclass(frozen=True)
class Step:
    baseline: Baseline
    trend: OpenTrend | None
    action: str | None  # "opened" | "continued" | "closed" | None
    zscore: float
    edits: int


def _update(b: Baseline, x: float, alpha: float) -> tuple[float, float]:
    diff = x - b.mean
    incr = alpha * diff
    return b.mean + incr, (1 - alpha) * (b.var + diff * incr)


def decay_to(b: Baseline, window: datetime, p: Params) -> Baseline:
    """Apply the zero-edit windows between b.last_window and `window` (exclusive)."""
    if b.last_window is None:
        return b
    gap = int((window - b.last_window) / WINDOW) - 1
    if gap <= 0:
        return b
    mean, var = b.mean, b.var
    for _ in range(min(gap, p.max_decay_windows)):
        mean, var = _update(Baseline(mean, var), 0.0, p.alpha)
    return replace(b, mean=mean, var=var, windows_seen=b.windows_seen + gap)


def zscore(b: Baseline, x: float, p: Params) -> float:
    return (x - b.mean) / math.sqrt(b.var + p.var_floor)


def qualifies(obs: Obs | None, z: float, p: Params) -> bool:
    if obs is None or obs.edits < p.min_edits or obs.distinct_editors < p.min_editors:
        return False
    if obs.bot_edits / obs.edits > p.max_bot_share:
        return False
    if obs.reverts / obs.edits >= p.max_revert_share:
        return False
    return z >= p.z_start


def step(
    baseline: Baseline, trend: OpenTrend | None, obs: Obs | None, window: datetime, p: Params
) -> Step:
    """Advance one page by one window. `obs` is None when the page had no edits."""
    b = decay_to(baseline, window, p)
    x = obs.edits if obs else 0
    z = zscore(b, x, p)
    action = None

    if trend is not None:
        quiet = z < p.z_end
        trend = replace(
            trend,
            page_title=obs.page_title if obs else trend.page_title,
            total_edits=trend.total_edits + x,
            distinct_editors=max(trend.distinct_editors, obs.distinct_editors if obs else 0),
            quiet_windows=trend.quiet_windows + 1 if quiet else 0,
            last_active_window=trend.last_active_window if quiet else window,
        )
        if z > trend.peak_zscore:
            trend = replace(trend, peak_zscore=z, peak_at=window, peak_edits=x)
        action = "closed" if trend.quiet_windows >= p.end_after else "continued"
    elif qualifies(obs, z, p):
        assert obs is not None
        trend = OpenTrend(
            trend_id=None,
            page_title=obs.page_title,
            started_at=window,
            peak_at=window,
            peak_zscore=z,
            peak_edits=x,
            distinct_editors=obs.distinct_editors,
            total_edits=x,
            quiet_windows=0,
            last_active_window=window,
        )
        action = "opened"

    mean, var = _update(b, x, p.alpha)
    new_b = Baseline(mean, var, window, b.windows_seen + 1)
    return Step(new_b, trend, action, z, x)
