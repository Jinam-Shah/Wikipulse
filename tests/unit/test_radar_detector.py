"""Trend detector on synthetic spikes (spec Part I: 'the trend detector on synthetic spikes')."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from wikipulse.radar.detector import WINDOW, Baseline, Obs, Params, decay_to, step, zscore

T0 = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
P = Params()


EMPTY = Baseline()


def run(series, p=P, start_baseline=EMPTY):
    """series: list of Obs-or-None per consecutive window. Returns the list of Steps."""
    b, trend, steps = start_baseline, None, []
    for i, obs in enumerate(series):
        s = step(b, trend, obs, T0 + WINDOW * i, p)
        b, trend = s.baseline, (None if s.action == "closed" else s.trend)
        steps.append(s)
    return steps


def quiet(n, edits=0, editors=0):
    return [Obs(edits, editors, 0, "Page") if edits else None for _ in range(n)]


def spike(edits=12, editors=6, bots=0):
    return Obs(edits, editors, bots, "Page")


def test_ewma_converges_to_the_steady_rate():
    steps = run(quiet(500, edits=2, editors=1))
    b = steps[-1].baseline
    assert b.mean == pytest.approx(2.0, abs=0.05)
    assert b.var < 0.05


def test_zero_windows_decay_lazily_in_closed_form():
    b = Baseline(mean=4.0, var=2.0, last_window=T0, windows_seen=10)
    decayed = decay_to(b, T0 + WINDOW * 73, P)  # 72 empty windows = one half-life
    assert decayed.mean == pytest.approx(2.0, rel=1e-6)
    assert decayed.windows_seen == 82


def test_quiet_page_spike_with_several_editors_opens_a_trend():
    steps = run(quiet(50) + [spike()])
    assert steps[-1].action == "opened"
    assert steps[-1].zscore > P.z_start
    assert steps[-1].trend.started_at == T0 + WINDOW * 50


def test_one_editor_cannot_create_a_trend():
    steps = run(quiet(50) + [Obs(30, 1, 0, "Page")])
    assert steps[-1].action is None and steps[-1].trend is None


def test_bot_burst_is_not_a_trend():
    steps = run(quiet(50) + [spike(edits=12, editors=4, bots=9)])
    assert steps[-1].trend is None


def test_vandalism_and_revert_fight_is_not_a_trend():
    assert run(quiet(50) + [Obs(8, 4, 0, "Page", reverts=3)])[-1].trend is None  # 37 %
    assert run(quiet(50) + [Obs(8, 4, 0, "Page", reverts=1)])[-1].action == "opened"  # 12 %


def test_too_few_edits_is_not_a_trend():
    steps = run(quiet(50) + [spike(edits=4, editors=4)])
    assert steps[-1].trend is None


def test_busy_page_needs_a_bigger_spike():
    busy = [Obs(6, 3, 0, "Busy")] * 300  # always ~6 edits / 5 min from 3 editors
    assert run(busy + [spike(edits=8, editors=4)])[-1].trend is None
    assert run(busy + [spike(edits=25, editors=8)])[-1].action == "opened"


def test_trend_tracks_peak_and_closes_after_three_quiet_windows():
    series = quiet(50) + [spike(8, 4), spike(20, 9), spike(10, 5)] + quiet(5)
    steps = run(series)
    actions = [s.action for s in steps[50:]]
    assert actions[:3] == ["opened", "continued", "continued"]
    assert actions[3:6] == ["continued", "continued", "closed"]
    closed = steps[55].trend
    assert closed.peak_at == T0 + WINDOW * 51 and closed.peak_edits == 20
    assert closed.total_edits == 38 and closed.distinct_editors == 9
    assert closed.last_active_window == T0 + WINDOW * 52


def test_a_new_burst_after_closing_opens_a_new_trend():
    series = quiet(30) + [spike()] + quiet(4) + quiet(40) + [spike(15, 7)]
    opened = [s for s in run(series) if s.action == "opened"]
    assert len(opened) == 2


def test_zscore_uses_the_variance_floor():
    assert zscore(Baseline(), 1, P) == pytest.approx(1.0)
    assert zscore(Baseline(mean=0.0, var=3.0), 4, P) == pytest.approx(2.0)


def test_detector_is_deterministic():
    series = quiet(20, 1, 1) + [spike()] + quiet(10)
    a, b = run(series), run(series)
    assert [(s.action, s.zscore) for s in a] == [(s.action, s.zscore) for s in b]
