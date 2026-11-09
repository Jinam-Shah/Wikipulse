from datetime import UTC, datetime, timedelta

import pytest

from wikipulse.quality import checks
from wikipulse.quality.runner import evaluate

NOW = datetime(2026, 11, 9, 18, 5, tzinfo=UTC)


@pytest.mark.parametrize(("minutes_ago", "status"), [(2, "pass"), (16, "warn"), (61, "fail")])
def test_freshness_thresholds(minutes_ago, status):
    r = checks.freshness(NOW - timedelta(minutes=minutes_ago), NOW)
    assert r.status == status and r.observed == pytest.approx(minutes_ago)


def test_freshness_fails_when_there_is_no_data():
    assert checks.freshness(None, NOW).status == "fail"


def test_volume_needs_history_before_judging():
    r = checks.volume("enwiki", 0, [5000, 5100])
    assert r.status == "pass" and "need 6" in r.message


@pytest.mark.parametrize(
    ("edits", "status"),
    [(5000, "pass"), (900, "fail"), (30_000, "warn"), (1100, "pass")],
)
def test_volume_against_median(edits, status):
    history = [5000, 5200, 4800, 5100, 4900, 5000]
    assert checks.volume("enwiki", edits, history).status == status


def test_volume_with_zero_median():
    assert checks.volume("w", 0, [0] * 6).status == "pass"
    assert checks.volume("w", 10, [0] * 6).status == "warn"


def test_reconciliation():
    assert checks.reconciliation("enwiki", 100, 100).status == "pass"
    r = checks.reconciliation("enwiki", 100, 97)
    assert r.status == "fail" and r.observed == -3
    assert checks.reconciliation("enwiki", 100, None).status == "fail"


def test_revert_rate_ignores_tiny_hours():
    assert checks.revert_rate("w", 10, 5).status == "pass"
    assert checks.revert_rate("w", 100, 30).status == "warn"
    assert checks.revert_rate("w", 100, 3).status == "pass"


def test_evaluate_covers_configured_and_observed_wikis():
    pg = {
        "latest_processed_at": NOW - timedelta(minutes=1),
        "edits": {"samplewiki": 300},
        "reverts": {"samplewiki": 9},
        "history": {"samplewiki": [300] * 8},
    }
    results = evaluate(NOW, NOW, pg, {"samplewiki": 300}, wikis=["enwiki"])
    names = [(r.name, r.wiki) for r in results]
    assert ("stream_freshness", None) in names
    assert ("hourly_volume", "enwiki") in names  # configured, even with no edits
    assert ("lake_reconciliation", "samplewiki") in names
    assert ("lake_reconciliation", "enwiki") not in names  # nothing to reconcile
    assert all(r.status == "pass" for r in results)
