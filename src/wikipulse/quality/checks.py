"""Pure check functions: numbers in, a pass/warn/fail verdict out. Easy to unit-test.

Thresholds are deliberately simple and documented (docs/DECISIONS.md D-026):
- freshness: the stream must keep delivering; a stall is the most likely real failure.
- volume: an hour far below or above its recent norm usually means a broken input
  (or a genuinely huge news event; that's why "too high" only warns).
- reconciliation: lake totals must equal Postgres for the same hour.
- revert rate: a sudden jump suggests the revert detector or the input changed.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

Status = Literal["pass", "warn", "fail"]


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: Status
    message: str
    wiki: str | None = None
    observed: float | None = None
    threshold: str | None = None
    details: dict = field(default_factory=dict)


def freshness(
    latest_processed_at: datetime | None,
    now: datetime,
    *,
    warn_minutes: float = 15,
    fail_minutes: float = 60,
) -> CheckResult:
    threshold = f"warn > {warn_minutes:g} min, fail > {fail_minutes:g} min"
    if latest_processed_at is None:
        return CheckResult(
            "stream_freshness", "fail", "no edits in Postgres at all", None, None, threshold
        )
    age_min = (now - latest_processed_at).total_seconds() / 60
    if age_min > fail_minutes:
        status: Status = "fail"
    elif age_min > warn_minutes:
        status = "warn"
    else:
        status = "pass"
    return CheckResult(
        "stream_freshness",
        status,
        f"last edit processed {age_min:.1f} min ago",
        None,
        round(age_min, 2),
        threshold,
        {"latest_processed_at": latest_processed_at.isoformat()},
    )


def volume(
    wiki: str,
    hour_edits: int,
    previous_hours: list[int],
    *,
    min_history: int = 6,
    low_ratio: float = 0.2,
    high_ratio: float = 5.0,
) -> CheckResult:
    """Compare an hour with the median of the previous hours (that had data collected)."""
    threshold = f"fail < {low_ratio:g}x median, warn > {high_ratio:g}x median"
    if len(previous_hours) < min_history:
        return CheckResult(
            "hourly_volume",
            "pass",
            f"{hour_edits} edits; only {len(previous_hours)} earlier hours, "
            f"need {min_history} to judge",
            wiki,
            float(hour_edits),
            threshold,
            {"history_hours": len(previous_hours)},
        )
    median = statistics.median(previous_hours)
    ratio = hour_edits / median if median else float("inf") if hour_edits else 1.0
    if ratio < low_ratio:
        status: Status = "fail"
    elif ratio > high_ratio:
        status = "warn"
    else:
        status = "pass"
    return CheckResult(
        "hourly_volume",
        status,
        f"{hour_edits} edits vs median {median:g} "
        f"of last {len(previous_hours)} hours ({ratio:.2f}x)",
        wiki,
        round(ratio, 3),
        threshold,
        {"hour_edits": hour_edits, "median": median, "history_hours": len(previous_hours)},
    )


def reconciliation(wiki: str, postgres_edits: int, lake_edits: int | None) -> CheckResult:
    threshold = "lake edits == Postgres edits"
    if lake_edits is None:
        return CheckResult(
            "lake_reconciliation",
            "fail",
            "hour missing from gold.wiki_hourly",
            wiki,
            None,
            threshold,
            {"postgres_edits": postgres_edits},
        )
    diff = lake_edits - postgres_edits
    return CheckResult(
        "lake_reconciliation",
        "pass" if diff == 0 else "fail",
        f"lake {lake_edits} vs Postgres {postgres_edits} edits (diff {diff:+d})",
        wiki,
        float(diff),
        threshold,
        {"postgres_edits": postgres_edits, "lake_edits": lake_edits},
    )


def revert_rate(
    wiki: str, edits: int, reverts: int, *, warn_rate: float = 0.15, min_edits: int = 50
) -> CheckResult:
    threshold = f"warn > {warn_rate:.0%} (when >= {min_edits} edits)"
    rate = reverts / edits if edits else 0.0
    status: Status = "warn" if edits >= min_edits and rate > warn_rate else "pass"
    return CheckResult(
        "revert_rate",
        status,
        f"{reverts} of {edits} edits are reverts ({rate:.1%})",
        wiki,
        round(rate, 4),
        threshold,
    )
