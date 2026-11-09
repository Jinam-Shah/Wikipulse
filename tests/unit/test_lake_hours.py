from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wikipulse.lake.export import (
    complete_hours,
    floor_hour,
    hours_between,
    oldest_exportable_hour,
)
from wikipulse.lake.paths import LakePaths

NOW = datetime(2026, 11, 9, 6, 42, 17, tzinfo=UTC)


def test_floor_hour_converts_to_utc():
    from zoneinfo import ZoneInfo

    local = datetime(2026, 11, 9, 1, 30, tzinfo=ZoneInfo("America/New_York"))
    assert floor_hour(local) == datetime(2026, 11, 9, 6, tzinfo=UTC)


def test_complete_hours_excludes_the_running_hour():
    assert complete_hours(NOW, 3) == [
        datetime(2026, 11, 9, 3, tzinfo=UTC),
        datetime(2026, 11, 9, 4, tzinfo=UTC),
        datetime(2026, 11, 9, 5, tzinfo=UTC),
    ]


def test_hours_between_is_end_exclusive_and_floors():
    start = datetime(2026, 11, 8, 22, 15, tzinfo=UTC)
    end = datetime(2026, 11, 9, 1, 0, tzinfo=UTC)
    assert [h.hour for h in hours_between(start, end)] == [22, 23, 0]
    assert hours_between(end, end) == []


def test_oldest_exportable_hour_leaves_an_hour_of_margin():
    oldest = oldest_exportable_hour(NOW, retention_days=7)
    assert oldest == floor_hour(NOW - timedelta(days=7)) + timedelta(hours=1)


def test_lake_paths_are_absolute_and_hive_style(tmp_path):
    paths = LakePaths.at(tmp_path / "lake")
    assert paths.root.is_absolute()
    hour_dir = paths.raw_hour_dir(datetime(2026, 11, 9, 5, tzinfo=UTC))
    assert hour_dir.parts[-2:] == ("date=2026-11-09", "hour=05")
    assert paths.catalog_uri.startswith("sqlite:///")
    assert "\\" not in paths.catalog_uri
    props = paths.catalog_properties()
    assert props["type"] == "sql" and Path(props["warehouse"]).is_absolute()


@pytest.mark.parametrize("hours", [1, 3])
def test_sample_generator_is_deterministic_and_realistic(hours):
    from wikipulse.devtools.sample import SAMPLE_WIKI, generate

    end = datetime(2026, 11, 9, 6, tzinfo=UTC)
    a = generate(hours, end, edits_per_hour=200, seed=1)
    b = generate(hours, end, edits_per_hour=200, seed=1)
    assert [e.event_id for e in a] == [e.event_id for e in b]
    assert {e.wiki for e in a} == {SAMPLE_WIKI}
    assert all(end - timedelta(hours=hours) <= e.ts < end for e in a)
    bot_share = sum(e.is_bot for e in a) / len(a)
    assert 0.35 < bot_share < 0.55
    assert len({e.event_id for e in a}) == len(a)
