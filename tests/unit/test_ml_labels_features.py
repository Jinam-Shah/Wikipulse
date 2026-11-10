"""Labels (was it reverted?) and point-in-time features on small synthetic page histories."""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from wikipulse.ml.features import FEATURES, build_features
from wikipulse.ml.labels import find_reverts

T0 = pd.Timestamp("2026-11-09 12:00", tz="UTC")


def edits(rows):
    """rows: (event_id, page, rev, minutes_after_T0, sha, parent_sha, user[, comment])."""
    out = []
    for r in rows:
        eid, page, rev, mins, sha, parent, user = r[:7]
        comment = r[7] if len(r) > 7 else "edit"
        out.append({
            "event_id": eid, "wiki": "enwiki", "change_kind": "edit", "page_id": page,
            "page_title": f"P{page}", "revision_id": rev, "rev_sha1": sha,
            "parent_rev_sha1": parent, "user_name": user, "is_bot": False,
            "is_anon": user.startswith("~"),
            "user_edit_count": None if user.startswith("~") else 100,
            "user_registered_at": None if user.startswith("~") else T0 - pd.Timedelta(days=400),
            "comment": comment, "is_minor": False, "bytes_old": 1000, "bytes_new": 1010,
            "ts": T0 + pd.Timedelta(minutes=mins), "revert_kind": None,
        })  # fmt: skip
    df = pd.DataFrame(out)
    df["user_registered_at"] = pd.to_datetime(df["user_registered_at"], utc=True)
    return df


W = timedelta(hours=6)


def reverted(df, window=W):
    return set(find_reverts(df, window).reverted_at.dropna().index)


def test_single_edit_undone():
    df = edits([("a", 1, 1, 0, "A", "Z", "Alice"), ("b", 1, 2, 5, "B", "A", "~vandal"),
                ("c", 1, 3, 6, "A", "B", "Bob")])  # fmt: skip
    assert reverted(df) == {"b"}
    info = find_reverts(df, W)
    assert info.reverted_by["b"] == "c"
    assert info.reverted_at["b"] == T0 + pd.Timedelta(minutes=6)


def test_rollback_of_several_consecutive_edits():
    df = edits([("a", 1, 1, 0, "A", "Z", "Alice"), ("b", 1, 2, 1, "B", "A", "~v"),
                ("c", 1, 3, 2, "C", "B", "~v"), ("d", 1, 4, 3, "D", "C", "~v"),
                ("e", 1, 5, 4, "A", "D", "Bot")])  # fmt: skip
    assert reverted(df) == {"b", "c", "d"}


def test_restored_revision_older_than_our_data_uses_parent_hash():
    # We never saw a revision with hash "OLD", but edit b was made on top of it.
    df = edits([("b", 1, 2, 0, "B", "OLD", "~v"), ("c", 1, 3, 1, "C", "B", "~v"),
                ("d", 1, 4, 2, "OLD", "C", "Bob")])  # fmt: skip
    assert reverted(df) == {"b", "c"}


def test_revert_after_the_window_does_not_count():
    df = edits([("a", 1, 1, 0, "A", "Z", "Alice"), ("b", 1, 2, 1, "B", "A", "~v"),
                ("c", 1, 3, 60 * 7, "A", "B", "Bob")])  # fmt: skip
    assert reverted(df) == set()
    assert reverted(df, window=timedelta(hours=8)) == {"b"}


def test_null_edit_is_not_a_revert_and_other_pages_are_independent():
    df = edits(
        [
            ("a", 1, 1, 0, "A", "Z", "Alice"),
            ("b", 1, 2, 1, "B", "A", "Bob"),
            ("n", 1, 3, 2, "B", "B", "Admin"),  # protection dummy revision: no change
            ("x", 2, 4, 3, "A", "Q", "Carol"),
        ]
    )  # same hash on another page
    assert reverted(df) == set()


def test_revert_depth_is_capped_at_15():
    rows = [("a", 1, 1, 0, "S0", "Z", "Alice")]
    for i in range(1, 21):
        rows.append((f"e{i}", 1, 1 + i, i * 0.1, f"S{i}", f"S{i - 1}", "~v"))
    rows.append(("r", 1, 30, 3, "S0", "S20", "Bob"))
    assert reverted(edits(rows)) == {f"e{i}" for i in range(6, 21)}


# ------------------------------------------------------------------------ features


def history():
    return edits([
        ("p1", 1, 1, 0, "A", "Z", "~v"), ("p2", 1, 2, 2, "A2", "A", "Bob"),
        ("p3", 1, 3, 4, "B", "A2", "~v", "/* Intro */ LOL!!!"),
        ("p4", 1, 4, 5, "A2", "B", "Bob", "Reverted edits by ~v"),
        ("t", 1, 5, 30, "C", "A2", "~v", "FUCK THIS PAGE"),
    ])  # fmt: skip


def feats(df, target_ids):
    info = find_reverts(df, W)
    targets = df[df["event_id"].isin(target_ids)].reset_index(drop=True)
    return build_features(df, targets, info.reverted_at)


def test_feature_values_for_a_known_edit():
    f = feats(history(), ["t"]).iloc[0]
    assert list(feats(history(), ["t"]).columns) == FEATURES
    assert f["is_anon"] == 1 and f["user_edit_count_missing"] == 1
    assert f["user_edits_1h"] == 2  # p1 and p3 by ~v, strictly before t
    assert f["user_reverted_24h"] == 1  # p3 was reverted (by p4) before t
    assert f["page_edits_1h"] == 4
    assert f["comment_profanity"] == 1 and f["comment_caps_ratio"] == pytest.approx(1.0)
    assert f["prev_page_editor_same"] == 0
    assert f["log_secs_since_page_edit"] == pytest.approx(np.log1p(25 * 60), rel=1e-4)


def test_features_never_look_into_the_future():
    base = history()
    future = edits([
        ("f1", 1, 6, 31, "A2", "C", "Bob"),   # reverts t one minute later
        ("f2", 1, 7, 32, "D", "A2", "~v"),
        ("f3", 2, 8, 33, "E", "Q", "~v"),
    ])  # fmt: skip
    before = feats(base, ["t", "p3"])
    after = feats(pd.concat([base, future], ignore_index=True), ["t", "p3"])
    pd.testing.assert_frame_equal(before, after)


def test_training_refuses_too_little_data():
    from wikipulse.ml.train import InsufficientData, make_dataset

    with pytest.raises(InsufficientData):
        make_dataset(history().iloc[0:0], W, T0, T0 + pd.Timedelta(days=1))
    with pytest.raises(InsufficientData):
        make_dataset(history(), W, T0, T0 + pd.Timedelta(days=1))  # 5 edits only


def test_time_split_is_chronological():
    from wikipulse.ml.train import split_by_time

    tr, va, te = split_by_time(100)
    assert (len(tr), len(va), len(te)) == (70, 10, 20)
    assert tr.max() < va.min() and va.max() < te.min()
