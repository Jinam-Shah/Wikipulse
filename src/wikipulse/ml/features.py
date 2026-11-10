"""Features for the vandalism risk model - the SAME code for training and live scoring.

Every feature uses only what was knowable when the edit was saved (point in time):
counts look strictly backwards from the edit's timestamp, and "user's recently reverted
edits" only counts reverts that had already happened by then. A unit test checks that
adding future edits never changes an edit's features.

Not used (docs/DECISIONS.md D-035): diff text (would need one Action API call per edit,
far beyond polite rate limits for ~5,000 edits/hour) and page protection level (needs
API calls too). The comment text, editor history and page activity stand in for them.
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

HOUR = np.timedelta64(3600, "s")
DAY = np.timedelta64(86400, "s")

# A short, generic list; the point is the signal, not an exhaustive filter.
_PROFANITY = re.compile(
    r"\b(?:fuck\w*|shit\w*|bitch\w*|cunt\w*|dick\w*|cock\w*|pussy|asshole\w*|bastard\w*|"
    r"slut\w*|whore\w*|fag\w*|retard\w*|penis|vagina|poop\w*|gay|sex\w*|nigg\w*|idiot\w*|"
    r"stupid|dumb|suck\w*|lol+|haha\w*|hello|hi)\b",
    re.IGNORECASE,
)
_SECTION = re.compile(r"^\s*/\*.*?\*/")

FEATURES = [
    "is_anon", "is_bot", "is_minor", "is_create", "is_revert_edit",
    "log_user_edit_count", "user_edit_count_missing",
    "log_account_age_days", "account_age_missing",
    "bytes_delta", "log_abs_bytes_delta", "log_bytes_new", "rel_change",
    "comment_empty", "comment_len", "comment_caps_ratio", "comment_has_section",
    "comment_profanity", "comment_exclaim", "comment_auto_summary",
    "hour_sin", "hour_cos", "is_weekend",
    "user_edits_1h", "user_edits_24h", "user_reverted_24h",
    "page_edits_1h", "page_edits_24h", "page_reverts_24h",
    "log_secs_since_page_edit", "prev_page_editor_same",
]  # fmt: skip


def _comment_features(comment: pd.Series) -> pd.DataFrame:
    c = comment.fillna("")
    body = c.str.replace(_SECTION, "", regex=True).str.strip()
    letters = body.str.count(r"[A-Za-z]")
    caps = body.str.count(r"[A-Z]")
    return pd.DataFrame(
        {
            "comment_empty": (body.str.len() == 0).astype(np.int8),
            "comment_len": c.str.len().clip(upper=500).astype(np.float32),
            "comment_caps_ratio": np.where(letters >= 4, caps / letters.clip(lower=1), 0.0),
            "comment_has_section": c.str.match(_SECTION).astype(np.int8),
            "comment_profanity": body.str.contains(_PROFANITY).astype(np.int8),
            "comment_exclaim": body.str.count("!").clip(upper=10).astype(np.float32),
            # MediaWiki's automatic summaries for page blanking / replacement
            "comment_auto_summary": c.str.contains(
                r"^(?:Blanked the page|Replaced content with|Removed redirect)", regex=True
            ).astype(np.int8),
        },
        index=comment.index,
    )


def _count_before(times: np.ndarray, at: np.ndarray, width: np.timedelta64) -> np.ndarray:
    """For each t in `at`: how many of the sorted `times` fall in [t - width, t)."""
    return np.searchsorted(times, at, side="left") - np.searchsorted(times, at - width, "left")


def _group_window_counts(
    df: pd.DataFrame, key: str, targets: pd.DataFrame, width: np.timedelta64
) -> np.ndarray:
    out = np.zeros(len(targets), dtype=np.float32)
    ctx = df[[key, "ts64"]].dropna(subset=[key]).sort_values("ts64")
    by_key = {k: g["ts64"].to_numpy() for k, g in ctx.groupby(key, sort=False)}
    pos = np.arange(len(targets))
    for k, idx in pd.Series(pos, index=targets[key].values).groupby(level=0):
        times = by_key.get(k)
        if times is not None:
            out[idx.values] = _count_before(times, targets["ts64"].to_numpy()[idx.values], width)
    return out


def build_features(
    context: pd.DataFrame, targets: pd.DataFrame, reverted_at: pd.Series
) -> pd.DataFrame:
    """Feature matrix for `targets` (rows of edits), using `context` (all edits around them,
    which must include the 24 h before each target) and the revert times from labels.py."""
    ctx = context.copy()
    ctx["ts64"] = ctx["ts"].dt.tz_convert("UTC").dt.tz_localize(None).to_numpy("datetime64[ns]")
    ctx["user_key"] = ctx["wiki"] + "|" + ctx["user_name"].fillna("")
    ctx.loc[ctx["user_name"].isna(), "user_key"] = None
    ctx["page_key"] = ctx["wiki"] + "|" + ctx["page_id"].astype(str)
    t = targets.copy()
    t["ts64"] = t["ts"].dt.tz_convert("UTC").dt.tz_localize(None).to_numpy("datetime64[ns]")
    t["user_key"] = t["wiki"] + "|" + t["user_name"].fillna("")
    t.loc[t["user_name"].isna(), "user_key"] = None
    t["page_key"] = t["wiki"] + "|" + t["page_id"].astype(str)

    f = pd.DataFrame(index=t.index)
    f["is_anon"] = t["is_anon"].astype(np.int8)
    f["is_bot"] = t["is_bot"].astype(np.int8)
    f["is_minor"] = t["is_minor"].astype(np.int8)
    f["is_create"] = (t["change_kind"] == "create").astype(np.int8)
    f["is_revert_edit"] = t["revert_kind"].notna().astype(np.int8)
    uec = t["user_edit_count"].astype("float64")
    f["log_user_edit_count"] = np.log1p(uec.fillna(0))
    f["user_edit_count_missing"] = uec.isna().astype(np.int8)
    age = (t["ts"] - t["user_registered_at"]).dt.total_seconds() / 86400
    f["log_account_age_days"] = np.log1p(age.clip(lower=0).fillna(0))
    f["account_age_missing"] = age.isna().astype(np.int8)
    old = t["bytes_old"].astype("float64").fillna(0)
    new = t["bytes_new"].astype("float64").fillna(0)
    delta = new - old
    f["bytes_delta"] = delta.clip(-100_000, 100_000)
    f["log_abs_bytes_delta"] = np.log1p(delta.abs())
    f["log_bytes_new"] = np.log1p(new)
    f["rel_change"] = (delta / old.clip(lower=1)).clip(-1, 10)
    f = f.join(_comment_features(t["comment"]))
    hour = t["ts"].dt.hour + t["ts"].dt.minute / 60
    f["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    f["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    f["is_weekend"] = (t["ts"].dt.day_of_week >= 5).astype(np.int8)

    f["user_edits_1h"] = _group_window_counts(ctx, "user_key", t, HOUR)
    f["user_edits_24h"] = _group_window_counts(ctx, "user_key", t, DAY)
    f["page_edits_1h"] = _group_window_counts(ctx, "page_key", t, HOUR)
    f["page_edits_24h"] = _group_window_counts(ctx, "page_key", t, DAY)
    reverts = ctx[ctx["revert_kind"].notna()]
    f["page_reverts_24h"] = _group_window_counts(reverts, "page_key", t, DAY)

    # The user's edits that had ALREADY been reverted (revert time < this edit's time).
    rv = ctx[["user_key", "event_id"]].copy()
    rv["ts64"] = (
        reverted_at.reindex(rv["event_id"]).dt.tz_convert("UTC").dt.tz_localize(None).to_numpy()
    )
    f["user_reverted_24h"] = _group_window_counts(rv.dropna(subset=["ts64"]), "user_key", t, DAY)

    # Time since the page's previous edit, and whether that edit was by the same user.
    pc = ctx.sort_values(["page_key", "ts64", "revision_id"])
    prev_ts, prev_user = {}, {}
    for _key, g in pc.groupby("page_key", sort=False):
        ts_prev = g["ts64"].shift(1)
        user_prev = g["user_key"].shift(1)
        prev_ts.update(zip(g["event_id"], ts_prev, strict=True))
        prev_user.update(zip(g["event_id"], user_prev, strict=True))
    secs = (t["ts64"] - t["event_id"].map(prev_ts)).dt.total_seconds()
    f["log_secs_since_page_edit"] = np.log1p(secs.clip(lower=0).fillna(30 * 86400))
    f["prev_page_editor_same"] = (
        (t["event_id"].map(prev_user) == t["user_key"]) & t["user_key"].notna()
    ).astype(np.int8)
    return f[FEATURES].astype(np.float32)
