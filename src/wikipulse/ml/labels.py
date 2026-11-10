"""Labels: was an edit reverted (and when)?

An edit E counts as REVERTED when a later edit R on the same page, within the label
window, restores the page's content to exactly what it was *before* E. "Exactly" means
the same SHA-1 content hash, the same test MediaWiki uses for its own `mw-reverted` tag
(manual reverts, undo and rollback all produce it).

For each edit R with content hash S, the edits it reverted are the ones after the most
recent point where the page was in state S. That point is found either as an earlier
revision whose hash is S, or as an earlier edit whose *parent* hash is S (needed when
the restored revision is older than our data). At most 15 edits are counted per revert,
MediaWiki's default revert depth. Edits that don't change content (rev hash == parent
hash, e.g. protection dummy revisions) never count as reverts.

Known limits (docs/DECISIONS.md D-034): reverts that don't restore an exact earlier state
(partial or manual rewrites) are missed; good-faith edits reverted in content disputes
count as "reverted" (that's what the label means, same as Lift Wing's training target).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pandas as pd

MAX_DEPTH = 15


@dataclass(frozen=True)
class RevertInfo:
    reverted_at: pd.Series  # event_id -> timestamp of the reverting edit (NaT if never)
    reverted_by: pd.Series  # event_id -> event_id of the reverting edit


def find_reverts(edits: pd.DataFrame, window: timedelta) -> RevertInfo:
    """`edits` needs: event_id, wiki, page_id, revision_id, ts, rev_sha1, parent_rev_sha1."""
    reverted_at: dict[str, pd.Timestamp] = {}
    reverted_by: dict[str, str] = {}
    cols = ["event_id", "revision_id", "ts", "rev_sha1", "parent_rev_sha1"]
    ordered = edits.sort_values(["wiki", "page_id", "revision_id"])
    for _, page in ordered.groupby(["wiki", "page_id"], sort=False):
        ids, _revs, times, sha, parent = (page[c].tolist() for c in cols)
        last_with_sha: dict[str, int] = {}  # hash -> latest index whose revision has it
        last_with_parent: dict[str, int] = {}  # hash -> latest index whose parent has it
        for j in range(len(ids)):
            s = sha[j]
            if s and s != parent[j]:
                candidates = []
                if s in last_with_sha:
                    candidates.append(last_with_sha[s] + 1)
                if s in last_with_parent:
                    candidates.append(last_with_parent[s])
                if candidates:
                    start = max(max(candidates), j - MAX_DEPTH)
                    for k in range(start, j):
                        if ids[k] not in reverted_at and times[j] - times[k] <= window:
                            reverted_at[ids[k]] = times[j]
                            reverted_by[ids[k]] = ids[j]
            if s:
                last_with_sha[s] = j
            if parent[j]:
                last_with_parent[parent[j]] = j
    idx = edits["event_id"]
    return RevertInfo(
        reverted_at=pd.Series(
            pd.to_datetime(idx.map(reverted_at), utc=True).array, index=idx.to_numpy()
        ),
        reverted_by=pd.Series(idx.map(reverted_by).to_numpy(), index=idx.to_numpy(), dtype=object),
    )
