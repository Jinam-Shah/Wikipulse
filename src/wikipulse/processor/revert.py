"""Spot reverts from the edit summary.

This is one of two revert signals. The other (content hash identical to an earlier
revision of the same page) is computed in SQL, see store.py. Wikimedia's own
`mw-reverted` / `mw-undo` / `mw-rollback` tags arrive on a separate stream and are
added for model labels in Phase 6. Known gaps are listed in docs/DECISIONS.md (D-015).
"""

from __future__ import annotations

import re

# Leading "/* Section name */" that MediaWiki adds when editing a section.
_SECTION = re.compile(r"^\s*/\*.*?\*/\s*")
# Optional wiki-link wrapper, e.g. "[[Help:Reverting|Reverted]] edits by ...".
_REVERT = re.compile(
    r"""^\s*(?:\[\[[^\]|]*\|)?
        (?: reverted | reverting | revert(?:ed)?\b | undid | undo\b
          | restored\ revision | rollback | rvv?\b | rv/ )
    """,
    re.IGNORECASE | re.VERBOSE,
)


def comment_looks_like_revert(comment: str | None) -> bool:
    if not comment:
        return False
    return bool(_REVERT.match(_SECTION.sub("", comment, count=1)))
