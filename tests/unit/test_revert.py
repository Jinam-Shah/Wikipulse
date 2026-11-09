import pytest

from wikipulse.processor.revert import comment_looks_like_revert


@pytest.mark.parametrize(
    "comment",
    [
        "Reverted edits by Example (talk) to last version by Other",
        "Reverted 1 edit by Example (talk): unsourced",
        "[[Help:Reverting|Reverted]] edits by [[Special:Contributions/X|X]]",
        "Undid revision 1384605051 by Example (talk)",
        "Restored revision 1384589598 by Example",
        "Reverting possible vandalism by Example to version by Other. Report False Positive?",
        "rv vandalism",
        "rvv",
        "RV: unsourced",
        "/* Early life */ Undid revision 123 by X",
        "revert, see talk page",
        "Rollback of edits by X",
    ],
)
def test_detects_revert_comments(comment):
    assert comment_looks_like_revert(comment)


@pytest.mark.parametrize(
    "comment",
    [
        None,
        "",
        "copyedit",
        "Added a source about the reverted decision",  # "reverted" not at the start
        "rvalue semantics explained",  # "rv" must be a whole word
        "/* Reversal */ expanded section",
        "Revenue figures updated",
        "undoubtedly better wording",
    ],
)
def test_ignores_normal_comments(comment):
    assert not comment_looks_like_revert(comment)
