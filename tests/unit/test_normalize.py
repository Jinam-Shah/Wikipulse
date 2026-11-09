from __future__ import annotations

from datetime import UTC, datetime

import pytest

from conftest import as_json, make_page_change
from wikipulse.producer.normalize import Filters, Skipped, normalize
from wikipulse.producer.schemas import DeadLetter, EditEvent

FILTERS = Filters(wikis=frozenset({"enwiki"}), namespaces=frozenset({0}))
NOW = datetime(2026, 11, 9, 2, 35, 30, tzinfo=UTC)


def run(doc) -> EditEvent | Skipped | DeadLetter:
    raw = doc if isinstance(doc, str) else as_json(doc)
    return normalize(raw, FILTERS, sse_id="sse-1", now=lambda: NOW)


def test_valid_edit_is_normalized():
    ev = run(make_page_change())
    assert isinstance(ev, EditEvent)
    assert ev.event_id == "11111111-2222-3333-4444-555555555555"
    assert ev.kafka_key == "enwiki:4242"
    assert ev.page_title == "Example Page"
    assert (ev.revision_id, ev.parent_revision_id) == (2000, 1999)
    assert (ev.bytes_old, ev.bytes_new) == (13327, 13400)
    assert ev.user_name == "ExampleEditor" and ev.user_edit_count == 523
    assert ev.is_bot is False and ev.is_anon is False
    assert ev.ingested_at == NOW
    assert ev.ts == datetime(2026, 11, 9, 2, 35, 22, tzinfo=UTC)


def test_page_creation_has_no_old_size():
    doc = make_page_change(page_change_kind="create", changelog_kind="insert")
    del doc["prior_state"]
    doc["revision"]["rev_parent_id"] = 0
    ev = run(doc)
    assert isinstance(ev, EditEvent)
    assert ev.change_kind == "create"
    assert ev.bytes_old is None and ev.parent_revision_id is None


def test_temporary_account_is_anonymous():
    ev = run(make_page_change(performer={"user_text": "~2026-12345-67", "is_temp": True}))
    assert ev.is_anon is True


def test_ip_editor_without_user_id_is_anonymous():
    doc = make_page_change()
    doc["performer"] = {"user_text": "203.0.113.5", "groups": ["*"], "is_temp": False}
    assert run(doc).is_anon is True


def test_bot_detected_from_flag_or_group():
    assert run(make_page_change(performer={"is_bot": True})).is_bot is True
    assert run(make_page_change(performer={"groups": ["bot", "user"]})).is_bot is True


def test_suppressed_username_and_comment_are_allowed():
    doc = make_page_change()
    del doc["performer"]["user_text"]
    del doc["revision"]["comment"]
    ev = run(doc)
    assert isinstance(ev, EditEvent)
    assert ev.user_name is None and ev.comment is None and ev.is_anon is False


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"wiki_id": "dewiki"}, "wiki"),
        ({"page_change_kind": "delete"}, "kind"),
        ({"page_change_kind": "move"}, "kind"),
        ({"page": {"namespace_id": 1}}, "namespace"),
        ({"meta": {"domain": "canary"}}, "canary"),
    ],
)
def test_untracked_events_are_skipped_not_dead_lettered(overrides, reason):
    assert run(make_page_change(**overrides)) == Skipped(reason)


def test_other_wikis_are_skipped_even_if_malformed():
    # Must not inflate our dead-letter rate with events we never wanted.
    doc = {"wiki_id": "frwiki", "meta": {"domain": "fr.wikipedia.org"}}
    assert run(doc) == Skipped("wiki")


def test_invalid_json_goes_to_dead_letter():
    dl = run("{not json")
    assert isinstance(dl, DeadLetter)
    assert dl.reason == "invalid_json" and dl.raw == "{not json" and dl.sse_id == "sse-1"


def test_non_object_json_goes_to_dead_letter():
    assert run("[1, 2]").reason == "schema_validation"


def test_missing_required_field_goes_to_dead_letter():
    doc = make_page_change()
    del doc["revision"]["rev_id"]
    dl = run(doc)
    assert isinstance(dl, DeadLetter) and dl.reason == "schema_validation"
    assert "rev_id" in dl.error


def test_wrong_type_goes_to_dead_letter():
    dl = run(make_page_change(page={"page_id": "not-a-number"}))
    assert isinstance(dl, DeadLetter) and "page_id" in dl.error


def test_dead_letter_raw_payload_is_truncated():
    dl = run("x" * 200_000)
    assert isinstance(dl, DeadLetter) and len(dl.raw) == 100_000


def test_unknown_fields_are_ignored():
    assert isinstance(run(make_page_change(brand_new_field={"x": 1})), EditEvent)
