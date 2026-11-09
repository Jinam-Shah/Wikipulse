"""Shared test helpers. Events are synthetic but follow the real
mediawiki.page_change.v1 shape observed on 2026-11-09 (usernames are made up)."""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest


def _deep_merge(base: dict, overrides: dict) -> dict:
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


BASE_EVENT: dict[str, Any] = {
    "$schema": "/mediawiki/page/change/1.12.0",
    "changelog_kind": "update",
    "page_change_kind": "edit",
    "dt": "2026-11-09T02:35:22Z",
    "wiki_id": "enwiki",
    "meta": {
        "stream": "mediawiki.page_change.v1",
        "uri": "https://en.wikipedia.org/wiki/Example_Page",
        "id": "11111111-2222-3333-4444-555555555555",
        "domain": "en.wikipedia.org",
        "dt": "2026-11-09T02:35:24.725Z",
        "topic": "codfw.mediawiki.page_change.v1",
        "partition": 0,
        "offset": 861548088,
        "key": {"wiki_id": "enwiki", "page_id": 4242},
    },
    "page": {
        "page_id": 4242,
        "page_title": "Example_Page",
        "namespace_id": 0,
        "is_redirect": False,
        "namespace_is_content": True,
    },
    "performer": {
        "user_text": "ExampleEditor",
        "groups": ["*", "user", "autoconfirmed"],
        "is_temp": False,
        "user_id": 1001,
        "edit_count": 523,
        "registration_dt": "2020-01-02T03:04:05Z",
        "is_bot": False,
        "is_system": False,
    },
    "revision": {
        "rev_id": 2000,
        "rev_dt": "2026-11-09T02:35:22Z",
        "is_minor_edit": False,
        "rev_sha1": "abc123",
        "rev_size": 13400,
        "rev_parent_id": 1999,
        "comment": "copyedit",
        "is_comment_visible": True,
    },
    "prior_state": {
        "revision": {
            "rev_id": 1999,
            "rev_dt": "2026-09-12T21:36:26Z",
            "is_minor_edit": True,
            "rev_sha1": "def456",
            "rev_size": 13327,
        }
    },
}


def make_page_change(**overrides: Any) -> dict[str, Any]:
    """A page_change event dict; nested dicts in overrides are merged, others replaced."""
    return _deep_merge(copy.deepcopy(BASE_EVENT), overrides)


def as_json(doc: dict[str, Any]) -> str:
    return json.dumps(doc)


@pytest.fixture
def page_change():
    return make_page_change
