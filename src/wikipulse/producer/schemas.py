"""Pydantic models: the incoming Wikimedia event, and what WikiPulse publishes.

Incoming: `mediawiki.page_change.v1` (schema /mediawiki/page/change/1.x).
Only the fields WikiPulse uses are modelled; unknown fields are ignored so that
Wikimedia adding fields never breaks us. A missing *required* field is a validation
error and the event goes to the dead-letter topic.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1


class _Lenient(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


# --------------------------------------------------------------------------- incoming


class Meta(_Lenient):
    id: str
    dt: datetime
    domain: str
    stream: str
    uri: str | None = None


class Page(_Lenient):
    page_id: int
    page_title: str
    namespace_id: int
    is_redirect: bool = False


class User(_Lenient):
    user_text: str | None = None  # absent when the username is suppressed
    user_id: int | None = None  # absent for IP editors on wikis without temp accounts
    is_temp: bool = False  # temporary account = logged-out editor
    is_bot: bool = False
    groups: list[str] = Field(default_factory=list)
    edit_count: int | None = None
    registration_dt: datetime | None = None


class Revision(_Lenient):
    rev_id: int
    rev_dt: datetime
    rev_parent_id: int | None = None
    rev_size: int | None = None
    rev_sha1: str | None = None
    is_minor_edit: bool = False
    comment: str | None = None  # absent when the comment is suppressed
    editor: User | None = None


class PriorState(_Lenient):
    revision: Revision | None = None


class PageChange(_Lenient):
    meta: Meta
    wiki_id: str
    page_change_kind: str
    dt: datetime
    page: Page
    revision: Revision
    performer: User | None = None
    prior_state: PriorState | None = None


# --------------------------------------------------------------------------- outgoing


class EditEvent(BaseModel):
    """One edit or page creation, as published to the `wikipulse.edits.v1` topic."""

    model_config = ConfigDict(frozen=True)

    schema_version: int = SCHEMA_VERSION
    event_id: str  # Wikimedia meta.id (UUID): our idempotency key downstream
    wiki: str
    change_kind: Literal["edit", "create"]
    page_id: int
    page_title: str  # human-readable, spaces not underscores
    namespace: int
    revision_id: int
    parent_revision_id: int | None
    rev_sha1: str | None  # content hash: identical hashes reveal exact reverts
    user_name: str | None
    user_id: int | None
    is_bot: bool
    is_anon: bool  # temporary account or IP editor
    user_edit_count: int | None
    user_registered_at: datetime | None
    comment: str | None
    is_minor: bool
    bytes_old: int | None
    bytes_new: int | None
    ts: datetime  # when the revision was saved on Wikipedia
    event_dt: datetime  # when Wikimedia emitted the event
    ingested_at: datetime  # when WikiPulse read it (for end-to-end latency)

    @property
    def kafka_key(self) -> str:
        # Same page -> same partition -> per-page ordering for the stream processor.
        return f"{self.wiki}:{self.page_id}"


class DeadLetter(BaseModel):
    """An event we could not use, published to the dead-letter topic with the reason."""

    # processor_validation: a message on the edits topic that the stream processor
    # couldn't parse (e.g. written by an incompatible producer version).
    reason: Literal["invalid_json", "schema_validation", "unexpected_error", "processor_validation"]
    error: str
    raw: str  # truncated original payload
    sse_id: str | None
    received_at: datetime
