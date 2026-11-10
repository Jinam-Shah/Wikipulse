"""Raw SSE payload -> EditEvent, Skipped, or DeadLetter. Pure functions, easy to test."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import ValidationError

from wikipulse.producer.schemas import DeadLetter, EditEvent, PageChange, User

KEPT_KINDS = frozenset({"edit", "create"})
MAX_RAW_CHARS = 100_000  # cap what we copy into the dead-letter topic


@dataclass(frozen=True)
class Filters:
    wikis: frozenset[str]
    namespaces: frozenset[int]


@dataclass(frozen=True)
class Skipped:
    """Valid-looking event that WikiPulse doesn't track (not an error)."""

    reason: str


Result = EditEvent | Skipped | DeadLetter


def _now() -> datetime:
    return datetime.now(UTC)


def _dead(reason: str, error: str, raw: str, sse_id: str | None, now: datetime) -> DeadLetter:
    return DeadLetter(
        reason=reason,  # type: ignore[arg-type]
        error=error[:2000],
        raw=raw[:MAX_RAW_CHARS],
        sse_id=sse_id,
        received_at=now,
    )


def prefilter(doc: dict, filters: Filters) -> Skipped | None:
    """Cheap checks on the raw dict, so other wikis' events never reach validation.

    Returns None when the event should be validated. If a field needed to decide is
    missing or malformed we also return None, so validation reports it properly.
    """
    meta = doc.get("meta")
    if isinstance(meta, dict) and meta.get("domain") == "canary":
        return Skipped("canary")  # Wikimedia's synthetic health-check events
    wiki = doc.get("wiki_id")
    if isinstance(wiki, str) and wiki not in filters.wikis:
        return Skipped("wiki")
    kind = doc.get("page_change_kind")
    if isinstance(kind, str) and kind not in KEPT_KINDS:
        return Skipped("kind")
    page = doc.get("page")
    if isinstance(page, dict):
        ns = page.get("namespace_id")
        if isinstance(ns, int) and ns not in filters.namespaces:
            return Skipped("namespace")
    return None


def _is_anon(user: User) -> bool:
    if user.is_temp:
        return True
    # IP editors (wikis without temporary accounts) have a name but no user id.
    return user.user_text is not None and not user.user_id


def to_edit_event(pc: PageChange, ingested_at: datetime) -> EditEvent:
    user = pc.performer or pc.revision.editor or User()
    prior = pc.prior_state.revision if pc.prior_state else None
    return EditEvent(
        event_id=pc.meta.id,
        wiki=pc.wiki_id,
        change_kind=pc.page_change_kind,  # type: ignore[arg-type]  # prefiltered
        page_id=pc.page.page_id,
        page_title=pc.page.page_title.replace("_", " "),
        namespace=pc.page.namespace_id,
        revision_id=pc.revision.rev_id,
        parent_revision_id=pc.revision.rev_parent_id or None,
        rev_sha1=pc.revision.rev_sha1,
        parent_rev_sha1=prior.rev_sha1 if prior else None,
        user_name=user.user_text,
        user_id=user.user_id or None,
        is_bot=user.is_bot or "bot" in user.groups,
        is_anon=_is_anon(user),
        user_edit_count=user.edit_count,
        user_registered_at=user.registration_dt,
        comment=pc.revision.comment,
        is_minor=pc.revision.is_minor_edit,
        bytes_old=prior.rev_size if prior else None,
        bytes_new=pc.revision.rev_size,
        ts=pc.revision.rev_dt,
        event_dt=pc.meta.dt,
        ingested_at=ingested_at,
    )


def normalize(
    raw: str,
    filters: Filters,
    sse_id: str | None = None,
    now: Callable[[], datetime] = _now,
) -> Result:
    received = now()
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError as exc:
        return _dead("invalid_json", str(exc), raw, sse_id, received)
    if not isinstance(doc, dict):
        return _dead("schema_validation", "payload is not a JSON object", raw, sse_id, received)

    skipped = prefilter(doc, filters)
    if skipped:
        return skipped

    try:
        pc = PageChange.model_validate(doc)
    except ValidationError as exc:
        return _dead("schema_validation", str(exc), raw, sse_id, received)

    # Validation may reveal values the prefilter couldn't judge (e.g. a string namespace).
    if pc.wiki_id not in filters.wikis:
        return Skipped("wiki")
    if pc.page_change_kind not in KEPT_KINDS:
        return Skipped("kind")
    if pc.page.namespace_id not in filters.namespaces:
        return Skipped("namespace")

    try:
        return to_edit_event(pc, received)
    except (ValidationError, ValueError) as exc:
        return _dead("unexpected_error", str(exc), raw, sse_id, received)
