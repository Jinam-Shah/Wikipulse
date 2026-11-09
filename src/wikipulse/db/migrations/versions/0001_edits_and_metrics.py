"""Phase 2: edits, page_metrics_5min, wiki_metrics_1min.

Revision ID: 0001
Revises:
Create Date: 2026-11-09
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Raw edits: a rolling window (default 7 days, pruned by the processor).
    # event_id is Wikimedia's event UUID, so a replayed event is ignored (ON CONFLICT).
    op.execute(
        """
        CREATE TABLE edits (
            event_id            text PRIMARY KEY,
            wiki                text        NOT NULL,
            change_kind         text        NOT NULL CHECK (change_kind IN ('edit', 'create')),
            page_id             bigint      NOT NULL,
            page_title          text        NOT NULL,
            namespace           integer     NOT NULL,
            revision_id         bigint      NOT NULL,
            parent_revision_id  bigint,
            rev_sha1            text,
            user_name           text,
            user_id             bigint,
            is_bot              boolean     NOT NULL,
            is_anon             boolean     NOT NULL,
            user_edit_count     integer,
            user_registered_at  timestamptz,
            comment             text,
            is_minor            boolean     NOT NULL,
            bytes_old           integer,
            bytes_new           integer,
            ts                  timestamptz NOT NULL,
            event_dt            timestamptz NOT NULL,
            ingested_at         timestamptz NOT NULL,
            revert_kind         text CHECK (revert_kind IN ('comment', 'sha1')),
            risk_score          real,
            model_version       text,
            processed_at        timestamptz NOT NULL DEFAULT now()
        );
        CREATE INDEX edits_page_ts_idx   ON edits (wiki, page_id, ts);
        CREATE INDEX edits_page_sha1_idx ON edits (wiki, page_id, rev_sha1);
        CREATE INDEX edits_ts_idx        ON edits (ts);
        CREATE INDEX edits_wiki_ts_idx   ON edits (wiki, ts);
        """
    )
    # Per-page 5-minute windows (event time). Rows are recomputed from `edits`,
    # never incremented, so replays can't double-count.
    op.execute(
        """
        CREATE TABLE page_metrics_5min (
            wiki              text        NOT NULL,
            page_id           bigint      NOT NULL,
            window_start      timestamptz NOT NULL,
            page_title        text        NOT NULL,
            edits             integer     NOT NULL,
            distinct_editors  integer     NOT NULL,
            bot_edits         integer     NOT NULL,
            anon_edits        integer     NOT NULL,
            reverts           integer     NOT NULL,
            bytes_delta       bigint      NOT NULL,
            updated_at        timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (wiki, page_id, window_start)
        );
        CREATE INDEX page_metrics_5min_window_idx ON page_metrics_5min (window_start);
        """
    )
    # Whole-wiki 1-minute windows for the live dashboard (edits/min, bot and anon share).
    op.execute(
        """
        CREATE TABLE wiki_metrics_1min (
            wiki              text        NOT NULL,
            window_start      timestamptz NOT NULL,
            edits             integer     NOT NULL,
            new_pages         integer     NOT NULL,
            bot_edits         integer     NOT NULL,
            anon_edits        integer     NOT NULL,
            reverts           integer     NOT NULL,
            distinct_pages    integer     NOT NULL,
            distinct_editors  integer     NOT NULL,
            bytes_delta       bigint      NOT NULL,
            updated_at        timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (wiki, window_start)
        );
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS wiki_metrics_1min")
    op.execute("DROP TABLE IF EXISTS page_metrics_5min")
    op.execute("DROP TABLE IF EXISTS edits")
