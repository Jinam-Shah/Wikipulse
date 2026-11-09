"""Phase 5: Breaking News Radar (baselines, trends, trend timeline, page views).

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-09
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- Each page's "normal" edit rate: EWMA mean/variance of edits per 5-minute window.
        CREATE TABLE page_baselines (
            wiki         text             NOT NULL,
            page_id      bigint           NOT NULL,
            mean         double precision NOT NULL,
            var          double precision NOT NULL,
            last_window  timestamptz      NOT NULL,
            windows_seen integer          NOT NULL,
            PRIMARY KEY (wiki, page_id)
        );
        CREATE INDEX page_baselines_last_window_idx ON page_baselines (last_window);

        CREATE TABLE trends (
            trend_id            bigserial PRIMARY KEY,
            wiki                text             NOT NULL,
            page_id             bigint           NOT NULL,
            page_title          text             NOT NULL,
            status              text             NOT NULL CHECK (status IN ('open', 'ended')),
            started_at          timestamptz      NOT NULL,  -- first spiking window
            peak_at             timestamptz      NOT NULL,
            ended_at            timestamptz,               -- end of the last active window
            -- wall-clock time the radar flagged it (differs from started_at in a replay)
            detected_at         timestamptz      NOT NULL DEFAULT now(),
            peak_zscore         double precision NOT NULL,
            peak_edits          integer          NOT NULL,
            -- most distinct editors in any one window of the trend
            distinct_editors    integer          NOT NULL,
            total_edits         integer          NOT NULL,
            quiet_windows       integer          NOT NULL DEFAULT 0,
            last_active_window  timestamptz      NOT NULL,
            views_status        text             NOT NULL DEFAULT 'pending'
                CHECK (views_status IN ('pending', 'confirmed', 'not_confirmed', 'unavailable')),
            confirmed_by_views  boolean,
            views_peak          integer,
            views_baseline      double precision,
            views_ratio         double precision,
            views_checked_at    timestamptz,
            cluster_id          bigint,
            updated_at          timestamptz      NOT NULL DEFAULT now()
        );
        -- At most one open trend per page.
        CREATE UNIQUE INDEX trends_one_open_per_page
            ON trends (wiki, page_id) WHERE status = 'open';
        CREATE INDEX trends_started_idx ON trends (started_at DESC);
        CREATE INDEX trends_views_pending_idx
            ON trends (views_status) WHERE views_status = 'pending';

        -- Window-by-window timeline of each trend (for charts and lead-time analysis).
        CREATE TABLE trend_windows (
            trend_id          bigint      NOT NULL REFERENCES trends (trend_id) ON DELETE CASCADE,
            window_start      timestamptz NOT NULL,
            edits             integer     NOT NULL,
            distinct_editors  integer     NOT NULL,
            zscore            double precision NOT NULL,
            PRIMARY KEY (trend_id, window_start)
        );

        -- Radar progress (event-time watermark).
        CREATE TABLE radar_state (
            key          text PRIMARY KEY,
            last_window  timestamptz NOT NULL,
            updated_at   timestamptz NOT NULL DEFAULT now()
        );

        -- Cache of Wikimedia Pageviews API responses (hourly, user agent type "user").
        CREATE TABLE page_views_hourly (
            project     text        NOT NULL,
            page_title  text        NOT NULL,
            hour        timestamptz NOT NULL,
            views       integer     NOT NULL,
            fetched_at  timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (project, page_title, hour)
        );
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TABLE IF EXISTS page_views_hourly, radar_state, trend_windows, trends, page_baselines"
    )
