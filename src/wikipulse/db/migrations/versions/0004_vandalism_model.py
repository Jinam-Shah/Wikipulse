"""Phase 6: vandalism model (parent content hash, Lift Wing scores, model runs).

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-10
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- Content hash of the revision each edit was made on (labels: "was it reverted?").
        ALTER TABLE edits ADD COLUMN parent_rev_sha1 text;
        CREATE INDEX edits_page_rev_idx ON edits (wiki, page_id, revision_id);
        CREATE INDEX edits_unscored_idx ON edits (ts) WHERE risk_score IS NULL;

        -- Wikimedia's own revert-risk predictions (Lift Wing), from the
        -- mediawiki.page_revert_risk_prediction_change.v1 stream.
        CREATE TABLE liftwing_scores (
            wiki           text             NOT NULL,
            revision_id    bigint           NOT NULL,
            model_name     text             NOT NULL,
            model_version  text             NOT NULL,
            prob_revert    double precision NOT NULL,
            event_dt       timestamptz      NOT NULL,
            PRIMARY KEY (wiki, revision_id, model_name)
        );

        CREATE TABLE model_runs (
            version        text PRIMARY KEY,
            trained_at     timestamptz NOT NULL DEFAULT now(),
            metrics        jsonb       NOT NULL,
            params         jsonb       NOT NULL DEFAULT '{}'::jsonb,
            threshold      double precision NOT NULL,
            mlflow_run_id  text,
            is_current     boolean     NOT NULL DEFAULT false
        );
        CREATE UNIQUE INDEX model_runs_one_current ON model_runs (is_current) WHERE is_current;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS model_runs, liftwing_scores;
        DROP INDEX IF EXISTS edits_unscored_idx;
        DROP INDEX IF EXISTS edits_page_rev_idx;
        ALTER TABLE edits DROP COLUMN IF EXISTS parent_rev_sha1;
        """
    )
