"""Phase 4: data_quality_checks (history of every check run by Airflow).

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-09
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE data_quality_checks (
            id           bigserial PRIMARY KEY,
            run_at       timestamptz NOT NULL DEFAULT now(),
            target_hour  timestamptz NOT NULL,
            check_name   text        NOT NULL,
            wiki         text,
            status       text        NOT NULL CHECK (status IN ('pass', 'warn', 'fail')),
            observed     double precision,
            threshold    text,
            message      text        NOT NULL,
            details      jsonb       NOT NULL DEFAULT '{}'::jsonb
        );
        CREATE INDEX data_quality_checks_run_idx ON data_quality_checks (run_at DESC);
        CREATE INDEX data_quality_checks_name_idx ON data_quality_checks (check_name, target_hour);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS data_quality_checks")
