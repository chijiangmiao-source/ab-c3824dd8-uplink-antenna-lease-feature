"""lease progress: last_command_sequence + last_progress_at on leases

Revision ID: 0002_lease_progress
Revises: 0001_initial
Create Date: 2026-09-18
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_lease_progress"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Both columns stay NULL until the holder reports progress, so leases
    # created before this migration need no backfill.
    op.add_column(
        "leases",
        sa.Column("last_command_sequence", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "leases",
        sa.Column("last_progress_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Sequences are non-negative and the pair is always written together by
    # the same UPDATE, so a row may never carry one without the other.
    op.create_check_constraint(
        "ck_leases_progress_sequence_nonneg",
        "leases",
        "last_command_sequence >= 0",
    )
    op.create_check_constraint(
        "ck_leases_progress_pair",
        "leases",
        "(last_command_sequence IS NULL) = (last_progress_at IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_leases_progress_pair", "leases", type_="check")
    op.drop_constraint(
        "ck_leases_progress_sequence_nonneg", "leases", type_="check"
    )
    op.drop_column("leases", "last_progress_at")
    op.drop_column("leases", "last_command_sequence")
