"""lease command progress: last_command_sequence, last_progress_at

Revision ID: 0002_lease_progress
Revises: 0001_initial
Create Date: 2026-09-18

Operators watch which command of a pass the current holder has already
executed. Both columns are nullable on purpose: leases that never reported
progress — including every row written before this migration — read back as
NULL and need no backfill.
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
    op.add_column(
        "leases",
        sa.Column("last_command_sequence", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "leases",
        sa.Column("last_progress_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Command sequences are non-negative by definition.
    op.create_check_constraint(
        "leases_progress_sequence_nonneg",
        "leases",
        "last_command_sequence >= 0",
    )
    # The pair is always written together: a recorded sequence without its
    # database-clock timestamp (or vice versa) is meaningless.
    op.create_check_constraint(
        "leases_progress_pair",
        "leases",
        "(last_command_sequence IS NULL) = (last_progress_at IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("leases_progress_pair", "leases", type_="check")
    op.drop_constraint(
        "leases_progress_sequence_nonneg", "leases", type_="check"
    )
    op.drop_column("leases", "last_progress_at")
    op.drop_column("leases", "last_command_sequence")
