"""add caption language to meeting

Revision ID: 8e4a6f5c9b21
Revises: f55b6dc85c7a
Create Date: 2026-03-16 16:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = "8e4a6f5c9b21"
down_revision: Union[str, Sequence[str], None] = "f55b6dc85c7a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = inspect(bind)
    column_names = {column["name"] for column in inspector.get_columns("meeting")}

    if "caption_language" not in column_names:
        with op.batch_alter_table("meeting", schema=None) as batch_op:
            batch_op.add_column(
                sa.Column(
                    "caption_language",
                    sa.String(),
                    nullable=False,
                    server_default="tr",
                )
            )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = inspect(bind)
    column_names = {column["name"] for column in inspector.get_columns("meeting")}

    if "caption_language" in column_names:
        with op.batch_alter_table("meeting", schema=None) as batch_op:
            batch_op.drop_column("caption_language")
