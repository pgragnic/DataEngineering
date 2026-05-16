"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-05-16
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "inspections",
        sa.Column("id", PG_UUID(as_uuid=True), primary_key=True),
        sa.Column("client_name", sa.String(255), nullable=False),
        sa.Column("client_siret", sa.String(20), nullable=True),
        sa.Column("site_name", sa.String(255), nullable=False),
        sa.Column("site_address", sa.String(500), nullable=True),
        sa.Column("auditor_name", sa.String(255), nullable=False),
        sa.Column(
            "referential", sa.String(100), nullable=False, server_default="ISO 9001:2015"
        ),
        sa.Column("scope", sa.String(2000), nullable=False),
        sa.Column(
            "status", sa.String(50), nullable=False, server_default="prepared"
        ),
        sa.Column("checklist_json", JSONB, nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_table(
        "constats",
        sa.Column("id", PG_UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "inspection_id",
            PG_UUID(as_uuid=True),
            sa.ForeignKey("inspections.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("checklist_point_id", sa.String(50), nullable=True),
        sa.Column("raw_text", sa.Text, nullable=False),
        sa.Column("reformulated_text", sa.Text, nullable=False),
        sa.Column("classification", sa.String(50), nullable=False),
        sa.Column("norm_reference", sa.String(100), nullable=True),
        sa.Column("norm_excerpt", sa.Text, nullable=True),
        sa.Column("suggested_evidence", sa.Text, nullable=True),
        sa.Column("suggested_action", sa.Text, nullable=True),
        sa.Column("rag_chunks", JSONB, nullable=True),
        sa.Column("photo_path", sa.String(500), nullable=True),
        sa.Column("audio_path", sa.String(500), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index("ix_constats_inspection_id", "constats", ["inspection_id"])


def downgrade() -> None:
    op.drop_index("ix_constats_inspection_id", table_name="constats")
    op.drop_table("constats")
    op.drop_table("inspections")
