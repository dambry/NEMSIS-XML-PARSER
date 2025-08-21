"""rename_text_content_to_dynamic_value_columns

This migration renames the 'text_content' column to '{table_name}_value' in all dynamic tables.
It excludes system tables like SchemaVersions and XMLFilesProcessed.

Revision ID: 1941212973eb
Revises:
Create Date: 2025-08-21 04:43:18.677690

"""

from typing import Sequence, Union
import os
import sys

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# Add project root to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

try:
    from config import PG_SCHEMA
except ImportError:
    PG_SCHEMA = "public"

# revision identifiers, used by Alembic.
revision: str = "1941212973eb"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema: Rename text_content columns to {table_name}_value."""
    # Get database connection
    conn = op.get_bind()

    # Find all tables in our schema that have a 'text_content' column
    # but exclude system tables
    query = text(
        """
        SELECT t.table_name 
        FROM information_schema.tables t
        INNER JOIN information_schema.columns c 
            ON t.table_name = c.table_name 
            AND t.table_schema = c.table_schema
        WHERE t.table_schema = :schema
            AND t.table_type = 'BASE TABLE'
            AND c.column_name = 'text_content'
            AND t.table_name NOT IN ('SchemaVersions', 'XMLFilesProcessed')
            AND t.table_name NOT LIKE 'pg_%'
    """
    )

    result = conn.execute(query, {"schema": PG_SCHEMA})
    tables_with_text_content = [row[0] for row in result]

    print(f"Found {len(tables_with_text_content)} tables with text_content column")

    # For each table, rename text_content to {table_name}_value
    for table_name in tables_with_text_content:
        new_column_name = f"{table_name}_value"
        print(f"Renaming text_content to {new_column_name} in table {table_name}")

        # Use raw SQL for the rename operation
        rename_sql = text(
            f"""
            ALTER TABLE "{PG_SCHEMA}"."{table_name}" 
            RENAME COLUMN "text_content" TO "{new_column_name}"
        """
        )
        conn.execute(rename_sql)


def downgrade() -> None:
    """Downgrade schema: Rename {table_name}_value columns back to text_content."""
    # Get database connection
    conn = op.get_bind()

    # Find all tables in our schema that have columns ending with '_value'
    # but exclude system tables
    query = text(
        """
        SELECT t.table_name, c.column_name
        FROM information_schema.tables t
        INNER JOIN information_schema.columns c 
            ON t.table_name = c.table_name 
            AND t.table_schema = c.table_schema
        WHERE t.table_schema = :schema
            AND t.table_type = 'BASE TABLE'
            AND c.column_name LIKE '%_value'
            AND t.table_name NOT IN ('SchemaVersions', 'XMLFilesProcessed')
            AND t.table_name NOT LIKE 'pg_%'
            AND c.column_name = t.table_name || '_value'  -- Only columns that match the pattern
    """
    )

    result = conn.execute(query, {"schema": PG_SCHEMA})
    table_column_pairs = [(row[0], row[1]) for row in result]

    print(f"Found {len(table_column_pairs)} tables with dynamic value columns")

    # For each table, rename {table_name}_value back to text_content
    for table_name, column_name in table_column_pairs:
        print(f"Renaming {column_name} back to text_content in table {table_name}")

        # Use raw SQL for the rename operation
        rename_sql = text(
            f"""
            ALTER TABLE "{PG_SCHEMA}"."{table_name}" 
            RENAME COLUMN "{column_name}" TO "text_content"
        """
        )
        conn.execute(rename_sql)
