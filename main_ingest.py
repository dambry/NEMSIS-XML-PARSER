import sys
import psycopg2
import psycopg2.extras
from psycopg2 import errors as psycopg2_errors  # Import for specific error codes
import uuid
import datetime
import os
import hashlib
import argparse
import shutil
import re  # For more advanced sanitization if needed

# Project-specific imports
try:
    from config import PG_SCHEMA
    from database_setup import get_db_connection  # Expects database_setup to be updated
    from xml_handler import (
        parse_xml_file,
        _sanitize_name as sanitize_xml_name,
    )  # Use sanitizer from xml_handler
except ImportError as e:
    print(f"Error: Could not import necessary project modules: {e}")
    print(
        "Please ensure config.py, database_setup.py (updated), and xml_handler.py are in the PYTHONPATH."
    )
    exit(1)

ARCHIVE_DIR = "processed_xml_archive"
ERROR_DIR = "error_files"
# Schema version for the ingestion LOGIC, not the data schema itself which is now dynamic
INGESTION_LOGIC_VERSION_NUMBER = "1.0.0-dynamic-ingestor-v4"


# --- Utility Functions ---
def generate_unique_file_id():
    return str(uuid.uuid4())


def get_file_md5(file_path):
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        return None  # Error printed by caller if needed
    except Exception as e:
        print(f"Error calculating MD5 for {file_path}: {e}")
        return None


def get_ingestion_logic_schema_id(conn, version_number):
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(
                f'SELECT SchemaVersionID FROM "{PG_SCHEMA}".SchemaVersions WHERE VersionNumber = %s',
                (version_number,),
            )
            result = cursor.fetchone()
            return result["schemaversionid"] if result else None
    except psycopg2.Error as e:
        print(f"DB Error getting schema id: {e}")
        return None


def log_processed_file(
    conn,
    processed_file_id,
    original_file_name,
    md5_hash,
    status,
    schema_version_id,
):
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f'INSERT INTO "{PG_SCHEMA}".XMLFilesProcessed (ProcessedFileID, OriginalFileName, MD5Hash, ProcessingTimestamp, Status, SchemaVersionID, DemographicGroup) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                (
                    processed_file_id,
                    original_file_name,
                    md5_hash,
                    timestamp,
                    status,
                    schema_version_id,
                    None,
                ),
            )
        conn.commit()
        print(
            f"Logged file {original_file_name} (ID: {processed_file_id}) with status {status}."
        )
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f"DB error logging processed file {original_file_name}: {e}")
        return False


def archive_file(file_path, archive_directory):
    if not os.path.exists(file_path):
        return False
    try:
        if not os.path.exists(archive_directory):
            os.makedirs(archive_directory)
        base_filename = os.path.basename(file_path)
        archive_path = os.path.join(archive_directory, base_filename)
        if os.path.exists(archive_path):
            print(f"Warning: File {base_filename} already in archive. Overwriting.")
        shutil.move(file_path, archive_path)
        print(f"File {file_path} archived to {archive_path}")
        return True
    except Exception as e:
        print(f"Error archiving file {file_path}: {e}")
        return False


def move_to_error_directory(file_path, error_directory=ERROR_DIR):
    """Moves a file to the error directory when processing fails."""
    if not os.path.exists(file_path):
        return False
    try:
        if not os.path.exists(error_directory):
            os.makedirs(error_directory)
        base_filename = os.path.basename(file_path)
        error_path = os.path.join(error_directory, base_filename)
        if os.path.exists(error_path):
            # Add timestamp to make filename unique
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            name, ext = os.path.splitext(base_filename)
            base_filename = f"{name}_error_{timestamp}{ext}"
            error_path = os.path.join(error_directory, base_filename)
        shutil.move(file_path, error_path)
        print(f"File {file_path} moved to error directory: {error_path}")
        return True
    except Exception as e:
        print(f"Error moving file {file_path} to error directory: {e}")
        return False


# --- Dynamic Schema and Data Insertion Functions ---

_table_column_cache = {}  # Cache for table schemas: {table_name: {column_names}}


def get_table_columns(conn, table_name):
    """Retrieves column names for a given table, using a cache."""
    safe_table_name = sanitize_xml_name(table_name)
    if safe_table_name in _table_column_cache:
        return _table_column_cache[safe_table_name]

    cols = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (PG_SCHEMA, safe_table_name.lower()),
            )
            cols = {row[0] for row in cursor.fetchall()}
            _table_column_cache[safe_table_name] = cols
    except psycopg2.Error as e:
        if "does not exist" not in str(e).lower():
            print(f"Error getting columns for {safe_table_name}: {e}")
        _table_column_cache[safe_table_name] = set()
    return cols


def ensure_table_and_columns(
    conn, table_name_suggestion, element_attributes, common_db_columns
):
    """Ensures a table exists with all necessary common and attribute-derived columns."""
    cursor = conn.cursor()
    table_name_raw = sanitize_xml_name(table_name_suggestion)
    if not table_name_raw:
        print("Error: Table name suggestion is empty after sanitization.")
        return None, set()

    table_name = f'"{table_name_raw.lower()}"'

    existing_columns = get_table_columns(conn, table_name_raw)

    common_cols_sql = [
        '"element_id" TEXT PRIMARY KEY',
        '"parent_element_id" TEXT',
        '"pcr_uuid_context" TEXT',
        '"original_tag_name" TEXT',
        '"text_content" TEXT',
    ]

    if not existing_columns:
        attr_cols_for_create = []
        current_common_names = {c.split()[0].strip('"') for c in common_cols_sql}
        for attr in element_attributes.keys():
            sanitized_attr = sanitize_xml_name(attr).lower()
            if sanitized_attr not in current_common_names:
                attr_cols_for_create.append(f'"{sanitized_attr}" TEXT')
                current_common_names.add(sanitized_attr)

        final_cols_for_create = common_cols_sql + list(set(attr_cols_for_create))
        columns_sql = ", ".join(final_cols_for_create)
        create_sql = (
            f'CREATE TABLE IF NOT EXISTS "{PG_SCHEMA}".{table_name} ({columns_sql});'
        )
        try:
            cursor.execute(create_sql)
            # Set table comment to element_path (from the first element)
            if "element_path" in element_attributes:
                element_path_str = element_attributes["element_path"]
                cursor.execute(
                    f'COMMENT ON TABLE "{PG_SCHEMA}".{table_name} IS %s;',
                    (element_path_str,),
                )
            created_cols = {
                col_def.split()[0].strip('"').lower()
                for col_def in final_cols_for_create
            }
            _table_column_cache[table_name_raw] = created_cols
            print(f"Table {table_name} created.")
        except psycopg2.Error as e:
            print(f"Error creating table {table_name}: {e}")
            conn.rollback()
            return None, set()

    current_table_cols = get_table_columns(conn, table_name_raw)
    missing_attr_cols = set()
    for attr in element_attributes.keys():
        sanitized_attr = sanitize_xml_name(attr).lower()
        if sanitized_attr not in current_table_cols and sanitized_attr not in {
            c.split()[0].strip('"') for c in common_cols_sql
        }:
            missing_attr_cols.add(sanitized_attr)

    for col_name in missing_attr_cols:
        col_name_quoted = f'"{col_name}"'
        try:
            cursor.execute(
                f'ALTER TABLE "{PG_SCHEMA}".{table_name} ADD COLUMN {col_name_quoted} TEXT;'
            )
            print(f"Added column {col_name_quoted} to {table_name}")
            _table_column_cache[table_name_raw].add(col_name)
        except psycopg2.Error as e:
            print(f"Error adding {col_name_quoted} to {table_name}: {e}")
            conn.rollback()

    return table_name_raw, get_table_columns(conn, table_name_raw)


def delete_existing_pcr_data(conn, pcr_uuid):
    """Deletes all records associated with a given pcr_uuid across all dynamic tables."""
    if not pcr_uuid:
        return
    print(
        f"Checking if PatientCareReport UUID: {pcr_uuid} exists in any dynamic tables. If found, it will be deleted before new data is inserted."
    )
    deleted_total = 0
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                AND table_name NOT LIKE 'pg_%%'
                AND table_name NOT IN ('SchemaVersions', 'XMLFilesProcessed')
            """,
                (PG_SCHEMA,),
            )
            tables_to_check = [row[0] for row in cursor.fetchall()]

            for table_name_raw in tables_to_check:
                columns = get_table_columns(conn, table_name_raw)
                if "pcr_uuid_context" in columns:
                    table_name_quoted = f'"{PG_SCHEMA}"."{table_name_raw}"'
                    try:
                        cursor.execute(
                            f'DELETE FROM {table_name_quoted} WHERE "pcr_uuid_context" = %s',
                            (pcr_uuid,),
                        )
                        deleted_total += cursor.rowcount
                        if cursor.rowcount > 0:
                            print(
                                f"  Deleted {cursor.rowcount} rows from {table_name_quoted}"
                            )
                    except psycopg2.Error as e:
                        print(f"Error deleting from {table_name_quoted}: {e}")
                        raise  # Re-raise the exception
        if deleted_total > 0:
            print(f"Total rows deleted for PCR {pcr_uuid}: {deleted_total}")
    except psycopg2.Error as e:
        print(f"DB error during PCR deletion: {e}")
        raise  # Re-raise the exception


def process_xml_file(db_conn, xml_file_path, ingestion_schema_id):
    print(f"\nProcessing XML: {xml_file_path}")
    processed_file_id = generate_unique_file_id()
    original_file_name = os.path.basename(xml_file_path)
    md5_hash = get_file_md5(xml_file_path)

    if md5_hash is None and os.path.exists(xml_file_path):
        log_processed_file(
            db_conn,
            processed_file_id,
            original_file_name,
            None,
            "Error_MD5",
            ingestion_schema_id,
        )
        move_to_error_directory(xml_file_path)
        return False
    if not os.path.exists(xml_file_path):
        print(f"Error: XML file not found at {xml_file_path}. Aborting.")
        log_processed_file(
            db_conn,
            processed_file_id,
            original_file_name,
            md5_hash if md5_hash else "N/A",
            "Error_FileNotFound",
            ingestion_schema_id,
        )
        # File doesn't exist, so no need to move it
        return False

    elements_data = parse_xml_file(xml_file_path)

    if not elements_data:
        print(f"No elements parsed from {xml_file_path} or parsing error occurred.")
        log_processed_file(
            db_conn,
            processed_file_id,
            original_file_name,
            md5_hash,
            "Error_Parsing_Empty",
            ingestion_schema_id,
        )
        move_to_error_directory(xml_file_path)
        return False

    # Collect all unique PCR UUIDs from the current file
    unique_pcr_uuids_in_file = set()
    for el in elements_data:
        if el.get("pcr_uuid_context"):
            unique_pcr_uuids_in_file.add(el["pcr_uuid_context"])

    common_db_columns = {
        "element_id",
        "parent_element_id",
        "pcr_uuid_context",
        "original_tag_name",
        "text_content",
    }

    cursor = db_conn.cursor()
    try:
        # Delete existing data for all PCRs found in this file BEFORE inserting new data
        if unique_pcr_uuids_in_file:
            print(
                f"Found {len(unique_pcr_uuids_in_file)} unique PatientCareReport UUID(s) in this file for potential data overwrite."
            )
            for pcr_uuid in unique_pcr_uuids_in_file:
                delete_existing_pcr_data(db_conn, pcr_uuid)
        else:
            print(
                "No PatientCareReport UUIDs found in this file; no pre-deletion of data will occur."
            )

        current_file_foreign_keys = set()  # Using a set to store tuples for uniqueness

        for element in elements_data:
            # Retrieve parent_table_suggestion from the element
            parent_table_suggestion_raw = element.get("parent_table_suggestion")

            table_name_raw, actual_table_columns = ensure_table_and_columns(
                db_conn,
                element["table_suggestion"],
                element["attributes"],
                common_db_columns,
            )

            if not table_name_raw or not actual_table_columns:
                print(
                    f"Skipping element due to table creation/alteration error for suggested table {table_name_raw}"
                )
                raise psycopg2.Error(
                    f"Failed to ensure table/columns for {table_name_raw}"
                )  # Trigger rollback

            # Logic for preparing foreign key definition
            if parent_table_suggestion_raw and element.get("parent_element_id"):
                # Ensure parent_table_suggestion is sanitized and lowercased, similar to child table names
                sanitized_parent_table_name = sanitize_xml_name(
                    parent_table_suggestion_raw
                )
                if (
                    sanitized_parent_table_name
                ):  # Ensure it's not empty after sanitization
                    # Add to set as a tuple: (child_table_raw, parent_table_raw_sanitized)
                    # table_name_raw is already sanitized from ensure_table_and_columns
                    current_file_foreign_keys.add(
                        (table_name_raw, sanitized_parent_table_name)
                    )

            # Prepare data for insertion
            insert_data = {
                "element_id": element["element_id"],
                "parent_element_id": element.get("parent_element_id"),
                "pcr_uuid_context": element.get("pcr_uuid_context"),
                "original_tag_name": element["element_tag"],
                "text_content": element.get("text_content"),
            }
            for attr_key, attr_value in element["attributes"].items():
                insert_data[sanitize_xml_name(attr_key).lower()] = attr_value

            # Filter data to only include columns that actually exist in the table
            filtered_insert_data = {
                k: v
                for k, v in insert_data.items()
                if k.lower() in actual_table_columns
            }

            cols_for_sql = ", ".join([f'"{k}"' for k in filtered_insert_data.keys()])
            placeholders = ", ".join(["%s"] * len(filtered_insert_data))
            values = tuple(filtered_insert_data.values())

            table_name_quoted = f'"{PG_SCHEMA}"."{table_name_raw.lower()}"'
            sql = f"INSERT INTO {table_name_quoted} ({cols_for_sql}) VALUES ({placeholders})"
            try:
                cursor.execute(sql, values)
            except psycopg2.Error as e:
                print(f"DB Insert Error: {e} SQL: {sql} VALS:{values}")
                raise  # Reraise to trigger transaction rollback

        print(
            "--- Successfully completed data insertion loop. Proceeding to Foreign Key creation. ---"
        )
        # After processing all elements, attempt to create foreign key constraints
        if current_file_foreign_keys:
            print(
                f"Attempting to create {len(current_file_foreign_keys)} unique foreign key constraints for {xml_file_path}..."
            )
            for (
                child_table_raw,
                parent_table_raw_sanitized,
            ) in current_file_foreign_keys:
                child_table_name_lowercase = child_table_raw.lower()
                parent_table_name_lowercase = parent_table_raw_sanitized.lower()

                ideal_constraint_name = (
                    f"fk_{child_table_raw}_{parent_table_raw_sanitized}"
                )
                fk_constraint_name_unquoted = ""

                if len(ideal_constraint_name) <= 63:
                    fk_constraint_name_unquoted = ideal_constraint_name
                else:
                    hash_suffix = hashlib.md5(
                        ideal_constraint_name.encode()
                    ).hexdigest()[:6]
                    prefix = "fk_"
                    len_prefix = len(prefix)
                    len_hash = len(hash_suffix)
                    len_separator_before_hash = 1  # for the _ before the hash

                    # Max length for the combined "childpart_parentpart" string
                    max_len_for_tables_part = (
                        63 - len_prefix - len_hash - len_separator_before_hash
                    )

                    child_part = child_table_raw
                    parent_part = parent_table_raw_sanitized

                    # Available length for "childpart_parentpart" (child + parent + 1 underscore)
                    available_for_names_plus_underscore = max_len_for_tables_part

                    # If current combined length (child + _ + parent) is too long
                    if (
                        len(child_part) + 1 + len(parent_part)
                    ) > available_for_names_plus_underscore:
                        # Max length for each name part, aiming for roughly equal truncation
                        # -1 for the underscore separating child and parent parts
                        available_for_child_and_parent_only = (
                            available_for_names_plus_underscore - 1
                        )

                        # Attempt to allocate space, giving more to longer names if possible,
                        # but simple equal split first.
                        max_len_child_part = available_for_child_and_parent_only // 2
                        max_len_parent_part = (
                            available_for_child_and_parent_only - max_len_child_part
                        )

                        if len(child_part) > max_len_child_part:
                            child_part = child_part[:max_len_child_part]
                            # Recalculate max_len_parent_part based on actual truncated child_part length
                            max_len_parent_part = (
                                available_for_child_and_parent_only - len(child_part)
                            )

                        if len(parent_part) > max_len_parent_part:
                            parent_part = parent_part[:max_len_parent_part]

                        # Final check if parent_part truncation made child_part too long again (if parent was very short initially)
                        if (
                            len(child_part) + 1 + len(parent_part)
                        ) > available_for_names_plus_underscore:
                            child_part = child_part[
                                : available_for_child_and_parent_only
                                - len(parent_part)
                                - 1
                            ]

                    fk_constraint_name_unquoted = (
                        f"{prefix}{child_part}_{parent_part}_{hash_suffix}"
                    )

                    # Absolute final truncation if logic somehow failed (should be rare)
                    if len(fk_constraint_name_unquoted) > 63:
                        fk_constraint_name_unquoted = fk_constraint_name_unquoted[:63]

                fk_constraint_name_quoted = f'"{fk_constraint_name_unquoted}"'

                try:
                    # Check if constraint already exists
                    check_fk_sql = (
                        "SELECT constraint_name "
                        "FROM information_schema.table_constraints "
                        "WHERE table_schema = %s "
                        "  AND table_name = %s "
                        "  AND constraint_name = %s;"
                    )
                    cursor.execute(
                        check_fk_sql,
                        (
                            PG_SCHEMA,
                            child_table_name_lowercase,
                            fk_constraint_name_unquoted,
                        ),
                    )
                    existing_fk = cursor.fetchone()

                    if existing_fk is None:
                        # Define alter_sql only if we need to create the constraint
                        alter_sql = f"""
                            ALTER TABLE "{PG_SCHEMA}"."{child_table_name_lowercase}"
                            ADD CONSTRAINT {fk_constraint_name_quoted}
                            FOREIGN KEY ("parent_element_id")
                            REFERENCES "{PG_SCHEMA}"."{parent_table_name_lowercase}" ("element_id")
                            ON DELETE CASCADE;
                        """
                        print(f"Attempting to execute FK DDL: {alter_sql.strip()}")
                        cursor.execute(alter_sql)
                        print(
                            f"Successfully created FK: {fk_constraint_name_quoted} on table {child_table_name_lowercase} referencing {parent_table_name_lowercase}"
                        )
                    else:
                        if "--verbose" in sys.argv:
                            print(
                                f"Warning: FK constraint {fk_constraint_name_quoted} on table {child_table_name_lowercase} already exists according to information_schema. Skipping creation."
                            )

                except psycopg2.Error as e:
                    # Catch errors from either the SELECT or the ALTER TABLE
                    # If alter_sql was defined, include it in the error message
                    current_sql_attempt = "N/A"
                    if (
                        "alter_sql" in locals() and existing_fk is None
                    ):  # only if ALTER was attempted
                        current_sql_attempt = alter_sql.strip()
                    elif "check_fk_sql" in locals():  # if error was in the check
                        current_sql_attempt = check_fk_sql.strip()

                    print(
                        f"Critical Error during FK operation for constraint {fk_constraint_name_quoted} on table {child_table_name_lowercase}."
                    )
                    print(f"Attempted SQL (or check query): {current_sql_attempt}")
                    print(f"Error Details: {e}")
                    raise  # Re-raise to trigger transaction rollback for the file
            print("Foreign key constraint creation phase completed.")

        db_conn.commit()  # Commit transaction if all elements and FKs processed successfully
        print(
            f"All elements and FKs from {xml_file_path} successfully ingested and committed."
        )
        log_processed_file(
            db_conn,
            processed_file_id,
            original_file_name,
            md5_hash,
            "Staged_Dynamic_PG_V4",
            ingestion_schema_id,
        )

        if not archive_file(xml_file_path, ARCHIVE_DIR):
            print(f"Warning: Data staged for {xml_file_path}, but failed to archive.")
        return True

    except psycopg2.Error as e:
        db_conn.rollback()
        print(f"DB Tx error (PG) for {xml_file_path}: {e}. Rolled back.")
        log_processed_file(
            db_conn,
            processed_file_id,
            original_file_name,
            md5_hash,
            "Error_Staging_Tx_PG_V4",
            ingestion_schema_id,
        )
        move_to_error_directory(xml_file_path)
        return False
    except Exception as e:
        db_conn.rollback()
        print(
            f"Unexpected critical error processing {xml_file_path}: {e}. Rolled back."
        )
        log_processed_file(
            db_conn,
            processed_file_id,
            original_file_name,
            md5_hash,
            "Error_Unexpected_PG_V4",
            ingestion_schema_id,
        )
        move_to_error_directory(xml_file_path)
        return False
    finally:
        _table_column_cache.clear()  # Clear cache after processing each file


def main():
    global ARCHIVE_DIR
    parser = argparse.ArgumentParser(
        description="NEMSIS XML Dynamic Data Ingestion Tool V4 (PostgreSQL)"
    )
    parser.add_argument("xml_file", help="Path to the NEMSIS XML file to process.")
    parser.add_argument(
        "--archive-dir",
        default=ARCHIVE_DIR,
        help=f"Archive directory. Default: {ARCHIVE_DIR}",
    )

    args = parser.parse_args()
    ARCHIVE_DIR = args.archive_dir

    # The script will now always use the INGESTION_LOGIC_VERSION_NUMBER constant
    target_ingestion_logic_version = INGESTION_LOGIC_VERSION_NUMBER

    print(f"--- NEMSIS Dynamic Data Ingestion V4 (PostgreSQL) --- ")
    print(
        f"Archive: {ARCHIVE_DIR}, ErrorDir: {ERROR_DIR}, Schema: {PG_SCHEMA}, IngestionVersion: {target_ingestion_logic_version}"
    )

    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return

        if not os.path.exists(ARCHIVE_DIR):
            try:
                os.makedirs(ARCHIVE_DIR)
            except OSError as e:
                print(f"Error creating archive dir {ARCHIVE_DIR}: {e}")

        # Uses the constant directly
        ingestion_schema_id = get_ingestion_logic_schema_id(
            conn, target_ingestion_logic_version
        )
        if ingestion_schema_id is None:
            print(
                f"Ingestion logic version {target_ingestion_logic_version} not found in SchemaVersions. "
            )
            print(
                f"Please ensure database_setup.py has been run and its initial version matches this script ({target_ingestion_logic_version})."
            )
            return
        print(
            f"Using IngestionSchemaID: {ingestion_schema_id} for Version: {target_ingestion_logic_version}"
        )

        success = process_xml_file(conn, args.xml_file, ingestion_schema_id)

        if success:
            print(f"--- Ingestion for {args.xml_file} completed successfully. ---")
        else:
            print(f"--- Ingestion for {args.xml_file} failed. See logs. ---")

    except psycopg2.Error as e:
        print(f"Critical PostgreSQL error in main: {e}")
    except Exception as e:
        print(f"Critical error in main: {e}")
    finally:
        if conn:
            conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
