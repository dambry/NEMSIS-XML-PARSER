import psycopg2
import psycopg2.extras
import uuid
import datetime
import os
import hashlib
import argparse
import shutil
import re  # For more advanced sanitization if needed

# Project-specific imports
try:
    from config import PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
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
                "SELECT SchemaVersionID FROM SchemaVersions WHERE VersionNumber = %s",
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
                "INSERT INTO XMLFilesProcessed (ProcessedFileID, OriginalFileName, MD5Hash, ProcessingTimestamp, Status, SchemaVersionID, DemographicGroup) VALUES (%s, %s, %s, %s, %s, %s, %s)",
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
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s",
                (safe_table_name.lower(),),
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
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(final_cols_for_create)});"
        try:
            cursor.execute(create_sql)
            # Set table comment to element_path (from the first element)
            if "element_path" in element_attributes:
                element_path_str = element_attributes["element_path"]
                cursor.execute(
                    f"COMMENT ON TABLE {table_name} IS %s;", (element_path_str,)
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
                f"ALTER TABLE {table_name} ADD COLUMN {col_name_quoted} TEXT;"
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
                WHERE table_schema = 'public' AND table_name NOT LIKE 'pg_%%' 
                AND table_name NOT IN ('SchemaVersions', 'XMLFilesProcessed')
            """
            )
            tables_to_check = [row[0] for row in cursor.fetchall()]

            for table_name_raw in tables_to_check:
                columns = get_table_columns(conn, table_name_raw)
                if "pcr_uuid_context" in columns:
                    table_name_quoted = f'"{table_name_raw}"'
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
        if deleted_total > 0:
            print(f"Total rows deleted for PCR {pcr_uuid}: {deleted_total}")
    except psycopg2.Error as e:
        print(f"DB error during PCR deletion: {e}")


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

        for element in elements_data:
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

            table_name_quoted = f'"{table_name_raw.lower()}"'
            sql = f"INSERT INTO {table_name_quoted} ({cols_for_sql}) VALUES ({placeholders})"
            try:
                cursor.execute(sql, values)
            except psycopg2.Error as e:
                print(f"DB Insert Error: {e} SQL: {sql} VALS:{values}")
                raise  # Reraise to trigger transaction rollback

        db_conn.commit()  # Commit transaction if all elements processed successfully
        print(f"All elements from {xml_file_path} successfully ingested and committed.")
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
        f"DB Target: {PG_DATABASE}@{PG_HOST}:{PG_PORT}, Archive: {ARCHIVE_DIR}, IngestionVersion: {target_ingestion_logic_version}"
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
