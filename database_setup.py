import psycopg2  # Changed from sqlite3
import psycopg2.extras  # For dictionary cursor
import datetime
import uuid  # For generating initial schema version if needed, or other UUIDs

# Import PostgreSQL connection details from config.py
try:
    from config import PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD, PG_SCHEMA
except ImportError:
    print("Error: Could not import PostgreSQL configuration from config.py.")
    print(
        "Ensure config.py is present and defines PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD, PG_SCHEMA."
    )
    # Fallback or exit might be needed here if config is critical
    exit(1)


def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    if not all([PG_DATABASE, PG_USER, PG_PASSWORD]):
        print(
            "Database connection cannot be established: Missing PG_DATABASE, PG_USER, or PG_PASSWORD in config."
        )
        return None
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        # conn.row_factory is not a direct attribute. Use cursor_factory for dict-like rows.
        # psycopg2.extras.DictCursor will allow accessing columns by name.
        print(
            f"Successfully connected to PostgreSQL database: {PG_DATABASE} on {PG_HOST}:{PG_PORT}"
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None


def create_schema_if_not_exists(conn, schema_name):
    """Creates the schema if it doesn't exist."""
    if schema_name != "public":
        try:
            with conn.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
                print(f"Checked/Created schema: {schema_name}")
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error creating schema {schema_name}: {e}")
            conn.rollback()
            raise

def create_tables(conn, schema=PG_SCHEMA):
    """Creates the initial database tables if they don't exist (PostgreSQL syntax)."""
    # Create schema first if not public
    create_schema_if_not_exists(conn, schema)

    # Using psycopg2.extras.DictCursor for easier row access by name later, though not strictly needed for DDL
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        # SchemaVersions Table for PostgreSQL
        cursor.execute(
            f"""
# At the top of database_setup.py, alongside the existing imports:
import psycopg2  # Changed from sqlite3
import psycopg2.extras  # For dictionary cursor
import datetime
import uuid  # For generating initial schema version if needed, or other UUIDs
import re  # For schema name validation

# … later in the file …

def create_schema_if_not_exists(conn, schema_name):
    """Creates the schema if it doesn't exist."""
    # Validate schema name to prevent SQL injection
    if not schema_name or not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', schema_name):
        raise ValueError(f"Invalid schema name: {schema_name}")
    if schema_name != "public":
        with conn.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema_name}"')
        conn.commit()
            VersionNumber TEXT NOT NULL UNIQUE,
            CreationDate TIMESTAMPTZ NOT NULL, -- Use TIMESTAMPTZ for timezone awareness
            UpdateDate TIMESTAMPTZ,
            Description TEXT,
            DemographicGroup TEXT NULL -- Remains for now
        );
        """
        )
        print(f"Checked/Created {schema}.SchemaVersions table.")

        # XMLFilesProcessed Table for PostgreSQL
        cursor.execute(
            f"""
        CREATE TABLE IF NOT EXISTS "{schema}".XMLFilesProcessed (
            ProcessedFileID TEXT PRIMARY KEY,
            OriginalFileName TEXT NOT NULL,
            MD5Hash TEXT,
            ProcessingTimestamp TIMESTAMPTZ NOT NULL,
            Status TEXT NOT NULL, 
            SchemaVersionID INTEGER,
            DemographicGroup TEXT NULL, -- This will now receive NULL from main_ingest.py v4 logic
            FOREIGN KEY (SchemaVersionID) REFERENCES "{schema}".SchemaVersions(SchemaVersionID)
        );
        """
        )
        print(f"Checked/Created {schema}.XMLFilesProcessed table.")

    conn.commit()  # Commit DDL changes
    print(f"Core database tables checked/created successfully in schema: {schema}")


def add_initial_schema_version(
    conn,
    version_number="1.0.0-dynamic-ingestor-v4",
    description="Dynamic table logic v4 (PCR UUID based overwrite).",
    demographic_group=None,
    schema=PG_SCHEMA,
):
    """Adds an initial record to the SchemaVersions table if no versions exist."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        cursor.execute(
            f'SELECT COUNT(*) AS count FROM "{schema}".SchemaVersions'
        )  # Use AS for column name with DictCursor
        if cursor.fetchone()["count"] == 0:
            creation_date = datetime.datetime.now(
                datetime.timezone.utc
            )  # Use timezone-aware datetime
            try:
                cursor.execute(
                    f"""
                INSERT INTO "{schema}".SchemaVersions (VersionNumber, CreationDate, Description, DemographicGroup)
                VALUES (%s, %s, %s, %s)
                """,
                    (version_number, creation_date, description, demographic_group),
                )
                conn.commit()  # Commit this insert
                print(
                    f"Initial schema version {version_number} added to {schema}.SchemaVersions table."
                )
            except psycopg2.IntegrityError:
                conn.rollback()  # Rollback if insert fails (e.g. unique constraint)
                print(
                    f"Schema version {version_number} already exists in {schema} or other integrity error."
                )
            except psycopg2.Error as e:
                conn.rollback()
                print(f"Database error adding initial schema version: {e}")
        else:
            print(
                f"SchemaVersions table in {schema} already contains entries. Skipping initial version addition."
            )


if __name__ == "__main__":
    print(f"Initializing PostgreSQL database defined in config for dynamic schema v4.")
    db_conn = None  # Renamed from conn to avoid conflict with module-level conn if any
    try:
        db_conn = get_db_connection()
        if db_conn:
            create_tables(db_conn)
            add_initial_schema_version(
                db_conn, demographic_group="SystemInternal_PG_v4"
            )
        else:
            print("Could not establish database connection. Setup aborted.")
    except psycopg2.Error as e:
        print(f"PostgreSQL database error during setup: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during PostgreSQL setup: {e}")
    finally:
        if db_conn:
            db_conn.close()
            print("PostgreSQL database connection closed.")
    print("PostgreSQL Database setup script for dynamic schema v4 finished.")
