from database_setup import get_db_connection
import re
from structures import *
import sys
from create_definitions import setup_element_definitions

conn = get_db_connection()


def table_exists(cursor, table_name, schema="public"):
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        );
        """,
        (schema, table_name),
    )
    return cursor.fetchone()[0]


def get_table_columns(cursor, table_name, schema="public"):
    cursor.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
        """,
        (schema, table_name),
    )
    return {row[0] for row in cursor.fetchall()}


def filter_structure(structure, cursor, schema="public"):
    return [item for item in structure if table_exists(cursor, item["table"], schema)]


def debug_structure_tables_and_columns(structure, cursor, schema="public"):
    print("\n--- Debugging Structure Table/Column Existence ---")
    for item in structure:
        table = item["table"]
        exists = table_exists(cursor, table, schema)
        print(f"Table: {table} - Exists: {exists}")
        if exists:
            columns = get_table_columns(cursor, table, schema)
            print(f"  Columns: {columns}")
        else:
            print("  [!] Table does not exist!")
    print("--- End Debug ---\n")


def sanitize_alias(id_str):
    """Sanitize alias to avoid SQL errors"""
    return re.sub(r"[^a-zA-Z0-9_]", "_", id_str) + "_"


def generate_view_sql(view_name: str, structure: list, cursor) -> str:
    group_columns = ["element_id", "pcr_uuid_context", "correlationid"]
    element_columns = ["text_content", "nil", "nv", "correlationid", "Etco2type"]

    aliases = {}
    base_element = next(item for item in structure if item["parent_id"] is None)
    base_table_name = base_element["table"]
    base_alias = sanitize_alias(base_element["id"])
    aliases[base_element["id"]] = base_alias

    table_columns_cache = {
        item["table"]: get_table_columns(cursor, item["table"]) for item in structure
    }

    select_clauses = [
        f'"{base_alias}"."{col}" AS "{base_alias}{col}"'
        for col in group_columns
        if col in table_columns_cache[base_table_name]
    ]
    from_clause = f'FROM "public"."{base_table_name}" AS "{base_alias}"'
    join_clauses = []

    for element in [e for e in structure if e["parent_id"] is not None]:
        table_name = element["table"]
        alias = sanitize_alias(element["id"])
        aliases[element["id"]] = alias

        parent_alias = aliases[element["parent_id"]]
        cols_to_select = (
            group_columns if element["type"] == "group" else element_columns
        )

        existing_cols = table_columns_cache[table_name]
        for col in cols_to_select:
            if col in existing_cols:
                select_clauses.append(f'"{alias}"."{col}" AS "{alias}{col}"')

        join_clause = (
            f'FULL JOIN "public"."{table_name}" AS "{alias}" '
            f'ON "{parent_alias}"."element_id" = "{alias}"."parent_element_id"'
        )
        join_clauses.append(join_clause)

    final_sql = f"""
CREATE OR REPLACE VIEW public.{view_name} AS
SELECT
  {', '.join(select_clauses)}
{from_clause}
{' '.join(join_clauses)};
"""
    return final_sql


def create_view_in_db(conn, view_name, view_sql):
    """
    Creates or replaces a SQL view in the database using the provided SQL statement.

    Attempts to drop the existing view before creating the new one. Prints status messages and rolls back the transaction if view creation fails.
    """
    cursor = conn.cursor()
    drop_sql = f"DROP VIEW IF EXISTS {view_name} CASCADE;"
    try:
        cursor.execute(drop_sql)
    except Exception as drop_err:
        print(f"[WARNING] Could not drop view {view_name}: {drop_err}")
    try:
        # print(f"\nExecuting CREATE VIEW statement for {view_name}...")
        cursor.execute(view_sql)
        conn.commit()
        print(f"View {view_name} created successfully.")
    except Exception as e:
        print(f"Error creating view {view_name}: {e}")
        conn.rollback()
    cursor.close()


def generate_comment_sql(view_name: str, structure: list, cursor) -> list:
    group_columns = ["element_id", "pcr_uuid_context", "correlationid"]
    element_columns = ["text_content", "nil", "nv", "correlationid", "Etco2type"]

    aliases = {}
    base_element = next(item for item in structure if item["parent_id"] is None)
    base_alias = sanitize_alias(base_element["id"])
    aliases[base_element["id"]] = base_alias

    # Get actual columns in the view
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
    """,
        (view_name,),
    )
    actual_columns = {row[0] for row in cursor.fetchall()}

    comment_statements = []

    # Base group columns
    for col in group_columns:
        colname = f"{base_alias}{col}"
        desc = base_element.get("description") or ""
        if desc and colname in actual_columns:
            comment_statements.append(
                f'COMMENT ON COLUMN public."{view_name}"."{colname}" IS \'{desc}\';'
            )

    # Child elements/groups
    for element in [e for e in structure if e["parent_id"] is not None]:
        alias = sanitize_alias(element["id"])
        cols_to_select = (
            group_columns if element["type"] == "group" else element_columns
        )
        desc = element.get("description") or ""
        for col in cols_to_select:
            colname = f"{alias}{col}"
            if desc and colname in actual_columns:
                comment_statements.append(
                    f'COMMENT ON COLUMN public."{view_name}"."{colname}" IS \'{desc}\';'
                )
    return comment_statements


def comment_view_in_db(conn, view_name):
    comment_statements = generate_comment_sql(view_name, filtered_structure, cursor)
    for comment_statement in comment_statements:
        cursor.execute(comment_statement)
        conn.commit()


def debug_view_and_comments(conn, view_name, structure, cursor):
    # Print actual columns in the view
    print(f"\nColumns in view '{view_name}':")
    cursor.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position;
    """,
        (view_name,),
    )
    columns = cursor.fetchall()
    for col in columns:
        print(f"  {col[0]}")

    # Print generated COMMENT ON COLUMN statements
    print(f"\nGenerated COMMENT ON COLUMN statements for '{view_name}':")
    comment_sqls = generate_comment_sql(view_name, structure, cursor)
    for sql in comment_sqls:
        print(sql)


if __name__ == "__main__":
    conn = get_db_connection()
    if not conn:
        print("Could not connect to the database.")
    else:
        cursor = conn.cursor()
        # # vitals
        # filtered_structure = filter_structure(EVITALS_STRUCTURE, cursor)
        # view_name = "v_evitals_flat"
        # evitals_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{evitals_sql}\n")
        # create_view_in_db(conn, view_name, evitals_sql)
        # comment_view_in_db(conn, view_name)
        # # procedures
        # filtered_structure = filter_structure(EPROCEDURES_STRUCTURE, cursor)
        # view_name = "v_eprocedures_flat"
        # eprocedures_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{eprocedures_sql}\n")
        # create_view_in_db(conn, view_name, eprocedures_sql)
        # comment_view_in_db(conn, view_name)
        # # airway
        # filtered_structure = filter_structure(EAIRWAY_STRUCTURE, cursor)
        # view_name = "v_eairway_flat"
        # eairway_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{eairway_sql}\n")
        # create_view_in_db(conn, view_name, eairway_sql)
        # comment_view_in_db(conn, view_name)
        # # crew
        # filtered_structure = filter_structure(ECREW_STRUCTURE, cursor)
        # view_name = "v_ecrew_flat"
        # ecrew_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{ecrew_sql}\n")
        # create_view_in_db(conn, view_name, ecrew_sql)
        # comment_view_in_db(conn, view_name)
        # # device
        # filtered_structure = filter_structure(EDEVICE_STRUCTURE, cursor)
        # view_name = "v_edevice_flat"
        # edevice_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{edevice_sql}\n")
        # create_view_in_db(conn, view_name, edevice_sql)
        # comment_view_in_db(conn, view_name)
        # # arrest
        # filtered_structure = filter_structure(EARREST_STRUCTURE, cursor)
        # view_name = "v_earrest_flat"
        # earrest_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{earrest_sql}\n")
        # create_view_in_db(conn, view_name, earrest_sql)
        # comment_view_in_db(conn, view_name)
        # # dispatch
        # filtered_structure = filter_structure(EDISPATCH_STRUCTURE, cursor)
        # view_name = "v_edispatch_flat"
        # edispatch_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{edispatch_sql}\n")
        # create_view_in_db(conn, view_name, edispatch_sql)
        # comment_view_in_db(conn, view_name)
        # # disposition
        # filtered_structure = filter_structure(EDISPOSITION_STRUCTURE, cursor)
        # view_name = "v_edisposition_flat"
        # edisposition_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{edisposition_sql}\n")
        # create_view_in_db(conn, view_name, edisposition_sql)
        # comment_view_in_db(conn, view_name)
        # # exam
        # filtered_structure = filter_structure(EEXAM_STRUCTURE, cursor)
        # view_name = "v_eexam_flat"
        # eexam_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{eexam_sql}\n")
        # create_view_in_db(conn, view_name, eexam_sql)
        # comment_view_in_db(conn, view_name)
        # # history
        # filtered_structure = filter_structure(EHISTORY_STRUCTURE, cursor)
        # view_name = "v_ehistory_flat"
        # ehistory_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{ehistory_sql}\n")
        # create_view_in_db(conn, view_name, ehistory_sql)
        # comment_view_in_db(conn, view_name)
        # # injury
        # filtered_structure = filter_structure(EINJURY_STRUCTURE, cursor)
        # view_name = "v_einjury_flat"
        # einjury_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{einjury_sql}\n")
        # create_view_in_db(conn, view_name, einjury_sql)
        # comment_view_in_db(conn, view_name)
        # # lab
        # filtered_structure = filter_structure(ELABS_STRUCTURE, cursor)
        # view_name = "v_elabs_flat"
        # elabs_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{elabs_sql}\n")
        # create_view_in_db(conn, view_name, elabs_sql)
        # comment_view_in_db(conn, view_name)
        # # medications
        # filtered_structure = filter_structure(EMEDICATIONS_STRUCTURE, cursor)
        # view_name = "v_emedications_flat"
        # emedications_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{emedications_sql}\n")
        # create_view_in_db(conn, view_name, emedications_sql)
        # comment_view_in_db(conn, view_name)
        # # other
        # filtered_structure = filter_structure(EOTHER_STRUCTURE, cursor)
        # view_name = "v_eother_flat"
        # eother_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{eother_sql}\n")
        # create_view_in_db(conn, view_name, eother_sql)
        # comment_view_in_db(conn, view_name)
        # # outcome
        # filtered_structure = filter_structure(EOUTCOME_STRUCTURE, cursor)
        # view_name = "v_eoutcome_flat"
        # eoutcome_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{eoutcome_sql}\n")
        # create_view_in_db(conn, view_name, eoutcome_sql)
        # comment_view_in_db(conn, view_name)
        # # patient
        # filtered_structure = filter_structure(EPATIENT_STRUCTURE, cursor)
        # view_name = "v_epatient_flat"
        # epatient_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{epatient_sql}\n")
        # create_view_in_db(conn, view_name, epatient_sql)
        # comment_view_in_db(conn, view_name)
        # # payment
        # filtered_structure = filter_structure(EPAYMENT_STRUCTURE, cursor)
        # view_name = "v_epayment_flat"
        # epayment_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{epayment_sql}\n")
        # create_view_in_db(conn, view_name, epayment_sql)
        # comment_view_in_db(conn, view_name)
        # # protocols
        # filtered_structure = filter_structure(EPROTOCOLS_STRUCTURE, cursor)
        # view_name = "v_eprotocols_flat"
        # eprotocols_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{eprotocols_sql}\n")
        # create_view_in_db(conn, view_name, eprotocols_sql)
        # comment_view_in_db(conn, view_name)
        # # record
        # filtered_structure = filter_structure(ERECORD_STRUCTURE, cursor)
        # view_name = "v_erecord_flat"
        # erecord_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{erecord_sql}\n")
        # create_view_in_db(conn, view_name, erecord_sql)
        # comment_view_in_db(conn, view_name)
        # # response
        # filtered_structure = filter_structure(ERESPONSE_STRUCTURE, cursor)
        # view_name = "v_eresponse_flat"
        # eresponse_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{eresponse_sql}\n")
        # create_view_in_db(conn, view_name, eresponse_sql)
        # comment_view_in_db(conn, view_name)
        # # scene
        # filtered_structure = filter_structure(ESCENE_STRUCTURE, cursor)
        # view_name = "v_escene_flat"
        # escene_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{escene_sql}\n")
        # create_view_in_db(conn, view_name, escene_sql)
        # comment_view_in_db(conn, view_name)
        # # situation
        # filtered_structure = filter_structure(ESITUATION_STRUCTURE, cursor)
        # view_name = "v_esituation_flat"
        # esituation_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{esituation_sql}\n")
        # create_view_in_db(conn, view_name, esituation_sql)
        # comment_view_in_db(conn, view_name)
        # # times
        # filtered_structure = filter_structure(ETIMES_STRUCTURE, cursor)
        # view_name = "v_etimes_flat"
        # etimes_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{etimes_sql}\n")
        # create_view_in_db(conn, view_name, etimes_sql)
        # comment_view_in_db(conn, view_name)
        # # custom configuration
        # filtered_structure = filter_structure(ECUSTOMCONFIGURATION_STRUCTURE, cursor)
        # view_name = "v_ecustomconfiguration_flat"
        # ecustomconfiguration_sql = generate_view_sql(
        #     view_name, filtered_structure, cursor
        # )
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{ecustomconfiguration_sql}\n")
        # create_view_in_db(conn, view_name, ecustomconfiguration_sql)
        # comment_view_in_db(conn, view_name)
        #
        # # custom results
        # filtered_structure = filter_structure(ECUSTOMRESULTS_STRUCTURE, cursor)
        # view_name = "v_ecustomresults_flat"
        # ecustomresults_sql = generate_view_sql(view_name, filtered_structure, cursor)
        # if "--verbose" in sys.argv:
        #     print(f"\nGenerated SQL for {view_name}:\n{ecustomresults_sql}\n")
        # create_view_in_db(conn, view_name, ecustomresults_sql)
        # comment_view_in_db(conn, view_name)

        print("View creation for v_*_flat views has been disabled in this script.")
        print("Running setup_element_definitions...")
        setup_element_definitions(conn)

        conn.close()
