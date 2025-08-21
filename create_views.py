#!/usr/bin/env python3
"""
Create eAirway comprehensive view
This module creates a comprehensive view for eAirway that aggregates 0:M relationships as lists
"""

import psycopg2
import psycopg2.extras
from database_setup import get_db_connection


def create_eairway_view():
    """
    Creates a comprehensive view for eAirway that aggregates 0:M relationships as lists
    in the text_context column.

    Based on the NEMSIS structure, the 0:M fields are:
    - eAirway.01 (Indications for Invasive Airway) - 0:M
    - eAirway.04 (Airway Device Placement Confirmed Method) - 0:M within ConfirmationGroup
    - eAirway.08 (Airway Complications Encountered) - 0:M
    - eAirway.09 (Suspected Reasons for Failed Airway Management) - 0:M
    """

    conn = get_db_connection()
    if not conn:
        print("Failed to get database connection")
        return

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Drop view if it exists and create new one
            cursor.execute("DROP VIEW IF EXISTS v_eairway_comprehensive CASCADE;")

            view_sql = """
            CREATE VIEW v_eairway_comprehensive AS
            WITH airway_indications AS (
                -- Aggregate eAirway.01 (0:M) - Indications for Invasive Airway
                SELECT 
                    pcr_uuid_context,
                    STRING_AGG(DISTINCT eairway_01_value, '; ' ORDER BY eairway_01_value) as indications_list
                FROM eairway_01
                WHERE eairway_01_value IS NOT NULL AND eairway_01_value != ''
                GROUP BY pcr_uuid_context
            ),
            airway_complications AS (
                -- Aggregate eAirway.08 (0:M) - Airway Complications Encountered  
                SELECT 
                    pcr_uuid_context,
                    STRING_AGG(DISTINCT eairway_08_value, '; ' ORDER BY eairway_08_value) as complications_list
                FROM eairway_08
                WHERE eairway_08_value IS NOT NULL AND eairway_08_value != ''
                GROUP BY pcr_uuid_context
            ),
            airway_failure_reasons AS (
                -- Aggregate eAirway.09 (0:M) - Suspected Reasons for Failed Airway Management
                SELECT 
                    pcr_uuid_context,
                    STRING_AGG(DISTINCT eairway_09_value, '; ' ORDER BY eairway_09_value) as failure_reasons_list
                FROM eairway_09
                WHERE eairway_09_value IS NOT NULL AND eairway_09_value != ''
                GROUP BY pcr_uuid_context
            ),
            airway_confirmation_methods AS (
                -- Aggregate eAirway.04 (0:M) - Airway Device Placement Confirmed Method
                -- This is within ConfirmationGroup, so we need to join through the confirmation groups
                SELECT 
                    cg.pcr_uuid_context,
                    STRING_AGG(DISTINCT a04.eairway_04_value, '; ' ORDER BY a04.eairway_04_value) as confirmation_methods_list
                FROM eairway_confirmationgroup cg
                LEFT JOIN eairway_04 a04 ON a04.parent_element_id = cg.element_id
                WHERE a04.eairway_04_value IS NOT NULL AND a04.eairway_04_value != ''
                GROUP BY cg.pcr_uuid_context
            ),
            airway_confirmations AS (
                -- Get all confirmation group data aggregated by PCR
                SELECT 
                    pcr_uuid_context,
                    COUNT(*) as confirmation_count,
                    STRING_AGG(DISTINCT 
                        CASE WHEN eairway_confirmationgroup_value IS NOT NULL AND eairway_confirmationgroup_value != '' 
                             THEN 'Confirmation: ' || eairway_confirmationgroup_value 
                             ELSE NULL END, 
                        '; ' ORDER BY CASE WHEN eairway_confirmationgroup_value IS NOT NULL AND eairway_confirmationgroup_value != '' 
                                          THEN 'Confirmation: ' || eairway_confirmationgroup_value 
                                          ELSE NULL END
                    ) as confirmations_summary
                FROM eairway_confirmationgroup
                GROUP BY pcr_uuid_context
            ),
            airway_base_data AS (
                -- Get base airway data from any airway table that has PCR context
                SELECT DISTINCT
                    pcr_uuid_context,
                    -- Get decision and abandonment times from base tables if they exist
                    NULL as decision_time,  -- eAirway.10 
                    NULL as abandonment_time  -- eAirway.11
                FROM (
                    SELECT pcr_uuid_context FROM eairway_01
                    UNION 
                    SELECT pcr_uuid_context FROM eairway_08
                    UNION
                    SELECT pcr_uuid_context FROM eairway_09
                    UNION
                    SELECT pcr_uuid_context FROM eairway_confirmationgroup
                ) all_airway_pcrs
            )
            SELECT 
                abd.pcr_uuid_context,
                COALESCE(ai.indications_list, '') as airway_indications,
                COALESCE(ac.complications_list, '') as airway_complications, 
                COALESCE(afr.failure_reasons_list, '') as airway_failure_reasons,
                COALESCE(acm.confirmation_methods_list, '') as confirmation_methods,
                COALESCE(acf.confirmations_summary, '') as confirmations_summary,
                COALESCE(acf.confirmation_count, 0) as confirmation_count,
                -- Create comprehensive text_context with all 0:M data as lists
                CONCAT_WS(' | ',
                    CASE WHEN ai.indications_list IS NOT NULL AND ai.indications_list != '' 
                         THEN 'INDICATIONS: ' || ai.indications_list 
                         ELSE NULL END,
                    CASE WHEN ac.complications_list IS NOT NULL AND ac.complications_list != '' 
                         THEN 'COMPLICATIONS: ' || ac.complications_list 
                         ELSE NULL END,
                    CASE WHEN afr.failure_reasons_list IS NOT NULL AND afr.failure_reasons_list != '' 
                         THEN 'FAILURE_REASONS: ' || afr.failure_reasons_list 
                         ELSE NULL END,
                    CASE WHEN acm.confirmation_methods_list IS NOT NULL AND acm.confirmation_methods_list != '' 
                         THEN 'CONFIRMATION_METHODS: ' || acm.confirmation_methods_list 
                         ELSE NULL END,
                    CASE WHEN acf.confirmations_summary IS NOT NULL AND acf.confirmations_summary != '' 
                         THEN 'CONFIRMATIONS: ' || acf.confirmations_summary 
                         ELSE NULL END
                ) as text_context
            FROM airway_base_data abd
            LEFT JOIN airway_indications ai ON ai.pcr_uuid_context = abd.pcr_uuid_context
            LEFT JOIN airway_complications ac ON ac.pcr_uuid_context = abd.pcr_uuid_context  
            LEFT JOIN airway_failure_reasons afr ON afr.pcr_uuid_context = abd.pcr_uuid_context
            LEFT JOIN airway_confirmation_methods acm ON acm.pcr_uuid_context = abd.pcr_uuid_context
            LEFT JOIN airway_confirmations acf ON acf.pcr_uuid_context = abd.pcr_uuid_context
            WHERE abd.pcr_uuid_context IS NOT NULL
            ORDER BY abd.pcr_uuid_context;
            """

            cursor.execute(view_sql)
            conn.commit()
            print("Successfully created v_eairway_comprehensive view")

    except psycopg2.Error as e:
        print(f"Error creating eAirway view: {e}")
        conn.rollback()
    finally:
        conn.close()


def main():
    """Main function to create the eAirway view"""
    print("Creating eAirway comprehensive view...")
    create_eairway_view()
    print("eAirway view creation complete.")


if __name__ == "__main__":
    main()
