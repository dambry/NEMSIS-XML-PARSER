```markdown
# Direct Querying Guide for Dynamically Generated EMS Tables

This guide explains how to query the dynamically generated tables created by the NEMSIS XML ingestion process. Since the flat views (e.g., `v_evitals_flat`) have been removed, you will need to query the base tables directly.

## Table Naming Convention

Tables are named based on the XML tags from the NEMSIS data. The naming convention is as follows:

1.  **Sanitization**: XML tag names are sanitized to be SQL-friendly. This typically involves:
    *   Replacing characters like `.` with `_`.
    *   Removing other non-alphanumeric characters (except `_`).
    *   Ensuring the name doesn't start with a number (an underscore `_` might be prepended).
2.  **Case**: Table names are generally stored in lowercase.

For example:
*   An XML tag like `<eVitals.VitalGroup>` would likely correspond to a table named `evitals_vitalgroup`.
*   A tag `<eDisposition.01>` would correspond to `edisposition_01`.

You can inspect the available tables in your database using `psql`'s `\dt` command or by querying `information_schema.tables`.

## Key Columns for Relationships

Each dynamically created table contains several common columns that are essential for understanding relationships and data context:

*   `element_id` (TEXT, Primary Key): A unique UUID assigned to each XML element instance. This is the primary key for its table.
*   `parent_element_id` (TEXT): For child elements, this column stores the `element_id` of its direct parent element. For root elements of a section (e.g., the main `evitals` element), this will be `NULL`.
*   `pcr_uuid_context` (TEXT): The UUID of the PatientCareReport this element belongs to. Useful for filtering data for a specific report.
*   `original_tag_name` (TEXT): The original XML tag name before sanitization (e.g., `eVitals.VitalGroup`).
*   `text_content` (TEXT): The text content of the XML element, if any.
*   Additional columns are dynamically added based on the attributes found in the XML elements. For example, if an element `<eVitals.01 DateTime="2023-01-01T12:00:00">` exists, the `evitals_01` table would likely have a `datetime` column.

## Querying Parent-Child Relationships

You can join tables using `element_id` and `parent_element_id` to reconstruct the hierarchy.

**Example: Get eVitals.01 (DateTime Vitals Taken) and its parent eVitals.VitalGroup**

Assuming `evitals_vitalgroup` is the parent table and `evitals_01` is the child table:

```sql
SELECT
    vg.element_id AS vitalgroup_id,
    vg.pcr_uuid_context,
    e01.element_id AS evitals01_id,
    e01.text_content AS evitals01_text_content,
    -- Assuming evitals_01 has a 'datetime' column from an attribute
    e01.datetime AS evitals01_datetime
FROM
    evitals_vitalgroup vg
JOIN
    evitals_01 e01 ON vg.element_id = e01.parent_element_id
WHERE
    vg.pcr_uuid_context = 'your_specific_pcr_uuid'; -- Optional: filter by PCR
```

**Explanation:**
*   We select columns from both the parent (`evitals_vitalgroup` aliased as `vg`) and child (`evitals_01` aliased as `e01`).
*   The `JOIN` condition `vg.element_id = e01.parent_element_id` links the child to its parent.

## Querying Hierarchies with Recursive CTEs

For more complex hierarchical queries (e.g., getting all descendants of an element, or finding a specific ancestor), you can use recursive Common Table Expressions (CTEs) in PostgreSQL.

**Example: Get all descendant elements of a specific `eVitals.VitalGroup` instance**

Let's say you have the `element_id` of a specific `eVitals.VitalGroup` row and want to find all elements nested under it, regardless of which table they are in. This is more complex because children can be in different tables.

A more practical recursive query would be to trace relationships within a known set of related tables, or to find all elements belonging to a parent across *any* table that might reference it.

**Generalized Example: Find all descendants of a given `parent_id`**

This query will find all `element_id`s that are descendants of a starting `known_parent_element_id`. You would typically run this by first identifying the `element_id` of the top-level element you're interested in.

```sql
WITH RECURSIVE element_hierarchy AS (
    -- Anchor member: Select the starting parent element
    -- You'll need to know which table your starting element is in,
    -- or search across all tables if element_id is globally unique (which it is).
    -- For this example, let's assume we start with an ID from 'evitals_vitalgroup'
    SELECT
        element_id,
        parent_element_id,
        original_tag_name,
        0 AS level,
        original_tag_name AS path
    FROM
        public.evitals_vitalgroup -- Replace with actual starting table if known
    WHERE
        element_id = 'your_starting_element_id' -- Replace with the actual ID

    UNION ALL

    -- Recursive member: Find children in *any* table
    -- This requires querying all possible tables. A more optimized version
    -- would join with a catalog of table names if performance is an issue.
    -- For simplicity, we'll show a conceptual join with one potential child table.
    -- You'd need to UNION ALL similar blocks for all potential child table types.

    -- Example for children in 'evitals_01' table
    SELECT
        e01.element_id,
        e01.parent_element_id,
        e01.original_tag_name,
        eh.level + 1,
        eh.path || '/' || e01.original_tag_name
    FROM
        public.evitals_01 e01 -- Replace with actual child table name
    JOIN
        element_hierarchy eh ON e01.parent_element_id = eh.element_id

    -- UNION ALL for children in 'evitals_cardiacrhythmgroup' table
    UNION ALL
    SELECT
        ecg.element_id,
        ecg.parent_element_id,
        ecg.original_tag_name,
        eh.level + 1,
        eh.path || '/' || ecg.original_tag_name
    FROM
        public.evitals_cardiacrhythmgroup ecg -- Replace with actual child table name
    JOIN
        element_hierarchy eh ON ecg.parent_element_id = eh.element_id

    -- ... add more UNION ALL blocks for other potential child tables ...
    -- To make this truly generic, you would need dynamic SQL or a function
    -- that queries information_schema.columns to find all tables with a
    -- 'parent_element_id' column and then builds the query.
)
SELECT
    element_id,
    parent_element_id,
    original_tag_name,
    level,
    path
FROM
    element_hierarchy
ORDER BY
    path;
```

**Important Note on the Generalized Recursive CTE:**
The example above is illustrative. A truly generic "find all descendants across all tables" CTE is complex in SQL because it would ideally need to dynamically query all tables that *could* contain a child.
*   For practical use, you might write recursive CTEs that are specific to a known hierarchy (e.g., all elements within the `eVitals` structure by UNIONing all `evitals_*` tables).
*   The `element_id` values are UUIDs and should be unique across all tables. The `parent_element_id` refers to one of these unique `element_id`s.

**Alternative Recursive CTE: Tracing a specific element and its children within their specific tables (using `structures.py` as a guide)**

If you know the structure (e.g., from `structures.py` in the codebase, which defines parent-child relationships and table names):

```sql
-- Example: Get eVitals.VitalGroup and all its known children from their specific tables
WITH RECURSIVE vital_hierarchy AS (
    -- Anchor: Starting element (e.g., a specific VitalGroup)
    SELECT
        element_id,
        parent_element_id,
        pcr_uuid_context,
        original_tag_name,
        text_content,
        NULL AS datetime_value, -- Placeholder for eVitals.01 specific column
        0 as level
    FROM evitals_vitalgroup
    WHERE element_id = 'your_vitalgroup_element_id' -- Specify the starting parent

    UNION ALL

    -- Children of VitalGroup that are eVitals.01
    SELECT
        child.element_id,
        child.parent_element_id,
        child.pcr_uuid_context,
        child.original_tag_name,
        child.text_content,
        child.datetime AS datetime_value, -- Actual column from evitals_01
        vh.level + 1
    FROM evitals_01 child
    JOIN vital_hierarchy vh ON child.parent_element_id = vh.element_id
    WHERE vh.original_tag_name = 'eVitals.VitalGroup' -- Ensure parent is of the correct type

    UNION ALL

    -- Children of VitalGroup that are eVitals.CardiacRhythmGroup
    SELECT
        child_group.element_id,
        child_group.parent_element_id,
        child_group.pcr_uuid_context,
        child_group.original_tag_name,
        child_group.text_content,
        NULL, -- No datetime for this group
        vh.level + 1
    FROM evitals_cardiacrhythmgroup child_group
    JOIN vital_hierarchy vh ON child_group.parent_element_id = vh.element_id
    WHERE vh.original_tag_name = 'eVitals.VitalGroup'

    -- Add more UNION ALL clauses here for other direct children of VitalGroup
    -- and then for children of those children if you want to go deeper and know the structure
)
SELECT * FROM vital_hierarchy ORDER BY level, original_tag_name;
```

This second recursive CTE example is more verbose but also more explicit about which tables are being joined, which is often necessary for practical querying of heterogeneous hierarchies.

## Tips for Querying
*   Start by identifying the `pcr_uuid_context` for the patient care report you are interested in.
*   Use `original_tag_name` to understand what kind of data a row represents.
*   When constructing joins, ensure you are joining `parent_table.element_id` to `child_table.parent_element_id`.
*   For performance, ensure that `element_id` and `parent_element_id` columns are indexed if they are not already (primary key `element_id` will be indexed). `parent_element_id` might benefit from an explicit index.

This guide provides a starting point. You may need to adapt these query patterns based on the specific data you are trying to retrieve and the complexity of the relationships.
```
