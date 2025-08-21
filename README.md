# NEMSIS Database Ingestion

This project ingests NEMSIS-compliant XML files into a dynamic PostgreSQL database schema, creating tables and columns dynamically based on XML structure. It is designed for scalable, flexible EMS data warehousing and analysis from exported NEMSIS compliant software vendors. As long as the software is set to export agency specific custom questions as well they will be included in the database for analysis.

The system uses semantic column naming where each table's data column is named `{table_name}_value` (e.g., `evitals_01_value`, `edisposition_02_value`) instead of generic `text_content`, making the schema self-documenting and more intuitive for querying.

The hope is that by allowing agencies to build data lakes or datawarehouses internally it will reduce the need for form building or external reporting mechanisms for KPI gathering and quality management systems.


## Cautions

While this tool is useful for creating an accessible database for use as a datalake and to ensure up-to-date and accurate KPI creation and visual analysis of data, it is crucial to remember that implementation must involve a proper compliance team specialized in healthcare IT. These records *DO* contain Protected Health Information (PHI) and must be handled in accordance with the principle of minimum necessary to achieve the task at hand. Compliance with HIPAA and the 21st Century CURES Act is necessary. Always ensure that all data handling, storage, and access are reviewed and approved by qualified compliance professionals to protect patient privacy and meet all legal and regulatory requirements.

## Features
- **Dynamic Table Creation:** Tables are created based on XML tag structure.
- **Semantic Column Naming:** Each table uses `{table_name}_value` column naming for self-documenting schema.
- **UUID-based Overwrite:** Data is keyed by PatientCareReport UUID for safe updates.
- **Database Migration Support:** Alembic integration for schema versioning and safe upgrades.
- **Table Comments:** Each table stores its XML path as a PostgreSQL table comment.
- **Foreign Key Relationships:** Automatic creation of referential integrity constraints.
- **Bulk Ingestion:** Easily process all XML files in a directory.
- **Error Handling:** Failed files are systematically moved to error directory for troubleshooting.

## Requirements
- Python 3.8+
- PostgreSQL (with a database you can connect to)
- Python packages:
**Missing dependencies:** Install with `pip install -r requirements.txt`.
Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration
Create a `.env` file in the project root with your PostgreSQL connection details:

```
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_database
PG_USER=your_user
PG_PASSWORD=your_password
PG_SCHEMA=public  # Optional: specify a custom schema (defaults to 'public')
```

## Initial Setup

### For New Installations
1. **Create the PostgreSQL database** (if it does not exist):
   Connect to your PostgreSQL server and run:
   ```sql
   CREATE DATABASE your_database;
   ```
2. **Create tables and initialize schema:**
   ```bash
   python database_setup.py
   ```
   This will create the required tables in your database with the new semantic column naming.

### For Existing Database Upgrades
If you have an existing database with the old `text_content` column naming, upgrade it to the new `{table_name}_value` format:

1. **Run the database migration:**
   ```bash
   alembic upgrade head
   ```
   This will safely rename all `text_content` columns to the new semantic format without data loss.

2. **Rollback if needed (optional):**
   ```bash
   alembic downgrade -1
   ```
   This will revert columns back to `text_content` if you need to rollback.

## Ingesting XML Files
To ingest a single XML file:
```bash
python main_ingest.py nemsis_xml/your_file.xml
```

To ingest **all XML files in a directory** (PowerShell example):
```powershell
Get-ChildItem -Path .\nemsis_xml\*.xml | ForEach-Object { python main_ingest.py $_.FullName }
```

Or (CMD example):
```cmd
for %f in (nemsis_xml\*.xml) do python main_ingest.py "%f"
```

## Advanced Usage

### 1. Downloading and Populating NEMSIS Element/Field Definitions

To manually (re)populate the `ElementDefinitions` and `FieldDefinitions` tables from the official NEMSIS sources:

```bash
python create_definitions.py
```
- This will download the latest definitions and update the tables in your database.

### 2. Importing Vendor-Specific Excel Data

To import vendor-specific Excel exports into your database, use:

```bash
python vendor_import.py -file_path <path_to_excel> -vendor <vendor_name> -source <source_name>
```
- Example:
  ```bash
  python vendor_import.py -file_path "./vendor_data.xlsx" -vendor imagetrend -source new_hampshire
  ```
- The script will create tables named `<source>_<sheetname>` for each supported sheet, using only non-blank rows and the columns specified in the script for that vendor.
- Sheet and column mappings are hardcoded per vendor in the script. Update `VENDOR_SPECS` in `vendor_import.py` to add or modify vendor logic.

## Output
- Processed XML files are archived in the `processed_xml_archive/` directory.
- Failed XML files are moved to the `error_files/` directory for troubleshooting.
- Data is available in your PostgreSQL database, with dynamic tables for each XML tag type and vendor import.
- Element and field definitions are available in the `ElementDefinitions` and `FieldDefinitions` tables.

## Notes
- The ingestion script will skip or update data based on the PatientCareReport UUID.
- Each table uses semantic column naming: `{table_name}_value` (e.g., `evitals_01_value`, `epatient_15_value`).
- Table comments in PostgreSQL will contain the XML path for each table.
- Foreign key relationships are automatically created between parent and child tables.
- You can use standard SQL tools to query and analyze the ingested data.
- Views and querying documentation will be provided after manual view creation is complete.

## Troubleshooting
- **Database connection errors:** Ensure your `.env` is correct and the database exists.
- **Missing dependencies:** Install with `pip install -r requirements.txt` to get all required packages including Alembic.
- **Permission errors:** Make sure your PostgreSQL user has rights to create tables and comments.
- **Migration errors:** If Alembic migration fails, check that no other processes are accessing the database during migration.
- **Column naming issues:** After migration, verify that columns have been renamed correctly using `\d table_name` in psql.

## License
MIT License


