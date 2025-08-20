# NEMSIS Database Ingestion

This project ingests NEMSIS-compliant XML files into a dynamic PostgreSQL database schema, creating tables or colmns dynamically based based on XML structure. It is designed for scalable, flexible EMS data warehousing and analysis from exported NEMSIS compliant software vendors. As long as the software is set to export agency specfic custom question as well they will included in the database for analysis.

The hope is that by allowing agnecies to build data lakes or datawarehouses internally it will reduce the need for formbuilding or external reporting mechanisms for KPI gathering and quality management systems.


## Cautions

While this tool is useful for creating an accessible database for use as a datalake and to ensure up-to-date and accurate KPI creation and visual analysis of data, it is crucial to remember that implementation must involve a proper compliance team specialized in healthcare IT. These records *DO* contain Protected Health Information (PHI) and must be handled in accordance with the principle of minimum necessary to achieve the task at hand. Compliance with HIPAA and the 21st Century CURES Act is necessary. Always ensure that all data handling, storage, and access are reviewed and approved by qualified compliance professionals to protect patient privacy and meet all legal and regulatory requirements.

## Features
- **Dynamic Table Creation:** Tables are created based on XML tag structure.
- **UUID-based Overwrite:** Data is keyed by PatientCareReport UUID for safe updates.
- **Table Comments:** Each table stores its XML path as a PostgreSQL table comment.
- **Bulk Ingestion:** Easily process all XML files in a directory.

## Requirements
- Python 3.8+
- PostgreSQL (with a database you can connect to)
- Python packages:
  - `psycopg2-binary`
  - `python-dotenv`
  - `requests`

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
```

## Initial Setup
1. **Create the PostgreSQL database** (if it does not exist):
   Connect to your PostgreSQL server and run:
   ```sql
   CREATE DATABASE your_database;
   ```
2. **Create tables and initialize schema:**
   ```bash
   python database_setup.py
   ```
   This will create the required tables in your database.

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
- Table comments in PostgreSQL will contain the XML path for each table.
- You can use standard SQL tools to query and analyze the ingested data.

## Troubleshooting
- **Database connection errors:** Ensure your `.env` is correct and the database exists.
- **Missing dependencies:** Install with `pip install psycopg2 python-dotenv`.
- **Permission errors:** Make sure your PostgreSQL user has rights to create tables and comments.

## License
MIT License


