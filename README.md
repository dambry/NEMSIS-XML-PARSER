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
  - `psycopg2`
  - `python-dotenv`

Install dependencies:
```bash
pip install psycopg2 python-dotenv
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

### 1. Creating and Refreshing SQL Views

To generate or refresh all normalized SQL views for the NEMSIS data, run:

```bash
python create_views.py [--verbose]
```
- The `--verbose` flag (optional) will print the generated SQL for each view.
- This script will also update the ElementDefinitions and FieldDefinitions tables from the latest NEMSIS sources.

### 2. Downloading and Populating NEMSIS Element/Field Definitions

To manually (re)populate the `ElementDefinitions` and `FieldDefinitions` tables from the official NEMSIS sources:

```bash
python create_definitions.py
```
- This will download the latest definitions and update the tables in your database.

### 3. Importing Vendor-Specific Excel Data

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
- Data is available in your PostgreSQL database, with dynamic tables for each XML tag type and vendor import.
- SQL views are available for normalized, analysis-ready querying.
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

## FastAPI Application

### Running the FastAPI Application

This project includes a FastAPI application for ingesting XML data and querying it.

**1. Environment Setup:**

*   Ensure you have Python 3.8+ installed.
*   Create and activate a virtual environment:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
*   Install dependencies from `requirements.txt` (this file will be created in a later step, but we'll document its use now):
    ```bash
    pip install -r requirements.txt
    ```
*   **Database Configuration:**
    *   Make sure you have a PostgreSQL server running.
    *   Copy the `.env.example` file to a new file named `.env`:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file with your actual PostgreSQL connection details:
        ```
        PG_HOST=your_db_host
        PG_PORT=your_db_port
        PG_DATABASE=your_db_name
        PG_USER=your_db_user
        PG_PASSWORD=your_db_password
        ```
    *   Run the database setup script to create necessary tables and initial schema version (if you haven't already):
        ```bash
        python database_setup.py
        python create_definitions.py
        python create_views.py
        ```

**2. Starting the API Server:**

*   To run the FastAPI application locally, use Uvicorn:
    ```bash
    uvicorn api:app --reload --host 0.0.0.0 --port 8000
    ```
    The `--reload` flag enables auto-reloading when code changes, useful for development.

*   Once started, the API documentation will be available at:
    *   Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)
    *   ReDoc: [http://localhost:8000/redoc](http://localhost:8000/redoc)

### API Endpoints

**1. Ingest XML Data**

*   **Endpoint:** `POST /ingest_xml/`
*   **Description:** Accepts an XML file, processes it using the existing ingestion logic, and stores the data into the database.
*   **Request:**
    *   `Content-Type: multipart/form-data`
    *   `file`: The XML file to be ingested.
*   **Response (`IngestResponse`):**
    ```json
    {
      "message": "XML file processed and data ingested successfully.",
      "original_filename": "example.xml",
      "status": "Success"
    }
    ```
*   **Example using cURL:**
    ```bash
    curl -X POST "http://localhost:8000/ingest_xml/" -H "accept: application/json" -H "Content-Type: multipart/form-data" -F "file=@/path/to/your/nemsis_file.xml"
    ```

**2. Query Data**

*   **Endpoint:** `GET /query/`
*   **Description:** Executes a pre-defined query (SQL View) with date range filtering and optional filtering by diagnosis, procedures, or medications.
*   **Query Parameters:**
    *   `query_id` (string, required): The ID of the pre-defined query (view name) to execute (e.g., `v_evitals_flat`, `v_eprocedures_flat`).
    *   `date_from` (datetime, required): Start of the date range in ISO format (e.g., `2023-01-01T00:00:00Z`).
    *   `date_to` (datetime, required): End of the date range in ISO format (e.g., `2023-12-31T23:59:59Z`).
    *   `diagnosis` (list of strings, optional): List of diagnosis codes (e.g., from `esituation_11` or `esituation_12`). Example: `diagnosis=I10&diagnosis=J44.9`
    *   `procedures` (list of strings, optional): List of procedure codes (e.g., from `eProcedures.03`). Example: `procedures=99285&procedures=99291`
    *   `medications` (list of strings, optional): List of medication codes (e.g., National Drug Codes from `eMedications.03`). Example: `medications=0002-8215-01`
*   **Response (`QueryResult`):**
    ```json
    {
      "query_id": "v_evitals_flat",
      "parameters": {
        "query_id": "v_evitals_flat",
        "date_from": "2023-01-01T00:00:00",
        "date_to": "2023-01-31T23:59:59",
        "diagnosis": null,
        "procedures": null,
        "medications": null
      },
      "count": 5,
      "data": [
        { "...record 1 fields..." },
        { "...record 2 fields..." },
        // ... more records
      ]
    }
    ```
*   **Example using cURL:**
    ```bash
    curl -X GET "http://localhost:8000/query/?query_id=v_etimes_flat&date_from=2023-01-01T00%3A00%3A00Z&date_to=2023-12-31T23%3A59%3A59Z&procedures=12345" -H "accept: application/json"
    ```
*   **Important Note on Query Filtering:**
    *   Date filtering currently assumes that the queried view (specified by `query_id`) contains a timestamp column named `pcr_nemsis_datetime`. This column should represent the primary NEMSIS Patient Care Report date/time.
    *   Filtering by `diagnosis`, `procedures`, and `medications` assumes that the codes are stored in specific tables (`esituation_11` or `esituation_12` for diagnosis, `eprocedures_03` for procedures, `emedications_03` for medications respectively) in their `text_content` column, and that these tables can be linked to the main query view via a `pcr_uuid_context` column. The actual table and column names might vary based on your specific NEMSIS XML structure and database schema.
