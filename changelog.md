# Changelog

All notable changes to the NEMSIS XML Parser project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Schema Support**: Added optional PostgreSQL schema configuration
  - New `PG_SCHEMA` environment variable with default to "public" for backward compatibility
  - Schema-qualified table and view creation across all modules
  - Automatic schema creation during database setup
- **Error File Management**: Added safety mechanism for failed XML processing
  - New `ERROR_DIR` constant pointing to "error_files" directory
  - `move_to_error_directory()` function moves failed files with timestamp handling
  - Failed files are preserved instead of being left in place
- **Foreign Key Relationships**: Enhanced data integrity with automatic FK creation
  - Dynamic foreign key constraint creation between parent and child tables
  - Intelligent constraint naming with hash-based truncation for long names
  - FK constraint existence checking to prevent duplicates
  - CASCADE delete support for data consistency
- **Database Migration System**: Added Alembic migration support
  - Complete Alembic configuration for database schema versioning
  - Migration scripts for schema changes
  - Support for both new installations and existing database upgrades

### Changed
- **Column Naming Architecture**: Complete overhaul from generic to semantic naming
  - Changed from generic `text_content` column to dynamic `{table_name}_value` format
  - Each table now has semantic column names (e.g., `evitals_01_value`, `edisposition_02_value`)
  - Migration system ensures existing databases can be upgraded safely
  - Updated XML parser and ingestion logic to generate appropriate column names
- **Database Setup (`database_setup.py`)**:
  - Updated table creation to support custom schemas
  - Added schema creation functionality
  - All table references now use schema-qualified names
- **Main Ingestion (`main_ingest.py`)**:
  - Updated all database operations to use schema-qualified table names
  - Added error file handling to all failure scenarios
  - Enhanced foreign key creation with comprehensive error handling
  - Improved logging with schema information
  - Modified to use dynamic column naming system
- **XML Handler (`xml_handler.py`)**:
  - Enhanced to generate dynamic column names based on element structure
  - Added `value_column_name` field to element data for proper column mapping
- **Configuration (`config.py`)**:
  - Added `PG_SCHEMA` configuration option

### Removed
- **FastAPI Dependencies**: Eliminated web API components to simplify the project
  - Removed `api.py` file entirely
  - Removed FastAPI, uvicorn, pydantic, and python-multipart dependencies
  - Updated `requirements.txt` to include only essential dependencies
  - Removed FastAPI documentation from README.md
- **View Creation Components**: Removed automated view creation for manual implementation
  - Removed `create_views.py` - views will be created manually based on specific schema requirements
  - Removed automated view generation logic to allow for custom view design
  - Removed `querying_guide.md` - to be recreated after manual views are implemented
  - Updated documentation to reflect manual view creation approach
- **Legacy Structure Management**: Removed outdated components
  - Removed `view_creator.py` - view creation functionality replaced with manual approach
  - Removed `structures.py` - NEMSIS structure definitions to be redesigned
- **Redundant Configuration**: Cleaned up duplicate imports
  - Removed direct config imports from `main_ingest.py` where redundant
  - Streamlined dependency management


### Technical Improvements
- **Better Error Handling**: Comprehensive error management across all modules
- **Code Organization**: Cleaner separation of concerns between modules
- **Database Schema Management**: Professional-grade schema support with migration system
- **File Management**: Systematic handling of processed and failed files
- **Foreign Key Integrity**: Automatic relationship management for data consistency
- **Semantic Database Design**: Move from generic to self-documenting column names
- **Migration Infrastructure**: Robust database versioning and upgrade system

### Migration Notes
- **Column Naming Upgrade**: Existing databases can be migrated using `alembic upgrade head`
  - Migrates from `text_content` to `{table_name}_value` column naming
  - Safe rollback available with `alembic downgrade -1`
  - No data loss during migration process
- **New Installations**: Automatically use new column naming scheme
- **Schema Support**: Can optionally specify `PG_SCHEMA` environment variable
- **Dependencies**: Run `pip install -r requirements.txt` to install Alembic and other new dependencies
- **View Creation**: Views must now be created manually - automated view creation removed
- **Documentation**: Querying guide removed temporarily - will be recreated after views are implemented

---

## Previous Versions

### [1.0.0-dynamic-ingestor-v4] - Initial Release
- Dynamic table creation based on XML structure
- UUID-based data overwrite functionality
- PostgreSQL integration with proper transaction management
- Vendor-specific Excel data import support
- Element and field definitions management
- Basic view creation for normalized querying
