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

### Changed
- **Database Setup (`database_setup.py`)**:
  - Updated table creation to support custom schemas
  - Added schema creation functionality
  - All table references now use schema-qualified names
- **Main Ingestion (`main_ingest.py`)**:
  - Updated all database operations to use schema-qualified table names
  - Added error file handling to all failure scenarios
  - Enhanced foreign key creation with comprehensive error handling
  - Improved logging with schema information
- **View Creator (`view_creator.py`)**:
  - Updated to work with custom schemas
  - All view and table operations now schema-aware
- **Configuration (`config.py`)**:
  - Added `PG_SCHEMA` configuration option

### Removed
- **FastAPI Dependencies**: Eliminated web API components to simplify the project
  - Removed `api.py` file entirely
  - Removed FastAPI, uvicorn, pydantic, and python-multipart dependencies
  - Updated `requirements.txt` to include only essential dependencies
  - Removed FastAPI documentation from README.md
- **View and Structure Management**: Removed view creation components for separate future development
  - Removed `view_creator.py` - view creation functionality to be rebuilt separately
  - Removed `structures.py` - NEMSIS structure definitions to be redesigned
  - Updated README.md to remove view creation documentation
- **Redundant Configuration**: Cleaned up duplicate imports
  - Removed direct config imports from `main_ingest.py` where redundant
  - Streamlined dependency management


### Technical Improvements
- **Better Error Handling**: Comprehensive error management across all modules
- **Code Organization**: Cleaner separation of concerns between modules
- **Database Schema Management**: Professional-grade schema support
- **File Management**: Systematic handling of processed and failed files
- **Foreign Key Integrity**: Automatic relationship management for data consistency

### Migration Notes
- **Backward Compatibility**: All changes maintain backward compatibility
- **New Installations**: Can optionally specify `PG_SCHEMA` environment variable
- **Existing Installations**: Will continue working with default "public" schema
- **Dependencies**: Run `pip install -r requirements.txt` to update dependencies

---

## Previous Versions

### [1.0.0-dynamic-ingestor-v4] - Initial Release
- Dynamic table creation based on XML structure
- UUID-based data overwrite functionality
- PostgreSQL integration with proper transaction management
- Vendor-specific Excel data import support
- Element and field definitions management
- Basic view creation for normalized querying
