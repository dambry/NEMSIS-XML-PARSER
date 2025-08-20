"""Configuration constants for the NEMSIS data import project."""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection details from environment variables
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE")  # No default, should be set
PG_USER = os.getenv("PG_USER")  # No default, should be set
PG_PASSWORD = os.getenv("PG_PASSWORD")  # No default, should be set
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")  # Default to 'public' schema

# Check if critical PostgreSQL environment variables are set
if not all([PG_DATABASE, PG_USER, PG_PASSWORD]):
    print(
        "Critical PostgreSQL environment variables (PG_DATABASE, PG_USER, PG_PASSWORD) are not set."
    )
    print("Please ensure they are defined in your .env file or system environment.")
