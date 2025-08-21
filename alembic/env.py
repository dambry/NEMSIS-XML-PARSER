from logging.config import fileConfig
import os
import sys

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlalchemy import create_engine

from alembic import context

# Add project root to path to import config
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import database configuration
try:
    from config import PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD, PG_SCHEMA

    database_url = (
        f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    )
except ImportError:
    print("Error: Could not import database configuration from config.py")
    database_url = None

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Set the database URL from our config
if database_url:
    config.set_main_option("sqlalchemy.url", database_url)

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run database migrations in "online" mode using a live DB connection.
    
    Creates an Engine from the alembic config (section prefixed with "sqlalchemy.", using a NullPool),
    opens a connection, configures the Alembic context with that connection and the module's
    target_metadata, and executes migrations inside a transaction. If a PG_SCHEMA value is
    available from the application's config and is not "public", the schema is used for the
    Alembic version table via `version_table_schema`.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        # Set up schema context if we have one
        context_args = {"connection": connection, "target_metadata": target_metadata}

        # Add schema context if it's not the default public schema
        try:
            from config import PG_SCHEMA

            if PG_SCHEMA and PG_SCHEMA != "public":
                context_args["version_table_schema"] = PG_SCHEMA
        except ImportError:
            pass

        context.configure(**context_args)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
