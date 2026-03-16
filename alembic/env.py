import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context

# 1. Add your project root to sys.path so we can import 'server'
sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), '..')))

# 2. Import your SQLModel metadata and Settings
from sqlmodel import SQLModel
from server.configs.settings import settings
# IMPORTANT: Import ALL models here so they are registered with SQLModel.metadata
from server.models.user import User
from server.models.lead import Lead

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# 3. Use your SQLModel metadata
target_metadata = SQLModel.metadata

def run_migrations_online() -> None:
    # 4. Overwrite the ini URL with our actual DATABASE_URL
    connectable = context.config.attributes.get("connection", None)

    if connectable is None:
        from sqlalchemy import create_engine
        connectable = create_engine(settings.DATABASE_URL, poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection, 
            target_metadata=target_metadata,
            render_as_batch=True # CRITICAL for SQLite renames/alters
        )

        with context.begin_transaction():
            context.run_migrations()

# Standard Alembic offline support
def run_migrations_offline() -> None:
    url = settings.DATABASE_URL
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True
    )

    with context.begin_transaction():
        context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()