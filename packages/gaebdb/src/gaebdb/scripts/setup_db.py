"""
Initializes the database for gaebdb:
- Ensures, required schema exist:
  - 'import'
- Creates all tables from the SQLAlchemy-ORM
"""

import asyncio

from gaebdb.engine import get_engine
from gaebdb.models.imports import SCHEMA as IMPORTS_SCHEMA
from gaebdb.models.imports import Base
from sqlalchemy import text


async def setup_database():
    print("🚧 Initialize database ...")

    engine = get_engine()

    async with engine.begin() as conn:
        # Make schema
        await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {IMPORTS_SCHEMA}"))
        print(f"✅ Schema '{IMPORTS_SCHEMA}' provided")

        # Make tables
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Tables created (or already existed)")

    print("🎉 Setup completed.")


if __name__ == "__main__":
    asyncio.run(setup_database())
