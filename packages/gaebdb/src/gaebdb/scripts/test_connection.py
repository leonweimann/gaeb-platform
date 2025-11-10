import asyncio

from gaebdb import get_sessionmaker
from sqlalchemy import text


async def main():
    Session = get_sessionmaker()  # uses automatically GAEBDB_TARGET=development
    async with Session() as session:
        result = await session.execute(text("SELECT current_database(), current_user"))
        db, user = result.one()
        print("✅ Connection successful:")
        print(f"   Database: {db}")
        print(f"   User:  {user}")


if __name__ == "__main__":
    asyncio.run(main())
