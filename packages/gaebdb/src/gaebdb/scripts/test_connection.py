import asyncio

from gaebdb import session_scope
from sqlalchemy import text


async def main():
    async with session_scope() as session:
        result = await session.execute(text("SELECT current_database(), current_user"))
        db, user = result.one()
        print("✅ Connection successful:")
        print(f"   Database: {db}")
        print(f"   User:  {user}")


if __name__ == "__main__":
    asyncio.run(main())
