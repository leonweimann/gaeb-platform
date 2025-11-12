from __future__ import annotations

from contextlib import asynccontextmanager
from functools import lru_cache
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .config import Target, get_dsn, normalize_target


@lru_cache(maxsize=None)
def get_engine(target: str | Target | None = None) -> AsyncEngine:
    """Returns a cached `AsyncEngine` for the given target."""
    t = normalize_target(target)
    dsn = get_dsn(t)

    engine = create_async_engine(
        dsn,
        echo=False,
        pool_pre_ping=True,
    )
    return engine


@lru_cache(maxsize=None)
def get_sessionmaker(
    target: str | Target | None = None,
) -> async_sessionmaker[AsyncSession]:
    """Returns a Async-Sessionmaker for the given target."""
    engine = get_engine(target)
    return async_sessionmaker(bind=engine, expire_on_commit=False)


@asynccontextmanager
async def session_scope(
    target: str | Target | None = None,
) -> AsyncIterator[AsyncSession]:
    """
    Convenience:

        async with session_scope() as session:
            ...

    Uses automatically GAEBDB_TARGET or explicitly declared target.
    """
    Session = get_sessionmaker(target)
    session = Session()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()
