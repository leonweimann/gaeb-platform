"""
gaebdb – central database-layer of gaeb-platform.

This package capsules:
- Configuration (config)
- Engine- and session-creation (engine)
- Datamodels (models)

Exampleusage:

    from gaebdb import session_scope, Project

    async with session_scope() as session:
        projects = (await session.execute(select(Project))).scalars().all()
"""

from .config import Target, get_dsn, normalize_target
from .engine import get_engine, get_sessionmaker, session_scope

__all__ = [
    # Config
    "Target",
    "get_dsn",
    "normalize_target",
    # Engine
    "get_engine",
    "get_sessionmaker",
    "session_scope",
    # Models
]
