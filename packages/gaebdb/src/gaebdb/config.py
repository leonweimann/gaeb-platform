from __future__ import annotations

import os
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv


class Target(str, Enum):
    DEVELOPMENT = "development"
    PRODUCTION = "production"


def _load_dotenv():
    """
    Loads a .env, if exists.
    Strategies:
    - prefers .env in current workspace folder
    - otherwise doesn't load anything (so environment / secrets will be used)
    """
    dotenv_path = Path.cwd() / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
        print(f"[GAEBDB] (config) Loaded environment from {dotenv_path}")


_load_dotenv()


def normalize_target(target: str | Target | None = None) -> Target:
    """
    Takes an input (None / Target / str) and guaranteed returns a `Target`-Enum.
    Allowed strings:
    - development, dev
    - production, prod
    """
    if isinstance(target, Target):
        return target

    if target is None:
        target = os.getenv("GAEBDB_TARGET", "development")

    t = str(target).strip().lower()
    if t in ("development", "dev"):
        return Target.DEVELOPMENT
    if t in ("production", "prod"):
        return Target.PRODUCTION

    raise ValueError(f"[GAEBDB] (config) Unknown GAEBDB_TARGET value: {target!r}")


def get_dsn(target: str | Target | None = None) -> str:
    """
    Returns DSN from the environment variables.
    - DEVELOPMENT -> GAEBDB_DSN_DEVELOPMENT
    - PRODUCTION -> GAEBDB_DSN_PRODUCTION
    """
    t = normalize_target(target)
    key = (
        "GAEBDB_DSN_PRODUCTION" if t is Target.PRODUCTION else "GAEBDB_DSN_DEVELOPMENT"
    )
    dsn = os.getenv(key)
    if not dsn:
        raise RuntimeError(
            f"[GAEBDB] (config) {key} is not set but required for target={t.value}. "
            "Check your .env / deployment secrets."
        )
    return dsn
