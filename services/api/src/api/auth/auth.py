import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from .. import crud
from ..database import DBUser, get_db
from .models import User

# One-off secret for local testing only. Replace with a stable env var in production.
SECRET_KEY = secrets.token_urlsafe(32)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["argon2", "bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def convert_db_user_to_user(db_user: DBUser) -> User:
    """Convert database user to Pydantic user model.

    Use getattr to avoid static-analysis complaints when the input type
    could be a SQLAlchemy declarative class (where class attributes are
    Column/InstrumentedAttribute objects). Callers should validate the
    instance shape before calling this helper.
    """
    username = getattr(db_user, "username", None)
    if username is None:
        raise ValueError("DB user instance has no 'username' attribute or it's None")
    email = getattr(db_user, "email", None)
    full_name = getattr(db_user, "full_name", None)
    is_active = getattr(db_user, "is_active", True)
    roles_attr = getattr(db_user, "roles", []) or []

    # Extract role names defensively
    roles = [getattr(r, "name", None) for r in roles_attr]
    roles = [r for r in roles if r is not None]

    return User(
        username=username,
        email=email,
        full_name=full_name,
        disabled=not is_active,
        roles=roles,
    )


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
    token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    """Get the current user from the JWT token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    db_user = crud.get_user_by_username(db, username=username)
    if db_user is None:
        raise credentials_exception
    # Defensive check: make sure we received an instance with concrete values
    # (not the declarative class or its Column attributes). If someone
    # accidentally passed the model class, attribute access will return
    # an InstrumentedAttribute/Column object which we must reject.
    try:
        # Accessing attributes should yield concrete values for instances.
        uname = db_user.username
    except Exception:
        raise credentials_exception

    if isinstance(uname, InstrumentedAttribute):
        # This means a class or class attribute was passed instead of an
        # instance. Treat it as invalid credentials and provide a helpful
        # error message.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                "Internal server error: expected user instance but got a "
                "SQLAlchemy attribute/class. Check that your CRUD returns "
                "a DBUser instance (not the class)."
            ),
        )

    return convert_db_user_to_user(db_user)


async def get_current_active_user(current_user: User = Depends(get_current_user)):
    """Get the current active user (not disabled)."""
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
