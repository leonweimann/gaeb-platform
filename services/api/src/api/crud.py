from passlib.context import CryptContext
from sqlalchemy.orm import Session

from .auth.models import UserCreate
from .database import DBRole, DBUser

pwd_context = CryptContext(schemes=["argon2", "bcrypt"], deprecated="auto")


def get_user_by_id(db: Session, user_id: int):
    """Get user by ID."""
    return db.query(DBUser).filter(DBUser.id == user_id).first()


def get_user_by_username(db: Session, username: str):
    """Get user by username."""
    return db.query(DBUser).filter(DBUser.username == username).first()


def get_user_by_email(db: Session, email: str):
    """Get user by email."""
    return db.query(DBUser).filter(DBUser.email == email).first()


def create_user(db: Session, user: UserCreate):
    """Create a new user."""
    hashed_password = pwd_context.hash(user.password)
    db_user = DBUser(
        username=user.username,
        email=user.email,
        full_name=user.full_name,
        hashed_password=hashed_password,
    )

    # Assign default user role
    user_role = db.query(DBRole).filter(DBRole.name == "user").first()
    if user_role:
        db_user.roles.append(user_role)

    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


def authenticate_user(db: Session, username: str, password: str):
    """Authenticate user with username and password."""
    user = get_user_by_username(db, username)
    if not user:
        return False
    if not pwd_context.verify(password, getattr(user, "hashed_password", None)):
        return False
    return user


def create_role(db: Session, name: str, description: str = ""):
    """Create a new role."""
    db_role = DBRole(name=name, description=description)
    db.add(db_role)
    db.commit()
    db.refresh(db_role)
    return db_role


def get_all_users(db: Session, skip: int = 0, limit: int = 100):
    """Get all users with pagination."""
    return db.query(DBUser).offset(skip).limit(limit).all()


def get_role_by_name(db: Session, name: str):
    """Get role by name."""
    return db.query(DBRole).filter(DBRole.name == name).first()
