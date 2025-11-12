from typing import Optional

from pydantic import BaseModel


class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None
    roles: Optional[list[str]] = None


class UserInDB(User):
    hashed_password: str


class UserCreate(BaseModel):
    username: str
    email: str
    full_name: str
    password: str
