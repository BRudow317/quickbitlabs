from pydantic import BaseModel, Field


class UserBase(BaseModel):
    username: str
    email: str

class UserCreate(UserBase):
    password: str = Field(min_length=8, max_length=128)

class UserUpdate(BaseModel):
    email: str | None = None

class UserOut(UserBase):
    """Safe API response shape - no credentials."""
    external_id: str | None = None
    role: str = "user"

class User(UserOut):
    """Internal server-side model - includes auth fields, never serialised directly."""
    hashed_password: str = ""
    is_active: bool = True
