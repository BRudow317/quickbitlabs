from typing import List, Optional, TYPE_CHECKING
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from server.models.lead import Lead

class UserBase(SQLModel):
    username: str = Field(unique=True, index=True)

class UserCreate(UserBase):
    password: str = Field(min_length=8, max_length=128)

# This model is used for internal operations and database interactions, while the User model can be used for API responses to exclude sensitive fields like hashed_password.
class User(UserBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    hashed_password: str
    is_active: bool = Field(default=True)
    leads: List["Lead"] = Relationship(back_populates="owner")