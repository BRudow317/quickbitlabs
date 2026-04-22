from typing import Optional, TYPE_CHECKING
from datetime import datetime
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from server.models.user import User

class LeadBase(SQLModel):
    first_name: str
    last_name: str
    email: str = Field(index=True)
    status: str = Field(default="New")

class LeadCreate(LeadBase):
    pass

class Lead(LeadBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    owner_id: Optional[int] = Field(default=None, foreign_key="user.id")
    owner: Optional["User"] = Relationship(back_populates="leads")