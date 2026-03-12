from typing import Optional, TYPE_CHECKING
from datetime import datetime
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from server.models.user import User

# 1. Base Schema (Common fields)
class LeadBase(SQLModel):
    first_name: str
    last_name: str  # Added as requested
    email: str = Field(index=True)
    status: str = Field(default="New")

# 2. Creation Schema (What the frontend sends)
class LeadCreate(LeadBase):
    pass # No ID or Timestamp needed from the user

# 3. Database Table
class Lead(LeadBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    owner_id: Optional[int] = Field(default=None, foreign_key="user.id")
    owner: Optional["User"] = Relationship(back_populates="leads")