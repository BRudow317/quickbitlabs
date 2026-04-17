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

from fastapi import APIRouter
from server.plugins.PluginModels import Entity, Column

router = APIRouter()

@router.get("/api/metadata/{system}/{table_name}", response_model=Entity)
async def get_dynamic_schema(system: str, table_name: str):
    """
    Returns the Entity and Column definitions on the fly, 
    rather than relying on hardcoded Python classes.
    """
    if system == "oracle":
        return await sniff_oracle_metadata(table_name)
    elif system == "salesforce":
        return await sniff_salesforce_describe(table_name)
    else:
        raise HTTPException(status_code=400, detail="Unknown system")
    
async def sniff_salesforce_describe(object_name: str) -> Entity:
    # 1. Hit the REST API
    sf_metadata = call_sf_api(f"/sobjects/{object_name}/describe")
    
    columns = []
    for field in sf_metadata["fields"]:
        columns.append(Column(
            name=field["name"],
            raw_type=field["type"],
            arrow_type_id=map_sf_type_to_arrow(field["type"]), # Your custom mapper
            is_nullable=field["nillable"],
            max_length=field["length"]
        ))
        
    return Entity(
        name=object_name,
        parent_names=["salesforce"],
        columns=columns
    )