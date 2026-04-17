from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from server.configs.db import get_session
from server.models.lead import Lead, LeadBase, LeadCreate
from server.api.auth import get_current_user # We'll define this helper next
from server.models.user import User

router = APIRouter()

@router.get("/", response_model=list[Lead], operation_id="get_leads")
def read_leads(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user)
):
    # Only return leads belonging to the current user
    statement = select(Lead).where(Lead.owner_id == current_user.id)
    return session.exec(statement).all()

@router.post("/", operation_id="create_lead", response_model=Lead)
def create_lead(
    lead_in: LeadCreate, 
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user)
):
    # Map the incoming data to the DB model and assign the owner
    db_lead = Lead.model_validate(lead_in, update={"owner_id": current_user.id})
    session.add(db_lead)
    session.commit()
    session.refresh(db_lead)
    return db_lead