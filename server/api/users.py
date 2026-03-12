from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session
from server.api.auth import get_current_user
from server.configs.db import get_session
from server.models.user import UserBase, UserCreate, User
from server.core.security import get_password_hash

router = APIRouter()

@router.post("/register", operation_id="post_user", response_model=UserBase)
def register_user(
    user_data: UserCreate, 
    session: Session = Depends(get_session) # The session is injected here
):
    # Hash plain 'password' and save to 'hashed_password'
    db_user = User(
        username=user_data.username,
        hashed_password=get_password_hash(user_data.password)
    )
    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return db_user

@router.get("/", operation_id="get_user", response_model=UserBase)
def get_user(current_user: User = Depends(get_current_user)):
    # The 'Session' work happened inside 'get_current_user'
    # 'current_user' is already a live SQLModel instance
    return current_user