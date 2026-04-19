from fastapi import APIRouter, Depends
from server.api.auth import get_current_user
from server.models.user import UserBase

router = APIRouter()


@router.get("/", operation_id="get_user", response_model=UserBase)
def get_user(current_user: UserBase = Depends(get_current_user)):
    return current_user
