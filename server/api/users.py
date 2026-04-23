from fastapi import APIRouter, Depends
from server.api.auth import get_current_user
from server.configs.db import oracle_client
from server.models.user import UserOut, User, UserUpdate

router = APIRouter()


@router.get("/", operation_id="get_user", response_model=UserOut)
def get_user(current_user: User = Depends(get_current_user)):
    return UserOut(
        username=current_user.username,
        email=current_user.email,
        external_id=current_user.external_id,
    )


@router.patch("/", operation_id="update_user", response_model=UserOut)
def update_user(
    update: UserUpdate,
    current_user: User = Depends(get_current_user),
):
    if update.email is not None:
        con = oracle_client.connect()
        with con.cursor() as cur:
            cur.execute(
                'UPDATE "USER" SET EMAIL = :email WHERE USERNAME = :username',
                email=update.email,
                username=current_user.username,
            )
        con.commit()

    return UserOut(
        username=current_user.username,
        email=update.email if update.email is not None else current_user.email,
        external_id=current_user.external_id,
    )
