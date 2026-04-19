from __future__ import annotations
import os
from datetime import timedelta
from typing import Annotated

import jwt
import oracledb
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer

from server.configs.settings import settings
from server.core.jwt import create_access_token
from server.models.AuthModels import Token
from server.models.user import UserBase

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")


@router.post("/login", response_model=Token, operation_id="login")
def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    """Authenticate by opening an Oracle connection with the supplied credentials."""
    try:
        con = oracledb.connect(
            user=form_data.username,
            password=form_data.password,
            host=os.getenv("ORACLE_HOST", ""),
            port=int(os.getenv("ORACLE_PORT", "1521")),
            service_name=os.getenv("ORACLE_SERVICE", ""),
        )
        con.close()
    except oracledb.Error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Oracle credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(
        data={"sub": form_data.username},
        expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return {"access_token": access_token, "token_type": "bearer"}


def get_current_user(token: str = Depends(oauth2_scheme)) -> UserBase:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.ALGORITHM])
        username: str | None = payload.get("sub")
        if username is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception

    return UserBase(username=username)
