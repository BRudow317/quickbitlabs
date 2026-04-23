from __future__ import annotations

from datetime import timedelta
from typing import Annotated

import jwt
import oracledb
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

from server.configs.db import oracle_client, session_service
from server.configs.settings import settings
from server.core.jwt import create_access_token
from server.models.AuthModels import Token
from server.models.user import UserBase

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")


@router.post("/login", response_model=Token, operation_id="login")
def login(
    request: Request,
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
):
    """
    Authenticate against Oracle using the supplied credentials.

    Rate-limited: too many failures from the same username or IP within the
    configured window will return 429 before the DB is even contacted.
    Every attempt — success or failure — is logged to USER_SIGN_IN.
    Successful logins create a USER_SESSION row.
    """
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent")
    username = form_data.username

    # --- Rate limit check --------------------------------------------------
    if session_service.is_rate_limited(
        username,
        ip,
        window_minutes=settings.LOGIN_RATE_LIMIT_WINDOW_MINUTES,
        max_failures=settings.LOGIN_RATE_LIMIT_MAX_FAILURES,
    ):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                f"Too many failed login attempts. "
                f"Please wait {settings.LOGIN_RATE_LIMIT_WINDOW_MINUTES} minutes before trying again."
            ),
        )

    # --- Credential validation ---------------------------------------------
    try:
        con = oracledb.connect(
            user=username,
            password=form_data.password,
            host=settings.ORACLE_HOST,
            port=settings.ORACLE_PORT,
            service_name=settings.ORACLE_SERVICE,
        )
        con.close()
    except oracledb.Error:
        session_service.log_sign_in(
            username,
            success=False,
            ip_address=ip,
            user_agent=ua,
            failure_reason="Invalid Oracle credentials",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # --- Issue token -------------------------------------------------------
    expires_delta = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": username},
        expires_delta=expires_delta,
    )

    # --- Persist session and audit -----------------------------------------
    from datetime import datetime, timezone
    expires_at = datetime.now(timezone.utc) + expires_delta
    try:
        session_service.create_session(
            username=username,
            token=access_token,
            expires_at=expires_at,
            ip_address=ip,
            user_agent=ua,
        )
        session_service.log_sign_in(username, success=True, ip_address=ip, user_agent=ua)
    except Exception:
        # Non-fatal — token was issued; log the error but don't block the response
        import logging
        logging.getLogger(__name__).exception("Failed to persist session/sign-in record for '%s'", username)

    return {"access_token": access_token, "token_type": "bearer"}


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT, operation_id="logout")
def logout(
    _: Annotated[UserBase, Depends(get_current_user)],
    token: Annotated[str, Depends(oauth2_scheme)],
):
    """Invalidate the current session token server-side."""
    session_service.invalidate_session(token)


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
