from __future__ import annotations

from datetime import timedelta
from typing import Annotated

import jwt
import oracledb
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel

from server.db.db import server_db, session_service
from server.configs.settings import settings
from server.core.jwt import create_access_token
from server.core.security import verify_password, get_password_hash
from server.models.user import UserBase, UserOut, User, UserCreate

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")


# ---------------------------------------------------------------------------
# Internal helpers - raw Oracle queries against USER_CREDENTIALS
# ---------------------------------------------------------------------------

def _get_credentials(username: str) -> dict | None:
    """Return the USER row for *username*, or None if not found."""
    sql = """
        SELECT EXTERNAL_ID, USERNAME, EMAIL, HASHED_PASSWORD, IS_ACTIVE
          FROM "USER"
         WHERE USERNAME = :username
    """
    con = server_db.connect()
    with con.cursor() as cur:
        cur.execute(sql, username=username)
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "external_id": str(row[0]) if row[0] is not None else None,
        "username": row[1],
        "email": row[2],
        "hashed_password": row[3],
        "is_active": bool(row[4]),
    }


def _username_exists(username: str) -> bool:
    sql = 'SELECT COUNT(*) FROM "USER" WHERE USERNAME = :u'
    with server_db.connect().cursor() as cur:
        cur.execute(sql, u=username)
        return (cur.fetchone() or (0,))[0] > 0


def _email_exists(email: str) -> bool:
    sql = 'SELECT COUNT(*) FROM "USER" WHERE EMAIL = :e'
    with server_db.connect().cursor() as cur:
        cur.execute(sql, e=email)
        return (cur.fetchone() or (0,))[0] > 0


def _insert_user(username: str, email: str, hashed_password: str) -> None:
    """INSERT a new row into the USER table."""
    sql = """
        INSERT INTO "USER"
            (USERNAME, EMAIL, HASHED_PASSWORD, IS_ACTIVE)
        VALUES
            (:username, :email, :hashed_password, 1)
    """
    con = server_db.connect()
    with con.cursor() as cur:
        cur.execute(sql, username=username, email=email, hashed_password=hashed_password)
    con.commit()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/register", response_model=UserOut, operation_id="register")
def register(user_in: UserCreate):
    """
    Register application credentials for a user.
    If the username exists in the Salesforce USER table it will be linked via EXTERNAL_ID.
    """
    if _username_exists(user_in.username):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username already registered")
    if _email_exists(user_in.email):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")

    hashed_password = get_password_hash(user_in.password)
    _insert_user(user_in.username, user_in.email, hashed_password)

    return UserOut(username=user_in.username, email=user_in.email)

class Token(BaseModel):
    access_token: str
    token_type: str

@router.post("/login", response_model=Token, operation_id="login")
def login(
    request: Request,
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
):
    """
    Authenticate against USER_CREDENTIALS.

    Rate-limited: too many failures from the same username or IP within the
    configured window will return 429 before the DB is even contacted.
    Every attempt - success or failure - is logged to USER_SIGN_IN.
    Successful logins create a USER_SESSION row.
    """
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent")
    username = form_data.username

    # --- Rate limit check --------------------------------------------------
    if session_service.is_rate_limited(
        username,
        ip,
        window_minutes=settings.login_rate_limit_window_minutes,
        max_failures=settings.login_rate_limit_max_failures,
    ):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                f"Too many failed login attempts. "
                f"Please wait {settings.login_rate_limit_window_minutes} minutes before trying again."
            ),
        )

    # --- Credential validation ---------------------------------------------
    creds = _get_credentials(username)

    if not creds or not verify_password(form_data.password, creds["hashed_password"]):
        session_service.log_sign_in(
            username,
            success=False,
            ip_address=ip,
            user_agent=ua,
            failure_reason="Invalid application credentials",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not creds["is_active"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account is disabled")

    # --- Issue token -------------------------------------------------------
    expires_delta = timedelta(minutes=settings.access_token_expire_minutes)
    access_token = create_access_token(
        data={
            "sub": username,
            "eid": creds["external_id"],   # Salesforce EXTERNAL_ID - may be None
            "email": creds["email"],
        },
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
        import logging
        logging.getLogger(__name__).exception("Failed to persist session/sign-in record for '%s'", username)

    return {"access_token": access_token, "token_type": "bearer"}


def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.jwt_secret.get_secret_value(), algorithms=[settings.jwt_algorithm])
        username: str | None = payload.get("sub")
        if username is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception

    return User(
        username=username,
        email=payload.get("email", ""),
        external_id=payload.get("eid"),
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT, operation_id="logout")
def logout(
    _: Annotated[User, Depends(get_current_user)],
    token: Annotated[str, Depends(oauth2_scheme)],
):
    """Invalidate the current session token server-side."""
    session_service.invalidate_session(token)
