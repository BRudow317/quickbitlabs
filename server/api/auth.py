from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Annotated

import jwt
import oracledb
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel

from server.db.db import server_db, session_service
from server.configs.settings import settings
from server.core.jwt import create_access_token
from server.core.security import verify_password, get_password_hash
from server.models.user import UserBase, UserOut, User, UserCreate

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")


# ===================================================--------
# Internal helpers - raw Oracle queries against USER_CREDENTIALS
# ===================================================--------

def _get_credentials(username: str) -> dict | None:
    """Return the QBL_USERS row for *username*, or None if not found."""
    sql = """
        SELECT USERNAME, EMAIL, password_hash, IS_ACTIVE,
               NVL(role_id, 'user')
          FROM QBL_USERS
         WHERE USERNAME = :username
    """
    with server_db.connect().cursor() as cur:
        cur.execute(sql, username=username)
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "username":        row[0],
        "email":           row[1],
        "hashed_password": row[2],
        "is_active":       bool(row[3]),
        "role":            row[4] or "user",
    }


def _username_exists(username: str) -> bool:
    sql = "SELECT COUNT(*) FROM QBL_USERS WHERE USERNAME = :u"
    with server_db.connect().cursor() as cur:
        cur.execute(sql, u=username)
        return (cur.fetchone() or (0,))[0] > 0


def _email_exists(email: str) -> bool:
    sql = "SELECT COUNT(*) FROM QBL_USERS WHERE EMAIL = :e"
    with server_db.connect().cursor() as cur:
        cur.execute(sql, e=email)
        return (cur.fetchone() or (0,))[0] > 0


def _insert_user(username: str, email: str, hashed_password: str) -> None:
    """INSERT a new row into QBL_USERS."""
    sql = """
        INSERT INTO QBL_USERS
            (USERNAME, EMAIL, password_hash, IS_ACTIVE)
        VALUES
            (:username, :email, :hashed_password, 1)
    """
    with server_db.connect().cursor() as cur:
        cur.execute(sql, username=username, email=email, hashed_password=hashed_password)
    server_db.connect().commit()


# ===================================================--------
# Endpoints
# ===================================================--------

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
    refresh_token: str
    token_type: str


class RefreshRequest(BaseModel):
    refresh_token: str | None = None


class SessionInfo(BaseModel):
    session_id: int
    ip_address: str | None
    user_agent: str | None
    issued_at: str
    expires_at: str


def _set_refresh_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key="refresh_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=settings.refresh_token_expire_days * 86400,
        path="/api/auth",
    )


def _build_access_token(username: str) -> str:
    creds = _get_credentials(username)
    if not creds:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return create_access_token(
        data={
            "sub":   username,
            "email": creds["email"],
            "role":  creds["role"],
        },
        expires_delta=timedelta(minutes=settings.access_token_expire_minutes),
    )


@router.post("/login", response_model=Token, operation_id="login")
def login(
    request: Request,
    response: Response,
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

    # --- Issue tokens ------------------------------------------------------
    access_expires = timedelta(minutes=settings.access_token_expire_minutes)
    access_token = create_access_token(
        data={
            "sub":   username,
            "email": creds["email"],
            "role":  creds["role"],
        },
        expires_delta=access_expires,
    )
    refresh_token_value = secrets.token_urlsafe(32)
    refresh_expires_at = datetime.now(timezone.utc) + timedelta(days=settings.refresh_token_expire_days)

    # --- Persist session and audit -----------------------------------------
    access_expires_at = datetime.now(timezone.utc) + access_expires
    try:
        session_id = session_service.create_session(
            username=username,
            token=access_token,
            expires_at=access_expires_at,
            ip_address=ip,
            user_agent=ua,
        )
        session_service.create_refresh_token(
            username=username,
            token=refresh_token_value,
            session_id=session_id,
            expires_at=refresh_expires_at,
            ip_address=ip,
            user_agent=ua,
        )
        session_service.log_sign_in(username, success=True, ip_address=ip, user_agent=ua)
    except Exception:
        import logging
        logging.getLogger(__name__).exception("Failed to persist session/sign-in record for '%s'", username)

    _set_refresh_cookie(response, refresh_token_value)
    return Token(access_token=access_token, refresh_token=refresh_token_value, token_type="bearer")


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
        role=payload.get("role", "user"),
    )


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT, operation_id="logout")
def logout(
    response: Response,
    user: Annotated[User, Depends(get_current_user)],
    token: Annotated[str, Depends(oauth2_scheme)],
):
    """Invalidate the current session and all refresh tokens for the user."""
    session_service.invalidate_session(token)
    session_service.revoke_all_refresh_tokens(user.username)
    response.delete_cookie("refresh_token", path="/api/auth")


@router.post("/refresh", response_model=Token, operation_id="refresh_token")
def refresh_token_endpoint(body: RefreshRequest, request: Request, response: Response):
    """
    Exchange a valid refresh token for a new access token + rotated refresh token.

    The refresh token is read from the HttpOnly cookie first; the request body
    value is used as a fallback for clients that cannot use cookies.
    If the refresh token is expired but the request carries a valid JWT in the
    Authorization header, the JWT is used to re-issue both tokens (allowing
    seamless re-issuance when the refresh token expires while the JWT is still live).
    If neither is valid, returns 401.
    """
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent")
    refresh_expires_at = datetime.now(timezone.utc) + timedelta(days=settings.refresh_token_expire_days)

    raw_refresh = request.cookies.get("refresh_token") or body.refresh_token

    if raw_refresh:
        row = session_service.validate_refresh_token(raw_refresh)

        if row and row["is_active"] and row["not_expired"]:
            username: str = row["username"]
            new_refresh = secrets.token_urlsafe(32)
            session_service.rotate_refresh_token(raw_refresh, username, new_refresh, refresh_expires_at, ip, ua)
            _set_refresh_cookie(response, new_refresh)
            return Token(
                access_token=_build_access_token(username),
                refresh_token=new_refresh,
                token_type="bearer",
            )

    # Refresh token is expired or not found — require a valid JWT to re-issue.
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token expired; re-authenticate to obtain a new token.",
        )

    jwt_token = auth_header.split(" ", 1)[1]
    try:
        payload = jwt.decode(
            jwt_token,
            settings.jwt_secret.get_secret_value(),
            algorithms=[settings.jwt_algorithm],
        )
        username = payload.get("sub") or ''
        if not username:
            raise ValueError("missing sub claim")
    except (jwt.PyJWTError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token expired and the provided access token is invalid.",
        )

    new_refresh = secrets.token_urlsafe(32)
    session_service.create_refresh_token(username, new_refresh, None, refresh_expires_at, ip, ua)
    _set_refresh_cookie(response, new_refresh)
    return Token(
        access_token=_build_access_token(username),
        refresh_token=new_refresh,
        token_type="bearer",
    )


@router.get("/sessions", response_model=list[SessionInfo], operation_id="list_sessions")
def list_sessions(user: Annotated[User, Depends(get_current_user)]):
    """List all active, non-expired sessions for the current user."""
    return session_service.list_active_sessions(user.username)


@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT, operation_id="revoke_session")
def revoke_session(session_id: int, user: Annotated[User, Depends(get_current_user)]):
    """Revoke a specific session by ID. Only the owning user can revoke their own sessions."""
    revoked = session_service.revoke_session_by_id(session_id, user.username)
    if not revoked:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
