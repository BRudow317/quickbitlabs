from datetime import datetime, timedelta, timezone
from typing import Any
import jwt
from server.configs.settings import settings

def create_access_token(data: dict[str, Any], expires_delta: timedelta | None = None) -> str:
    to_encode: dict[str, Any] = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    
    to_encode.update({"exp": expire})
    encoded_jwt: str = jwt.encode(
        to_encode, 
        settings.jwt_secret.get_secret_value(), 
        algorithm=settings.algorithm
        )
    return encoded_jwt