from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "server"

    # Loaded from env - master process injects these from .env before startup
    JWT_SECRET: str = "CHANGE_ME_FOR_PRODUCTION"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    DEBUG: bool = False
    CORS_ORIGINS: list[str] = ["http://localhost:5173"]

    # ORACLE_HOST: str = ""
    # ORACLE_PORT: int = 1521
    # ORACLE_SERVICE: str = ""

    # base64-encoded 32-byte AES key for encrypting uploaded files at rest.
    # Leave empty to store uploads without encryption.
    UPLOAD_ENCRYPTION_KEY: str = ""

    # Login rate limiting — sliding window applied per username AND per IP
    LOGIN_RATE_LIMIT_WINDOW_MINUTES: int = 15
    LOGIN_RATE_LIMIT_MAX_FAILURES: int = 5

settings = Settings()