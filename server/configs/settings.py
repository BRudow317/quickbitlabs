# from pydantic_settings import BaseSettings, SettingsConfigDict

# class Settings(BaseSettings):
#     PROJECT_NAME: str = "PyScriptStack"
#     DATABASE_URL: str = "sqlite:///./database/pyscript.db"
#     JWT_SECRET: str = "CHANGE_ME_FOR_PRODUCTION"
#     ALGORITHM: str = "HS256"
#     ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
#     DEBUG: bool = False
#     CORS_ORIGINS: list[str] = ["http://localhost:5173"]

#     model_config = SettingsConfigDict(env_file=".env")

# settings = Settings()