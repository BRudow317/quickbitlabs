from __future__ import annotations
from typing import Any, ClassVar, Mapping, Literal
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict, PydanticBaseSettingsSource, PyprojectTomlConfigSettingsSource
from pydantic_settings.sources import DotenvType, PathType

class Settings(BaseSettings):
    
    ## SettingsConfigDict fields
    """
        case_sensitive: bool
        nested_model_default_partial_update: bool | None
        env_prefix: str
        env_prefix_target: EnvPrefixTarget
        env_file: DotenvType | None
        env_file_encoding: str | None
        env_ignore_empty: bool
        env_nested_delimiter: str | None
        env_nested_max_split: int | None
        env_parse_none_str: str | None
        env_parse_enums: bool | None
        cli_prog_name: str | None
        cli_parse_args: bool | list[str] | tuple[str, ...] | None
        cli_parse_none_str: str | None
        cli_hide_none_type: bool
        cli_avoid_json: bool
        cli_enforce_required: bool
        cli_use_class_docs_for_groups: bool
        cli_exit_on_error: bool
        cli_prefix: str
        cli_flag_prefix_char: str
        cli_implicit_flags: bool | Literal['dual', 'toggle'] | None
        cli_ignore_unknown_args: bool | None
        cli_kebab_case: bool | Literal['all', 'no_enums'] | None
        cli_shortcuts: Mapping[str, str | list[str]] | None
        secrets_dir: PathType | None
        json_file: PathType | None
        json_file_encoding: str | None
        yaml_file: PathType | None
        yaml_file_encoding: str | None
        yaml_config_section: str | None
        pyproject_toml_depth: int
        pyproject_toml_table_header: tuple[str, ...]
        toml_file: PathType | None
        enable_decoding: bool
    """
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        # env_file=".env",
        pyproject_toml_table_header=("tool", "quickbitlabs"),
        extra="ignore",
        case_sensitive=False,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            PyprojectTomlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )

    project_name: str = "quickbitlabs"
    jwt_secret: SecretStr # In development, main.py generates an ephemeral 256-bit key at startup if this is unset.
    jwt_algorithm: str = "HS256"
    upload_encryption_key: SecretStr # base64-encoded 32-byte AES key for encrypting uploaded files at rest.
    py_project_root: str # app root directory
    frontend: str = './frontend/dist'
    access_token_expire_minutes: int = 30
    log_level: str = "INFO"
    cors_origins: list[str] = ["http://localhost:5173"]
    allow_credentials: bool = True
    allow_methods: list[str] = ["*"]
    allow_headers: list[str] = ["*"]
    login_rate_limit_window_minutes: int = 10
    login_rate_limit_max_failures: int = 10
    refresh_token_expire_days: int = 7

settings = Settings() # type: ignore[call-arg]