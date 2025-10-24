from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pathlib import Path

class Settings(BaseSettings):
    DATA_DIR: Path = Field(default=Path("data/"))
    RECORDER_REGISTRY_DIR: Path = Field(default=Path("registry/"))
    LOG_DIR: Path = Field(default=Path("logs/"))
    ENV: str = Field(default="dev")
    TIMEZONE: str = Field(default="Europe/Istanbul")

model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

settings = Settings()
# print(settings.model_dump_json())

"""
You can then use this in other modules like:

from src.config import settings

data_path = settings.DATA_DIR / session_name
data_path.mkdir(parents=True, exist_ok=True)

parquet_file = data_path / f"{session_name}_{timestamp}.parquet
"""