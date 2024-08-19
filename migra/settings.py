from typing import Optional

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pathlib import Path
from os import environ

def get_env():
    parent = Path(__file__).resolve().parent.parent
    env_file = Path.joinpath(parent, environ.get("DOTENV", ".env"))
    return env_file


class Settings(BaseSettings):
    DATABASE_WRITE_USER: str
    DATABASE_WRITE_PASSWORD: str
    DATABASE_DB: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    REMOTE_DATABASE_URL: str

    @computed_field(return_type=str, alias="DATABASE_WRITE_URL")
    @property
    def DATABASE_WRITE_URL(self):
        return f"postgresql://{self.DATABASE_WRITE_USER}:{self.DATABASE_WRITE_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DB}"

    model_config = SettingsConfigDict(extra="ignore", env_file=get_env())


settings = Settings()
