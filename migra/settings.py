from typing import Optional
from pydantic import BaseSettings, validator
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    DATABASE_WRITE_USER: str
    DATABASE_WRITE_PASSWORD: str
    DATABASE_DB: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    DATABASE_WRITE_URL: Optional[str]
    REMOTE_DATABASE_URL: str

    @validator('DATABASE_WRITE_URL', allow_reuse=True)
    def get_write_url(cls, v, values):
        return v or f"postgresql://{values['DATABASE_WRITE_USER']}:{values['DATABASE_WRITE_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"

    class Config:
        parent = Path(__file__).resolve().parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        elif 'ENV' in environ:
            env_file = Path.joinpath(parent, f".env.{environ['ENV']}")
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
