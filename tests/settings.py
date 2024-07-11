from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    )

from pydantic import computed_field

from os import environ

class Settings(BaseSettings):
    ENV: str = 'sandbox'
    DATABASE_READ_USER: str = 'postgres_read'
    DATABASE_READ_PASSWORD: str = 'postgres'
    DATABASE_WRITE_USER: str = 'postgres_write'
    DATABASE_WRITE_PASSWORD: str = 'postgres'
    DATABASE_POSTGRES_USER: str = 'postgres'
    DATABASE_POSTGRES_PASSWORD: str = 'postgres'
    DATABASE_HOST: str = 'localhost'
    DATABASE_PORT: int = '5432'
    DATABASE_DB: str = 'openaqdev'

    model_config = SettingsConfigDict(
        extra="ignore", env_file=f"{environ.get('DOTENV', '.env')}", env_file_encoding="utf-8"
    )

    @computed_field
    def DATABASE_READ_URL(self) -> str:
        return f"postgresql://{self.DATABASE_READ_USER}:{self.DATABASE_READ_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DB}"


settings = Settings()
