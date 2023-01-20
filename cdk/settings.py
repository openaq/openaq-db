from pydantic import BaseSettings
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    ENV: str
    DATABASE_READ_USER: str
    DATABASE_READ_PASSWORD: str
    DATABASE_WRITE_USER: str
    DATABASE_WRITE_PASSWORD: str
    DATABASE_POSTGRES_PASSWORD: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    DATABASE_DB: str
    KEY_NAME: str
    DATA_VOLUME_SIZE: int = 3000
    DATABASE_MONITOR_USER: str = None
    DATABASE_MONITOR_PASSWORD: str = None
    PG_SHARED_BUFFERS: str = ''
    PG_WAL_BUFFERS: str = ''
    PG_EFFECTIVE_CACHE_SIZE: str = ''
    PG_WORK_MEM: str = ''
    PG_MAINTENANCE_WORK_MEM: str = ''
    IP_ADDRESS: str = None
    ELASTIC_IP_ALLOCTION_ID: str = None
    SNAPSHOT_ID: str = None
    VPC_ID: str = None
    LINUX_VERSION: str = 'AMAZON_LINUX_2'  # UBUNTU | AMAZON_LINUX_2022
    INSTANCE_TYPE: str = None
    MACHINE_IMAGE_NAME: str = None

    class Config:
        parent = Path(__file__).resolve().parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        elif 'ENV' in environ:
            env_file = Path.joinpath(parent, f".env.{environ['ENV']}")
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
