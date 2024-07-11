from pydantic import BaseSettings
from pathlib import Path
from os import environ
from requests import get


ip = get('https://api.ipify.org').content.decode('utf8')

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
    KEY_NAME: str
    DATA_VOLUME_SIZE: int = 3000
    DATABASE_MONITOR_USER: str = None
    DATABASE_MONITOR_PASSWORD: str = None
    TRANSFER_URI: str = None
    PG_SHARED_BUFFERS: str = ''
    PG_WAL_BUFFERS: str = ''
    PG_EFFECTIVE_CACHE_SIZE: str = ''
    PG_WORK_MEM: str = ''
    PG_MAINTENANCE_WORK_MEM: str = ''
    IP_ADDRESS: str = f"{ip}/32"
    PRIVATE_IP_ADDRESS: str = None
    DEV_SECURITY_GROUP: str = None
    ELASTIC_IP_ALLOCTION_ID: str = None
    SNAPSHOT_ID: str = None
    VPC_ID: str = None
    LINUX_VERSION: str = 'AMAZON_LINUX_2023'  # UBUNTU | AMAZON_LINUX_2022
    INSTANCE_TYPE: str = 't3.large'
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
