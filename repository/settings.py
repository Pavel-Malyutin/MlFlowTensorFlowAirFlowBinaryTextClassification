import os
from functools import lru_cache

from pydantic import BaseSettings, PostgresDsn


class Settings(BaseSettings):
    asyncpg_url: PostgresDsn = PostgresDsn.build(
        scheme="postgresql+asyncpg",
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        host=os.getenv("PG_URL", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        path=f"/{os.getenv('DATABASE_NAME', 'airflow')}",
    )


@lru_cache
def get_settings():
    return Settings()
