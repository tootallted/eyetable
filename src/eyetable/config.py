"""Runtime settings, read from the environment (and a local .env file)."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

DEFAULT_DATABASE_URL = "postgresql+psycopg://eyetable:eyetable@localhost:5432/eyetable"


@dataclass(frozen=True)
class Settings:
    database_url: str
    meta_schema: str
    data_schema: str


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        database_url=os.environ.get("EYETABLE_DATABASE_URL", DEFAULT_DATABASE_URL),
        meta_schema=os.environ.get("EYETABLE_META_SCHEMA", "eyetable"),
        data_schema=os.environ.get("EYETABLE_DATA_SCHEMA", "eyetable_raw"),
    )
