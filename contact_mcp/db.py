from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from .config import get_db_config


def get_connection():
    config = get_db_config()
    missing = [key for key, value in config.items() if value is None and key != "sslmode"]
    if missing:
        raise RuntimeError(f"Missing DB config: {missing}")
    return psycopg.connect(row_factory=dict_row, **config)
