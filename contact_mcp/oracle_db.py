from __future__ import annotations

import oracledb

from .config import get_oracle_config


def _build_dsn(config: dict) -> str:
    if config.get("dsn"):
        return str(config["dsn"])
    host = config.get("host")
    service = config.get("service")
    port = config.get("port", 1521)
    if not host or not service:
        raise RuntimeError("Missing Oracle config: ORACLE_HOST/ORACLE_SERVICE or ORACLE_DSN")
    return oracledb.makedsn(host, port, service_name=service)


def get_oracle_connection():
    config = get_oracle_config()
    user = config.get("user")
    password = config.get("password")
    if not user or not password:
        raise RuntimeError("Missing Oracle config: ORACLE_USER/ORACLE_PASSWORD")
    dsn = _build_dsn(config)
    return oracledb.connect(user=user, password=password, dsn=dsn)


def fetch_all_dicts(cursor) -> list[dict]:
    columns = [col[0].lower() for col in cursor.description or []]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]
