import os


def get_db_config() -> dict:
    return {
        "host": os.getenv("PGHOST"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "sslmode": os.getenv("PGSSLMODE", "require"),
    }


def get_oracle_config() -> dict:
    return {
        "dsn": os.getenv("ORACLE_DSN"),
        "host": os.getenv("ORACLE_HOST"),
        "port": int(os.getenv("ORACLE_PORT", "1521")),
        "service": os.getenv("ORACLE_SERVICE"),
        "user": os.getenv("ORACLE_USER"),
        "password": os.getenv("ORACLE_PASSWORD"),
    }


def get_oracle_client_table() -> str:
    return os.getenv("ORACLE_CLIENT_TABLE", "lvousr.client")


def get_transaction_archive_count() -> int:
    raw = os.getenv("TX_ARCHIVE_COUNT", "2")
    try:
        count = int(raw)
    except ValueError:
        count = 2
    return max(0, min(2, count))


def get_db_type(db_link: str) -> str:
    """
    Determine the database type (oracle or postgres) based on the database link name.
    
    Naming convention:
    - lv*stg4a -> Oracle
    - lv*stg4b -> PostgreSQL
    """
    if not db_link:
        return "oracle"  # default
    
    link_lower = db_link.lower()
    
    # Check for postgres patterns
    if "stg4b" in link_lower:
        return "postgres"
    
    # Default to oracle (includes stg4a and others)
    return "oracle"
