from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Mapping, Sequence

from .config import get_oracle_client_table, get_transaction_archive_count


TRANSACTION_COLUMNS = {
    "ACCT_TRANSACTION_ID",
    "ACCOUNT",
    "PATIENT_FIRSTNAME",
    "PATIENT_LASTNAME",
    "GUARANTOR_FIRSTNAME",
    "GUARANTOR_LASTNAME",
    "PATIENT_PHONE1",
    "PATIENT_PHONE2",
    "PATIENT_DOB",
    "PATIENT_EMAIL",
    "PATIENT_SSN",
    "PATIENT_FIRST_ID",
    "PATIENT_SECOND_ID",
    "PRACTICE_ID",
    "CLIENT_ID",
    "CLIENT_PRACTICE_ID",
    "PRACTICE_PHONE",
    "PRACTICE_PHONE_ALTERNATE",
    "OPERATOR_PHONE",
    "PLACE_OF_SERVICE_ID",
    "ALT_LANGUAGE_1",
    "ALT_LANGUAGE_2",
    "ALT_LANGUAGE_3",
    "LANGUAGE_CALLBACK_1",
    "LANGUAGE_CALLBACK_2",
    "LANGUAGE_CALLBACK_3",
    "PRACTICE_FAX",
    "INSURANCE_TYPE",
    "INSURANCE_COMPANY",
    "TEMPLATE_ID",
    "REQUEUE_ID",
    "LAST_PAYMENT_DATE",
    "TOTAL_AMOUNT",
    "MINIMUM_PAYMENT_AMOUNT",
    "ACCOUNT_TO_SPEAK",
    "AMOUNT_TO_SPEAK",
    "DISCOUNT_AMOUNT_TO_SPEAK",
    "DISCOUNT_PERCENTAGE_TO_SPEAK",
    "DAYS_TO_SPEAK",
    "AMOUNT_1",
    "DAYS_DUE_1",
    "AMOUNT_2",
    "DAYS_DUE_2",
    "AMOUNT_3",
    "DAYS_DUE_3",
    "AMOUNT_4",
    "DAYS_DUE_4",
    "AMOUNT_5",
    "DAYS_DUE_5",
    "AMOUNT_6",
    "DAYS_DUE_6",
    "INPUT_ID_MENU",
    "INPUT_DYNAMIC_MENU_1",
    "INPUT_DYNAMIC_MENU_2",
    "INPUT_CREDIT_CARD_NUMBER",
    "INPUT_CREDIT_CARD_EXP_DATE",
    "INPUT_APPROVAL_CODE",
    "INPUT_PAYMENT_AMOUNT",
    "INPUT_OTHER_PHONE_NUMBER",
    "INPUT_FAX_PHONE_NUMBER",
    "INPUT_SSN",
    "INPUT_DOB",
    "INPUT_FIRST_ID",
    "INPUT_SECOND_ID",
    "CONFIRM_CREDIT_CARD_NUMBER",
    "CONFIRM_CREDIT_CARD_EXP_DATE",
    "CONFIRM_APPROVAL_CODE",
    "CONFIRM_PAYMENT_AMOUNT",
    "CONFIRM_OTHER_PHONE_NUMBER",
    "CONFIRM_FAX_PHONE_NUMBER",
    "CONFIRM_FIRST_ID",
    "CONFIRM_SECOND_ID",
    "LANGUAGE",
    "EXTRA_1",
    "EXTRA_2",
    "EXTRA_3",
    "EXTRA_4",
    "EXTRA_5",
    "EXTRA_6",
    "EXTRA_7",
    "EXTRA_8",
    "EXTRA_9",
    "EXTRA_10",
    "EXTRA_11",
    "EXTRA_12",
    "EXTRA_13",
    "EXTRA_14",
    "EXTRA_15",
    "EXTRA_16",
    "EXTRA_17",
    "EXTRA_18",
    "EXTRA_19",
    "EXTRA_20",
    "LIVE_PERSON",
    "MACHINE",
    "NOT_AVAILABLE",
    "LEAD_1",
    "LEAD_2",
    "LEAD_3",
    "LEAD_4",
    "LEAD_5",
    "LEAD_6",
    "LEAD_CREDIT_CARD_NUMBER",
    "LEAD_OTHER_PHONE_NUMBER",
    "LEAD_OTHER_FAX_NUMBER",
    "NO_INPUT",
    "QUEUED",
    "B_ACTIVE",
    "TRANSACTION_UPDATE",
    "ATTEMPT",
    "CALL_START_TIME",
    "CALL_CONNECT_TIME",
    "CALL_FINISH_TIME",
    "CALL_DURATION",
    "OUTCOME",
    "RESULT1",
    "RESULT2",
    "TFH_RESULT",
    "PHONE_DIALED",
    "PHONE_UPDATE",
    "TRANSFER_CONNECT_TIME",
    "TRANSFER_DURATION",
    "DATE_MODIFIED",
    "BILLING",
    "SESSION_ID",
    "CAMPAIGN_ID",
    "LVTRANSACTION_TYPE",
    "ORIGINAL_ACCOUNT_NUMBER",
    "CHAT_RATING",
    "LVTRANSACTION_SUBTYPE",
    "ASSIGNED_THREAD_OWNER_AGENT_ID",
    "ACTIVE_THREAD",
}


@dataclass
class OracleQuery:
    sql: str
    params: dict


@dataclass
class PostgresQuery:
    sql: str
    params: list


def _normalize_identifier(name: str) -> str:
    return name.strip().upper()


def _convert_to_date(value: object) -> object:
    """Convert string date values to Python date/datetime objects for Oracle."""
    if isinstance(value, (date, datetime)):
        return value
    if isinstance(value, str):
        # Try common date formats
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y", "%d-%b-%y"):
            try:
                parsed = datetime.strptime(value, fmt)
                # Return date if no time component, otherwise datetime
                if fmt == "%Y-%m-%d" or fmt == "%d-%b-%Y" or fmt == "%d-%b-%y":
                    return parsed.date()
                return parsed
            except ValueError:
                continue
    return value


def _validate_columns(columns: Iterable[str] | None, table: str = "transaction") -> list[str]:
    if not columns:
        return ["*"]
    
    if table == "contact":
        from .query import CONTACT_COLUMNS as CONTACT_TABLE_COLUMNS
        allowed_columns = {c.upper() for c in CONTACT_TABLE_COLUMNS}
    else:
        allowed_columns = TRANSACTION_COLUMNS

    normalized = [_normalize_identifier(col) for col in columns]
    invalid = [col for col in normalized if col not in allowed_columns]
    if invalid:
        raise ValueError(f"Invalid columns for {table}: {invalid}")
    return normalized


def _validate_filters(filters: Mapping[str, object] | None, table: str = "transaction") -> tuple[str, dict]:
    if not filters:
        return "", {}
    
    if table == "contact":
        from .query import CONTACT_COLUMNS as CONTACT_TABLE_COLUMNS
        allowed_columns = {c.upper() for c in CONTACT_TABLE_COLUMNS}
    else:
        allowed_columns = TRANSACTION_COLUMNS

    clauses = []
    params: dict = {}
    counter = 1
    for key, value in filters.items():
        column = _normalize_identifier(key)
        if column not in allowed_columns:
            raise ValueError(f"Invalid filter column for {table}: {column}")

        op = "="
        operand = value
        if isinstance(value, (list, tuple)) and len(value) == 2:
            op = str(value[0]).lower()
            operand = value[1]
        elif isinstance(value, Mapping) and "op" in value:
            op = str(value["op"]).lower()
            operand = value.get("value")

        def next_param() -> str:
            nonlocal counter
            name = f"p{counter}"
            counter += 1
            return name

        if op in {"eq", "="}:
            param = next_param()
            clauses.append(f"{column} = :{param}")
            params[param] = _convert_to_date(operand)
        elif op in {"neq", "!=", "<>"}:
            param = next_param()
            clauses.append(f"{column} <> :{param}")
            params[param] = _convert_to_date(operand)
        elif op in {"gt", ">"}:
            param = next_param()
            clauses.append(f"{column} > :{param}")
            params[param] = _convert_to_date(operand)
        elif op in {"gte", ">="}:
            param = next_param()
            clauses.append(f"{column} >= :{param}")
            params[param] = _convert_to_date(operand)
        elif op in {"lt", "<"}:
            param = next_param()
            clauses.append(f"{column} < :{param}")
            params[param] = _convert_to_date(operand)
        elif op in {"lte", "<="}:
            param = next_param()
            clauses.append(f"{column} <= :{param}")
            params[param] = _convert_to_date(operand)
        elif op == "like":
            param = next_param()
            clauses.append(f"{column} LIKE :{param}")
            params[param] = operand
        elif op == "not_like":
            param = next_param()
            clauses.append(f"{column} NOT LIKE :{param}")
            params[param] = operand
        elif op == "ilike":
            param = next_param()
            clauses.append(f"LOWER({column}) LIKE LOWER(:{param})")
            params[param] = operand
        elif op == "not_ilike":
            param = next_param()
            clauses.append(f"LOWER({column}) NOT LIKE LOWER(:{param})")
            params[param] = operand
        elif op in {"in", "not_in"}:
            if not isinstance(operand, (list, tuple, set)) or not operand:
                raise ValueError(f"{op} operator requires a non-empty list")
            bind_names = []
            for item in operand:
                param = next_param()
                params[param] = _convert_to_date(item)
                bind_names.append(f":{param}")
            comparator = "IN" if op == "in" else "NOT IN"
            clauses.append(f"{column} {comparator} ({', '.join(bind_names)})")
        elif op == "between":
            if not isinstance(operand, (list, tuple)) or len(operand) != 2:
                raise ValueError("between operator requires a list of two values")
            start_param = next_param()
            end_param = next_param()
            params[start_param] = _convert_to_date(operand[0])
            params[end_param] = _convert_to_date(operand[1])
            clauses.append(f"{column} BETWEEN :{start_param} AND :{end_param}")
        elif op == "is_null":
            clauses.append(f"{column} IS NULL")
        elif op == "is_not_null":
            clauses.append(f"{column} IS NOT NULL")
        else:
            raise ValueError(f"Unsupported filter operator: {op}")

    return " WHERE " + " AND ".join(clauses), params


def _validate_order_by(order_by: str | None) -> str:
    if not order_by:
        return ""
    parts = order_by.split()
    column = _normalize_identifier(parts[0])
    direction = parts[1].upper() if len(parts) > 1 else "ASC"
    if column not in TRANSACTION_COLUMNS:
        raise ValueError(f"Invalid order_by column for transaction: {column}")
    if direction not in {"ASC", "DESC"}:
        raise ValueError("order_by direction must be ASC or DESC")
    return f" ORDER BY {column} {direction}"


def build_transaction_union(
    table_names: Sequence[str],
    filters: Mapping[str, object] | None = None,
    columns: Sequence[str] | None = None,
    limit: int = 100,
    order_by: str | None = None,
) -> OracleQuery:
    if not table_names:
        raise ValueError("At least one transaction table must be provided")
    limit = max(1, min(int(limit), 1000))

    select_cols = _validate_columns(columns)
    where_sql, params = _validate_filters(filters)
    order_sql = _validate_order_by(order_by)

    columns_sql = "*" if select_cols == ["*"] else ", ".join(select_cols)
    parts = [f"SELECT {columns_sql} FROM {name}{where_sql}" for name in table_names]
    union_sql = " UNION ALL ".join(parts)

    sql = (
        "SELECT * FROM ("
        + union_sql
        + ")"
        + order_sql
        + " FETCH FIRST :limit ROWS ONLY"
    )
    params = {**params, "limit": limit}
    return OracleQuery(sql=sql, params=params)


def _previous_months(today: date, count: int) -> list[tuple[int, int]]:
    year = today.year
    month = today.month
    months = []
    for _ in range(count):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        months.append((month, year))
    return months


def resolve_transaction_tables(
    dialing_db: str,
    reporting_db: str | None,
    today: date | None = None,
) -> list[str]:
    if not dialing_db:
        raise ValueError("dialing_db is required")
    table_names = [f"LVOUSR.TRANSACTION@{dialing_db}"]

    archive_count = get_transaction_archive_count()
    if reporting_db and archive_count > 0:
        ref_date = today or date.today()
        for month, year in _previous_months(ref_date, archive_count):
            suffix = f"{month:02d}{year % 100:02d}"
            table_names.append(f"LVOUSR.TRANSACTION_{suffix}@{reporting_db}")

    return table_names


def get_client_db_links_query(client_id: int) -> tuple[str, dict]:
    client_table = get_oracle_client_table()
    sql = f"SELECT dialing_db, reporting_db FROM {client_table} WHERE client_id = :client_id"
    return sql, {"client_id": client_id}


def get_skills_for_client_query(client_id: int) -> tuple[str, dict]:
    """Get all skill IDs associated with a client.
    
    The skillxclient table links clients to skills. The CLIENT_ID column in the
    transaction table is actually the skill_id (legacy naming).
    
    Args:
        client_id: The actual client ID
        
    Returns:
        Tuple of (SQL string, parameters dict)
    """
    sql = "SELECT skill_id FROM lvousr.skillxclient WHERE client_id = :client_id"
    return sql, {"client_id": client_id}


def get_client_for_skill_query(skill_id: int) -> tuple[str, dict]:
    """Get the client ID for a given skill.
    
    Args:
        skill_id: The skill ID (which appears as CLIENT_ID in transactions)
        
    Returns:
        Tuple of (SQL string, parameters dict)
    """
    sql = "SELECT client_id FROM lvousr.skill WHERE skill_id = :skill_id"
    return sql, {"skill_id": skill_id}


def build_oracle_contact_select(
    dialing_db: str,
    filters: Mapping[str, object] | None = None,
    columns: Sequence[str] | None = None,
    limit: int = 100,
    order_by: str | None = None,
) -> OracleQuery:
    """Builds a select query for the contact table on an Oracle database."""
    if not dialing_db:
        raise ValueError("dialing_db is required")
    limit = max(1, min(int(limit), 1000))

    select_cols = _validate_columns(columns, table="contact")
    where_sql, params = _validate_filters(filters, table="contact")
    order_sql = _validate_order_by(order_by)

    columns_sql = "*" if select_cols == ["*"] else ", ".join(select_cols)
    table_name = f"lvousr.contact@{dialing_db}"

    sql = (
        f"SELECT {columns_sql} FROM {table_name}{where_sql}{order_sql}"
        f" FETCH FIRST :limit ROWS ONLY"
    )
    params = {**params, "limit": limit}
    return OracleQuery(sql=sql, params=params)


def _validate_filters_postgres(filters: Mapping[str, object] | None, table: str = "transaction") -> tuple[str, list]:
    """Validate filters and build WHERE clause for PostgreSQL (uses %s placeholders)."""
    if not filters:
        return "", []
    
    if table == "contact":
        from .query import CONTACT_COLUMNS as CONTACT_TABLE_COLUMNS
        allowed_columns = {c.lower() for c in CONTACT_TABLE_COLUMNS}
    else:
        allowed_columns = {c.lower() for c in TRANSACTION_COLUMNS}

    clauses = []
    params: list = []
    for key, value in filters.items():
        column = key.strip().lower()
        if column not in allowed_columns:
            raise ValueError(f"Invalid filter column for {table}: {column}")

        op = "="
        operand = value
        if isinstance(value, (list, tuple)) and len(value) == 2:
            op = str(value[0]).lower()
            operand = value[1]
        elif isinstance(value, Mapping) and "op" in value:
            op = str(value["op"]).lower()
            operand = value.get("value")

        if op in {"eq", "="}:
            clauses.append(f"{column} = %s")
            params.append(operand)
        elif op in {"neq", "!=", "<>"}:
            clauses.append(f"{column} <> %s")
            params.append(operand)
        elif op in {"gt", ">"}:
            clauses.append(f"{column} > %s")
            params.append(operand)
        elif op in {"gte", ">="}:
            clauses.append(f"{column} >= %s")
            params.append(operand)
        elif op in {"lt", "<"}:
            clauses.append(f"{column} < %s")
            params.append(operand)
        elif op in {"lte", "<="}:
            clauses.append(f"{column} <= %s")
            params.append(operand)
        elif op == "like":
            clauses.append(f"{column} LIKE %s")
            params.append(operand)
        elif op == "ilike":
            clauses.append(f"{column} ILIKE %s")
            params.append(operand)
        elif op in {"in", "not_in"}:
            if not isinstance(operand, (list, tuple, set)) or not operand:
                raise ValueError(f"{op} operator requires a non-empty list")
            placeholders = ", ".join(["%s"] * len(operand))
            comparator = "IN" if op == "in" else "NOT IN"
            clauses.append(f"{column} {comparator} ({placeholders})")
            params.extend(list(operand))
        elif op == "between":
            if not isinstance(operand, (list, tuple)) or len(operand) != 2:
                raise ValueError("between operator requires a list of two values")
            clauses.append(f"{column} BETWEEN %s AND %s")
            params.extend([operand[0], operand[1]])
        elif op == "is_null":
            clauses.append(f"{column} IS NULL")
        elif op == "is_not_null":
            clauses.append(f"{column} IS NOT NULL")
        else:
            raise ValueError(f"Unsupported filter operator: {op}")

    return " WHERE " + " AND ".join(clauses), params


def _validate_order_by_postgres(order_by: str | None, table: str = "transaction") -> str:
    """Validate ORDER BY clause for PostgreSQL."""
    if not order_by:
        return ""
    parts = order_by.split()
    column = parts[0].lower()
    direction = parts[1].upper() if len(parts) > 1 else "ASC"
    
    if table == "contact":
        from .query import CONTACT_COLUMNS as CONTACT_TABLE_COLUMNS
        allowed_columns = {c.lower() for c in CONTACT_TABLE_COLUMNS}
    else:
        allowed_columns = {c.lower() for c in TRANSACTION_COLUMNS}
    
    if column not in allowed_columns:
        raise ValueError(f"Invalid order_by column for {table}: {column}")
    if direction not in {"ASC", "DESC"}:
        raise ValueError("order_by direction must be ASC or DESC")
    return f" ORDER BY {column} {direction}"


def build_postgres_transaction_union(
    table_names: Sequence[str],
    filters: Mapping[str, object] | None = None,
    columns: Sequence[str] | None = None,
    limit: int = 100,
    order_by: str | None = None,
) -> PostgresQuery:
    """Build a UNION ALL query for PostgreSQL transaction tables."""
    if not table_names:
        raise ValueError("At least one transaction table must be provided")
    limit = max(1, min(int(limit), 1000))

    # For PostgreSQL, use lowercase column names
    if columns:
        select_cols = [c.lower() for c in columns]
    else:
        select_cols = ["*"]
    
    where_sql, params = _validate_filters_postgres(filters, table="transaction")
    order_sql = _validate_order_by_postgres(order_by, table="transaction")

    columns_sql = "*" if select_cols == ["*"] else ", ".join(select_cols)
    
    # For PostgreSQL, table names should be lowercase without the @dblink suffix
    pg_table_names = []
    for name in table_names:
        base_name = name.split("@")[0].lower()
        pg_table_names.append(base_name)
    
    # Build UNION ALL with repeated params for each table
    all_params: list = []
    parts = []
    for table in pg_table_names:
        parts.append(f"SELECT {columns_sql} FROM {table}{where_sql}")
        all_params.extend(params)
    
    union_sql = " UNION ALL ".join(parts)

    sql = (
        "SELECT * FROM ("
        + union_sql
        + ") AS combined"
        + order_sql
        + " LIMIT %s"
    )
    all_params.append(limit)
    return PostgresQuery(sql=sql, params=all_params)


def build_postgres_contact_select(
    filters: Mapping[str, object] | None = None,
    columns: Sequence[str] | None = None,
    limit: int = 100,
    order_by: str | None = None,
) -> PostgresQuery:
    """Build a SELECT query for the PostgreSQL contact table."""
    limit = max(1, min(int(limit), 1000))

    # For PostgreSQL, use lowercase column names
    if columns:
        select_cols = [c.lower() for c in columns]
    else:
        select_cols = ["*"]
    
    where_sql, params = _validate_filters_postgres(filters, table="contact")
    order_sql = _validate_order_by_postgres(order_by, table="contact")

    columns_sql = "*" if select_cols == ["*"] else ", ".join(select_cols)
    table_name = "lvousr.contact"

    sql = f"SELECT {columns_sql} FROM {table_name}{where_sql}{order_sql} LIMIT %s"
    params.append(limit)
    return PostgresQuery(sql=sql, params=params)
