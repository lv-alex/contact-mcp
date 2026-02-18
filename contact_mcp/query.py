from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


CONTACT_COLUMNS = {
    "lvaccount_id",
    "client_id",
    "account",
    "b_active",
    "account_to_speak",
    "first_name",
    "last_name",
    "dob",
    "email_address",
    "ssn",
    "phone1",
    "phone2",
    "phone3",
    "phone4",
    "phone5",
    "phone6",
    "phone7",
    "phone8",
    "phone9",
    "phone10",
    "address1",
    "address2",
    "city",
    "state",
    "postalcode",
    "country_id",
    "guarantor_firstname",
    "guarantor_lastname",
    "paymentbalance",
    "amount_to_speak",
    "account_due_date",
    "callattemptstoday",
    "callattemptslifetime",
    "createdate",
    "createuser",
    "modifydate",
    "modifyuser",
    "initial_load_date",
    "initial_load_campaignid",
    "last_load_date",
    "last_load_campaignid",
    "do_not_dial",
    "do_not_dial_daily",
    "group_id",
    "original_account_number",
    "phone1_attempts_today",
    "phone1_attempts_lifetime",
    "phone1_dnd",
    "phone1_dnd_daily",
    "phone2_attempts_today",
    "phone2_attempts_lifetime",
    "phone2_dnd",
    "phone2_dnd_daily",
    "phone3_attempts_today",
    "phone3_attempts_lifetime",
    "phone3_dnd",
    "phone3_dnd_daily",
    "phone4_attempts_today",
    "phone4_attempts_lifetime",
    "phone4_dnd",
    "phone4_dnd_daily",
    "phone5_attempts_today",
    "phone5_attempts_lifetime",
    "phone5_dnd",
    "phone5_dnd_daily",
    "phone6_attempts_today",
    "phone6_attempts_lifetime",
    "phone6_dnd",
    "phone6_dnd_daily",
    "phone7_attempts_today",
    "phone7_attempts_lifetime",
    "phone7_dnd",
    "phone7_dnd_daily",
    "phone8_attempts_today",
    "phone8_attempts_lifetime",
    "phone8_dnd",
    "phone8_dnd_daily",
    "phone9_attempts_today",
    "phone9_attempts_lifetime",
    "phone9_dnd",
    "phone9_dnd_daily",
    "phone10_attempts_today",
    "phone10_attempts_lifetime",
    "phone10_dnd",
    "phone10_dnd_daily",
    "sms",
    "email",
    "phone1_sms_consent",
    "phone1_cell_consent",
    "phone2_sms_consent",
    "phone2_cell_consent",
    "phone3_sms_consent",
    "phone3_cell_consent",
    "phone4_sms_consent",
    "phone4_cell_consent",
    "phone5_sms_consent",
    "phone5_cell_consent",
    "phone6_sms_consent",
    "phone6_cell_consent",
    "phone7_sms_consent",
    "phone7_cell_consent",
    "phone8_sms_consent",
    "phone8_cell_consent",
    "phone9_sms_consent",
    "phone9_cell_consent",
    "phone10_sms_consent",
    "phone10_cell_consent",
    "primary_email_consent",
    "agent_id",
    "agent_team_id",
    "description",
    "department",
    "salutation",
    "title",
    "happiness_index",
    "happiness_trend",
    "happiness_ndx_updated",
}

CONTACT_DETAILS_BASE = {"lvaccount_id", "client_id", "account"}
CONTACT_DETAILS_COLUMNS = CONTACT_DETAILS_BASE | {f"col{i}" for i in range(1, 101)}

TABLES = {
    "contact": ("lvousr.contact", CONTACT_COLUMNS),
    "contact_details": ("lvousr.contact_details", CONTACT_DETAILS_COLUMNS),
}


@dataclass
class BuiltQuery:
    sql: str
    params: list


def _validate_columns(table: str, columns: Iterable[str] | None) -> list[str]:
    _, allowed = TABLES[table]
    if not columns:
        return ["*"]
    normalized = [col.strip() for col in columns]
    invalid = [col for col in normalized if col not in allowed]
    if invalid:
        raise ValueError(f"Invalid columns for {table}: {invalid}")
    return normalized


def _validate_filters(
    table: str,
    filters: Mapping[str, object] | None,
    table_alias: str | None = None,
) -> tuple[str, list]:
    if not filters:
        return "", []
    _, allowed = TABLES[table]
    clauses = []
    params: list = []
    for key, value in filters.items():
        if key not in allowed:
            raise ValueError(f"Invalid filter column for {table}: {key}")
        column = f"{table_alias}.{key}" if table_alias else key

        op = "eq"
        operand = value
        if isinstance(value, Mapping) and "op" in value:
            op = str(value["op"]).lower()
            operand = value.get("value")

        if op in {"eq", "="}:
            clauses.append(f"{column} = %s")
            params.append(operand)
        elif op in {"neq", "!="}:
            clauses.append(f"{column} <> %s")
            params.append(operand)
        elif op == "gt":
            clauses.append(f"{column} > %s")
            params.append(operand)
        elif op == "gte":
            clauses.append(f"{column} >= %s")
            params.append(operand)
        elif op == "lt":
            clauses.append(f"{column} < %s")
            params.append(operand)
        elif op == "lte":
            clauses.append(f"{column} <= %s")
            params.append(operand)
        elif op == "like":
            clauses.append(f"{column} LIKE %s")
            params.append(operand)
        elif op == "not_like":
            clauses.append(f"{column} NOT LIKE %s")
            params.append(operand)
        elif op == "ilike":
            clauses.append(f"{column} ILIKE %s")
            params.append(operand)
        elif op == "not_ilike":
            clauses.append(f"{column} NOT ILIKE %s")
            params.append(operand)
        elif op in {"in", "not_in"}:
            if not isinstance(operand, (list, tuple, set)) or not operand:
                raise ValueError(f"{op} operator requires a non-empty list")
            placeholders = ", ".join(["%s"] * len(operand))
            comparator = "IN" if op == "in" else "NOT IN"
            clauses.append(f"{column} {comparator} ({placeholders})")
            params.extend(list(operand))
        elif op == "between":
            if (
                not isinstance(operand, (list, tuple))
                or len(operand) != 2
            ):
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


def _validate_order_by(table: str, order_by: str | None) -> str:
    if not order_by:
        return ""
    parts = order_by.split()
    column = parts[0]
    direction = parts[1].upper() if len(parts) > 1 else "ASC"
    _, allowed = TABLES[table]
    if column not in allowed:
        raise ValueError(f"Invalid order_by column for {table}: {column}")
    if direction not in {"ASC", "DESC"}:
        raise ValueError("order_by direction must be ASC or DESC")
    return f" ORDER BY {column} {direction}"


def build_select(
    table: str,
    filters: Mapping[str, object] | None = None,
    columns: Sequence[str] | None = None,
    limit: int = 100,
    offset: int = 0,
    order_by: str | None = None,
) -> BuiltQuery:
    if table not in TABLES:
        raise ValueError(f"Invalid table: {table}")

    limit = max(1, min(int(limit), 1000))
    offset = max(0, int(offset))

    table_name, _ = TABLES[table]
    select_cols = _validate_columns(table, columns)
    where_sql, params = _validate_filters(table, filters)
    order_sql = _validate_order_by(table, order_by)

    sql = f"SELECT {', '.join(select_cols)} FROM {table_name}{where_sql}{order_sql} LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    return BuiltQuery(sql=sql, params=params)


def build_count(
    table: str,
    filters: Mapping[str, object] | None = None,
) -> BuiltQuery:
    if table not in TABLES:
        raise ValueError(f"Invalid table: {table}")

    table_name, _ = TABLES[table]
    where_sql, params = _validate_filters(table, filters)
    sql = f"SELECT COUNT(*) AS count FROM {table_name}{where_sql}"
    return BuiltQuery(sql=sql, params=params)


def build_contact_with_details_select(
    contact_filters: Mapping[str, object] | None = None,
    details_filters: Mapping[str, object] | None = None,
    contact_columns: Sequence[str] | None = None,
    details_columns: Sequence[str] | None = None,
    limit: int = 100,
    offset: int = 0,
    order_by: str | None = None,
) -> BuiltQuery:
    limit = max(1, min(int(limit), 1000))
    offset = max(0, int(offset))

    contact_cols = _validate_columns("contact", contact_columns)
    details_cols = _validate_columns("contact_details", details_columns)
    if contact_cols == ["*"]:
        contact_cols = sorted(CONTACT_COLUMNS)
    if details_cols == ["*"]:
        details_cols = sorted(CONTACT_DETAILS_COLUMNS)

    contact_select = [f"c.{col} AS contact_{col}" for col in contact_cols]
    details_select = [f"d.{col} AS details_{col}" for col in details_cols]

    where_contact_sql, contact_params = _validate_filters(
        "contact", contact_filters, table_alias="c"
    )
    where_details_sql, details_params = _validate_filters(
        "contact_details", details_filters, table_alias="d"
    )

    where_sql = ""
    params: list = []
    if where_contact_sql and where_details_sql:
        where_sql = where_contact_sql + " AND " + where_details_sql.lstrip(" WHERE ")
    elif where_contact_sql:
        where_sql = where_contact_sql
    elif where_details_sql:
        where_sql = where_details_sql

    params.extend(contact_params)
    params.extend(details_params)

    order_sql = ""
    if order_by:
        parts = order_by.split()
        column = parts[0]
        direction = parts[1].upper() if len(parts) > 1 else "ASC"
        if column not in CONTACT_COLUMNS:
            raise ValueError(f"Invalid order_by column for contact: {column}")
        if direction not in {"ASC", "DESC"}:
            raise ValueError("order_by direction must be ASC or DESC")
        order_sql = f" ORDER BY c.{column} {direction}"

    sql = (
        "SELECT "
        + ", ".join(contact_select + details_select)
        + " FROM lvousr.contact c LEFT JOIN lvousr.contact_details d"
        + " ON c.lvaccount_id = d.lvaccount_id"
        + where_sql
        + order_sql
        + " LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])
    return BuiltQuery(sql=sql, params=params)
