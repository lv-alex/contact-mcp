"""Guardrails and safety restrictions for the contact MCP server.

This module defines limits and restrictions to prevent resource exhaustion,
protect sensitive data, and ensure safe database operations.
"""

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class QueryLimits:
    """Maximum limits for database queries."""

    # Maximum number of rows that can be returned in a single query
    MAX_ROWS: int = 10_000

    # Default row limit if not specified
    DEFAULT_LIMIT: int = 100

    # Maximum offset for pagination (prevents scanning huge result sets)
    MAX_OFFSET: int = 100_000

    # Query timeout in milliseconds (PostgreSQL)
    POSTGRES_TIMEOUT_MS: int = 30_000

    # Query timeout in seconds (Oracle)
    ORACLE_TIMEOUT_SECONDS: int = 30

    # Maximum number of values in an IN clause
    MAX_IN_VALUES: int = 1000

    # Maximum number of filter conditions
    MAX_FILTERS: int = 20


@dataclass(frozen=True)
class RateLimits:
    """Rate limiting configuration."""

    # Maximum queries per minute per tool
    MAX_QUERIES_PER_MINUTE: int = 60

    # Maximum concurrent connections
    MAX_CONCURRENT_CONNECTIONS: int = 5


# Tables that are allowed to be queried (SELECT)
ALLOWED_SELECT_TABLES = frozenset({
    "client",
    "contact",
    "contact_details",
    "campaign",
})

# Tables that are allowed for INSERT operations
ALLOWED_INSERT_TABLES = frozenset({
    "campaign",
    "transaction",
})

# Columns that should never be returned in query results (sensitive data)
REDACTED_COLUMNS = frozenset({
    "ssn",
    "dob",
    "password",
    "password_hash",
    "api_key",
    "secret",
    "token",
})

# Columns that require explicit selection (not included in SELECT *)
EXPLICIT_ONLY_COLUMNS = frozenset({
    "ssn",
    "dob",
})

# Operations that are never allowed (global)
DISALLOWED_OPERATIONS = frozenset({
    "DELETE",
    "UPDATE",
    "DROP",
    "TRUNCATE",
    "ALTER",
    "CREATE",
    "GRANT",
    "REVOKE",
})

# Required columns for campaign insert
CAMPAIGN_REQUIRED_COLUMNS = frozenset({
    "client_id",
})

# Allowed columns for campaign insert (subset of all columns)
CAMPAIGN_INSERT_COLUMNS = frozenset({
    "client_id",
    "filename",
    "start_time",
    "end_time",
    "leave_messages",
    "operator_phone",
    "callback_phone",
    "caller_id",
    "voice_id",
    "b_active",
    "skill_id",
    "dialing_strategy_id",
    "am_option",
    "campaign_type_id",
    "contact_source",
    "email_from",
    "campaign_subtype",
})

# Default query limits instance
QUERY_LIMITS = QueryLimits()
RATE_LIMITS = RateLimits()


class GuardrailError(Exception):
    """Raised when a guardrail restriction is violated."""

    pass


def validate_limit(limit: int | None) -> int:
    """Validate and constrain the row limit.

    Args:
        limit: Requested row limit

    Returns:
        Validated limit within allowed bounds

    Raises:
        GuardrailError: If limit is invalid
    """
    if limit is None:
        return QUERY_LIMITS.DEFAULT_LIMIT

    if not isinstance(limit, int) or limit < 1:
        raise GuardrailError("Limit must be a positive integer")

    return min(limit, QUERY_LIMITS.MAX_ROWS)


def validate_offset(offset: int | None) -> int:
    """Validate and constrain the offset.

    Args:
        offset: Requested offset

    Returns:
        Validated offset within allowed bounds

    Raises:
        GuardrailError: If offset is invalid
    """
    if offset is None:
        return 0

    if not isinstance(offset, int) or offset < 0:
        raise GuardrailError("Offset must be a non-negative integer")

    if offset > QUERY_LIMITS.MAX_OFFSET:
        raise GuardrailError(
            f"Offset exceeds maximum allowed value of {QUERY_LIMITS.MAX_OFFSET}"
        )

    return offset


def validate_table(table: str) -> None:
    """Validate that a table is allowed to be queried (SELECT).

    Args:
        table: Table name to validate

    Raises:
        GuardrailError: If table is not allowed
    """
    if not table or not isinstance(table, str):
        raise GuardrailError("Table name must be a non-empty string")

    if table.lower() not in ALLOWED_SELECT_TABLES:
        raise GuardrailError(
            f"Table '{table}' is not allowed for SELECT. Allowed tables: {sorted(ALLOWED_SELECT_TABLES)}"
        )


def validate_insert_table(table: str) -> None:
    """Validate that a table is allowed for INSERT operations.

    Args:
        table: Table name to validate

    Raises:
        GuardrailError: If table is not allowed for INSERT
    """
    if not table or not isinstance(table, str):
        raise GuardrailError("Table name must be a non-empty string")

    if table.lower() not in ALLOWED_INSERT_TABLES:
        raise GuardrailError(
            f"Table '{table}' is not allowed for INSERT. Allowed tables: {sorted(ALLOWED_INSERT_TABLES)}"
        )


def validate_filters(filters: Mapping[str, Any] | None) -> None:
    """Validate filter constraints.

    Args:
        filters: Filter dictionary to validate

    Raises:
        GuardrailError: If filters violate constraints
    """
    if filters is None:
        return

    if not isinstance(filters, Mapping):
        raise GuardrailError("Filters must be a dictionary")

    if len(filters) > QUERY_LIMITS.MAX_FILTERS:
        raise GuardrailError(
            f"Too many filters. Maximum allowed: {QUERY_LIMITS.MAX_FILTERS}"
        )

    for key, value in filters.items():
        # Check for IN clause size limits
        if isinstance(value, dict) and "in" in value:
            in_values = value["in"]
            if isinstance(in_values, (list, tuple, set)):
                if len(in_values) > QUERY_LIMITS.MAX_IN_VALUES:
                    raise GuardrailError(
                        f"IN clause for '{key}' exceeds maximum of "
                        f"{QUERY_LIMITS.MAX_IN_VALUES} values"
                    )


def validate_columns(columns: Sequence[str] | None, strict: bool = False) -> None:
    """Validate that requested columns don't include redacted fields.

    Args:
        columns: List of column names to validate
        strict: If True, raise error for redacted columns; if False, they'll be filtered

    Raises:
        GuardrailError: If strict mode and redacted columns are requested
    """
    if columns is None:
        return

    if not isinstance(columns, (list, tuple)):
        raise GuardrailError("Columns must be a list")

    if strict:
        requested_redacted = set(c.lower() for c in columns) & REDACTED_COLUMNS
        if requested_redacted:
            raise GuardrailError(
                f"Cannot access redacted columns: {sorted(requested_redacted)}"
            )


def filter_redacted_columns(columns: Sequence[str] | None) -> list[str] | None:
    """Filter out redacted columns from a column list.

    Args:
        columns: List of column names

    Returns:
        Filtered list without redacted columns, or None if input was None
    """
    if columns is None:
        return None

    return [c for c in columns if c.lower() not in REDACTED_COLUMNS]


def redact_row(row: dict) -> dict:
    """Redact sensitive columns from a result row.

    Args:
        row: Dictionary representing a database row

    Returns:
        Row with sensitive columns redacted
    """
    return {
        k: "***REDACTED***" if k.lower() in REDACTED_COLUMNS else v
        for k, v in row.items()
    }


def redact_results(rows: list[dict]) -> list[dict]:
    """Redact sensitive columns from all result rows.

    Args:
        rows: List of result row dictionaries

    Returns:
        Rows with sensitive columns redacted
    """
    return [redact_row(row) for row in rows]


def validate_sql_safety(sql: str, allow_insert: bool = False) -> None:
    """Basic SQL safety check to prevent dangerous operations.

    Args:
        sql: SQL query string to validate
        allow_insert: If True, allows INSERT statements (for specific tables)

    Raises:
        GuardrailError: If SQL contains disallowed operations
    """
    sql_upper = sql.upper().strip()

    for op in DISALLOWED_OPERATIONS:
        # Check if operation appears at start or after whitespace/semicolon
        if sql_upper.startswith(op) or f" {op}" in sql_upper or f";{op}" in sql_upper:
            raise GuardrailError(f"Operation '{op}' is not allowed")

    # Check allowed statement types
    if sql_upper.startswith("SELECT"):
        return  # SELECT is always allowed
    elif sql_upper.startswith("INSERT") and allow_insert:
        return  # INSERT allowed when explicitly permitted
    else:
        raise GuardrailError("Only SELECT queries are allowed (INSERT requires explicit permission)")


def validate_client_id_required(
    filters: Mapping[str, Any] | None, tool_name: str
) -> None:
    """Ensure client_id is provided in filters for tools that require it.

    Args:
        filters: Filter dictionary
        tool_name: Name of the tool for error message

    Raises:
        GuardrailError: If client_id is not provided
    """
    if filters is None or "client_id" not in filters:
        raise GuardrailError(
            f"client_id is required for {tool_name}. "
            "Please provide a client_id filter."
        )


def validate_campaign_insert(data: Mapping[str, Any]) -> None:
    """Validate campaign insert data.

    Args:
        data: Dictionary of column names to values for insert

    Raises:
        GuardrailError: If data is invalid or contains disallowed columns
    """
    if not data or not isinstance(data, Mapping):
        raise GuardrailError("Campaign data must be a non-empty dictionary")

    # Check required columns
    missing = CAMPAIGN_REQUIRED_COLUMNS - set(data.keys())
    if missing:
        raise GuardrailError(f"Missing required columns for campaign: {sorted(missing)}")

    # Check for disallowed columns
    provided = set(k.lower() for k in data.keys())
    disallowed = provided - CAMPAIGN_INSERT_COLUMNS
    if disallowed:
        raise GuardrailError(
            f"Columns not allowed for campaign insert: {sorted(disallowed)}. "
            f"Allowed columns: {sorted(CAMPAIGN_INSERT_COLUMNS)}"
        )

    # Validate client_id is a positive integer
    client_id = data.get("client_id")
    if not isinstance(client_id, int) or client_id < 1:
        raise GuardrailError("client_id must be a positive integer")

    # Validate am_option if provided
    am_option = data.get("am_option")
    if am_option is not None:
        valid_am_options = {"DONT_LEAVE_MESSAGES", "LEAVE_MESSAGES", "NO_AM"}
        if am_option not in valid_am_options:
            raise GuardrailError(
                f"am_option must be one of: {sorted(valid_am_options)}"
            )

    # Validate contact_source if provided
    contact_source = data.get("contact_source")
    if contact_source is not None:
        valid_contact_sources = {"CAMPAIGN", "CONTACT"}
        if contact_source not in valid_contact_sources:
            raise GuardrailError(
                f"contact_source must be one of: {sorted(valid_contact_sources)}"
            )
