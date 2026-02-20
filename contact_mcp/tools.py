"""Tool definitions for the contact MCP."""
import logging
import sys
import time
from typing import Any, Mapping, Sequence

import oracledb
from mcp.server.fastmcp import FastMCP

from .campaign import (
    build_campaign_insert,
    build_count_query_for_campaign,
    build_source_query_for_campaign,
    build_transaction_insert_from_select,
)
from .config import get_db_type
from .db import get_connection
from .guardrails import GuardrailError, validate_campaign_insert
from .oracle_db import fetch_all_dicts, get_oracle_connection
from .query import build_contact_with_details_select, build_count, build_select
from .transaction import (
    build_oracle_contact_select,
    build_postgres_contact_select,
    build_postgres_transaction_union,
    build_transaction_union,
    get_client_db_links_query,
    get_client_for_skill_query,
    get_skills_for_client_query,
    resolve_transaction_tables,
    _validate_filters,
)

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger("contact-mcp")

mcp = FastMCP("contact")


def log_step(step: str, **data: object) -> None:
    if data:
        logger.info("%s | %s", step, data)
    else:
        logger.info("%s", step)


@mcp.tool()
def select_records(
    table: str,
    filters: Mapping[str, object] | None = None,
    columns: Sequence[str] | None = None,
    limit: int = 100,
    offset: int = 0,
    order_by: str | None = None,
) -> list[dict]:
    """Select records from allowed tables with optional filters."""
    log_step("tool:select_records:received", table=table)
    build_start = time.perf_counter()
    query = build_select(
        table=table,
        filters=filters,
        columns=columns,
        limit=limit,
        offset=offset,
        order_by=order_by,
    )
    build_ms = (time.perf_counter() - build_start) * 1000
    log_step("tool:select_records:built", ms=round(build_ms, 2))
    log_step("db:query", sql=query.sql)
    conn_start = time.perf_counter()
    with get_connection() as conn:
        conn_ms = (time.perf_counter() - conn_start) * 1000
        log_step("db:connect", ms=round(conn_ms, 2))
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 10000")
            log_step("db:timeout:set", ms=10000)
            exec_start = time.perf_counter()
            log_step("db:execute:start")
            cur.execute(query.sql, query.params)
            exec_ms = (time.perf_counter() - exec_start) * 1000
            log_step("db:execute", ms=round(exec_ms, 2))
            fetch_start = time.perf_counter()
            rows = cur.fetchall()
            fetch_ms = (time.perf_counter() - fetch_start) * 1000
        log_step("db:fetch", ms=round(fetch_ms, 2), rows=len(rows))
    return rows


@mcp.tool()
def count_records(
    table: str,
    filters: Mapping[str, object] | None = None,
) -> int:
    """Count records in an allowed table with optional filters."""
    log_step("tool:count_records:received", table=table)
    query = build_count(table=table, filters=filters)
    log_step("db:query", sql=query.sql)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query.sql, query.params)
            row = cur.fetchone()
    return int(row["count"]) if row else 0


@mcp.tool()
def get_contact_with_details(lvaccount_id: str) -> dict:
    """Get a single contact and its details by lvaccount_id."""
    log_step("tool:get_contact_with_details:received", lvaccount_id=lvaccount_id)
    contact_sql = "SELECT * FROM lvousr.contact WHERE lvaccount_id = %s"
    details_sql = "SELECT * FROM lvousr.contact_details WHERE lvaccount_id = %s"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(contact_sql, (lvaccount_id,))
            contact = cur.fetchone()
            cur.execute(details_sql, (lvaccount_id,))
            details = cur.fetchone()
    return {"contact": contact, "details": details}


@mcp.tool()
def select_contacts_with_details(
    contact_filters: Mapping[str, object] | None = None,
    details_filters: Mapping[str, object] | None = None,
    contact_columns: Sequence[str] | None = None,
    details_columns: Sequence[str] | None = None,
    limit: int = 100,
    offset: int = 0,
    order_by: str | None = None,
) -> list[dict]:
    """Select contacts joined with details (one row per contact)."""
    log_step("tool:select_contacts_with_details:received")
    build_start = time.perf_counter()
    query = build_contact_with_details_select(
        contact_filters=contact_filters,
        details_filters=details_filters,
        contact_columns=contact_columns,
        details_columns=details_columns,
        limit=limit,
        offset=offset,
        order_by=order_by,
    )
    build_ms = (time.perf_counter() - build_start) * 1000
    log_step("tool:select_contacts_with_details:built", ms=round(build_ms, 2))
    log_step("db:query", sql=query.sql)
    conn_start = time.perf_counter()
    with get_connection() as conn:
        conn_ms = (time.perf_counter() - conn_start) * 1000
        log_step("db:connect", ms=round(conn_ms, 2))
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 10000")
            exec_start = time.perf_counter()
            cur.execute(query.sql, query.params)
            exec_ms = (time.perf_counter() - exec_start) * 1000
            log_step("db:execute", ms=round(exec_ms, 2))
            fetch_start = time.perf_counter()
            rows = cur.fetchall()
            fetch_ms = (time.perf_counter() - fetch_start) * 1000
        log_step("db:fetch", ms=round(fetch_ms, 2), rows=len(rows))
    return rows


@mcp.tool()
def select_transactions(
    client_id: int,
    filters: Mapping[str, object] | None = None,
    columns: Sequence[str] | None = None,
    limit: int = 100,
    order_by: str | None = None,
) -> list[dict]:
    """Select transactions across current and recent archive tables."""
    log_step("tool:select_transactions:received", client_id=client_id)
    if not client_id or client_id <= 0:
        raise ValueError("A positive client_id is required to select transactions.")

    # First, get the db links from the Oracle config database
    with get_oracle_connection() as conn:
        with conn.cursor() as cur:
            sql, params = get_client_db_links_query(client_id)
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                raise ValueError(f"No client found for client_id {client_id}")
            dialing_db, reporting_db = row

    # Determine the database type
    db_type = get_db_type(dialing_db)
    log_step("tool:select_transactions:db_type", db_type=db_type, dialing_db=dialing_db)

    table_names = resolve_transaction_tables(dialing_db, reporting_db)

    if db_type == "postgres":
        # Use PostgreSQL connection
        query = build_postgres_transaction_union(
            table_names=table_names,
            filters=filters,
            columns=columns,
            limit=limit,
            order_by=order_by,
        )
        log_step("db:query", sql=query.sql)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 30000")
                cur.execute(query.sql, query.params)
                rows = cur.fetchall()
        log_step("db:fetch", rows=len(rows))
        return rows
    else:
        # Use Oracle connection via db link
        query = build_transaction_union(
            table_names=table_names,
            filters=filters,
            columns=columns,
            limit=limit,
            order_by=order_by,
        )
        log_step("db:query", sql=query.sql)
        with get_oracle_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query.sql, query.params)
                rows = fetch_all_dicts(cur)
        log_step("db:fetch", rows=len(rows))
        return rows


@mcp.tool()
def select_contact(
    client_id: int,
    filters: Mapping[str, object] | None = None,
    columns: Sequence[str] | None = None,
    limit: int = 100,
    order_by: str | None = None,
) -> list[dict]:
    """Select records from the contact table using a client_id to connect to the correct database."""
    log_step("tool:select_contact:received", client_id=client_id)
    if not client_id or client_id <= 0:
        raise ValueError("A positive client_id is required to select contacts.")

    # First, get the db links from the Oracle config database
    with get_oracle_connection() as conn:
        with conn.cursor() as cur:
            sql, params = get_client_db_links_query(client_id)
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                raise ValueError(f"No client found for client_id {client_id}")
            dialing_db, _ = row

    # Determine the database type
    db_type = get_db_type(dialing_db)
    log_step("tool:select_contact:db_type", db_type=db_type, dialing_db=dialing_db)

    if db_type == "postgres":
        # Use PostgreSQL connection
        query = build_postgres_contact_select(
            filters=filters,
            columns=columns,
            limit=limit,
            order_by=order_by,
        )
        log_step("db:query", sql=query.sql)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 10000")
                cur.execute(query.sql, query.params)
                rows = cur.fetchall()
        log_step("db:fetch", rows=len(rows))
        return rows
    else:
        # Use Oracle connection via db link
        query = build_oracle_contact_select(
            dialing_db=dialing_db,
            filters=filters,
            columns=columns,
            limit=limit,
            order_by=order_by,
        )
        log_step("db:query", sql=query.sql)
        with get_oracle_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query.sql, query.params)
                rows = fetch_all_dicts(cur)
        log_step("db:fetch", rows=len(rows))
        return rows


@mcp.tool()
def select_campaigns(
    filters: Mapping[str, object] | None = None,
    columns: Sequence[str] | None = None,
    limit: int = 100,
    order_by: str | None = None,
) -> list[dict]:
    """Select campaigns from the Oracle config database.

    Args:
        filters: Optional filters to apply, e.g.:
            - client_id: Filter by client ID
            - skill_id: Filter by skill ID
            - create_date: Filter by creation date, e.g. [">=", "2026-02-19 12:00:00"]
            - b_active: Filter by active status (1 or 0)
            - campaign_type_id: Filter by campaign type
            - Supports operators: =, !=, >, >=, <, <=, like, in, not_in, between
        columns: Optional list of columns to select (defaults to common columns)
        limit: Maximum rows to return (default 100, max 1000)
        order_by: Optional column to order by (e.g., "create_date DESC")

    Returns:
        List of campaign records as dictionaries
    """
    from .campaign import build_campaign_select

    log_step("tool:select_campaigns:received", filters=filters)
    build_start = time.perf_counter()
    query = build_campaign_select(
        filters=filters,
        columns=list(columns) if columns else None,
        limit=limit,
        order_by=order_by,
    )
    build_ms = (time.perf_counter() - build_start) * 1000
    log_step("tool:select_campaigns:built", ms=round(build_ms, 2))
    log_step("db:query", sql=query.sql)

    conn_start = time.perf_counter()
    with get_oracle_connection() as conn:
        conn_ms = (time.perf_counter() - conn_start) * 1000
        log_step("db:connect", ms=round(conn_ms, 2))
        with conn.cursor() as cur:
            exec_start = time.perf_counter()
            cur.execute(query.sql, query.params)
            exec_ms = (time.perf_counter() - exec_start) * 1000
            log_step("db:execute", ms=round(exec_ms, 2))
            fetch_start = time.perf_counter()
            rows = fetch_all_dicts(cur)
            fetch_ms = (time.perf_counter() - fetch_start) * 1000
        log_step("db:fetch", ms=round(fetch_ms, 2), rows=len(rows))
    return rows


@mcp.tool()
def create_campaign(
    client_id: int,
    data: Mapping[str, Any],
) -> dict:
    """Create a new campaign for a client.

    Args:
        client_id: The client ID (required)
        data: Campaign data including:
            - filename: Campaign filename (optional)
            - start_time: Campaign start time (optional)
            - end_time: Campaign end time (optional)
            - leave_messages: Whether to leave messages (optional)
            - operator_phone: Operator phone number (optional)
            - callback_phone: Callback phone number (optional)
            - caller_id: Caller ID to display (optional)
            - voice_id: Voice ID for messages (optional)
            - b_active: Whether campaign is active (optional)
            - skill_id: Skill ID (optional)
            - dialing_strategy_id: Dialing strategy ID (optional)
            - am_option: AM option - DONT_LEAVE_MESSAGES, LEAVE_MESSAGES, NO_AM (optional)
            - campaign_type_id: Campaign type ID (optional)
            - contact_source: CAMPAIGN or CONTACT (optional, default CAMPAIGN)
            - email_from: Email from address (optional)
            - campaign_subtype: Campaign subtype (optional)

    Returns:
        Dictionary with created campaign_id and status
    """
    log_step("tool:create_campaign:received", client_id=client_id)

    # Merge client_id into data
    campaign_data = dict(data)
    campaign_data["client_id"] = client_id

    # Validate using guardrails
    try:
        validate_campaign_insert(campaign_data)
    except GuardrailError as e:
        log_step("tool:create_campaign:validation_error", error=str(e))
        return {"success": False, "error": str(e)}

    # Verify the client exists
    with get_oracle_connection() as conn:
        with conn.cursor() as cur:
            sql, params = get_client_db_links_query(client_id)
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return {"success": False, "error": f"No client found for client_id {client_id}"}

    # Build the INSERT statement (campaigns go into config DB directly)
    insert = build_campaign_insert(campaign_data)
    log_step("db:query", sql=insert.sql)

    # Execute the INSERT in the Oracle config DB
    with get_oracle_connection() as conn:
        with conn.cursor() as cur:
            # Create output variable for RETURNING clause
            out_campaign_id = cur.var(oracledb.NUMBER)
            insert.params["out_campaign_id"] = out_campaign_id

            cur.execute(insert.sql, insert.params)
            conn.commit()

            campaign_id = int(out_campaign_id.getvalue()[0])

    log_step("tool:create_campaign:created", campaign_id=campaign_id)

    return {
        "success": True,
        "campaign_id": campaign_id,
        "client_id": client_id,
    }


@mcp.tool()
def create_campaign_from_query(
    client_id: int,
    query_filters: Mapping[str, Any],
    campaign_data: Mapping[str, Any] | None = None,
    max_records: int | None = None,
) -> dict:
    """Create a campaign and insert transaction records based on a query.

    This tool first checks if any records match the query criteria. If records
    exist, it creates a new campaign and inserts the matching records into the
    transaction table with the new campaign_id.

    IMPORTANT: The CLIENT_ID column in the transaction table is actually the
    skill_id (legacy naming). When filtering transactions:
    - Provide CLIENT_ID in query_filters to filter by skill_id directly
    - If no CLIENT_ID filter is provided, the tool will find all skills for
      the client and filter transactions by those skill_ids

    Use this for scenarios like:
    - Retry failed SMS for a skill: {"client_id": 150747, "lvtransaction_type": "SMS", "outcome": "FAILED", "date_modified": [">=", "2026-02-17"]}
    - Requeue unanswered calls: {"outcome": "NO_ANSWER", "date_modified": [">=", "2026-02-18"]}

    Args:
        client_id: The actual client ID (used for campaign creation and DB lookup)
        query_filters: Filters to identify source records, e.g.:
            - client_id: The skill_id to filter transactions (note: this is skill_id in tx table!)
            - lvtransaction_type: SMS, VOICE, etc.
            - outcome: FAILED, NO_ANSWER, SUCCESS, etc.
            - date_modified: Date filter like [">=", "2026-02-17"]
            - Any other transaction column filters
        campaign_data: Optional campaign settings:
            - filename: Campaign filename
            - skill_id: Skill ID for the campaign (auto-detected if client_id filter provided)
            - am_option: DONT_LEAVE_MESSAGES, LEAVE_MESSAGES, NO_AM
            - contact_source: CAMPAIGN or CONTACT
            - Other campaign columns
        max_records: Optional maximum number of records to requeue. If not specified,
            all matching records are requeued.

    Returns:
        Dictionary with:
            - success: True/False
            - campaign_id: Created campaign ID (if successful)
            - records_inserted: Number of records inserted
            - error: Error message (if failed)
    """
    log_step("tool:create_campaign_from_query:received", client_id=client_id)

    # Get the dialing_db for this client
    with get_oracle_connection() as conn:
        with conn.cursor() as cur:
            sql, params = get_client_db_links_query(client_id)
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return {"success": False, "error": f"No client found for client_id {client_id}"}
            dialing_db, reporting_db = row

    log_step("tool:create_campaign_from_query:db_links", dialing_db=dialing_db)

    # Check database type - this tool only works with Oracle for now
    db_type = get_db_type(dialing_db)
    if db_type != "oracle":
        return {
            "success": False,
            "error": "create_campaign_from_query currently only supports Oracle databases",
        }

    # Use only the main transaction table for campaign requeue operations.
    # Archive tables have different column structures and are for historical
    # reporting only - not for creating new campaigns.
    table_names = [f"LVOUSR.TRANSACTION@{dialing_db}"]
    log_step("tool:create_campaign_from_query:tables", tables=table_names)

    # Handle skill_id / client_id relationship
    # The CLIENT_ID column in transaction table is actually skill_id
    filters = dict(query_filters)
    skill_id_for_campaign = None
    
    # Check if user provided CLIENT_ID filter (which is actually skill_id)
    client_id_key = None
    for k in filters.keys():
        if k.upper() == "CLIENT_ID":
            client_id_key = k
            break
    
    if client_id_key:
        # User provided skill_id directly via CLIENT_ID filter
        skill_id_for_campaign = filters[client_id_key]
        if isinstance(skill_id_for_campaign, (list, tuple)):
            # Handle operator format like ["=", 150747]
            skill_id_for_campaign = skill_id_for_campaign[1] if len(skill_id_for_campaign) > 1 else skill_id_for_campaign[0]
        log_step("tool:create_campaign_from_query:skill_from_filter", skill_id=skill_id_for_campaign)
    else:
        # No skill filter provided - get all skills for this client
        with get_oracle_connection() as conn:
            with conn.cursor() as cur:
                sql, params = get_skills_for_client_query(client_id)
                cur.execute(sql, params)
                rows = cur.fetchall()
                skill_ids = [r[0] for r in rows] if rows else []
        
        if not skill_ids:
            return {
                "success": False,
                "error": f"No active skills found for client_id {client_id}",
            }
        
        log_step("tool:create_campaign_from_query:skills_for_client", skill_ids=skill_ids)
        
        if len(skill_ids) == 1:
            # Single skill - use simple equality filter
            filters["client_id"] = skill_ids[0]
            skill_id_for_campaign = skill_ids[0]
        else:
            # Multiple skills - use IN filter
            filters["client_id"] = {"op": "in", "value": skill_ids}
            skill_id_for_campaign = skill_ids[0]  # Use first skill for campaign

    # First, count matching records (across main + archive tables)
    try:
        count_sql, count_params = build_count_query_for_campaign(dialing_db, filters, table_names)
    except ValueError as e:
        return {"success": False, "error": f"Invalid query filters: {e}"}

    log_step("db:count_query", sql=count_sql)

    with get_oracle_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, count_params)
            row = cur.fetchone()
            record_count = row[0] if row else 0

    log_step("tool:create_campaign_from_query:count", records=record_count)

    if record_count == 0:
        return {
            "success": False,
            "error": "No records match the query criteria. Campaign not created.",
            "records_found": 0,
        }

    # Prepare campaign data
    camp_data = dict(campaign_data) if campaign_data else {}
    camp_data["client_id"] = client_id  # Actual client_id for campaign
    
    # Set skill_id in campaign if not provided
    if "skill_id" not in camp_data and skill_id_for_campaign:
        camp_data["skill_id"] = skill_id_for_campaign

    # Set default filename if not provided
    if "filename" not in camp_data:
        from datetime import datetime
        camp_data["filename"] = f"auto_campaign_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Validate campaign data
    try:
        validate_campaign_insert(camp_data)
    except GuardrailError as e:
        return {"success": False, "error": f"Invalid campaign data: {e}"}

    # Create the campaign
    insert = build_campaign_insert(camp_data)
    log_step("db:campaign_insert", sql=insert.sql)

    with get_oracle_connection() as conn:
        with conn.cursor() as cur:
            out_campaign_id = cur.var(oracledb.NUMBER)
            insert.params["out_campaign_id"] = out_campaign_id
            cur.execute(insert.sql, insert.params)
            conn.commit()
            campaign_id = int(out_campaign_id.getvalue()[0])

    log_step("tool:create_campaign_from_query:campaign_created", campaign_id=campaign_id)

    # Now insert the transaction records with the new campaign_id
    # Build the source SELECT query (across main + archive tables)
    source_query, where_params = build_source_query_for_campaign(filters, table_names, max_records=max_records)

    tx_insert_sql, tx_insert_params = build_transaction_insert_from_select(
        dialing_db=dialing_db,
        campaign_id=campaign_id,
        source_query=source_query,
        source_params=where_params,
    )

    log_step("db:transaction_insert", sql=tx_insert_sql)

    with get_oracle_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(tx_insert_sql, tx_insert_params)
            rows_inserted = cur.rowcount
            conn.commit()

    log_step("tool:create_campaign_from_query:complete", rows_inserted=rows_inserted)

    return {
        "success": True,
        "campaign_id": campaign_id,
        "client_id": client_id,
        "skill_id": skill_id_for_campaign,
        "records_found": record_count,
        "records_inserted": rows_inserted,
        "query_filters": filters,
    }
