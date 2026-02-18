import logging
import sys
import time
from typing import Mapping, Sequence

from mcp.server.fastmcp import FastMCP

from .db import get_connection
from .query import build_contact_with_details_select, build_count, build_select

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


def run(transport: str = "stdio") -> None:
    mcp.run(transport=transport)


def main() -> None:
    run()


if __name__ == "__main__":
    main()
