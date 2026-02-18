# Contact MCP Server

MCP server that queries a Postgres database for `lvousr.contact` and `lvousr.contact_details`.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

## Configure

Set environment variables (do not store passwords in files):

- `PGHOST`
- `PGPORT` (default: 5432)
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`
- `PGSSLMODE` (default: require)

## Run

```bash
contact-mcp
```

## Tools

- `select_records(table, filters, columns, limit, offset, order_by)`
- `get_contact_with_details(lvaccount_id)`

## Notes

- Select-only queries.
- Tables are restricted to `lvousr.contact` and `lvousr.contact_details`.
- Limit is capped at 1000.
# contact-mcp
