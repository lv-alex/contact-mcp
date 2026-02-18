# Contact MCP Server

MCP server that queries a Postgres database for `lvousr.contact` and `lvousr.contact_details`.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

## Configure

Set Postgres environment variables. You can export them in your shell or use a local `.env` file (recommended for development and already gitignored).

Required:

- `PGHOST`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

Optional:

- `PGPORT` (default: 5432)
- `PGSSLMODE` (default: require)

Example `.env`:

```bash
PGHOST=your-host
PGPORT=5432
PGDATABASE=your-db
PGUSER=your-user
PGPASSWORD=your-password
PGSSLMODE=require
```

## Run

With a local `.env`:

```bash
set -a && source .env && set +a
contact-mcp
```

Or directly:

```bash
contact-mcp
```

## Chat with it (MCP client)

This is an MCP server over stdio. Use any MCP-compatible client and configure it to run the server command.

Example (Claude Desktop-style config):

```json
{
	"mcpServers": {
		"contact": {
			"command": "/bin/bash",
			"args": [
				"-lc",
				"cd /path/to/contact && source .venv/bin/activate && set -a && source .env && set +a && contact-mcp"
			]
		}
	}
}
```

Adjust the path and activation command for your environment.

## Tools

- `select_records(table, filters, columns, limit, offset, order_by)`
- `count_records(table, filters)`
- `get_contact_with_details(lvaccount_id)`
- `select_contacts_with_details(contact_filters, details_filters, contact_columns, details_columns, limit, offset, order_by)`

## Notes

- Select-only queries.
- Tables are restricted to `lvousr.contact` and `lvousr.contact_details`.
- Limit is capped at 1000.
# contact-mcp
