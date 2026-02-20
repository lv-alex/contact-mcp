# Contact MCP Server

MCP server for querying and managing contacts, transactions, and campaigns across PostgreSQL and Oracle databases.

## Features

- **Multi-database support**: PostgreSQL and Oracle (via DB links)
- **Contact management**: Query contacts and contact details
- **Transaction queries**: Search across main and archive transaction tables
- **Campaign creation**: Create campaigns and requeue transactions
- **Guardrails**: Built-in security limits and data redaction

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      MCP Server                              │
├─────────────────────────────────────────────────────────────┤
│  Tools: select_records, select_transactions,                │
│         select_contact, create_campaign,                    │
│         create_campaign_from_query, ...                     │
├─────────────────────────────────────────────────────────────┤
│                    Database Layer                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ PostgreSQL  │  │ Oracle      │  │ Oracle DB Links     │ │
│  │ (contacts)  │  │ (config)    │  │ (tx/dw databases)   │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**Data Model:**
- `client` table → has `client_id`, `dialing_db`, `reporting_db`
- `skill` table → links `client_id` to `skill_id` (one client → many skills)
- `transaction` table → `CLIENT_ID` column is actually `skill_id` (legacy naming)
- `campaign` table → has both `client_id` and `skill_id`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Configuration

Create a `.env` file with the following variables:

### PostgreSQL (for contacts)

```bash
PGHOST=your-host
PGPORT=5432
PGDATABASE=your-db
PGUSER=your-user
PGPASSWORD=your-password
PGSSLMODE=require
```

### Oracle (for config, transactions, campaigns)

```bash
ORACLE_HOST=your-oracle-host
ORACLE_PORT=1521
ORACLE_SERVICE=your-service
ORACLE_USER=your-user
ORACLE_PASSWORD=your-password
```

### Optional

```bash
ORACLE_DSN=full-dsn-string           # Overrides host/service/port
ORACLE_CLIENT_TABLE=lvousr.client    # Default client table
TX_ARCHIVE_COUNT=2                   # Number of archive tables to scan (0-2)
```

## Run

```bash
source .venv/bin/activate
set -a && source .env && set +a
contact-mcp
```

## MCP Client Configuration

### VS Code (`.vscode/mcp.json`)

```json
{
  "servers": {
    "contact": {
      "command": "bash",
      "args": ["-c", "cd /path/to/contact && source .venv/bin/activate && contact-mcp"],
      "envFile": "${workspaceFolder}/.env"
    }
  }
}
```

### Claude Desktop

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

## Tools

### Query Tools

| Tool | Description |
|------|-------------|
| `select_records(table, filters, columns, limit, offset, order_by)` | Query allowed tables (contact, contact_details) |
| `count_records(table, filters)` | Count records in a table |
| `get_contact_with_details(lvaccount_id)` | Get contact with all details by ID |
| `select_contacts_with_details(...)` | Join contacts with details |
| `select_transactions(client_id, filters, columns, limit, order_by)` | Query transactions across main + archive tables |
| `select_contact(client_id, filters, columns, limit, order_by)` | Query contacts via Oracle DB link |
| `select_campaigns(filters, columns, limit, order_by)` | Query campaigns from config DB |

### Campaign Tools

| Tool | Description |
|------|-------------|
| `create_campaign(client_id, data)` | Create a new campaign |
| `create_campaign_from_query(client_id, query_filters, campaign_data)` | Create campaign and insert matching transactions |

## Filter Syntax

Filters support various operators:

```python
# Simple equality
{"outcome": "FAILED"}

# Operators via list [operator, value]
{"date_modified": [">=", "2026-02-17"]}
{"total_amount": [">", 1000]}

# Operators via dict
{"outcome": {"op": "in", "value": ["FAILED", "NO_ANSWER"]}}

# Supported operators:
# =, !=, >, >=, <, <=, like, not_like, in, not_in, between, is_null, is_not_null
```

## Usage Examples

### Query Contacts

```
"Get contact and details for lvaccount_id 123456."
"List 10 contacts created in the last 7 days."
"Find contacts with phone1_sms_consent = true and do_not_dial = false."
```

### Query Transactions

```
"Show the last 25 transactions for client 150723."
"List failed SMS transactions for skill 150747 in the last 2 days."
"Find transactions with outcome = 'NO_ANSWER' for client 150723."
```

**Important:** The `CLIENT_ID` column in transactions is actually `skill_id`. When filtering:
- Provide `client_id` in filters to filter by skill_id
- The tool parameter `client_id` is the actual client (for DB lookup)

### Create Campaigns

```
"Create a campaign for client 150723 with filename 'test_campaign'."

"Retry failed SMS for skill 150747 from the last 2 days."
# This uses create_campaign_from_query with:
# query_filters: {"client_id": 150747, "lvtransaction_type": "SMS", "outcome": "FAILED", "date_modified": [">=", "2026-02-18"]}
```

## Guardrails

The server includes built-in safety restrictions:

### Query Limits
- `MAX_ROWS`: 10,000 (maximum rows per query)
- `DEFAULT_LIMIT`: 100
- `MAX_OFFSET`: 100,000
- `POSTGRES_TIMEOUT_MS`: 30,000
- `ORACLE_TIMEOUT_SECONDS`: 30
- `MAX_IN_VALUES`: 1,000 (values in IN clauses)
- `MAX_FILTERS`: 20

### Allowed Operations
- **SELECT**: `contact`, `contact_details`, `campaign`
- **INSERT**: `campaign`, `transaction` (via create_campaign_from_query)
- **Blocked**: DELETE, UPDATE, DROP, TRUNCATE, ALTER, CREATE, GRANT, REVOKE

### Data Redaction
Sensitive columns are automatically redacted: `ssn`, `dob`, `password`, `password_hash`, `api_key`, `secret`, `token`

## Database Type Detection

The server automatically detects Oracle vs PostgreSQL based on DB link naming:
- `lv*stg4a*` → Oracle
- `lv*stg4b*` → PostgreSQL

## Development

### Run Tests

```bash
pytest tests/
```

### Check Syntax

```bash
python3 -m py_compile contact_mcp/*.py
```

## Notes

- Transactions are queried across main table + archive tables (configurable via `TX_ARCHIVE_COUNT`)
- Campaigns are always inserted into the Oracle config DB (not via DB link)
- Transaction inserts (for requeue) go to the dialing DB via DB link
