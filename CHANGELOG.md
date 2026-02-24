# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2026-02-20

### Added

#### Core Features
- **Multi-database support**: PostgreSQL and Oracle database connectivity via DB links
- **Contact management**: Query contacts and contact details from PostgreSQL
- **Transaction queries**: Search across main and archive transaction tables (configurable 0-2 archive tables)
- **Campaign creation**: Create campaigns and requeue transactions
- **Oracle integration**: Full Oracle database support with DB link queries

#### MCP Tools
- `select_records`: Query allowed tables (contact, contact_details)
- `count_records`: Count records in a table
- `get_contact_with_details`: Get contact with all details by ID
- `select_contacts_with_details`: Join contacts with details
- `select_transactions`: Query transactions across main + archive tables
- `select_contact`: Query contacts via Oracle DB link
- `select_campaigns`: Query campaigns from config DB
- `create_campaign`: Create a new campaign
- `create_campaign_from_query`: Create campaign and insert matching transactions

#### Security & Guardrails
- **Query limits**:
  - MAX_ROWS: 10,000 rows per query
  - DEFAULT_LIMIT: 100
  - MAX_OFFSET: 100,000
  - POSTGRES_TIMEOUT_MS: 30,000
  - ORACLE_TIMEOUT_SECONDS: 30
  - MAX_IN_VALUES: 1,000
  - MAX_FILTERS: 20
- **Allowed operations**: SELECT on specific tables, INSERT for campaigns/transactions
- **Blocked operations**: DELETE, UPDATE, DROP, TRUNCATE, ALTER, CREATE, GRANT, REVOKE
- **Data redaction**: Automatic redaction of sensitive columns (ssn, dob, password, password_hash, api_key, secret, token)
- **SQL validation**: Prevents SQL injection and unsafe queries

#### Filter Syntax
- Simple equality filters: `{"outcome": "FAILED"}`
- Operator support via list: `{"date_modified": [">=", "2026-02-17"]}`
- Operator support via dict: `{"outcome": {"op": "in", "value": ["FAILED", "NO_ANSWER"]}}`
- Supported operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `like`, `not_like`, `in`, `not_in`, `between`, `is_null`, `is_not_null`

#### Database Architecture
- PostgreSQL for contacts and contact details
- Oracle for configuration, transactions, and campaigns
- Automatic database type detection based on DB link naming:
  - `lv*stg4a*` → Oracle
  - `lv*stg4b*` → PostgreSQL
- Support for separate dialing and reporting databases via DB links

#### Data Model
- `client` table: has `client_id`, `dialing_db`, `reporting_db`
- `skill` table: links `client_id` to `skill_id` (one client → many skills)
- `transaction` table: `CLIENT_ID` column is actually `skill_id` (legacy naming)
- `campaign` table: has both `client_id` and `skill_id`

#### Configuration
- Environment-based configuration via `.env` file
- PostgreSQL connection: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE
- Oracle connection: ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD
- Optional overrides: ORACLE_DSN, ORACLE_CLIENT_TABLE, TX_ARCHIVE_COUNT

#### Testing
- Unit tests for transaction query building
- Query builder tests
- Test infrastructure using pytest

#### Documentation
- Comprehensive README with:
  - Architecture diagram
  - Setup instructions
  - Configuration guide
  - Usage examples
  - Tool reference
  - Filter syntax documentation
  - Guardrails documentation
  - MCP client configuration for VS Code and Claude Desktop

### Technical Details

#### New Modules
- `guardrails.py`: Query limits, rate limiting, SQL validation, column redaction (401 lines)
- `campaign.py`: Campaign INSERT to config DB, transaction insert from query (582 lines)
- `tools.py`: All MCP tool definitions and implementations (622 lines)
- `transaction.py`: Transaction/contact query builders, skill lookup (601 lines)
- `oracle_db.py`: Oracle connection utilities (32 lines)
- `query.py`: PostgreSQL query builders (342 lines)
- Updated `config.py`: Oracle config, database type detection (57 lines)
- Updated `db.py`: Database connection management (14 lines)

#### Infrastructure
- Entry point: `contact-mcp` command via pyproject.toml
- MCP server implementation using `mcp[cli]>=1.2.0`
- Dependencies: `oracledb>=2.2.0`, `psycopg[binary]>=3.1.18`
- Python 3.10+ required

### Removed
- `cli.py`: Replaced by MCP tools implementation

## [Initial] - 2026-02-24

### Added
- Initial project planning and setup
