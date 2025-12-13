# POET Cloud Cost ETL

A lightweight ETL pipeline for ingesting cloud cost data into PostgreSQL. Built for deployment on Coolify.

## Vision

Provide a simple, maintainable alternative to complex FinOps tools. Pull cost data from multiple cloud providers, normalize it into a common schema, and make it queryable via standard SQL.

**Why this exists:**
- CloudQuery and similar tools have format compatibility issues with legacy CUR data
- We need full control over table names and schema
- Simple Python + DuckDB is easier to maintain than complex ETL frameworks
- Data in PostgreSQL enables any frontend: Grafana, Supabase Studio, custom dashboards

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   AWS S3        │     │                 │     │   PostgreSQL    │
│   (CUR Data)    │────▶│  poet-cloud-    │────▶│   (Supabase)    │
│                 │     │  cost-etl       │     │                 │
├─────────────────┤     │                 │     ├─────────────────┤
│   GCP BigQuery  │────▶│  Python +       │────▶│  raw_aws_cur    │
│   (Future)      │     │  DuckDB         │     │  raw_gcp_costs  │
├─────────────────┤     │                 │     │  costs (view)   │
│   Azure Export  │────▶│                 │────▶│                 │
│   (Future)      │     └─────────────────┘     └─────────────────┘
└─────────────────┘
```

## Data Flow

1. **Extract**: DuckDB reads parquet files directly from S3 (no local download)
2. **Transform**: Normalize column names, add metadata (cloud provider, sync timestamp)
3. **Load**: Insert into PostgreSQL raw tables
4. **View**: Normalized `costs` view provides unified interface across providers

## Current Scope (v1)

- AWS Cost & Usage Reports (legacy CUR format with parquet files)
- Two report sources:
  - `cup/CUP-Cost-Usage-Report/` (Press costs)
  - `180431284529/CA-Cost-Usage-Report/` (CA costs)

## Future Roadmap

- [ ] GCP Billing Export (BigQuery)
- [ ] Azure Cost Management Export
- [ ] Incremental sync (only new data)
- [ ] Cost anomaly detection
- [ ] Slack/email alerts for budget thresholds

## Deployment (Coolify)

This project is designed to run as a Docker container on Coolify with scheduled execution.

### Environment Variables

```bash
# AWS Credentials (for S3 access)
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
AWS_REGION=eu-west-2

# S3 Bucket Configuration
S3_BUCKET=cupa-cost-usage-combined
CUR_PATHS=cup/CUP-Cost-Usage-Report/CUP-Cost-Usage-Report/,180431284529/CA-Cost-Usage-Report/CA-Cost-Usage-Report/

# PostgreSQL Connection (Supabase)
POSTGRES_HOST=supabase-db-f8wkccg888sggk4gw000wk40
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=xxx
POSTGRES_SCHEMA=cost_analytics
```

### Docker Compose for Coolify

See `docker-compose.yml` - configured to run with internal cron schedule.

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run sync manually
python src/main.py

# Run with specific date range (future feature)
python src/main.py --start-date 2025-01-01 --end-date 2025-11-01
```

## Schema

### Raw Tables

Raw tables preserve all original CUR columns:
- `cost_analytics.raw_aws_cur_press` - Press account costs
- `cost_analytics.raw_aws_cur_ca` - CA account costs

### Normalized View

The `cost_analytics.costs` view provides a unified interface:

```sql
SELECT * FROM cost_analytics.costs;

-- Columns:
-- date (DATE)
-- account_id (VARCHAR)
-- service (VARCHAR)
-- region (VARCHAR)
-- cost (DECIMAL)
-- currency (VARCHAR)
-- cloud_provider (VARCHAR) -- 'aws', 'gcp', 'azure'
-- source_table (VARCHAR)
-- sync_timestamp (TIMESTAMP)
```

### Example Queries

```sql
-- Total cost by service (last 30 days)
SELECT service, SUM(cost) as total
FROM cost_analytics.costs
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY service
ORDER BY total DESC;

-- Cost by account
SELECT account_id, SUM(cost) as total
FROM cost_analytics.costs
GROUP BY account_id;

-- Daily cost trend
SELECT date, SUM(cost) as daily_cost
FROM cost_analytics.costs
GROUP BY date
ORDER BY date;
```

## Project Structure

```
poet-cloud-cost-etl/
├── src/
│   ├── __init__.py
│   ├── main.py              # Entry point
│   ├── config.py            # Environment config
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── base.py          # Abstract source class
│   │   └── aws_cur.py       # AWS CUR S3 reader
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── postgresql.py    # PostgreSQL writer
│   └── transforms/
│       ├── __init__.py
│       └── normalize.py     # Column normalization
├── sql/
│   └── init.sql             # Schema setup & views
├── docker-compose.yml       # Coolify deployment
├── Dockerfile
├── requirements.txt
├── .env.example
├── CLAUDE.md                # Instructions for Claude Code
└── README.md
```

## Contributing

This project is part of the POET AI Lab infrastructure. See CLAUDE.md for development guidelines.

## License

Internal use - Cambridge University Press & Assessment
