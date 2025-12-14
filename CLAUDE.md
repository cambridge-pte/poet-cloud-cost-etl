# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

POET Cloud Cost ETL - Python ETL pipeline for ingesting AWS Cost & Usage Report (CUR) data from S3 parquet files into PostgreSQL. Designed for Coolify deployment with internal cron.

## Commands

```bash
# Local development
pip install -r requirements.txt
cd src && python main.py --help

# Run ETL sync
python src/main.py sync                    # Full sync
python src/main.py sync --raw-only         # Skip normalization
python src/main.py sync --dry-run          # Preview only

# Test connections
python src/main.py test-connection         # Test PostgreSQL
python src/main.py test-s3                 # Test S3 access

# Docker
docker build -t poet-cloud-cost-etl .
docker run --env-file .env poet-cloud-cost-etl
```

## Architecture

```
src/
├── main.py           # CLI entry (typer) - orchestration only
├── config.py         # Config dataclasses from env vars
├── sources/
│   ├── base.py       # BaseSource ABC (extract generator pattern)
│   └── aws_cur.py    # DuckDB S3 parquet reader via httpfs
├── loaders/
│   └── postgresql.py # Batch insert with execute_values, auto table creation
└── transforms/
    └── normalize.py  # AWS CUR → common schema mapping
```

**Data flow**: S3 parquet → DuckDB (in-memory) → pandas DataFrame → PostgreSQL

**Tables created**:
- `cost_analytics.raw_{source}` - All original CUR columns (~100+)
- `cost_analytics.{source}_normalized` - Common schema (date, account_id, service, region, cost, currency)
- `cost_analytics.costs` - Unified view across all sources

## Key Technical Decisions

- **DuckDB httpfs**: Reads parquet directly from S3 without downloading
- **Dual tables**: Raw preserves all columns, normalized enables cross-provider querying
- **Schema-first PostgreSQL**: All tables in `cost_analytics` schema (Supabase)

## Environment Variables

Required: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `POSTGRES_PASSWORD`

Optional with defaults:
- `AWS_REGION=eu-west-2`, `S3_BUCKET=cupa-cost-usage-combined`
- `POSTGRES_HOST=localhost`, `POSTGRES_PORT=5432`, `POSTGRES_DB=postgres`
- `POSTGRES_SCHEMA=cost_analytics`, `LOG_LEVEL=INFO`
- `CUR_PATHS` - Comma-separated S3 prefixes for CUR data
- `SYNC_SCHEDULE=0 2 * * *` - Cron schedule for Docker container

See `.env.example` for full list.

## Extending for New Cloud Providers

1. Create `src/sources/gcp_billing.py` - implement `BaseSource` interface (extract generator, get_source_name)
2. Add column mapping in `src/transforms/normalize.py` (see `AWS_CUR_COLUMN_MAPPING`)
3. The `costs` view auto-unions all `*_normalized` tables

## Infrastructure Notes

- **Behind Cloudflare** - All external traffic routes through Cloudflare
- **PostgreSQL access**:
  - Docker internal: `supabase-db-f8wkccg888sggk4gw000wk40:5432`
  - External: SSH tunnel to EC2 → `localhost:5432`
  - UI: https://supabase.cambridgedevedu.org
- **Deployment**: Coolify on EC2
- **Docker network**: Must join `f8wkccg888sggk4gw000wk40` (Supabase network)
  - Set in Coolify UI: Configuration → Advanced → Docker Network
  - Or manually: `docker network connect f8wkccg888sggk4gw000wk40 <container>`
- **Coolify app UUID**: `bsgk044gko04cwsgg84sc0oc`
