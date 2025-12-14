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

## Querying Cost Data

```sql
-- Total costs by service (last 30 days)
SELECT service, SUM(cost) as total_cost, currency
FROM cost_analytics.costs
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY service, currency
ORDER BY total_cost DESC;

-- Daily spend trend
SELECT date, SUM(cost) as daily_cost
FROM cost_analytics.costs
GROUP BY date
ORDER BY date DESC;

-- Costs by account
SELECT account_id, SUM(cost) as total_cost
FROM cost_analytics.costs
GROUP BY account_id;

-- Raw data has 100+ columns for detailed analysis
SELECT * FROM cost_analytics.raw_cup LIMIT 10;
```

Access via:
- **Supabase Studio**: https://supabase.cambridgedevedu.org
- **Direct SQL**: Connect to postgres via SSH tunnel
- **Grafana**: Connect as postgres datasource (TODO)

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
- **Redeploy**: Coolify auto-deploys on git push, or manually trigger in UI
- **Logs**: Coolify UI → Application → Logs, or `docker logs poet-cloud-cost-etl`

## Lessons Learned

- Coolify API requires Cloudflare Access bypass (service token) for external access
- Test Docker builds locally before deploying (start Docker Desktop)
- Container must join Supabase network manually if not set in Coolify UI
- CLI uses typer subcommands - always specify `sync` in entrypoint
- **DuckDB loads entire query result into memory** - even with filters, large datasets crash small servers
- Current EC2 (16GB) cannot handle full CUR dataset - need streaming/chunked approach

## TODO: Before Next Deployment

1. **Fix memory issue**: Refactor `extract_filtered()` to stream results instead of `fetchdf()` all at once
2. **Test locally first**: Start Docker Desktop, build image, run with SSH tunnel to postgres
3. **Start small**: Test with 1 account, 1 month before scaling up
4. **Consider alternatives**:
   - Process on larger instance (t4g.2xlarge = 32GB)
   - Use Athena to query S3 directly, only ETL aggregated summaries
   - Pre-aggregate in DuckDB before loading to postgres
