"""POET Cloud Cost ETL - Main entry point."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import typer
from rich.console import Console
from rich.logging import RichHandler

from config import Config
from sources import AWSCURSource
from loaders import PostgreSQLLoader
from transforms import normalize_aws_cur, create_normalized_view_sql
from accounts import get_account_ids, ACCOUNTS

# Setup logging
console = Console()
app = typer.Typer(help="POET Cloud Cost ETL - Ingest cloud cost data into PostgreSQL")


def setup_logging(level: str = "INFO"):
    """Configure logging with rich handler."""
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


@app.command()
def sync(
    months: int = typer.Option(1, "--months", "-m", help="Number of months to sync (default: 1)"),
    raw_only: bool = typer.Option(False, "--raw-only", help="Only load raw data, skip normalization"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without loading"),
    all_accounts: bool = typer.Option(False, "--all-accounts", help="Sync all accounts (ignore filter)"),
):
    """Run the ETL sync process with account and date filtering."""
    try:
        config = Config.from_env()
        setup_logging(config.log_level)
        logger = logging.getLogger(__name__)

        # Determine account filter
        account_ids = None if all_accounts else get_account_ids()
        account_count = "all" if all_accounts else len(account_ids)

        logger.info("Starting POET Cloud Cost ETL sync")
        logger.info(f"Months to sync: {months}")
        logger.info(f"Accounts to filter: {account_count}")
        logger.info(f"Target: {config.postgres.host}:{config.postgres.port}/{config.postgres.database}")
        logger.info(f"Schema: {config.postgres.schema}")

        if dry_run:
            logger.info("DRY RUN - No data will be loaded")
            if account_ids:
                logger.info(f"Account IDs: {account_ids[:5]}... ({len(account_ids)} total)")

        # Initialize loader (skip for dry-run)
        loader = None
        if not dry_run:
            loader = PostgreSQLLoader(config.postgres)
            loader.ensure_schema()

        processed_tables = []
        sync_timestamp = datetime.utcnow()
        total_rows = 0

        for path in config.aws.cur_paths:
            table_name = _path_to_table_name(path)
            logger.info(f"Processing: {path} -> {table_name}")

            # Initialize source with filters
            source = AWSCURSource(
                config.aws,
                path,
                table_name,
                account_ids=account_ids,
                months_back=months,
            )

            try:
                if dry_run:
                    partitions = source.get_month_partitions()
                    logger.info(f"Would extract from partitions: {partitions}")
                    logger.info(f"Filter: {source._build_where_clause()}")
                    continue

                # Use filtered extraction
                logger.info("Extracting filtered data from S3...")
                df = source.extract_filtered()

                if df.empty:
                    logger.warning(f"No data extracted from {path}")
                    continue

                logger.info(f"Extracted {len(df)} rows with {len(df.columns)} columns")

                # Load raw data
                raw_table = f"raw_{table_name}"
                rows = loader.load(df, raw_table, if_exists="replace")
                total_rows += rows
                logger.info(f"Loaded {rows} rows to {raw_table}")

                # Normalize and load
                if not raw_only:
                    logger.info("Normalizing data...")
                    normalized_df = normalize_aws_cur(df, table_name, sync_timestamp)

                    norm_table = f"{table_name}_normalized"
                    norm_rows = loader.load(normalized_df, norm_table, if_exists="replace")
                    logger.info(f"Loaded {norm_rows} normalized rows to {norm_table}")
                    processed_tables.append(table_name)

            finally:
                source.close()

        # Create unified costs view
        if processed_tables and not raw_only and not dry_run:
            logger.info("Creating unified costs view...")
            view_sql = create_normalized_view_sql(config.postgres.schema, processed_tables)
            conn = loader._get_connection()
            with conn.cursor() as cur:
                cur.execute(view_sql)
            conn.commit()
            logger.info("Created costs view successfully")

        if loader:
            loader.close()

        logger.info(f"Sync completed. Total rows loaded: {total_rows}")
        console.print(f"\n[bold green]Sync completed successfully![/bold green]")
        console.print(f"Total rows: {total_rows}")
        console.print(f"Tables: {', '.join(processed_tables) if processed_tables else 'None'}")

    except Exception as e:
        console.print(f"\n[bold red]Error: {e}[/bold red]")
        logging.exception("Sync failed")
        sys.exit(1)


@app.command()
def list_accounts():
    """List configured account IDs and names."""
    console.print("\n[bold]Configured Accounts:[/bold]\n")
    for account_id, config in ACCOUNTS.items():
        name = config.get("name", "unknown")
        region = config.get("region_filter", "")
        region_str = f" [dim](region: {region})[/dim]" if region else ""
        console.print(f"  {account_id}  {name}{region_str}")
    console.print(f"\n[dim]Total: {len(ACCOUNTS)} accounts[/dim]")


@app.command()
def test_connection():
    """Test database connection."""
    try:
        config = Config.from_env()
        setup_logging(config.log_level)
        logger = logging.getLogger(__name__)

        logger.info("Testing PostgreSQL connection...")
        loader = PostgreSQLLoader(config.postgres)
        conn = loader._get_connection()

        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]

        console.print(f"[green]Connected successfully![/green]")
        console.print(f"PostgreSQL version: {version[:50]}...")
        loader.close()

    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/red]")
        sys.exit(1)


@app.command()
def test_s3():
    """Test S3 access and list available parquet files."""
    try:
        config = Config.from_env()
        setup_logging(config.log_level)
        logger = logging.getLogger(__name__)

        logger.info("Testing S3 access...")

        for path in config.aws.cur_paths:
            source = AWSCURSource(config.aws, path, "test")
            console.print(f"\n[bold]Path: {path}[/bold]")
            console.print(f"S3 URI: {source.get_s3_uri()}")

            conn = source._get_connection()
            count = conn.execute(f"""
                SELECT COUNT(*) FROM glob('{source.get_s3_uri()}')
            """).fetchone()[0]

            console.print(f"[green]Found {count} parquet files[/green]")
            source.close()

    except Exception as e:
        console.print(f"[red]S3 access failed: {e}[/red]")
        sys.exit(1)


def _path_to_table_name(path: str) -> str:
    """Convert S3 path to a valid PostgreSQL table name."""
    parts = [p for p in path.split("/") if p]

    if not parts:
        return "unknown"

    name = parts[0]
    name = name.lower()
    name = name.replace("-", "_")

    if name.isdigit():
        name = f"account_{name}"

    return name


if __name__ == "__main__":
    app()
