"""AWS Cost & Usage Report source using DuckDB for S3 parquet reading."""

import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Generator, List, Optional
import duckdb
import pandas as pd

from sources.base import BaseSource
from config import AWSConfig
from accounts import ACCOUNTS, get_account_ids

logger = logging.getLogger(__name__)


class AWSCURSource(BaseSource):
    """Read AWS CUR parquet files from S3 using DuckDB."""

    def __init__(
        self,
        config: AWSConfig,
        path: str,
        table_name: str,
        account_ids: Optional[List[str]] = None,
        months_back: int = 1,
    ):
        """Initialize AWS CUR source.

        Args:
            config: AWS configuration with credentials
            path: S3 path prefix for CUR data (e.g., 'cup/CUP-Cost-Usage-Report/')
            table_name: Name for the destination table
            account_ids: List of account IDs to filter (None = use all from accounts.py)
            months_back: How many months of data to fetch (default 1)
        """
        self.config = config
        self.path = path
        self.table_name = table_name
        self.account_ids = account_ids or get_account_ids()
        self.months_back = months_back
        self._conn = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get DuckDB connection with S3 configured."""
        if self._conn is None:
            self._conn = duckdb.connect()
            self._conn.execute("INSTALL httpfs; LOAD httpfs;")
            self._conn.execute(f"""
                SET s3_region = '{self.config.region}';
                SET s3_access_key_id = '{self.config.access_key_id}';
                SET s3_secret_access_key = '{self.config.secret_access_key}';
            """)
        return self._conn

    def get_source_name(self) -> str:
        """Return source identifier."""
        return f"aws_cur:{self.table_name}"

    def get_s3_uri(self) -> str:
        """Build full S3 URI with glob pattern for parquet files."""
        return f"s3://{self.config.s3_bucket}/{self.path}**/*.parquet"

    def get_s3_uri_for_month(self, year: int, month: int) -> str:
        """Build S3 URI for a specific month partition."""
        return f"s3://{self.config.s3_bucket}/{self.path}year={year}/month={month}/*.parquet"

    def get_month_partitions(self) -> List[tuple]:
        """Get list of (year, month) tuples to process based on months_back."""
        partitions = []
        now = datetime.now()
        for i in range(self.months_back):
            dt = now - relativedelta(months=i)
            partitions.append((dt.year, dt.month))
        return partitions

    def _build_account_filter(self) -> str:
        """Build SQL WHERE clause for account filtering."""
        if not self.account_ids:
            return ""

        account_list = ", ".join(f"'{aid}'" for aid in self.account_ids)
        return f"line_item_usage_account_id IN ({account_list})"

    def _build_region_filters(self) -> str:
        """Build SQL for accounts with region restrictions."""
        region_conditions = []
        for account_id, config in ACCOUNTS.items():
            if "region_filter" in config and account_id in self.account_ids:
                region = config["region_filter"]
                region_conditions.append(
                    f"(line_item_usage_account_id = '{account_id}' AND product_region = '{region}')"
                )

        if not region_conditions:
            return ""

        # Accounts without region filter OR accounts with matching region
        accounts_with_region = [aid for aid, cfg in ACCOUNTS.items() if "region_filter" in cfg]
        accounts_without_region = [aid for aid in self.account_ids if aid not in accounts_with_region]

        if accounts_without_region:
            without_list = ", ".join(f"'{aid}'" for aid in accounts_without_region)
            region_conditions.append(f"line_item_usage_account_id IN ({without_list})")

        return " OR ".join(region_conditions)

    def _build_where_clause(self) -> str:
        """Build complete WHERE clause with all filters."""
        region_filter = self._build_region_filters()
        if region_filter:
            return f"WHERE ({region_filter})"

        account_filter = self._build_account_filter()
        if account_filter:
            return f"WHERE {account_filter}"

        return ""

    def extract_filtered(self) -> pd.DataFrame:
        """Extract filtered data for specific accounts and date range.

        This is the recommended method - filters at query time to minimize memory.
        """
        conn = self._get_connection()
        partitions = self.get_month_partitions()

        logger.info(f"Processing {len(partitions)} month(s) for {len(self.account_ids)} accounts")
        logger.info(f"Months: {partitions}")

        all_dfs = []
        where_clause = self._build_where_clause()

        for year, month in partitions:
            s3_uri = self.get_s3_uri_for_month(year, month)
            logger.info(f"Reading: year={year}, month={month}")

            try:
                # Check if partition exists
                count_query = f"SELECT COUNT(*) FROM glob('{s3_uri}')"
                file_count = conn.execute(count_query).fetchone()[0]

                if file_count == 0:
                    logger.warning(f"No files for year={year}, month={month}")
                    continue

                logger.info(f"Found {file_count} parquet files for {year}-{month:02d}")

                # Query with filters pushed to DuckDB
                query = f"""
                    SELECT *
                    FROM read_parquet('{s3_uri}', union_by_name=true)
                    {where_clause}
                """

                df = conn.execute(query).fetchdf()
                logger.info(f"Extracted {len(df)} rows for {year}-{month:02d}")

                if not df.empty:
                    all_dfs.append(df)

            except Exception as e:
                logger.error(f"Error processing {year}-{month:02d}: {e}")
                continue

        if not all_dfs:
            logger.warning("No data extracted from any partition")
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Total extracted: {len(result)} rows")
        return result

    def extract(self) -> Generator[pd.DataFrame, None, None]:
        """Extract CUR data from S3 parquet files.

        Yields DataFrames in chunks to handle large datasets efficiently.
        """
        conn = self._get_connection()
        s3_uri = self.get_s3_uri()

        logger.info(f"Reading parquet files from: {s3_uri}")

        try:
            count_query = f"""
                SELECT COUNT(*) as file_count
                FROM glob('{s3_uri}')
            """
            file_count = conn.execute(count_query).fetchone()[0]
            logger.info(f"Found {file_count} parquet files to process")

            if file_count == 0:
                logger.warning(f"No parquet files found at {s3_uri}")
                return

            query = f"""
                SELECT *
                FROM read_parquet('{s3_uri}', union_by_name=true)
            """

            chunk_size = 100000
            offset = 0

            while True:
                chunk_query = f"""
                    SELECT * FROM ({query}) LIMIT {chunk_size} OFFSET {offset}
                """
                df = conn.execute(chunk_query).fetchdf()

                if df.empty:
                    break

                logger.info(f"Extracted chunk: {len(df)} rows (offset {offset})")
                yield df

                offset += chunk_size

                if len(df) < chunk_size:
                    break

        except Exception as e:
            logger.error(f"Error extracting data from {s3_uri}: {e}")
            raise

    def extract_all(self) -> pd.DataFrame:
        """Extract all data into a single DataFrame.

        Use with caution for large datasets - prefer extract_filtered().
        """
        conn = self._get_connection()
        s3_uri = self.get_s3_uri()

        logger.info(f"Reading all parquet files from: {s3_uri}")

        query = f"""
            SELECT *
            FROM read_parquet('{s3_uri}', union_by_name=true)
        """

        return conn.execute(query).fetchdf()

    def close(self):
        """Close DuckDB connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
