"""AWS Cost & Usage Report source using DuckDB for S3 parquet reading."""

import logging
from typing import Generator
import duckdb
import pandas as pd

from sources.base import BaseSource
from config import AWSConfig

logger = logging.getLogger(__name__)


class AWSCURSource(BaseSource):
    """Read AWS CUR parquet files from S3 using DuckDB."""

    def __init__(self, config: AWSConfig, path: str, table_name: str):
        """Initialize AWS CUR source.

        Args:
            config: AWS configuration with credentials
            path: S3 path prefix for CUR data (e.g., 'cup/CUP-Cost-Usage-Report/')
            table_name: Name for the destination table
        """
        self.config = config
        self.path = path
        self.table_name = table_name
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

    def extract(self) -> Generator[pd.DataFrame, None, None]:
        """Extract CUR data from S3 parquet files.

        Yields DataFrames in chunks to handle large datasets efficiently.
        """
        conn = self._get_connection()
        s3_uri = self.get_s3_uri()

        logger.info(f"Reading parquet files from: {s3_uri}")

        try:
            # Count total files for progress logging
            count_query = f"""
                SELECT COUNT(*) as file_count
                FROM glob('{s3_uri}')
            """
            file_count = conn.execute(count_query).fetchone()[0]
            logger.info(f"Found {file_count} parquet files to process")

            if file_count == 0:
                logger.warning(f"No parquet files found at {s3_uri}")
                return

            # Read all parquet files into a single query
            # DuckDB handles this efficiently without loading all into memory
            query = f"""
                SELECT *
                FROM read_parquet('{s3_uri}', union_by_name=true)
            """

            # Fetch in chunks to handle large datasets
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

                # If we got fewer rows than chunk_size, we're done
                if len(df) < chunk_size:
                    break

        except Exception as e:
            logger.error(f"Error extracting data from {s3_uri}: {e}")
            raise

    def extract_all(self) -> pd.DataFrame:
        """Extract all data into a single DataFrame.

        Use with caution for large datasets - prefer extract() generator.
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
