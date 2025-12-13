"""PostgreSQL data loader."""

import logging
from typing import Optional
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from config import PostgresConfig

logger = logging.getLogger(__name__)


class PostgreSQLLoader:
    """Load data into PostgreSQL tables."""

    def __init__(self, config: PostgresConfig):
        """Initialize PostgreSQL loader.

        Args:
            config: PostgreSQL connection configuration
        """
        self.config = config
        self._conn = None

    def _get_connection(self):
        """Get PostgreSQL connection."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
            )
        return self._conn

    def ensure_schema(self):
        """Ensure the target schema exists."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.schema}")
        conn.commit()
        logger.info(f"Ensured schema exists: {self.config.schema}")

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        chunk_size: int = 10000,
    ) -> int:
        """Load DataFrame into PostgreSQL table.

        Args:
            df: Data to load
            table_name: Target table name (without schema)
            if_exists: How to handle existing table ('replace', 'append', 'fail')
            chunk_size: Number of rows to insert per batch

        Returns:
            Number of rows loaded
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping load to {table_name}")
            return 0

        full_table_name = f"{self.config.schema}.{table_name}"
        conn = self._get_connection()

        # Clean column names (PostgreSQL doesn't like special characters)
        df.columns = [self._clean_column_name(col) for col in df.columns]

        try:
            with conn.cursor() as cur:
                # Handle existing table
                if if_exists == "replace":
                    cur.execute(f"DROP TABLE IF EXISTS {full_table_name} CASCADE")
                    logger.info(f"Dropped existing table: {full_table_name}")

                # Create table from DataFrame schema
                self._create_table_from_df(cur, df, full_table_name)

                # Insert data in chunks
                columns = list(df.columns)
                total_rows = 0

                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i : i + chunk_size]
                    values = [tuple(row) for row in chunk.values]

                    insert_query = f"""
                        INSERT INTO {full_table_name} ({', '.join(columns)})
                        VALUES %s
                    """
                    execute_values(cur, insert_query, values, page_size=chunk_size)
                    total_rows += len(chunk)
                    logger.info(f"Inserted {total_rows} rows into {full_table_name}")

            conn.commit()
            logger.info(f"Successfully loaded {total_rows} rows into {full_table_name}")
            return total_rows

        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading data into {full_table_name}: {e}")
            raise

    def _clean_column_name(self, name: str) -> str:
        """Clean column name for PostgreSQL compatibility."""
        # Replace problematic characters
        clean = name.lower()
        clean = clean.replace("/", "_")
        clean = clean.replace(":", "_")
        clean = clean.replace("-", "_")
        clean = clean.replace(" ", "_")
        clean = clean.replace(".", "_")

        # Ensure it starts with a letter or underscore
        if clean[0].isdigit():
            clean = f"_{clean}"

        return clean

    def _create_table_from_df(self, cursor, df: pd.DataFrame, table_name: str):
        """Create table based on DataFrame schema."""
        type_mapping = {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "float64": "DOUBLE PRECISION",
            "float32": "REAL",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "object": "TEXT",
        }

        columns = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            pg_type = type_mapping.get(dtype, "TEXT")
            columns.append(f'"{col}" {pg_type}')

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            )
        """
        cursor.execute(create_sql)
        logger.debug(f"Created table: {table_name}")

    def execute_sql_file(self, filepath: str):
        """Execute SQL from a file."""
        with open(filepath, "r") as f:
            sql = f.read()

        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info(f"Executed SQL file: {filepath}")

    def close(self):
        """Close PostgreSQL connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
