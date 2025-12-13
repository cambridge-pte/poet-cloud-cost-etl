"""Configuration management via environment variables."""

import os
from dataclasses import dataclass
from typing import List


@dataclass
class AWSConfig:
    access_key_id: str
    secret_access_key: str
    region: str
    s3_bucket: str
    cur_paths: List[str]


@dataclass
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class Config:
    aws: AWSConfig
    postgres: PostgresConfig
    log_level: str

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        # Parse CUR paths from comma-separated string
        cur_paths_str = os.getenv("CUR_PATHS", "")
        cur_paths = [p.strip() for p in cur_paths_str.split(",") if p.strip()]

        return cls(
            aws=AWSConfig(
                access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                region=os.getenv("AWS_REGION", "eu-west-2"),
                s3_bucket=os.getenv("S3_BUCKET", "cupa-cost-usage-combined"),
                cur_paths=cur_paths,
            ),
            postgres=PostgresConfig(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DB", "postgres"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.environ["POSTGRES_PASSWORD"],
                schema=os.getenv("POSTGRES_SCHEMA", "cost_analytics"),
            ),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
