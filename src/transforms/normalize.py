"""Normalize cloud cost data to common schema."""

import logging
from datetime import datetime
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)

# Normalized schema columns
NORMALIZED_COLUMNS = [
    "date",
    "account_id",
    "service",
    "region",
    "cost",
    "currency",
    "cloud_provider",
    "source_table",
    "sync_timestamp",
]

# AWS CUR column mapping to normalized schema
AWS_CUR_COLUMN_MAPPING = {
    "line_item_usage_start_date": "date",
    "line_item_usage_account_id": "account_id",
    "product_servicename": "service",
    "product_region": "region",
    "line_item_unblended_cost": "cost",
    "line_item_currency_code": "currency",
}

# Alternative column names (CUR schema varies)
AWS_CUR_COLUMN_ALTERNATIVES = {
    "date": ["lineitem_usagestartdate", "usage_start_date"],
    "account_id": ["lineitem_usageaccountid", "usage_account_id", "bill_payeraccountid"],
    "service": ["product_productname", "lineitem_productcode", "product_name"],
    "region": ["product_location", "lineitem_availabilityzone"],
    "cost": ["lineitem_unblendedcost", "unblended_cost", "lineitem_blendedcost"],
    "currency": ["lineitem_currencycode", "currency_code"],
}


def normalize_aws_cur(
    df: pd.DataFrame,
    source_table: str,
    sync_timestamp: Optional[datetime] = None,
) -> pd.DataFrame:
    """Normalize AWS CUR data to common schema.

    Args:
        df: Raw AWS CUR DataFrame
        source_table: Name of the source table for tracking
        sync_timestamp: Timestamp of this sync (defaults to now)

    Returns:
        Normalized DataFrame with standard columns
    """
    if df.empty:
        return pd.DataFrame(columns=NORMALIZED_COLUMNS)

    if sync_timestamp is None:
        sync_timestamp = datetime.utcnow()

    # Lowercase all column names for consistent matching
    df.columns = [col.lower().replace("/", "_").replace(":", "_") for col in df.columns]

    normalized = pd.DataFrame()

    # Map columns using primary mapping and alternatives
    for norm_col, cur_col in AWS_CUR_COLUMN_MAPPING.items():
        cur_col_lower = cur_col.lower().replace("/", "_").replace(":", "_")

        if cur_col_lower in df.columns:
            normalized[norm_col] = df[cur_col_lower]
        else:
            # Try alternatives
            found = False
            alternatives = AWS_CUR_COLUMN_ALTERNATIVES.get(norm_col, [])
            for alt in alternatives:
                alt_lower = alt.lower().replace("/", "_").replace(":", "_")
                if alt_lower in df.columns:
                    normalized[norm_col] = df[alt_lower]
                    found = True
                    logger.debug(f"Used alternative column {alt} for {norm_col}")
                    break

            if not found:
                logger.warning(f"Column not found for {norm_col}, using NULL")
                normalized[norm_col] = None

    # Add metadata columns
    normalized["cloud_provider"] = "aws"
    normalized["source_table"] = source_table
    normalized["sync_timestamp"] = sync_timestamp

    # Convert date column to proper datetime
    if "date" in normalized.columns and normalized["date"] is not None:
        try:
            normalized["date"] = pd.to_datetime(normalized["date"]).dt.date
        except Exception as e:
            logger.warning(f"Could not convert date column: {e}")

    # Ensure cost is numeric
    if "cost" in normalized.columns:
        normalized["cost"] = pd.to_numeric(normalized["cost"], errors="coerce")

    logger.info(f"Normalized {len(normalized)} rows from {source_table}")
    return normalized


def create_normalized_view_sql(schema: str, source_tables: list[str]) -> str:
    """Generate SQL to create the unified costs view.

    Args:
        schema: PostgreSQL schema name
        source_tables: List of raw table names to union

    Returns:
        SQL string to create/replace the view
    """
    unions = []
    for table in source_tables:
        unions.append(f"""
            SELECT
                date,
                account_id,
                service,
                region,
                cost,
                currency,
                cloud_provider,
                source_table,
                sync_timestamp
            FROM {schema}.{table}_normalized
        """)

    union_sql = "\n            UNION ALL\n".join(unions)

    return f"""
        CREATE OR REPLACE VIEW {schema}.costs AS
        {union_sql};
    """
