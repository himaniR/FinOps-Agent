"""SQL helper utilities for Spark.

Provides:
- Serverless-aware Spark session bootstrap
- Safe SQL execution with logging
- DataFrame -> dict-of-records conversion
- Common cost-data query builders
- Pagination helpers

Works on:
- Databricks serverless jobs/notebooks (auto-provided `spark`)
- Databricks classic clusters (auto-provided `spark`)
- Local dev via Databricks Connect (serverless or cluster)
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ============================================================
# Spark session bootstrap (serverless-aware)
# ============================================================
def get_spark():
    """Return an active SparkSession, creating one if needed.

    Resolution order:
        1. Active session (Databricks notebook / job — both serverless & classic)
        2. Databricks Connect — serverless mode (local dev)
        3. Databricks Connect — cluster mode (local dev fallback)

    Returns:
        SparkSession

    Raises:
        RuntimeError: if no session can be obtained
    """
    from pyspark.sql import SparkSession

    # 1. Already-active session — covers serverless jobs, classic clusters,
    #    and notebooks where `spark` is pre-injected.
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    # 2. Local dev — try Databricks Connect serverless first
    try:
        from databricks.connect import DatabricksSession
        logger.info("Creating Databricks Connect serverless session")
        return DatabricksSession.builder.serverless().getOrCreate()
    except Exception as e:
        logger.debug("Serverless DatabricksSession unavailable: %s", e)

    # 3. Local dev — fall back to cluster-based Databricks Connect
    try:
        from databricks.connect import DatabricksSession
        logger.info("Creating Databricks Connect cluster session")
        return DatabricksSession.builder.getOrCreate()
    except Exception as e:
        raise RuntimeError(
            "No Spark session available. In Databricks notebooks/jobs the "
            "`spark` object is auto-provided. For local development, install "
            "`databricks-connect>=15.4` and run `databricks auth login`."
        ) from e


# ============================================================
# SQL execution
# ============================================================
def run_sql(spark, query: str, params: Optional[dict] = None) -> Any:
    """Execute a SQL query, returning a Spark DataFrame.

    Args:
        spark: Active SparkSession (use `get_spark()` if you don't have one)
        query: SQL string (use {placeholder} for params, e.g. {catalog})
        params: Dict of substitutions for str.format

    Returns:
        DataFrame
    """
    if params:
        query = query.format(**params)

    logger.info("Running SQL:\n%s", query.strip()[:500])
    return spark.sql(query)


def df_to_records(df, limit: int = 1000) -> list[dict]:
    """Convert a Spark DataFrame to a list of dicts (Python-native).

    Args:
        df: Spark DataFrame
        limit: Max rows to materialize (safety)

    Returns:
        list of dicts, one per row
    """
    pdf = df.limit(limit).toPandas()
    # Convert any numpy types to native Python (for JSON serialization)
    return pdf.astype(object).where(pdf.notnull(), None).to_dict(orient="records")


# ============================================================
# Catalog / schema / table introspection
# ============================================================
def table_exists(spark, fqn: str) -> bool:
    """Check if a table exists. fqn = fully qualified name e.g. 'finops.agent.runs'."""
    try:
        parts = fqn.split(".")
        if len(parts) != 3:
            raise ValueError(f"Expected catalog.schema.table, got: {fqn}")
        catalog, schema, table = parts
        result = spark.sql(
            f"SHOW TABLES IN `{catalog}`.`{schema}` LIKE '{table}'"
        ).collect()
        return len(result) > 0
    except Exception as e:
        logger.warning("table_exists check failed for %s: %s", fqn, e)
        return False


def schema_exists(spark, catalog: str, schema: str) -> bool:
    """Check if a UC schema exists."""
    try:
        result = spark.sql(
            f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'"
        ).collect()
        return len(result) > 0
    except Exception as e:
        logger.warning("schema_exists check failed for %s.%s: %s", catalog, schema, e)
        return False


def safe_count(spark, fqn: str) -> int:
    """Count rows in a table, returning 0 if it doesn't exist."""
    if not table_exists(spark, fqn):
        return 0
    try:
        return spark.sql(f"SELECT COUNT(*) AS c FROM {fqn}").collect()[0]["c"]
    except Exception as e:
        logger.warning("safe_count failed for %s: %s", fqn, e)
        return 0


# ============================================================
# Writes
# ============================================================
def write_table(
    df,
    fqn: str,
    mode: str = "overwrite",
    partition_by: Optional[list] = None,
) -> None:
    """Write a DataFrame to a UC table as Delta.

    Args:
        df: Spark DataFrame
        fqn: catalog.schema.table
        mode: 'overwrite' | 'append' | 'errorifexists'
        partition_by: Optional list of partition columns
    """
    writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(fqn)
    logger.info("✅ Wrote %d rows to %s (mode=%s)", df.count(), fqn, mode)


def merge_into_table(
    spark,
    source_df,
    target_fqn: str,
    merge_keys: list[str],
    update_columns: Optional[list[str]] = None,
) -> None:
    """Upsert (MERGE) a DataFrame into a Delta table.

    Args:
        spark: SparkSession
        source_df: Source DataFrame
        target_fqn: Target table name
        merge_keys: Columns that uniquely identify a row
        update_columns: Columns to update on match (default: all non-key columns)
    """
    temp_view = f"_merge_src_{abs(hash(target_fqn))}"
    source_df.createOrReplaceTempView(temp_view)

    on_clause = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])

    if update_columns is None:
        update_columns = [c for c in source_df.columns if c not in merge_keys]

    update_clause = ", ".join([f"t.{c} = s.{c}" for c in update_columns])
    insert_cols = ", ".join(source_df.columns)
    insert_vals = ", ".join([f"s.{c}" for c in source_df.columns])

    sql = f"""
    MERGE INTO {target_fqn} t
    USING {temp_view} s
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    spark.sql(sql)
    logger.info("✅ MERGE into %s complete", target_fqn)