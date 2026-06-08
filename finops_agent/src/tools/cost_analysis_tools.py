"""Cost analysis tools — slice and dice cost data.

All functions take a SparkSession and return Python-native dicts/lists
(not Spark DataFrames) so they can be passed to the LLM or serialized to JSON.
"""
from __future__ import annotations

import logging
from typing import Optional

from src.utils.sql_helpers import df_to_records, table_exists

logger = logging.getLogger(__name__)


# ============================================================
# 1. Total cost over a date range
# ============================================================
def get_total_cost(
    spark,
    catalog: str,
    days: int = 7,
    workspace_id: Optional[str] = None,
) -> dict:
    """Total cost over the last N days.

    Returns:
        {
            "total_cost_usd": float,
            "days": int,
            "avg_daily_cost_usd": float,
            "first_date": str,
            "last_date": str,
            "workspace_id": Optional[str],
        }
    """
    fqn = f"{catalog}.gold.daily_cost"
    if not table_exists(spark, fqn):
        logger.warning("Table %s does not exist", fqn)
        return {"total_cost_usd": 0.0, "days": days, "error": f"Table {fqn} not found"}

    where = f"usage_date >= DATE_SUB(CURRENT_DATE(), {days})"
    if workspace_id:
        where += f" AND workspace_id = '{workspace_id}'"

    sql = f"""
    SELECT
        ROUND(SUM(total_cost_usd), 2)                AS total_cost_usd,
        ROUND(SUM(total_cost_usd) / {days}, 2)       AS avg_daily_cost_usd,
        MIN(usage_date)                              AS first_date,
        MAX(usage_date)                              AS last_date,
        COUNT(DISTINCT usage_date)                   AS days_with_data
    FROM {fqn}
    WHERE {where}
    """
    rows = df_to_records(spark.sql(sql))
    result = rows[0] if rows else {}
    result.update({"days": days, "workspace_id": workspace_id})
    logger.info("get_total_cost: %s", result)
    return result


# ============================================================
# 2. Cost breakdown by dimension (sku, user, team, etc.)
# ============================================================
def get_cost_breakdown_by_dimension(
    spark,
    catalog: str,
    dimension: str,
    days: int = 7,
    top_n: int = 10,
) -> list[dict]:
    """Cost breakdown by a dimension. Supported dimensions:
    - 'sku' (sku_name)
    - 'product' (billing_origin_product)
    - 'user' (run_as_user)
    - 'team' (tag_team)
    - 'project' (tag_project)
    - 'environment' (tag_environment)
    - 'workspace' (workspace_id)
    """
    silver_fqn = f"{catalog}.silver.usage_enriched"
    if not table_exists(spark, silver_fqn):
        return [{"error": f"Table {silver_fqn} not found"}]

    dimension_map = {
        "sku":         "sku_name",
        "product":     "billing_origin_product",
        "user":        "COALESCE(run_as_user, owned_by, tag_owner, 'unknown')",
        "team":        "tag_team",
        "project":     "tag_project",
        "environment": "tag_environment",
        "workspace":   "workspace_id",
    }
    if dimension not in dimension_map:
        raise ValueError(f"Unsupported dimension '{dimension}'. Use one of: {list(dimension_map)}")

    col_expr = dimension_map[dimension]
    sql = f"""
    SELECT
        {col_expr}                              AS dimension_value,
        ROUND(SUM(estimated_cost_usd), 2)       AS total_cost_usd,
        ROUND(SUM(usage_quantity), 2)           AS total_usage,
        COUNT(*)                                AS record_count
    FROM {silver_fqn}
    WHERE usage_date >= DATE_SUB(CURRENT_DATE(), {days})
    GROUP BY {col_expr}
    HAVING SUM(estimated_cost_usd) > 0
    ORDER BY total_cost_usd DESC
    LIMIT {top_n}
    """
    rows = df_to_records(spark.sql(sql))
    for r in rows:
        r["dimension"] = dimension
    logger.info("get_cost_breakdown_by_dimension(%s): %d rows", dimension, len(rows))
    return rows


# ============================================================
# 3. Top spenders (users, resources, etc.)
# ============================================================
def get_top_spenders(
    spark,
    catalog: str,
    spender_type: str = "user",  # "user" | "resource"
    days: int = 7,
    top_n: int = 10,
) -> list[dict]:
    """Top spenders by user or resource."""
    if spender_type == "user":
        fqn = f"{catalog}.gold.cost_by_user"
        if not table_exists(spark, fqn):
            return [{"error": f"Table {fqn} not found"}]
        sql = f"""
        SELECT
            user_email,
            ROUND(SUM(total_cost_usd), 2)       AS total_cost_usd,
            ROUND(AVG(total_cost_usd), 2)       AS avg_daily_cost_usd,
            SUM(distinct_clusters)              AS cluster_uses,
            SUM(distinct_warehouses)            AS warehouse_uses,
            SUM(distinct_jobs)                  AS job_uses
        FROM {fqn}
        WHERE usage_date >= DATE_SUB(CURRENT_DATE(), {days})
        GROUP BY user_email
        HAVING total_cost_usd > 0
        ORDER BY total_cost_usd DESC
        LIMIT {top_n}
        """
    elif spender_type == "resource":
        fqn = f"{catalog}.gold.cost_by_resource"
        if not table_exists(spark, fqn):
            return [{"error": f"Table {fqn} not found"}]
        sql = f"""
        SELECT
            resource_type,
            resource_id,
            ROUND(SUM(total_cost_usd), 2)   AS total_cost_usd,
            ROUND(AVG(total_cost_usd), 2)   AS avg_daily_cost_usd,
            COUNT(DISTINCT usage_date)      AS active_days
        FROM {fqn}
        WHERE usage_date >= DATE_SUB(CURRENT_DATE(), {days})
          AND resource_id != 'unknown'
        GROUP BY resource_type, resource_id
        HAVING total_cost_usd > 0
        ORDER BY total_cost_usd DESC
        LIMIT {top_n}
        """
    else:
        raise ValueError(f"Unknown spender_type: {spender_type}")

    rows = df_to_records(spark.sql(sql))
    logger.info("get_top_spenders(%s): %d rows", spender_type, len(rows))
    return rows


# ============================================================
# 4. Cost trend over time
# ============================================================
def get_cost_trend(
    spark,
    catalog: str,
    days: int = 30,
    granularity: str = "day",   # "day" | "week" | "month"
) -> list[dict]:
    """Cost time series."""
    fqn = f"{catalog}.gold.daily_cost"
    if not table_exists(spark, fqn):
        return [{"error": f"Table {fqn} not found"}]

    if granularity == "day":
        time_expr = "usage_date"
    elif granularity == "week":
        time_expr = "DATE_TRUNC('WEEK', usage_date)"
    elif granularity == "month":
        time_expr = "DATE_TRUNC('MONTH', usage_date)"
    else:
        raise ValueError(f"Bad granularity: {granularity}")

    sql = f"""
    SELECT
        {time_expr}                          AS period,
        ROUND(SUM(total_cost_usd), 2)        AS total_cost_usd,
        SUM(record_count)                    AS record_count
    FROM {fqn}
    WHERE usage_date >= DATE_SUB(CURRENT_DATE(), {days})
    GROUP BY {time_expr}
    ORDER BY period
    """
    rows = df_to_records(spark.sql(sql))

    # Compute period-over-period delta
    for i in range(1, len(rows)):
        prev = rows[i - 1].get("total_cost_usd") or 0
        curr = rows[i].get("total_cost_usd") or 0
        if prev > 0:
            rows[i]["pct_change_vs_prior"] = round((curr - prev) / prev * 100, 1)
        else:
            rows[i]["pct_change_vs_prior"] = None

    logger.info("get_cost_trend(%s, %dd): %d rows", granularity, days, len(rows))
    return rows