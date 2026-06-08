"""Anomaly detection tools — find cost spikes, idle resources, etc."""
from __future__ import annotations

import logging
from typing import Optional

from src.config.thresholds import THRESHOLDS
from src.utils.sql_helpers import df_to_records, table_exists

logger = logging.getLogger(__name__)


# ============================================================
# 1. Cost spikes (vs rolling baseline)
# ============================================================
def detect_cost_spikes(
    spark,
    catalog: str,
    lookback_days: int = 14,
    spike_threshold_pct: float = THRESHOLDS.cost_spike_pct,
    min_daily_usd: float = THRESHOLDS.anomaly_min_daily_usd,
    dimension: str = "workspace",   # "workspace" | "sku" | "resource"
) -> list[dict]:
    """Detect days where cost spiked >threshold% above 7-day rolling average.

    Strategy:
    1. For each (dimension, date), compute cost
    2. For each row, compute prior-7-day average
    3. Flag if today's cost > avg * (1 + threshold)

    Returns one row per (dimension, anomalous date).
    """
    silver_fqn = f"{catalog}.silver.usage_enriched"
    if not table_exists(spark, silver_fqn):
        return [{"error": f"Table {silver_fqn} not found"}]

    if dimension == "workspace":
        group_col = "workspace_id"
    elif dimension == "sku":
        group_col = "sku_name"
    elif dimension == "resource":
        group_col = "COALESCE(cluster_id, warehouse_id, job_id, 'unknown')"
    else:
        raise ValueError(f"Bad dimension: {dimension}")

    sql = f"""
    WITH daily AS (
        SELECT
            usage_date,
            {group_col} AS dim_value,
            SUM(estimated_cost_usd) AS daily_cost_usd
        FROM {silver_fqn}
        WHERE usage_date >= DATE_SUB(CURRENT_DATE(), {lookback_days})
        GROUP BY usage_date, {group_col}
    ),
    with_baseline AS (
        SELECT
            usage_date,
            dim_value,
            daily_cost_usd,
            AVG(daily_cost_usd) OVER (
                PARTITION BY dim_value
                ORDER BY usage_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS rolling_7d_avg,
            COUNT(daily_cost_usd) OVER (
                PARTITION BY dim_value
                ORDER BY usage_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS baseline_days
        FROM daily
    )
    SELECT
        usage_date,
        dim_value,
        ROUND(daily_cost_usd, 2)        AS daily_cost_usd,
        ROUND(rolling_7d_avg, 2)        AS baseline_avg_usd,
        baseline_days,
        ROUND(((daily_cost_usd - rolling_7d_avg) / NULLIF(rolling_7d_avg, 0)) * 100, 1) AS pct_above_baseline,
        ROUND(daily_cost_usd - rolling_7d_avg, 2) AS delta_usd
    FROM with_baseline
    WHERE daily_cost_usd >= {min_daily_usd}
      AND baseline_days >= 3
      AND rolling_7d_avg > 0
      AND ((daily_cost_usd - rolling_7d_avg) / rolling_7d_avg) * 100 >= {spike_threshold_pct}
    ORDER BY usage_date DESC, delta_usd DESC
    """

    rows = df_to_records(spark.sql(sql))
    for r in rows:
        r["dimension"] = dimension
        r["finding_type"] = "COST_SPIKE"
        delta = r.get("delta_usd") or 0
        if delta >= 5000:
            r["severity"] = "CRITICAL"
        elif delta >= 500:
            r["severity"] = "HIGH"
        elif delta >= 50:
            r["severity"] = "MEDIUM"
        else:
            r["severity"] = "LOW"

    logger.info("detect_cost_spikes(%s): %d findings", dimension, len(rows))
    return rows


# ============================================================
# 2. Idle clusters
# ============================================================
def detect_idle_clusters(
    spark,
    catalog: str,
    days: int = 7,
    min_runtime_hours: float = THRESHOLDS.min_cluster_hours_for_eval,
) -> list[dict]:
    """Find interactive clusters that ran but had little/no actual work.

    Heuristic: A cluster is "idle" if:
    - It accumulated significant DBU usage ($ > min)
    - But had no jobs/queries scheduled against it
    - OR auto_termination is disabled and it's an interactive cluster
    """
    silver_fqn = f"{catalog}.silver.usage_enriched"
    clusters_fqn = f"{catalog}.bronze.clusters"
    if not table_exists(spark, silver_fqn):
        return [{"error": f"Table {silver_fqn} not found"}]

    has_cluster_meta = table_exists(spark, clusters_fqn)

    if has_cluster_meta:
        sql = f"""
        WITH cluster_costs AS (
            SELECT
                cluster_id,
                workspace_id,
                COUNT(DISTINCT usage_date)        AS active_days,
                SUM(estimated_cost_usd)           AS total_cost_usd,
                SUM(usage_quantity)               AS total_dbu
            FROM {silver_fqn}
            WHERE cluster_id IS NOT NULL
              AND usage_date >= DATE_SUB(CURRENT_DATE(), {days})
            GROUP BY cluster_id, workspace_id
        ),
        latest_cluster_meta AS (
            SELECT
                cluster_id,
                cluster_name,
                owned_by,
                worker_node_type,
                driver_node_type,
                worker_count,
                auto_termination_minutes,
                cluster_source,
                tags,
                ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) AS rn
            FROM {clusters_fqn}
        )
        SELECT
            c.cluster_id,
            m.cluster_name,
            c.workspace_id,
            m.owned_by,
            m.worker_node_type,
            m.driver_node_type,
            m.worker_count,
            m.auto_termination_minutes,
            m.cluster_source,
            c.active_days,
            ROUND(c.total_cost_usd, 2)  AS total_cost_usd,
            ROUND(c.total_dbu, 2)       AS total_dbu
        FROM cluster_costs c
        LEFT JOIN latest_cluster_meta m
          ON c.cluster_id = m.cluster_id AND m.rn = 1
        WHERE c.total_cost_usd > 1.0
          AND (
              m.auto_termination_minutes IS NULL
              OR m.auto_termination_minutes = 0
              OR m.auto_termination_minutes > 120
          )
          AND COALESCE(m.cluster_source, 'UI') IN ('UI', 'API')
        ORDER BY c.total_cost_usd DESC
        """
    else:
        sql = f"""
        SELECT
            cluster_id,
            CAST(NULL AS STRING)         AS cluster_name,
            workspace_id,
            CAST(NULL AS STRING)         AS owned_by,
            COUNT(DISTINCT usage_date)   AS active_days,
            ROUND(SUM(estimated_cost_usd), 2) AS total_cost_usd,
            ROUND(SUM(usage_quantity), 2)     AS total_dbu
        FROM {silver_fqn}
        WHERE cluster_id IS NOT NULL
          AND usage_date >= DATE_SUB(CURRENT_DATE(), {days})
        GROUP BY cluster_id, workspace_id
        HAVING total_cost_usd > 5.0
        ORDER BY total_cost_usd DESC
        """

    rows = df_to_records(spark.sql(sql))
    for r in rows:
        r["finding_type"] = "IDLE_CLUSTER_RISK"
        r["severity"] = "HIGH" if (r.get("total_cost_usd") or 0) > 100 else "MEDIUM"
        auto_term = r.get("auto_termination_minutes")
        if auto_term in (None, 0):
            r["reason"] = "Auto-termination disabled"
        elif auto_term > 120:
            r["reason"] = f"Auto-termination too long ({auto_term} min)"
        else:
            r["reason"] = "Long-running interactive cluster"

    logger.info("detect_idle_clusters: %d findings", len(rows))
    return rows


# ============================================================
# 3. Idle warehouses
# ============================================================
def detect_idle_warehouses(
    spark,
    catalog: str,
    days: int = 7,
    underused_threshold_pct: float = THRESHOLDS.warehouse_underused_pct,
) -> list[dict]:
    """Find SQL warehouses that incurred cost but had few queries."""
    silver_fqn = f"{catalog}.silver.usage_enriched"
    if not table_exists(spark, silver_fqn):
        return [{"error": f"Table {silver_fqn} not found"}]

    sql = f"""
    SELECT
        warehouse_id,
        workspace_id,
        COUNT(DISTINCT usage_date)         AS active_days,
        ROUND(SUM(estimated_cost_usd), 2)  AS total_cost_usd,
        ROUND(SUM(usage_quantity), 2)      AS total_dbu
    FROM {silver_fqn}
    WHERE warehouse_id IS NOT NULL
      AND usage_date >= DATE_SUB(CURRENT_DATE(), {days})
    GROUP BY warehouse_id, workspace_id
    HAVING total_cost_usd > 5.0
    ORDER BY total_cost_usd DESC
    """
    rows = df_to_records(spark.sql(sql))
    for r in rows:
        r["finding_type"] = "WAREHOUSE_UNDERUSED_RISK"
        r["severity"] = "MEDIUM"
        r["reason"] = "High cost — verify query volume justifies size & uptime"

    logger.info("detect_idle_warehouses: %d findings", len(rows))
    return rows
