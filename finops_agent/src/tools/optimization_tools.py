"""Optimization recommendation tools."""
from __future__ import annotations

import logging
from typing import Optional

from src.config.thresholds import THRESHOLDS
from src.utils.sql_helpers import df_to_records, table_exists

logger = logging.getLogger(__name__)


# ============================================================
# 1. Right-sizing
# ============================================================
def recommend_rightsizing(
    spark,
    catalog: str,
    days: int = 14,
    min_cost_usd: float = 10.0,
) -> list[dict]:
    """Identify clusters that look over-provisioned.

    Heuristic: A cluster with high cost but low active days is a candidate.
    (Real utilization data requires Ganglia/cluster metrics — limited in OSS.)
    """
    silver_fqn = f"{catalog}.silver.usage_enriched"
    clusters_fqn = f"{catalog}.bronze.clusters"
    if not table_exists(spark, silver_fqn):
        return []

    has_meta = table_exists(spark, clusters_fqn)
    join_clause = f"""
        LEFT JOIN (
            SELECT cluster_id, cluster_name, owned_by, worker_node_type,
                   driver_node_type, worker_count, tags,
                   ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) AS rn
            FROM {clusters_fqn}
        ) m ON c.cluster_id = m.cluster_id AND m.rn = 1
    """ if has_meta else ""

    meta_cols = """
        m.cluster_name, m.owned_by, m.worker_node_type,
        m.driver_node_type, m.worker_count
    """ if has_meta else """
        CAST(NULL AS STRING) AS cluster_name,
        CAST(NULL AS STRING) AS owned_by,
        CAST(NULL AS STRING) AS worker_node_type,
        CAST(NULL AS STRING) AS driver_node_type,
        CAST(NULL AS INT)    AS worker_count
    """

    sql = f"""
    WITH c AS (
        SELECT
            cluster_id,
            workspace_id,
            COUNT(DISTINCT usage_date)        AS active_days,
            SUM(estimated_cost_usd)           AS total_cost_usd,
            SUM(usage_quantity)               AS total_dbu,
            AVG(estimated_cost_usd)           AS avg_daily_cost,
            STDDEV(estimated_cost_usd)        AS stddev_daily_cost
        FROM {silver_fqn}
        WHERE cluster_id IS NOT NULL
          AND usage_date >= DATE_SUB(CURRENT_DATE(), {days})
        GROUP BY cluster_id, workspace_id
    )
    SELECT
        c.cluster_id,
        c.workspace_id,
        {meta_cols},
        c.active_days,
        ROUND(c.total_cost_usd, 2)   AS total_cost_usd,
        ROUND(c.total_dbu, 2)        AS total_dbu,
        ROUND(c.avg_daily_cost, 2)   AS avg_daily_cost,
        ROUND(COALESCE(c.stddev_daily_cost, 0), 2) AS stddev_daily_cost,
        ROUND(c.total_cost_usd / GREATEST(c.active_days, 1), 2) AS cost_per_active_day
    FROM c
    {join_clause}
    WHERE c.total_cost_usd >= {min_cost_usd}
    ORDER BY c.total_cost_usd DESC
    """

    rows = df_to_records(spark.sql(sql))
    for r in rows:
        cost = r.get("total_cost_usd") or 0
        # Estimate ~30% savings from right-sizing one tier down
        r["recommendation_type"] = "RIGHT_SIZE"
        r["expected_savings_usd_monthly"] = round((cost / max(days, 1)) * 30 * 0.30, 2)
        r["confidence"] = "MEDIUM"
        r["title"] = f"Right-size cluster {r.get('cluster_name') or r['cluster_id'][:12]}"
        r["rationale"] = (
            f"Cluster cost ${cost} over {days} days. "
            f"Variance suggests over-provisioning during low-load periods."
        )

    logger.info("recommend_rightsizing: %d candidates", len(rows))
    return rows


# ============================================================
# 2. Autoscale
# ============================================================
def recommend_autoscale(
    spark,
    catalog: str,
    days: int = 14,
) -> list[dict]:
    """Recommend enabling autoscale for fixed-worker clusters with variable load."""
    silver_fqn = f"{catalog}.silver.usage_enriched"
    clusters_fqn = f"{catalog}.bronze.clusters"
    if not (table_exists(spark, silver_fqn) and table_exists(spark, clusters_fqn)):
        return []

    sql = f"""
    WITH variance AS (
        SELECT
            cluster_id,
            workspace_id,
            STDDEV(estimated_cost_usd) / NULLIF(AVG(estimated_cost_usd), 0) AS coefficient_of_variation,
            SUM(estimated_cost_usd) AS total_cost_usd,
            COUNT(DISTINCT usage_date) AS active_days
        FROM {silver_fqn}
        WHERE cluster_id IS NOT NULL
          AND usage_date >= DATE_SUB(CURRENT_DATE(), {days})
        GROUP BY cluster_id, workspace_id
        HAVING COUNT(DISTINCT usage_date) >= 3
    ),
    latest_meta AS (
        SELECT
            cluster_id, cluster_name, owned_by,
            worker_count, min_autoscale_workers, max_autoscale_workers,
            ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) AS rn
        FROM {clusters_fqn}
    )
    SELECT
        v.cluster_id,
        m.cluster_name,
        v.workspace_id,
        m.owned_by,
        m.worker_count,
        m.min_autoscale_workers,
        m.max_autoscale_workers,
        ROUND(v.coefficient_of_variation, 2) AS coefficient_of_variation,
        ROUND(v.total_cost_usd, 2) AS total_cost_usd,
        v.active_days
    FROM variance v
    JOIN latest_meta m ON v.cluster_id = m.cluster_id AND m.rn = 1
    WHERE m.min_autoscale_workers IS NULL              -- not currently autoscaling
      AND m.worker_count >= {THRESHOLDS.autoscale_min_workers}
      AND v.coefficient_of_variation > 0.4             -- meaningful variance
      AND v.total_cost_usd > 20
    ORDER BY v.total_cost_usd DESC
    """

    rows = df_to_records(spark.sql(sql))
    for r in rows:
        cost = r.get("total_cost_usd") or 0
        r["recommendation_type"] = "ENABLE_AUTOSCALE"
        # Variable workloads on fixed-size clusters typically save 15-25% with autoscale
        r["expected_savings_usd_monthly"] = round((cost / max(days, 1)) * 30 * 0.20, 2)
        r["confidence"] = "MEDIUM"
        r["title"] = f"Enable autoscale on {r.get('cluster_name') or r['cluster_id'][:12]}"
        r["rationale"] = (
            f"Fixed-size cluster ({r.get('worker_count')} workers) shows "
            f"high cost variance (CV={r['coefficient_of_variation']}). "
            f"Autoscale would reduce idle worker time."
        )

    logger.info("recommend_autoscale: %d candidates", len(rows))
    return rows


# ============================================================
# 3. Photon
# ============================================================
def recommend_photon(
    spark,
    catalog: str,
    days: int = 7,
    min_hours: float = THRESHOLDS.photon_recommend_hours_per_week,
) -> list[dict]:
    """Recommend Photon for SQL workloads not yet using it.

    System.billing.usage records Photon usage via product_features.
    """
    silver_fqn = f"{catalog}.silver.usage_enriched"
    if not table_exists(spark, silver_fqn):
        return []

    sql = f"""
    SELECT
        warehouse_id,
        workspace_id,
        sku_name,
        ROUND(SUM(estimated_cost_usd), 2)   AS total_cost_usd,
        ROUND(SUM(usage_quantity), 2)       AS total_dbu,
        COUNT(DISTINCT usage_date)          AS active_days
    FROM {silver_fqn}
    WHERE warehouse_id IS NOT NULL
      AND LOWER(sku_name) NOT LIKE '%photon%'
      AND LOWER(billing_origin_product) IN ('sql', 'jobs', 'all_purpose')
      AND usage_date >= DATE_SUB(CURRENT_DATE(), {days})
    GROUP BY warehouse_id, workspace_id, sku_name
    HAVING total_dbu >= {min_hours}
    ORDER BY total_cost_usd DESC
    """

    rows = df_to_records(spark.sql(sql))
    for r in rows:
        cost = r.get("total_cost_usd") or 0
        r["recommendation_type"] = "ENABLE_PHOTON"
        # Photon typically yields 2-3x speedup → ~40-50% cost reduction for SQL
        r["expected_savings_usd_monthly"] = round((cost / max(days, 1)) * 30 * 0.40, 2)
        r["confidence"] = "MEDIUM"
        r["title"] = f"Enable Photon on warehouse {r['warehouse_id'][:12]}"
        r["rationale"] = (
            f"Warehouse spent ${cost} on non-Photon SQL over {days} days. "
            f"Photon typically delivers 2-3x throughput, reducing $/query."
        )

    logger.info("recommend_photon: %d candidates", len(rows))
    return rows


# ============================================================
# 4. Serverless
# ============================================================
def recommend_serverless(
    spark,
    catalog: str,
    days: int = 14,
) -> list[dict]:
    """Recommend migrating short, bursty jobs to serverless compute.

    Heuristic: Jobs with low active_days but recurring spend benefit from
    serverless (no startup time, pay-per-second).
    """
    silver_fqn = f"{catalog}.silver.usage_enriched"
    if not table_exists(spark, silver_fqn):
        return []

    sql = f"""
    SELECT
        job_id,
        workspace_id,
        COUNT(DISTINCT usage_date)        AS active_days,
        ROUND(SUM(estimated_cost_usd), 2) AS total_cost_usd,
        ROUND(AVG(estimated_cost_usd), 2) AS avg_per_run_cost,
        ROUND(SUM(usage_quantity), 2)     AS total_dbu
    FROM {silver_fqn}
    WHERE job_id IS NOT NULL
      AND LOWER(sku_name) NOT LIKE '%serverless%'
      AND usage_date >= DATE_SUB(CURRENT_DATE(), {days})
    GROUP BY job_id, workspace_id
    HAVING total_cost_usd > 10
       AND avg_per_run_cost < 5            -- short jobs
    ORDER BY total_cost_usd DESC
    LIMIT 50
    """

    rows = df_to_records(spark.sql(sql))
    for r in rows:
        cost = r.get("total_cost_usd") or 0
        r["recommendation_type"] = "MIGRATE_SERVERLESS"
        # Eliminates ~5 min startup per run → savings depend on run frequency
        r["expected_savings_usd_monthly"] = round((cost / max(days, 1)) * 30 * 0.25, 2)
        r["confidence"] = "LOW"
        r["title"] = f"Migrate job {r['job_id']} to serverless"
        r["rationale"] = (
            f"Short, recurring job (avg ${r['avg_per_run_cost']}/run). "
            f"Serverless eliminates cluster startup overhead."
        )

    logger.info("recommend_serverless: %d candidates", len(rows))
    return rows