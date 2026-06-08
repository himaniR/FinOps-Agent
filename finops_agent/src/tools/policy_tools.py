"""Policy enforcement tools — tag compliance, expensive nodes, etc."""
from __future__ import annotations

import logging

from src.config.thresholds import THRESHOLDS
from src.utils.sql_helpers import df_to_records, table_exists

logger = logging.getLogger(__name__)


# ============================================================
# 1. Untagged resources
# ============================================================
def find_untagged_resources(
    spark,
    catalog: str,
    days: int = 7,
    min_cost_usd: float = 1.0,
) -> list[dict]:
    """Find resources without proper cost-allocation tags."""
    silver_fqn = f"{catalog}.silver.usage_enriched"
    if not table_exists(spark, silver_fqn):
        return []

    sql = f"""
    SELECT
        workspace_id,
        COALESCE(cluster_id, warehouse_id, job_id, pipeline_id, 'unknown') AS resource_id,
        CASE
            WHEN cluster_id IS NOT NULL   THEN 'cluster'
            WHEN warehouse_id IS NOT NULL THEN 'warehouse'
            WHEN job_id IS NOT NULL       THEN 'job'
            WHEN pipeline_id IS NOT NULL  THEN 'pipeline'
            ELSE 'other'
        END AS resource_type,
        tag_environment,
        tag_project,
        tag_team,
        tag_costcenter,
        ROUND(SUM(estimated_cost_usd), 2) AS total_cost_usd,
        ARRAY_DISTINCT(COLLECT_LIST(
            CASE
                WHEN tag_environment = 'untagged' THEN 'Environment'
                ELSE NULL
            END
        )) AS missing_env,
        CASE WHEN tag_environment = 'untagged' THEN 1 ELSE 0 END
        + CASE WHEN tag_project     = 'untagged' THEN 1 ELSE 0 END
        + CASE WHEN tag_team        = 'untagged' THEN 1 ELSE 0 END AS missing_count
    FROM {silver_fqn}
    WHERE usage_date >= DATE_SUB(CURRENT_DATE(), {days})
    GROUP BY workspace_id, resource_id, resource_type,
             tag_environment, tag_project, tag_team, tag_costcenter
    HAVING SUM(estimated_cost_usd) >= {min_cost_usd}
       AND (tag_environment = 'untagged'
            OR tag_project = 'untagged'
            OR tag_team    = 'untagged')
    ORDER BY total_cost_usd DESC
    LIMIT 200
    """

    rows = df_to_records(spark.sql(sql))
    for r in rows:
        missing = []
        if r.get("tag_environment") == "untagged": missing.append("Environment")
        if r.get("tag_project")     == "untagged": missing.append("Project")
        if r.get("tag_team")        == "untagged": missing.append("Team")
        r["missing_tags"] = missing
        r["finding_type"] = "UNTAGGED_RESOURCE"
        cost = r.get("total_cost_usd") or 0
        r["severity"] = "HIGH" if cost > 100 else "MEDIUM" if cost > 20 else "LOW"
        r["headline"] = f"{r['resource_type']} missing tags: {', '.join(missing)}"
        r.pop("missing_env", None)

    logger.info("find_untagged_resources: %d findings", len(rows))
    return rows


# ============================================================
# 2. Expensive node types
# ============================================================
def find_expensive_node_types(
    spark,
    catalog: str,
    days: int = 7,
) -> list[dict]:
    """Flag clusters using node types from the 'expensive' list."""
    clusters_fqn = f"{catalog}.bronze.clusters"
    silver_fqn = f"{catalog}.silver.usage_enriched"
    if not (table_exists(spark, clusters_fqn) and table_exists(spark, silver_fqn)):
        return []

    expensive = THRESHOLDS.expensive_node_types
    expensive_list = ", ".join([f"'{t}'" for t in expensive])

    sql = f"""
    WITH costs AS (
        SELECT
            cluster_id,
            workspace_id,
            ROUND(SUM(estimated_cost_usd), 2) AS total_cost_usd
        FROM {silver_fqn}
        WHERE cluster_id IS NOT NULL
          AND usage_date >= DATE_SUB(CURRENT_DATE(), {days})
        GROUP BY cluster_id, workspace_id
    ),
    latest_meta AS (
        SELECT
            cluster_id, cluster_name, owned_by,
            worker_node_type, driver_node_type, worker_count,
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
        c.total_cost_usd
    FROM costs c
    JOIN latest_meta m ON c.cluster_id = m.cluster_id AND m.rn = 1
    WHERE m.worker_node_type IN ({expensive_list})
       OR m.driver_node_type IN ({expensive_list})
    ORDER BY c.total_cost_usd DESC
    """

    rows = df_to_records(spark.sql(sql))
    for r in rows:
        r["finding_type"] = "EXPENSIVE_NODE_TYPE"
        r["severity"] = "MEDIUM"
        r["headline"] = (
            f"Cluster {r.get('cluster_name') or r['cluster_id'][:12]} uses "
            f"expensive node type {r['worker_node_type']}"
        )
        r["recommended_action"] = (
            "Verify workload requires this size; consider downgrading to a smaller VM."
        )
    logger.info("find_expensive_node_types: %d findings", len(rows))
    return rows


# ============================================================
# 3. Required tags compliance
# ============================================================
def check_required_tags(
    spark,
    catalog: str,
    days: int = 7,
) -> dict:
    """Calculate tag compliance score across the org."""
    silver_fqn = f"{catalog}.silver.usage_enriched"
    if not table_exists(spark, silver_fqn):
        return {"error": f"Table {silver_fqn} not found"}

    sql = f"""
    WITH base AS (
        SELECT
            ROUND(SUM(estimated_cost_usd), 2) AS total_cost,
            ROUND(SUM(CASE WHEN tag_environment != 'untagged' THEN estimated_cost_usd ELSE 0 END), 2) AS tagged_env_cost,
            ROUND(SUM(CASE WHEN tag_project     != 'untagged' THEN estimated_cost_usd ELSE 0 END), 2) AS tagged_proj_cost,
            ROUND(SUM(CASE WHEN tag_team        != 'untagged' THEN estimated_cost_usd ELSE 0 END), 2) AS tagged_team_cost,
            ROUND(SUM(CASE WHEN tag_owner       != 'untagged' THEN estimated_cost_usd ELSE 0 END), 2) AS tagged_owner_cost
        FROM {silver_fqn}
        WHERE usage_date >= DATE_SUB(CURRENT_DATE(), {days})
    )
    SELECT * FROM base
    """
    row = df_to_records(spark.sql(sql))[0] if spark.sql(sql).count() else {}
    total = row.get("total_cost") or 0
    if total == 0:
        return {"compliance_score": 100.0, "message": "No cost data to evaluate"}

    pct_env   = (row.get("tagged_env_cost")   or 0) / total * 100
    pct_proj  = (row.get("tagged_proj_cost")  or 0) / total * 100
    pct_team  = (row.get("tagged_team_cost")  or 0) / total * 100
    pct_owner = (row.get("tagged_owner_cost") or 0) / total * 100
    compliance = (pct_env + pct_proj + pct_team + pct_owner) / 4

    result = {
        "compliance_score": round(compliance, 1),
        "pct_cost_with_environment_tag": round(pct_env, 1),
        "pct_cost_with_project_tag": round(pct_proj, 1),
        "pct_cost_with_team_tag": round(pct_team, 1),
        "pct_cost_with_owner_tag": round(pct_owner, 1),
        "untagged_cost_usd": round(total - (row.get("tagged_env_cost") or 0), 2),
        "total_cost_usd": total,
    }
    logger.info("check_required_tags: %s", result)
    return result