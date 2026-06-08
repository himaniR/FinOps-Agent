"""Forecasting tools — month-end projection, burn rate, budget alerts."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from src.config.thresholds import THRESHOLDS
from src.utils.sql_helpers import df_to_records, table_exists

logger = logging.getLogger(__name__)


# ============================================================
# 1. Project end-of-month spend
# ============================================================
def project_month_end_spend(
    spark,
    catalog: str,
    workspace_id: str | None = None,
) -> dict:
    """Project total spend at end of current month based on month-to-date trend.

    Strategy: Linear projection using avg daily cost MTD × days in month.
    """
    fqn = f"{catalog}.gold.daily_cost"
    if not table_exists(spark, fqn):
        return {"error": f"Table {fqn} not found"}

    where = "DATE_FORMAT(usage_date, 'yyyy-MM') = DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')"
    if workspace_id:
        where += f" AND workspace_id = '{workspace_id}'"

    sql = f"""
    SELECT
        ROUND(SUM(total_cost_usd), 2)           AS mtd_total_usd,
        COUNT(DISTINCT usage_date)              AS days_with_data,
        MIN(usage_date)                         AS first_date,
        MAX(usage_date)                         AS last_date
    FROM {fqn}
    WHERE {where}
    """
    rows = df_to_records(spark.sql(sql))
    base = rows[0] if rows else {}

    mtd = base.get("mtd_total_usd") or 0.0
    days_with_data = base.get("days_with_data") or 0

    today = datetime.now(timezone.utc)
    first_of_month = today.replace(day=1)
    # Last day of current month
    if today.month == 12:
        next_month = today.replace(year=today.year + 1, month=1, day=1)
    else:
        next_month = today.replace(month=today.month + 1, day=1)
    days_in_month = (next_month - first_of_month).days
    days_elapsed = (today - first_of_month).days + 1
    days_remaining = days_in_month - days_elapsed

    avg_daily = mtd / days_with_data if days_with_data > 0 else 0
    projected_total = round(mtd + (avg_daily * days_remaining), 2)

    result = {
        "mtd_total_usd": round(mtd, 2),
        "avg_daily_cost_usd": round(avg_daily, 2),
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        "days_in_month": days_in_month,
        "projected_month_end_usd": projected_total,
        "projection_method": "linear_avg_daily",
        "workspace_id": workspace_id,
    }
    logger.info("project_month_end_spend: %s", result)
    return result


# ============================================================
# 2. Calculate burn rate vs budget
# ============================================================
def calculate_burn_rate(
    spark,
    catalog: str,
    monthly_budget_usd: float,
    workspace_id: str | None = None,
) -> dict:
    """Compare MTD spend to monthly budget. Returns alert level."""
    projection = project_month_end_spend(spark, catalog, workspace_id=workspace_id)
    if "error" in projection:
        return projection

    mtd = projection["mtd_total_usd"]
    projected = projection["projected_month_end_usd"]
    days_elapsed = projection["days_elapsed"]
    days_in_month = projection["days_in_month"]

    pct_budget_consumed = (mtd / monthly_budget_usd * 100) if monthly_budget_usd > 0 else 0
    pct_month_elapsed = (days_elapsed / days_in_month * 100) if days_in_month > 0 else 0
    pct_projected_overrun = ((projected - monthly_budget_usd) / monthly_budget_usd * 100) if monthly_budget_usd > 0 else 0

    # Alert level
    if pct_projected_overrun >= THRESHOLDS.forecast_overrun_pct:
        alert_level = "CRITICAL"
        message = f"Projected overrun: ${projected - monthly_budget_usd:,.0f} above ${monthly_budget_usd:,.0f} budget"
    elif pct_budget_consumed >= THRESHOLDS.budget_alert_pct and pct_month_elapsed < THRESHOLDS.budget_alert_pct:
        alert_level = "HIGH"
        message = f"Burning fast: {pct_budget_consumed:.0f}% of budget used at {pct_month_elapsed:.0f}% of month"
    elif pct_budget_consumed >= THRESHOLDS.budget_alert_pct:
        alert_level = "MEDIUM"
        message = f"Approaching budget cap ({pct_budget_consumed:.0f}% used)"
    else:
        alert_level = "OK"
        message = f"On track ({pct_budget_consumed:.0f}% of budget at {pct_month_elapsed:.0f}% of month)"

    result = {
        "monthly_budget_usd": monthly_budget_usd,
        "mtd_spend_usd": mtd,
        "projected_month_end_usd": projected,
        "pct_budget_consumed": round(pct_budget_consumed, 1),
        "pct_month_elapsed": round(pct_month_elapsed, 1),
        "pct_projected_overrun": round(pct_projected_overrun, 1),
        "alert_level": alert_level,
        "message": message,
        "workspace_id": workspace_id,
    }
    logger.info("calculate_burn_rate: %s", result)
    return result