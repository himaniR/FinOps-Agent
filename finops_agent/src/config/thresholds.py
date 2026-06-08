"""FinOps thresholds — the rules that drive anomaly detection and recommendations.

Centralizing thresholds here makes them auditable and easy to tune.
Each threshold has a comment explaining its rationale.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Thresholds:
    """All thresholds the agent uses. Frozen = immutable after construction."""

    # ----- Anomaly Detection -----
    # A daily cost is considered a SPIKE if it's >X% above the 7-day rolling avg
    cost_spike_pct: float = 30.0

    # A cost is considered a SUSTAINED ANOMALY if spiked for >=N consecutive days
    sustained_anomaly_days: int = 2

    # Minimum daily cost (USD) to consider — avoids noise on tiny costs
    anomaly_min_daily_usd: float = 5.0

    # ----- Cluster Optimization -----
    # Cluster considered IDLE if avg CPU utilization <X% during runtime
    idle_cluster_cpu_pct: float = 10.0

    # Minimum cluster runtime hours/day to evaluate utilization (avoid 5-min jobs)
    min_cluster_hours_for_eval: float = 1.0

    # Recommend right-sizing if max observed CPU/memory <X% of allocated
    rightsize_max_utilization_pct: float = 40.0

    # Recommend autoscale if cluster has >X workers and worker variance is high
    autoscale_min_workers: int = 2

    # ----- SQL Warehouse Optimization -----
    # Warehouse is UNDERUSED if active queries <X% of uptime
    warehouse_underused_pct: float = 20.0

    # Warehouse should be smaller if peak concurrent queries <N
    warehouse_oversized_concurrent_queries: int = 2

    # ----- Storage Optimization -----
    # A table is "stale" if not queried in >N days (candidate for archival)
    stale_table_days: int = 90

    # A table is "large stale" if >N GB and >M days unused
    large_stale_size_gb: float = 100.0
    large_stale_days: int = 30

    # ----- Tagging / Policy -----
    # Required tags every cluster/job/warehouse must have
    required_tags: tuple[str, ...] = ("Environment", "Project", "Owner")

    # Node types considered "expensive" — flag for justification
    expensive_node_types: tuple[str, ...] = (
        "Standard_E64s_v3",
        "Standard_E64s_v5",
        "Standard_M128s",
        "Standard_NC24s_v3",
        "Standard_ND40rs_v2",
    )

    # ----- Budget / Forecasting -----
    # Alert when month-to-date spend exceeds X% of monthly budget
    budget_alert_pct: float = 80.0

    # Alert when projected month-end spend exceeds budget by X%
    forecast_overrun_pct: float = 10.0

    # ----- Photon -----
    # Recommend Photon if SQL workload runs >N hours/week without it
    photon_recommend_hours_per_week: float = 5.0


# Singleton — import this everywhere
THRESHOLDS = Thresholds()
