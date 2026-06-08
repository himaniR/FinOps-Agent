"""
Agent state — the typed dict passed between LangGraph nodes.

Each node receives `AgentState`, may mutate it, and returns it.
LangGraph merges returned dicts into the running state.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, TypedDict

import pandas as pd


class Finding(TypedDict):
    """A single anomaly or noteworthy observation detected by the agent."""
    finding_id: str            # UUID
    category: str              # 'anomaly' | 'trend' | 'concentration' | 'idle'
    severity: Literal["low", "medium", "high", "critical"]
    title: str
    description: str
    affected_resource: str     # e.g. workspace_id, sku_name, cluster_id
    metric_value: float        # e.g. cost in USD, % change
    baseline_value: float | None
    detected_at: datetime
    evidence: dict[str, Any]   # raw numbers / SQL refs for traceability


class Recommendation(TypedDict):
    """An actionable suggestion derived from one or more findings."""
    recommendation_id: str     # UUID
    finding_ids: list[str]     # parent finding(s)
    action_type: str           # 'rightsize' | 'shutdown' | 'reserve' | 'tag' | 'investigate'
    title: str
    rationale: str             # LLM-generated reasoning
    estimated_monthly_savings_usd: float
    confidence: Literal["low", "medium", "high"]
    effort: Literal["low", "medium", "high"]
    target_resource: str
    suggested_command: str | None  # e.g. SQL or CLI command operator can run
    created_at: datetime


class AgentState(TypedDict, total=False):
    """
    Full state object the LangGraph passes node-to-node.

    `total=False` means every key is optional — nodes populate
    progressively as the graph runs.
    """
    # --- Run identity ---
    run_id: str
    started_at: datetime
    triggered_by: str          # 'manual' | 'schedule' | 'webhook'
    config: dict[str, Any]     # lookback days, thresholds, etc.

    # --- Loaded context (from Part 3 silver/gold tables) ---
    cost_df: pd.DataFrame             # daily cost timeseries
    usage_df: pd.DataFrame            # detailed usage rows
    top_skus_df: pd.DataFrame         # top-N SKUs by spend

    # --- Computed results ---
    summary_metrics: dict[str, float]  # total_cost, mom_change_pct, etc.
    findings: list[Finding]
    recommendations: list[Recommendation]

    # --- Outputs ---
    report_markdown: str
    persisted: bool

    # --- Errors / observability ---
    errors: list[str]
    node_timings_ms: dict[str, float]


def new_state(run_id: str, triggered_by: str = "manual",
              config: dict[str, Any] | None = None) -> AgentState:
    """Factory for a fresh state object at the start of a run."""
    return AgentState(
        run_id=run_id,
        started_at=datetime.utcnow(),
        triggered_by=triggered_by,
        config=config or {
            "lookback_days": 30,
            "anomaly_zscore_threshold": 2.5,
            "min_finding_cost_usd": 5.0,
            "top_n_skus": 10,
        },
        findings=[],
        recommendations=[],
        errors=[],
        node_timings_ms={},
        persisted=False,
    )