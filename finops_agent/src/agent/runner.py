"""
Top-level entrypoint for executing one FinOps agent run.

Usage:
    from src.agent.runner import run_agent
    final_state = run_agent(triggered_by="manual")
    print(final_state["report_markdown"])
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

from src.agent.graph import build_graph
from src.agent.state import AgentState, new_state

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def run_agent(triggered_by: str = "manual",
              config_overrides: dict[str, Any] | None = None) -> AgentState:
    """
    Execute one full FinOps agent run.

    Args:
        triggered_by: 'manual' | 'schedule' | 'webhook'
        config_overrides: dict to override defaults (lookback_days, etc.)

    Returns:
        The final AgentState dict, including report_markdown and persisted=True.
    """
    run_id = str(uuid.uuid4())
    log.info(f"🚀 Starting FinOps agent run: {run_id}")

    state = new_state(run_id=run_id, triggered_by=triggered_by)
    if config_overrides:
        state["config"] = {**state["config"], **config_overrides}

    log.info(f"   Config: {state['config']}")

    graph = build_graph()
    final_state: AgentState = graph.invoke(state)

    # Log summary
    metrics = final_state.get("summary_metrics", {})
    log.info(f"✅ Run complete: {run_id}")
    log.info(f"   Total cost analysed: ${metrics.get('total_cost_usd', 0):,.2f}")
    log.info(f"   Findings: {len(final_state.get('findings', []))}")
    log.info(f"   Recommendations: {len(final_state.get('recommendations', []))}")
    log.info(f"   Persisted: {final_state.get('persisted', False)}")
    log.info(f"   Errors: {final_state.get('errors', [])}")
    log.info(f"   Node timings (ms): {final_state.get('node_timings_ms', {})}")

    return final_state