"""FinOps agent tools — atomic operations the agent can invoke.

Each tool:
- Has a single, well-defined purpose
- Returns serializable Python primitives (dict/list)
- Logs its activity
- Handles missing data gracefully
"""
from src.tools.cost_analysis_tools import (
    get_total_cost,
    get_cost_breakdown_by_dimension,
    get_top_spenders,
    get_cost_trend,
)
from src.tools.anomaly_tools import (
    detect_cost_spikes,
    detect_idle_clusters,
    detect_idle_warehouses,
)
from src.tools.optimization_tools import (
    recommend_rightsizing,
    recommend_autoscale,
    recommend_photon,
    recommend_serverless,
)
from src.tools.forecasting_tools import (
    project_month_end_spend,
    calculate_burn_rate,
)
from src.tools.policy_tools import (
    find_untagged_resources,
    find_expensive_node_types,
    check_required_tags,
)
from src.tools.reporting_tools import (
    build_cost_summary_markdown,
    build_findings_markdown,
    build_recommendations_markdown,
)

__all__ = [
    # cost analysis
    "get_total_cost", "get_cost_breakdown_by_dimension",
    "get_top_spenders", "get_cost_trend",
    # anomaly
    "detect_cost_spikes", "detect_idle_clusters", "detect_idle_warehouses",
    # optimization
    "recommend_rightsizing", "recommend_autoscale",
    "recommend_photon", "recommend_serverless",
    # forecasting
    "project_month_end_spend", "calculate_burn_rate",
    # policy
    "find_untagged_resources", "find_expensive_node_types", "check_required_tags",
    # reporting
    "build_cost_summary_markdown", "build_findings_markdown",
    "build_recommendations_markdown",
]