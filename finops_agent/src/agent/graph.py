"""
LangGraph state machine wiring all FinOps agent nodes together.

Graph topology:

    START ─► load_context ─► analyze_costs ─► detect_anomalies
                                                     │
                                                     ▼
              persist ◄── generate_report ◄── generate_recs
                │
                ▼
               END
"""
from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from src.agent.nodes import (
    analyze_costs,
    detect_anomalies,
    generate_recs,
    generate_report,
    load_context,
    persist,
)
from src.agent.state import AgentState


def build_graph():
    """Compile the LangGraph state machine."""
    graph = StateGraph(AgentState)

    # Register nodes
    graph.add_node("load_context", load_context)
    graph.add_node("analyze_costs", analyze_costs)
    graph.add_node("detect_anomalies", detect_anomalies)
    graph.add_node("generate_recs", generate_recs)
    graph.add_node("generate_report", generate_report)
    graph.add_node("persist", persist)

    # Wire edges
    graph.add_edge(START, "load_context")
    graph.add_edge("load_context", "analyze_costs")
    graph.add_edge("analyze_costs", "detect_anomalies")
    graph.add_edge("detect_anomalies", "generate_recs")
    graph.add_edge("generate_recs", "generate_report")
    graph.add_edge("generate_report", "persist")
    graph.add_edge("persist", END)

    return graph.compile()
