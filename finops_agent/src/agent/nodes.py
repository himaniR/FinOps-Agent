"""
LangGraph nodes — each is a pure function: (state) -> partial state update.

Node responsibilities:
- load_context        : pull silver/gold data from UC into pandas
- analyze_costs       : compute summary metrics (totals, MoM, top SKUs)
- detect_anomalies    : z-score & rule-based detection → findings
- generate_recs       : LLM-driven recommendations from findings
- generate_report     : LLM-driven markdown narrative
- persist             : write findings/recs/report back to finops.agent.*
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from databricks.connect import DatabricksSession
from mlflow.deployments import get_deploy_client

from src.agent.state import AgentState, Finding, Recommendation

# ---------------------------------------------------------------- #
# Helpers                                                          #
# ---------------------------------------------------------------- #

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"


def _spark():
    """Get an active Spark session (works inside a DB notebook OR via Connect)."""
    try:
        from pyspark.sql import SparkSession
        s = SparkSession.getActiveSession()
        if s is not None:
            return s
    except Exception:
        pass
    return DatabricksSession.builder.getOrCreate()


def _time_node(name: str):
    """Decorator: stamps elapsed ms into state.node_timings_ms."""
    def deco(fn):
        def wrapper(state: AgentState) -> dict[str, Any]:
            t0 = time.perf_counter()
            try:
                update = fn(state)
            except Exception as e:
                errs = state.get("errors", []) + [f"{name}: {type(e).__name__}: {e}"]
                return {"errors": errs}
            elapsed = (time.perf_counter() - t0) * 1000
            timings = {**state.get("node_timings_ms", {}), name: round(elapsed, 1)}
            update.setdefault("node_timings_ms", timings)
            return update
        return wrapper
    return deco


def _llm_call(system: str, user: str, max_tokens: int = 800) -> str:
    """Single-shot LLM call returning the assistant text."""
    client = get_deploy_client("databricks")
    resp = client.predict(
        endpoint=LLM_ENDPOINT,
        inputs={
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.2,
        },
    )
    return resp["choices"][0]["message"]["content"].strip()


# ---------------------------------------------------------------- #
# Node 1: load_context                                             #
# ---------------------------------------------------------------- #
@_time_node("load_context")
def load_context(state: AgentState) -> dict[str, Any]:
    """Pull silver/gold tables into pandas for downstream analysis."""
    cfg = state["config"]
    lookback = cfg["lookback_days"]
    top_n = cfg["top_n_skus"]

    spark = _spark()
    cutoff = (datetime.utcnow() - timedelta(days=lookback)).strftime("%Y-%m-%d")

    # Daily cost timeseries (gold)
    cost_df = spark.sql(f"""
        SELECT usage_date, total_cost_usd, total_usage_quantity
        FROM finops.gold.daily_cost_summary
        WHERE usage_date >= DATE('{cutoff}')
        ORDER BY usage_date
    """).toPandas()

    # Detailed usage (silver) for anomaly drill-downs
    usage_df = spark.sql(f"""
        SELECT usage_date, sku_name, workspace_id,
               cost_usd, usage_quantity
        FROM finops.silver.usage_enriched
        WHERE usage_date >= DATE('{cutoff}')
    """).toPandas()

    # Top-N SKUs by total spend
    top_skus_df = (
        usage_df.groupby("sku_name", as_index=False)["cost_usd"]
        .sum()
        .sort_values("cost_usd", ascending=False)
        .head(top_n)
    )

    return {
        "cost_df": cost_df,
        "usage_df": usage_df,
        "top_skus_df": top_skus_df,
    }


# ---------------------------------------------------------------- #
# Node 2: analyze_costs                                            #
# ---------------------------------------------------------------- #
@_time_node("analyze_costs")
def analyze_costs(state: AgentState) -> dict[str, Any]:
    """Compute headline metrics — totals, MoM change, concentration."""
    cost_df = state["cost_df"]
    if cost_df.empty:
        return {"summary_metrics": {"total_cost_usd": 0.0}}

    total_cost = float(cost_df["total_cost_usd"].sum())
    days = cost_df["usage_date"].nunique()
    avg_daily = total_cost / max(days, 1)

    # Last 7d vs prior 7d momentum
    cost_df = cost_df.sort_values("usage_date")
    last_7 = float(cost_df.tail(7)["total_cost_usd"].sum())
    prior_7 = float(cost_df.iloc[-14:-7]["total_cost_usd"].sum()) if len(cost_df) >= 14 else 0.0
    wow_change_pct = ((last_7 - prior_7) / prior_7 * 100) if prior_7 > 0 else 0.0

    # Concentration: top SKU share of total spend
    top_skus = state["top_skus_df"]
    top_sku_share_pct = (
        float(top_skus.iloc[0]["cost_usd"]) / total_cost * 100
        if total_cost > 0 and not top_skus.empty else 0.0
    )

    return {
        "summary_metrics": {
            "total_cost_usd": round(total_cost, 2),
            "avg_daily_cost_usd": round(avg_daily, 2),
            "last_7d_cost_usd": round(last_7, 2),
            "prior_7d_cost_usd": round(prior_7, 2),
            "wow_change_pct": round(wow_change_pct, 2),
            "top_sku_share_pct": round(top_sku_share_pct, 2),
            "lookback_days": days,
        }
    }


# ---------------------------------------------------------------- #
# Node 3: detect_anomalies                                         #
# ---------------------------------------------------------------- #
@_time_node("detect_anomalies")
def detect_anomalies(state: AgentState) -> dict[str, Any]:
    """Z-score on daily totals + rule-based SKU-level spikes."""
    cfg = state["config"]
    threshold = cfg["anomaly_zscore_threshold"]
    min_cost = cfg["min_finding_cost_usd"]

    findings: list[Finding] = []
    cost_df = state["cost_df"].copy()

    # --- Detector 1: Daily total z-score ---
    if len(cost_df) >= 7:
        mean = cost_df["total_cost_usd"].mean()
        std = cost_df["total_cost_usd"].std() or 1e-9
        cost_df["zscore"] = (cost_df["total_cost_usd"] - mean) / std

        for _, row in cost_df[cost_df["zscore"].abs() >= threshold].iterrows():
            if row["total_cost_usd"] < min_cost:
                continue
            direction = "spike" if row["zscore"] > 0 else "drop"
            findings.append(Finding(
                finding_id=str(uuid.uuid4()),
                category="anomaly",
                severity="high" if abs(row["zscore"]) >= 3 else "medium",
                title=f"Daily cost {direction} on {row['usage_date']}",
                description=(f"Daily cost was ${row['total_cost_usd']:.2f}, "
                             f"{abs(row['zscore']):.1f} std devs from the "
                             f"{cfg['lookback_days']}-day mean of ${mean:.2f}."),
                affected_resource="workspace_total",
                metric_value=float(row["total_cost_usd"]),
                baseline_value=float(mean),
                detected_at=datetime.utcnow(),
                evidence={"zscore": float(row["zscore"]),
                          "mean": float(mean), "std": float(std)},
            ))

    # --- Detector 2: SKU concentration risk ---
    metrics = state.get("summary_metrics", {})
    if metrics.get("top_sku_share_pct", 0) > 60:
        top = state["top_skus_df"].iloc[0]
        findings.append(Finding(
            finding_id=str(uuid.uuid4()),
            category="concentration",
            severity="medium",
            title=f"High concentration in SKU {top['sku_name']}",
            description=(f"{metrics['top_sku_share_pct']:.1f}% of total spend "
                         f"comes from a single SKU. Diversification or commitment "
                         f"discounts may be appropriate."),
            affected_resource=str(top["sku_name"]),
            metric_value=float(top["cost_usd"]),
            baseline_value=float(metrics["total_cost_usd"]),
            detected_at=datetime.utcnow(),
            evidence={"share_pct": metrics["top_sku_share_pct"]},
        ))

    # --- Detector 3: Week-over-week jump ---
    if metrics.get("wow_change_pct", 0) >= 25:
        findings.append(Finding(
            finding_id=str(uuid.uuid4()),
            category="trend",
            severity="high" if metrics["wow_change_pct"] >= 50 else "medium",
            title=f"Spend up {metrics['wow_change_pct']:.1f}% week-over-week",
            description=(f"Last 7 days: ${metrics['last_7d_cost_usd']:.2f} vs "
                         f"prior 7 days: ${metrics['prior_7d_cost_usd']:.2f}."),
            affected_resource="workspace_total",
            metric_value=metrics["last_7d_cost_usd"],
            baseline_value=metrics["prior_7d_cost_usd"],
            detected_at=datetime.utcnow(),
            evidence={"wow_change_pct": metrics["wow_change_pct"]},
        ))

    return {"findings": findings}


# ---------------------------------------------------------------- #
# Node 4: generate_recs                                            #
# ---------------------------------------------------------------- #
@_time_node("generate_recs")
def generate_recs(state: AgentState) -> dict[str, Any]:
    """Use the LLM to turn findings into concrete recommendations."""
    findings = state.get("findings", [])
    if not findings:
        return {"recommendations": []}

    system = (
        "You are a senior FinOps analyst. Given cost findings, propose concrete, "
        "actionable recommendations. Return STRICT JSON: a list of objects with keys "
        "'finding_id', 'action_type' (rightsize|shutdown|reserve|tag|investigate), "
        "'title', 'rationale', 'estimated_monthly_savings_usd' (number), "
        "'confidence' (low|medium|high), 'effort' (low|medium|high), "
        "'suggested_command' (string or null). No prose outside the JSON."
    )
    payload = [
        {"finding_id": f["finding_id"], "category": f["category"],
         "severity": f["severity"], "title": f["title"],
         "description": f["description"], "metric_value": f["metric_value"],
         "baseline_value": f["baseline_value"]}
        for f in findings
    ]
    user = f"Findings:\n{json.dumps(payload, default=str, indent=2)}"

    raw = _llm_call(system, user, max_tokens=1500)

    # Robust JSON parse — strip code fences if model added them
    cleaned = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        items = json.loads(cleaned)
    except json.JSONDecodeError:
        items = []

    recs: list[Recommendation] = []
    for item in items:
        fid = item.get("finding_id")
        parent = next((f for f in findings if f["finding_id"] == fid), None)
        recs.append(Recommendation(
            recommendation_id=str(uuid.uuid4()),
            finding_ids=[fid] if fid else [],
            action_type=item.get("action_type", "investigate"),
            title=item.get("title", "Investigate finding")[:200],
            rationale=item.get("rationale", ""),
            estimated_monthly_savings_usd=float(item.get("estimated_monthly_savings_usd") or 0),
            confidence=item.get("confidence", "medium"),
            effort=item.get("effort", "medium"),
            target_resource=parent["affected_resource"] if parent else "unknown",
            suggested_command=item.get("suggested_command"),
            created_at=datetime.utcnow(),
        ))

    return {"recommendations": recs}


# ---------------------------------------------------------------- #
# Node 5: generate_report                                          #
# ---------------------------------------------------------------- #
@_time_node("generate_report")
def generate_report(state: AgentState) -> dict[str, Any]:
    """Compose a markdown executive report."""
    metrics = state.get("summary_metrics", {})
    findings = state.get("findings", [])
    recs = state.get("recommendations", [])

    system = (
        "You are a FinOps analyst writing an executive cost summary in markdown. "
        "Be concise (under 400 words). Sections: '## Executive Summary', "
        "'## Key Metrics', '## Findings', '## Recommendations', '## Next Steps'. "
        "Use bullet points and bold key numbers. No code fences."
    )
    user = json.dumps({
        "run_id": state["run_id"],
        "metrics": metrics,
        "findings": findings,
        "recommendations": recs,
    }, default=str, indent=2)

    md = _llm_call(system, user, max_tokens=1200)

    # Header prefix for traceability
    header = (f"# FinOps Agent Report\n"
              f"_Run: `{state['run_id']}` · "
              f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}_\n\n")
    return {"report_markdown": header + md}


# ---------------------------------------------------------------- #
# Node 6: persist                                                  #
# ---------------------------------------------------------------- #
@_time_node("persist")
def persist(state: AgentState) -> dict[str, Any]:
    """Write run, findings, recommendations, and report to finops.agent.* tables."""
    spark = _spark()

    run_row = [{
        "run_id": state["run_id"],
        "started_at": state["started_at"],
        "completed_at": datetime.utcnow(),
        "triggered_by": state.get("triggered_by", "manual"),
        "config_json": json.dumps(state.get("config", {})),
        "summary_metrics_json": json.dumps(state.get("summary_metrics", {})),
        "node_timings_json": json.dumps(state.get("node_timings_ms", {})),
        "errors_json": json.dumps(state.get("errors", [])),
        "report_markdown": state.get("report_markdown", ""),
        "num_findings": len(state.get("findings", [])),
        "num_recommendations": len(state.get("recommendations", [])),
    }]
    spark.createDataFrame(run_row).write.mode("append").saveAsTable("finops.agent.runs")

    findings = state.get("findings", [])
    if findings:
        f_rows = [{**f, "run_id": state["run_id"],
                   "evidence_json": json.dumps(f.get("evidence", {}), default=str)}
                  for f in findings]
        for r in f_rows:
            r.pop("evidence", None)
        spark.createDataFrame(f_rows).write.mode("append").saveAsTable("finops.agent.findings")

    recs = state.get("recommendations", [])
    if recs:
        r_rows = [{**r, "run_id": state["run_id"],
                   "finding_ids_json": json.dumps(r.get("finding_ids", []))}
                  for r in recs]
        for r in r_rows:
            r.pop("finding_ids", None)
        spark.createDataFrame(r_rows).write.mode("append").saveAsTable("finops.agent.recommendations")

    return {"persisted": True}