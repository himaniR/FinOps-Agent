"""Reporting tools — markdown generators for findings, recommendations, summaries."""
from __future__ import annotations

import logging

from src.utils.formatters import fmt_usd, to_markdown_table, truncate

logger = logging.getLogger(__name__)


# ============================================================
# 1. Cost summary markdown
# ============================================================
def build_cost_summary_markdown(
    total_cost: dict,
    breakdown_by_product: list[dict],
    top_users: list[dict],
    trend: list[dict],
) -> str:
    """Build a markdown cost summary block."""
    days = total_cost.get("days", 7)
    md = [f"## 💰 Cost Summary (last {days} days)\n"]
    md.append(f"**Total spend:** {fmt_usd(total_cost.get('total_cost_usd'))}  ")
    md.append(f"**Avg daily:** {fmt_usd(total_cost.get('avg_daily_cost_usd'))}  ")
    md.append(f"**Window:** {total_cost.get('first_date')} → {total_cost.get('last_date')}\n")

    if breakdown_by_product:
        md.append("\n### By Product")
        rows = [
            {
                "Product": r.get("dimension_value") or "(unknown)",
                "Cost (USD)": fmt_usd(r.get("total_cost_usd")),
                "Records": f"{r.get('record_count', 0):,}",
            }
            for r in breakdown_by_product
        ]
        md.append(to_markdown_table(rows, ["Product", "Cost (USD)", "Records"]))

    if top_users:
        md.append("\n### Top 5 Users")
        rows = [
            {
                "User": truncate(r.get("user_email") or "unknown", 40),
                "Cost (USD)": fmt_usd(r.get("total_cost_usd")),
                "Avg/day": fmt_usd(r.get("avg_daily_cost_usd")),
            }
            for r in top_users[:5]
        ]
        md.append(to_markdown_table(rows, ["User", "Cost (USD)", "Avg/day"]))

    if trend and len(trend) >= 2:
        md.append("\n### Trend")
        latest = trend[-1]
        prev = trend[-2]
        change = latest.get("pct_change_vs_prior")
        arrow = "📈" if (change or 0) > 0 else "📉" if (change or 0) < 0 else "➡️"
        md.append(
            f"{arrow} Latest period: {fmt_usd(latest.get('total_cost_usd'))} "
            f"({change}% vs prior)" if change is not None
            else f"Latest period: {fmt_usd(latest.get('total_cost_usd'))}"
        )

    return "\n".join(md)


# ============================================================
# 2. Findings markdown
# ============================================================
def build_findings_markdown(findings: list[dict]) -> str:
    """Format findings into a markdown report."""
    if not findings:
        return "## 🚨 Findings\n\n_No anomalies or policy violations detected._"

    by_sev: dict[str, list[dict]] = {}
    for f in findings:
        sev = f.get("severity", "LOW")
        by_sev.setdefault(sev, []).append(f)

    md = [f"## 🚨 Findings ({len(findings)} total)\n"]

    for sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        items = by_sev.get(sev, [])
        if not items:
            continue
        emoji = {"CRITICAL": "🔥", "HIGH": "⚠️", "MEDIUM": "🟡", "LOW": "ℹ️"}[sev]
        md.append(f"\n### {emoji} {sev} ({len(items)})")
        for f in items[:10]:
            headline = f.get("headline") or f.get("finding_type") or "Finding"
            extra = []
            if f.get("daily_cost_usd"):
                extra.append(f"cost: {fmt_usd(f['daily_cost_usd'])}")
            if f.get("pct_above_baseline"):
                extra.append(f"+{f['pct_above_baseline']}% vs baseline")
            if f.get("dim_value"):
                extra.append(f"target: {truncate(str(f['dim_value']), 30)}")
            suffix = f" ({'; '.join(extra)})" if extra else ""
            md.append(f"- **{headline}**{suffix}")
        if len(items) > 10:
            md.append(f"- _...and {len(items) - 10} more_")

    return "\n".join(md)


# ============================================================
# 3. Recommendations markdown
# ============================================================
def build_recommendations_markdown(recommendations: list[dict]) -> str:
    """Format recommendations into a markdown report."""
    if not recommendations:
        return "## 💡 Recommendations\n\n_No optimization opportunities found._"

    sorted_recs = sorted(
        recommendations,
        key=lambda r: r.get("expected_savings_usd_monthly") or 0,
        reverse=True,
    )

    total_savings = sum(r.get("expected_savings_usd_monthly") or 0 for r in sorted_recs)
    md = [
        f"## 💡 Optimization Recommendations ({len(sorted_recs)} total)",
        f"\n**Estimated total monthly savings: {fmt_usd(total_savings)}**\n",
    ]

    for i, rec in enumerate(sorted_recs[:15], start=1):
        title = rec.get("title") or rec.get("recommendation_type", "Recommendation")
        savings = rec.get("expected_savings_usd_monthly") or 0
        confidence = rec.get("confidence", "MEDIUM")
        rationale = rec.get("rationale", "")

        md.append(f"\n**{i}. {title}**  ")
        md.append(f"_Savings:_ {fmt_usd(savings)}/mo · _Confidence:_ {confidence}  ")
        if rationale:
            md.append(f"{rationale}")

    if len(sorted_recs) > 15:
        md.append(f"\n_...and {len(sorted_recs) - 15} more_")
    return "\n".join(md)