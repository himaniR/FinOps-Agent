# Databricks notebook source
# MAGIC %md
# MAGIC # FinOps Agent — Tool Smoke Tests
# MAGIC
# MAGIC Quick test of every tool against real data.
# MAGIC Run this AFTER `01_ingest_costs.py` to verify everything works.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Make src/ importable. In a deployed bundle, files live at /Workspace/.../files/
import sys, os
nb_dir = os.path.dirname(os.path.abspath("__file__")) if "__file__" in dir() else "/Workspace"
# Walk up to find the bundle root (where src/ lives)
candidate = os.getcwd()
for _ in range(5):
    if os.path.isdir(os.path.join(candidate, "src")):
        break
    candidate = os.path.dirname(candidate)
if candidate not in sys.path:
    sys.path.insert(0, candidate)

print(f"sys.path includes: {candidate}")

# COMMAND ----------

dbutils.widgets.text("catalog", "finops", "Catalog")
catalog = dbutils.widgets.get("catalog")

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cost analysis tools

# COMMAND ----------

from src.tools.cost_analysis_tools import (
    get_total_cost, get_cost_breakdown_by_dimension,
    get_top_spenders, get_cost_trend,
)

print("=== get_total_cost ===")
print(get_total_cost(spark, catalog, days=7))

# COMMAND ----------

print("=== get_cost_breakdown_by_dimension(sku) ===")
for r in get_cost_breakdown_by_dimension(spark, catalog, "sku", days=7, top_n=5):
    print(r)

# COMMAND ----------

print("=== get_top_spenders(user) ===")
for r in get_top_spenders(spark, catalog, "user", days=7, top_n=5):
    print(r)

# COMMAND ----------

print("=== get_cost_trend ===")
for r in get_cost_trend(spark, catalog, days=14, granularity="day"):
    print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Anomaly tools

# COMMAND ----------

from src.tools.anomaly_tools import (
    detect_cost_spikes, detect_idle_clusters, detect_idle_warehouses,
)

print("=== detect_cost_spikes ===")
spikes = detect_cost_spikes(spark, catalog, lookback_days=14)
print(f"Found {len(spikes)} spikes")
for s in spikes[:3]: print(s)

# COMMAND ----------

print("=== detect_idle_clusters ===")
idle = detect_idle_clusters(spark, catalog, days=7)
print(f"Found {len(idle)} idle-risk clusters")
for c in idle[:3]: print(c)

# COMMAND ----------

print("=== detect_idle_warehouses ===")
idle_wh = detect_idle_warehouses(spark, catalog, days=7)
print(f"Found {len(idle_wh)} warehouse risks")
for w in idle_wh[:3]: print(w)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Optimization tools

# COMMAND ----------

from src.tools.optimization_tools import (
    recommend_rightsizing, recommend_autoscale,
    recommend_photon, recommend_serverless,
)

print("=== recommend_rightsizing ===")
recs = recommend_rightsizing(spark, catalog, days=14)
print(f"Found {len(recs)}"); [print(r) for r in recs[:3]]

# COMMAND ----------

print("=== recommend_autoscale ===")
recs = recommend_autoscale(spark, catalog, days=14)
print(f"Found {len(recs)}"); [print(r) for r in recs[:3]]

# COMMAND ----------

print("=== recommend_photon ===")
recs = recommend_photon(spark, catalog, days=7)
print(f"Found {len(recs)}"); [print(r) for r in recs[:3]]

# COMMAND ----------

print("=== recommend_serverless ===")
recs = recommend_serverless(spark, catalog, days=14)
print(f"Found {len(recs)}"); [print(r) for r in recs[:3]]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Forecasting tools

# COMMAND ----------

from src.tools.forecasting_tools import project_month_end_spend, calculate_burn_rate

print("=== project_month_end_spend ===")
print(project_month_end_spend(spark, catalog))

# COMMAND ----------

print("=== calculate_burn_rate ===")
print(calculate_burn_rate(spark, catalog, monthly_budget_usd=1000.0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Policy tools

# COMMAND ----------

from src.tools.policy_tools import (
    find_untagged_resources, find_expensive_node_types, check_required_tags,
)

print("=== find_untagged_resources ===")
untagged = find_untagged_resources(spark, catalog, days=7)
print(f"Found {len(untagged)}"); [print(r) for r in untagged[:3]]

# COMMAND ----------

print("=== find_expensive_node_types ===")
expensive = find_expensive_node_types(spark, catalog, days=7)
print(f"Found {len(expensive)}"); [print(r) for r in expensive[:3]]

# COMMAND ----------

print("=== check_required_tags ===")
print(check_required_tags(spark, catalog, days=7))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reporting tools

# COMMAND ----------

from src.tools.reporting_tools import (
    build_cost_summary_markdown, build_findings_markdown, build_recommendations_markdown,
)

total_cost = get_total_cost(spark, catalog, days=7)
breakdown = get_cost_breakdown_by_dimension(spark, catalog, "product", days=7, top_n=5)
top_u = get_top_spenders(spark, catalog, "user", days=7, top_n=5)
trend = get_cost_trend(spark, catalog, days=14)

cost_md = build_cost_summary_markdown(total_cost, breakdown, top_u, trend)
findings_md = build_findings_markdown(spikes + idle + untagged)
recs_md = build_recommendations_markdown(
    recommend_rightsizing(spark, catalog, 14)
    + recommend_autoscale(spark, catalog, 14)
    + recommend_photon(spark, catalog, 7)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Render the report
from IPython.display import HTML, Markdown, display
# COMMAND ----------
try:
    display(HTML(f"""
    <style>
        body {{ font-family: -apple-system, Segoe UI, sans-serif; max-width: 900px; margin: auto; }}
        h2 {{ color: #2c3e50; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f4f4f4; }}
    </style>
    <div>{cost_md}</div>
    <hr/>
    <div>{findings_md}</div>
    <hr/>
    <div>{recs_md}</div>
    </body>
    </html>
    """))
except Exception:
    display(Markdown(cost_md))
    display(Markdown(findings_md))
    display(Markdown(recs_md))

# COMMAND ----------

# Also dump as plain markdown text
print(cost_md)
print("\n" + "="*60 + "\n")
print(findings_md)
print("\n" + "="*60 + "\n")
print(recs_md)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")