# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Run FinOps Agent
# MAGIC
# MAGIC End-to-end agent execution. One cell → full analysis → persisted findings,
# MAGIC recommendations, and a markdown executive report.
# MAGIC
# MAGIC **Outputs written to:**
# MAGIC - `finops.agent.runs`
# MAGIC - `finops.agent.findings`
# MAGIC - `finops.agent.recommendations`

# COMMAND ----------
# MAGIC %pip install -q langgraph mlflow databricks-sdk

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Resolve project root and import the agent

# COMMAND ----------
import sys, os

# Derive project root from this notebook's own path — works on classic,
# serverless, DABs, and Repos uniformly.
notebook_path = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)
# e.g. /Users/me/.bundle/finops/dev/files/notebooks/03_run_agent
#  → /Workspace/Users/me/.bundle/finops/dev/files
project_root = "/Workspace" + os.path.dirname(os.path.dirname(notebook_path))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"PROJECT_ROOT = {project_root}")
print(f"Contents     = {sorted(os.listdir(project_root))}")

from src.agent.runner import run_agent  # noqa: E402

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read job parameters (optional)

# COMMAND ----------
dbutils.widgets.text("triggered_by", "manual")
dbutils.widgets.text("lookback_days", "30")
dbutils.widgets.text("catalog", "finops")
dbutils.widgets.text("dry_run", "false")

triggered_by  = dbutils.widgets.get("triggered_by")
lookback_days = int(dbutils.widgets.get("lookback_days"))
catalog       = dbutils.widgets.get("catalog")
dry_run       = dbutils.widgets.get("dry_run").lower() == "true"

print(f"triggered_by  = {triggered_by}")
print(f"lookback_days = {lookback_days}")
print(f"catalog       = {catalog}")
print(f"dry_run       = {dry_run}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Run the agent

# COMMAND ----------
final_state = run_agent(
    triggered_by=triggered_by,
    config_overrides={
        "lookback_days": lookback_days,
        "catalog": catalog,
        "dry_run": dry_run,
    },
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Display report

# COMMAND ----------
from IPython.display import Markdown, display
display(Markdown(final_state.get("report_markdown", "_no report_")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Inspect findings & recommendations

# COMMAND ----------
import pandas as pd

findings_df = pd.DataFrame(final_state.get("findings", []))
recs_df     = pd.DataFrame(final_state.get("recommendations", []))

print(f"Findings: {len(findings_df)}")
if len(findings_df):
    display(findings_df)
else:
    print("No findings.")

print(f"\nRecommendations: {len(recs_df)}")
if len(recs_df):
    display(recs_df)
else:
    print("No recommendations.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Job exit (for schedulers)

# COMMAND ----------
dbutils.notebook.exit(final_state["run_id"])