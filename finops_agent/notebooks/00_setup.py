# Databricks notebook source
# MAGIC %md
# MAGIC # FinOps Agent — Setup Notebook
# MAGIC
# MAGIC **Run this ONCE** after deploying the bundle for the first time.
# MAGIC
# MAGIC Creates:
# MAGIC - `finops.agent.runs` — One row per agent run (audit log)
# MAGIC - `finops.agent.findings` — Anomalies & policy violations detected
# MAGIC - `finops.agent.recommendations` — Optimization recommendations
# MAGIC - `finops.agent.actions` — Actions taken (or proposed) by the agent
# MAGIC - `finops.agent.llm_calls` — LLM call audit trail (for debugging & cost tracking)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "finops", "Catalog name")
catalog = dbutils.widgets.get("catalog")
print(f"Catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify catalog & schemas exist

# COMMAND ----------

# Catalog must already exist (created in Phase 1.8)
catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
assert catalog in catalogs, (
    f"❌ Catalog '{catalog}' not found. "
    f"Run the SQL from Phase 1.8 first to create it."
)
print(f"✅ Catalog '{catalog}' exists")

# Ensure all 4 schemas exist
for schema in ["bronze", "silver", "gold", "agent"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    print(f"✅ Schema {catalog}.{schema} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create `agent.runs` — one row per agent invocation

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.agent.runs (
    run_id          STRING NOT NULL,
    job_run_id      STRING,
    started_at      TIMESTAMP NOT NULL,
    ended_at        TIMESTAMP,
    status          STRING,                      -- RUNNING | SUCCESS | FAILED
    triggered_by    STRING,                      -- user email or 'scheduler'
    mode            STRING,                      -- daily | weekly | adhoc
    dry_run         BOOLEAN,
    lookback_days   INT,
    config_snapshot STRING,                      -- JSON of settings used
    error_message   STRING,
    duration_seconds DOUBLE,
    findings_count  INT DEFAULT 0,
    recommendations_count INT DEFAULT 0,
    llm_calls       INT DEFAULT 0,
    llm_tokens      BIGINT DEFAULT 0
)
USING DELTA
COMMENT 'Audit log: one row per FinOps agent run'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.columnMapping.mode' = 'name'
)
""")
print(f"✅ Created {catalog}.agent.runs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create `agent.findings` — anomalies & policy violations

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.agent.findings (
    finding_id      STRING NOT NULL,
    run_id          STRING NOT NULL,
    detected_at     TIMESTAMP NOT NULL,
    finding_type    STRING NOT NULL,             -- COST_SPIKE | IDLE_CLUSTER | POLICY_VIOLATION | UNTAGGED | etc
    severity        STRING NOT NULL,             -- LOW | MEDIUM | HIGH | CRITICAL
    resource_type   STRING,                       -- cluster | warehouse | job | table
    resource_id     STRING,
    resource_name   STRING,
    workspace_id    STRING,
    metric_name     STRING,                       -- e.g. 'daily_cost_usd'
    metric_value    DOUBLE,
    baseline_value  DOUBLE,
    delta_pct       DOUBLE,
    headline        STRING,
    explanation     STRING,                       -- LLM-generated narrative
    raw_data        STRING,                       -- JSON snapshot of underlying data
    status          STRING DEFAULT 'OPEN'        -- OPEN | ACKNOWLEDGED | RESOLVED | IGNORED
)
USING DELTA
CLUSTER BY (detected_at)
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Anomalies, policy violations, and other findings detected by the agent'
""")
print(f"✅ Created {catalog}.agent.findings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create `agent.recommendations` — optimization suggestions

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.agent.recommendations (
    recommendation_id        STRING NOT NULL,
    run_id                   STRING NOT NULL,
    created_at               TIMESTAMP NOT NULL,
    recommendation_type      STRING NOT NULL,    -- RIGHT_SIZE | ENABLE_AUTOSCALE | etc
    resource_type            STRING,
    resource_id              STRING,
    resource_name            STRING,
    workspace_id             STRING,
    title                    STRING NOT NULL,
    rationale                STRING,
    expected_savings_usd_monthly DOUBLE,
    confidence               STRING,             -- LOW | MEDIUM | HIGH
    priority_score           DOUBLE,             -- 0-100
    priority_tier            STRING,             -- P0 | P1 | P2 | P3
    implementation_steps     STRING,             -- JSON array
    risks                    STRING,             -- JSON array
    status                   STRING DEFAULT 'PROPOSED', -- PROPOSED | ACCEPTED | REJECTED | IMPLEMENTED
    accepted_by              STRING,
    implemented_at           TIMESTAMP,
    actual_savings_usd       DOUBLE
)
USING DELTA
CLUSTER BY (created_at)
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Optimization recommendations generated by the agent'
""")
print(f"✅ Created {catalog}.agent.recommendations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create `agent.actions` — automated/proposed actions

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.agent.actions (
    action_id       STRING NOT NULL,
    run_id          STRING NOT NULL,
    recommendation_id STRING,
    finding_id      STRING,
    proposed_at     TIMESTAMP NOT NULL,
    executed_at     TIMESTAMP,
    action_type     STRING NOT NULL,             -- TAG_RESOURCE | TERMINATE_CLUSTER | RESIZE_WAREHOUSE | NOTIFY | etc
    target_resource STRING,
    payload         STRING,                       -- JSON describing the action
    dry_run         BOOLEAN,
    status          STRING,                       -- PROPOSED | EXECUTED | FAILED | SKIPPED
    error_message   STRING
)
USING DELTA
COMMENT 'Actions proposed or executed by the agent'
""")
print(f"✅ Created {catalog}.agent.actions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create `agent.llm_calls` — LLM audit trail

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.agent.llm_calls (
    call_id         STRING NOT NULL,
    run_id          STRING NOT NULL,
    called_at       TIMESTAMP NOT NULL,
    endpoint        STRING,
    purpose         STRING,                       -- e.g. 'anomaly_explanation'
    prompt_tokens   INT,
    completion_tokens INT,
    total_tokens    INT,
    latency_seconds DOUBLE,
    success         BOOLEAN,
    error_message   STRING
)
USING DELTA
CLUSTER BY (called_at)
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Audit log of every LLM call made by the agent'
""")
print(f"✅ Created {catalog}.agent.llm_calls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {catalog}.agent").collect()
print(f"\n📋 Tables in {catalog}.agent:")
for t in tables:
    print(f"  - {t.tableName}")

assert len(tables) >= 5, "Expected at least 5 tables in agent schema"
print(f"\n✅ Setup complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC Next steps:
# MAGIC 1. Run `notebooks/01_ingest_costs.py` to populate bronze/silver tables (Part 4)
# MAGIC 2. Run `notebooks/03_run_agent.py` to invoke the agent (Part 5)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")