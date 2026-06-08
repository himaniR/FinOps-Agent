# Databricks notebook source
# MAGIC %md
# MAGIC # FinOps Agent — Cost Data Ingestion
# MAGIC
# MAGIC Reads from `system.billing.usage` and Databricks system tables, then writes:
# MAGIC - **Bronze**: Raw billing data (1:1 with system tables, partitioned by date)
# MAGIC - **Silver**: Cleaned + enriched (joined with list prices, tags extracted)
# MAGIC - **Gold**: Aggregations (daily totals, by-user, by-workspace, by-SKU)
# MAGIC
# MAGIC Run daily.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "finops", "Catalog")
dbutils.widgets.text("lookback_days", "7", "Lookback days")
dbutils.widgets.text("azure_subscription_id", "", "Azure Subscription ID (optional)")

catalog = dbutils.widgets.get("catalog")
lookback_days = int(dbutils.widgets.get("lookback_days"))
azure_sub_id = dbutils.widgets.get("azure_subscription_id") or None

print(f"Catalog: {catalog}")
print(f"Lookback days: {lookback_days}")
print(f"Azure subscription: {azure_sub_id or '(not set)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify system tables are accessible

# COMMAND ----------

try:
    cnt = spark.sql("SELECT COUNT(*) AS c FROM system.billing.usage LIMIT 1").collect()[0].c
    print(f"✅ system.billing.usage accessible — {cnt:,} total rows")
    if cnt == 0:
        print("⚠️ Table is empty. Data takes 24-48h to populate after first enabling.")
        print(" Continuing anyway — downstream tables will be empty too.")
except Exception as e:
    print(f"❌ Cannot read system.billing.usage: {e}")
    print("Run the system tables enablement script from Phase 1.7.")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. BRONZE — Raw billing usage
# MAGIC
# MAGIC We snapshot the last N days from `system.billing.usage` into our bronze layer.
# MAGIC This gives us:
# MAGIC - A stable copy we control (system tables can have schema changes)
# MAGIC - Faster queries (smaller table)
# MAGIC - The ability to enrich with custom columns

# COMMAND ----------

bronze_usage_sql = f"""
CREATE OR REPLACE TABLE {catalog}.bronze.billing_usage AS
SELECT
    record_id,
    account_id,
    workspace_id,
    sku_name,
    cloud,
    usage_start_time,
    usage_end_time,
    usage_date,
    DATE_FORMAT(usage_date, 'yyyy-MM') AS usage_month,
    custom_tags,
    usage_unit,
    usage_quantity,
    usage_metadata,
    identity_metadata,
    record_type,
    ingestion_date,
    billing_origin_product,
    product_features,
    usage_type
FROM system.billing.usage
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), {lookback_days})
  AND usage_quantity > 0
"""

spark.sql(bronze_usage_sql)
bronze_count = spark.sql(f"SELECT COUNT(*) AS c FROM {catalog}.bronze.billing_usage").collect()[0].c
print(f"✅ {catalog}.bronze.billing_usage: {bronze_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. BRONZE — List prices (DBU $ rates)

# COMMAND ----------

try:
    bronze_prices_sql = f"""
    CREATE OR REPLACE TABLE {catalog}.bronze.list_prices AS
    SELECT
        price_start_time,
        price_end_time,
        account_id,
        sku_name,
        cloud,
        currency_code,
        usage_unit,
        pricing
    FROM system.billing.list_prices
    """
    spark.sql(bronze_prices_sql)
    prices_count = spark.sql(f"SELECT COUNT(*) AS c FROM {catalog}.bronze.list_prices").collect()[0].c
    print(f"✅ {catalog}.bronze.list_prices: {prices_count:,} rows")
except Exception as e:
    print(f"⚠️ list_prices not available: {e}")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.bronze.list_prices (
        price_start_time TIMESTAMP, price_end_time TIMESTAMP,
        account_id STRING, sku_name STRING, cloud STRING,
        currency_code STRING, usage_unit STRING, pricing STRUCT<default:DOUBLE>
    ) USING DELTA
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. BRONZE — Cluster metadata (for joining tags)

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.bronze.clusters AS
    SELECT
        account_id,
        workspace_id,
        cluster_id,
        cluster_name,
        owned_by,
        create_time,
        delete_time,
        driver_node_type,
        worker_node_type,
        worker_count,
        min_autoscale_workers,
        max_autoscale_workers,
        auto_termination_minutes,
        enable_elastic_disk,
        tags,
        cluster_source,
        init_scripts,
        aws_attributes,
        azure_attributes,
        gcp_attributes,
        driver_instance_pool_id,
        worker_instance_pool_id,
        dbr_version,
        change_time,
        change_date,
        data_security_mode,
        policy_id
    FROM system.compute.clusters
    """)
    print(f"✅ {catalog}.bronze.clusters created")
except Exception as e:
    print(f"⚠️ system.compute.clusters not yet available: {e}")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.bronze.clusters (
        account_id STRING, workspace_id STRING, cluster_id STRING,
        cluster_name STRING, owned_by STRING, tags MAP<STRING,STRING>,
        worker_node_type STRING, driver_node_type STRING,
        change_time TIMESTAMP
    ) USING DELTA
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. BRONZE — Job run history

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.bronze.job_runs AS
    SELECT
        account_id,
        workspace_id,
        job_id,
        run_id,
        period_start_time,
        period_end_time,
        trigger_type,
        run_type,
        run_name,
        compute_ids,
        result_state,
        termination_code
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= DATE_SUB(CURRENT_DATE(), {lookback_days})
    """)
    print(f"✅ {catalog}.bronze.job_runs created")
except Exception as e:
    print(f"⚠️ system.lakeflow.job_run_timeline not available: {e}")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.bronze.job_runs (
        account_id STRING, workspace_id STRING, job_id STRING,
        run_id STRING, period_start_time TIMESTAMP, period_end_time TIMESTAMP,
        result_state STRING
    ) USING DELTA
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. SILVER — Enriched usage with $ cost & tags

# COMMAND ----------

silver_usage_sql = f"""
CREATE OR REPLACE TABLE {catalog}.silver.usage_enriched AS
WITH price_lookup AS (
    SELECT
        sku_name,
        cloud,
        currency_code,
        pricing.default AS price_per_unit,
        price_start_time,
        COALESCE(price_end_time, CURRENT_TIMESTAMP()) AS price_end_time
    FROM {catalog}.bronze.list_prices
),
usage_with_price AS (
    SELECT
        u.*,
        p.price_per_unit,
        p.currency_code,
        u.usage_quantity * COALESCE(p.price_per_unit, 0.0) AS estimated_cost_usd
    FROM {catalog}.bronze.billing_usage u
    LEFT JOIN price_lookup p
      ON u.sku_name = p.sku_name
     AND u.cloud = p.cloud
     AND u.usage_start_time >= p.price_start_time
     AND u.usage_start_time < p.price_end_time
)
SELECT
    record_id,
    workspace_id,
    sku_name,
    cloud,
    usage_date,
    usage_month,
    usage_unit,
    usage_quantity,
    estimated_cost_usd,
    COALESCE(custom_tags['Environment'], 'untagged') AS tag_environment,
    COALESCE(custom_tags['Project'], 'untagged') AS tag_project,
    COALESCE(custom_tags['Owner'], 'untagged') AS tag_owner,
    COALESCE(custom_tags['Team'], 'untagged') AS tag_team,
    COALESCE(custom_tags['CostCenter'], 'untagged') AS tag_costcenter,
    custom_tags,
    usage_metadata.cluster_id AS cluster_id,
    usage_metadata.warehouse_id AS warehouse_id,
    usage_metadata.job_id AS job_id,
    usage_metadata.notebook_id AS notebook_id,
    usage_metadata.dlt_pipeline_id AS pipeline_id,
    identity_metadata.run_as AS run_as_user,
    identity_metadata.owned_by AS owned_by,
    billing_origin_product,
    usage_type,
    record_type
FROM usage_with_price
"""

spark.sql(silver_usage_sql)
silver_count = spark.sql(f"SELECT COUNT(*) AS c FROM {catalog}.silver.usage_enriched").collect()[0].c
print(f"✅ {catalog}.silver.usage_enriched: {silver_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. GOLD — Daily cost rollup

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.daily_cost AS
SELECT
    usage_date,
    workspace_id,
    sku_name,
    billing_origin_product,
    SUM(usage_quantity) AS total_usage,
    SUM(estimated_cost_usd) AS total_cost_usd,
    COUNT(*) AS record_count
FROM {catalog}.silver.usage_enriched
GROUP BY usage_date, workspace_id, sku_name, billing_origin_product
""")
print(f"✅ {catalog}.gold.daily_cost")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. GOLD — Cost by user/owner

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.cost_by_user AS
SELECT
    usage_date,
    workspace_id,
    COALESCE(run_as_user, owned_by, tag_owner, 'unknown') AS user_email,
    SUM(estimated_cost_usd) AS total_cost_usd,
    SUM(usage_quantity) AS total_usage,
    COUNT(DISTINCT cluster_id) AS distinct_clusters,
    COUNT(DISTINCT warehouse_id) AS distinct_warehouses,
    COUNT(DISTINCT job_id) AS distinct_jobs
FROM {catalog}.silver.usage_enriched
GROUP BY usage_date, workspace_id, user_email
""")
print(f"✅ {catalog}.gold.cost_by_user")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. GOLD — Cost by tags (chargeback)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.cost_by_tags AS
SELECT
    usage_date,
    workspace_id,
    tag_environment,
    tag_project,
    tag_team,
    tag_costcenter,
    SUM(estimated_cost_usd) AS total_cost_usd,
    SUM(usage_quantity) AS total_usage,
    COUNT(*) AS record_count
FROM {catalog}.silver.usage_enriched
GROUP BY usage_date, workspace_id, tag_environment, tag_project, tag_team, tag_costcenter
""")
print(f"✅ {catalog}.gold.cost_by_tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. GOLD — Cost by resource (cluster/warehouse/job)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.cost_by_resource AS
SELECT
    usage_date,
    workspace_id,
    CASE
        WHEN cluster_id IS NOT NULL THEN 'cluster'
        WHEN warehouse_id IS NOT NULL THEN 'warehouse'
        WHEN job_id IS NOT NULL THEN 'job'
        WHEN pipeline_id IS NOT NULL THEN 'pipeline'
        ELSE 'other'
    END AS resource_type,
    COALESCE(cluster_id, warehouse_id, job_id, pipeline_id, 'unknown') AS resource_id,
    SUM(estimated_cost_usd) AS total_cost_usd,
    SUM(usage_quantity) AS total_usage,
    COUNT(*) AS record_count
FROM {catalog}.silver.usage_enriched
GROUP BY usage_date, workspace_id, resource_type, resource_id
""")
print(f"✅ {catalog}.gold.cost_by_resource")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Summary

# COMMAND ----------

print("="*60)
print("INGESTION SUMMARY")
print("="*60)
for tbl in ["bronze.billing_usage", "bronze.list_prices",
            "silver.usage_enriched",
            "gold.daily_cost", "gold.cost_by_user", "gold.cost_by_tags", "gold.cost_by_resource"]:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {catalog}.{tbl}").collect()[0].c
        print(f" {catalog}.{tbl}: {cnt:,} rows")
    except Exception as e:
        print(f" {catalog}.{tbl}: ERROR - {e}")

# COMMAND ----------

# Show top costs (if data exists)
result = spark.sql(f"""
SELECT
    usage_date,
    SUM(total_cost_usd) AS daily_total_usd
FROM {catalog}.gold.daily_cost
GROUP BY usage_date
ORDER BY usage_date DESC
LIMIT 14
""")
display(result)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")