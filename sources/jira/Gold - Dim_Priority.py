# Fabric notebook source
# "Gold - Dim_Priority"
# Attach: Silver + Gold lakehouses + env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
df = spark.sql(f"""
    SELECT
        CAST(priority_id AS STRING) AS Priority_Id,
        COALESCE(priority_name, 'Unknown') AS Priority_Name,
        status_color AS Status_Color
    FROM Silver.jira.priorities
""")

fmt.merge(spark, df, fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_priority",
    table_type="dim",
    merge_fields=["Priority_Id"],
    key_column="Priority_Key",
))

print("Dim_Priority built")
