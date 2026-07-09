# Fabric notebook source
# "Gold - Dim_Status"
# Attach: Silver + Gold lakehouses + env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
df = spark.sql(f"""
    SELECT
        CAST(status_id AS STRING) AS Status_Id,
        COALESCE(status_name, 'Unknown') AS Status_Name,
        COALESCE(status_category, 'Unknown') AS Status_Category
    FROM Silver.jira.statuses
""")

fmt.merge(spark, df, fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_status",
    table_type="dim",
    merge_fields=["Status_Id"],
    key_column="Status_Key",
))

print("Dim_Status built")
