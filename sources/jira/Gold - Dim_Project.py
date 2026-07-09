# Fabric notebook source
# "Gold - Dim_Project"
# Attach: Silver + Gold lakehouses + env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
df = spark.sql(f"""
    SELECT
        CAST(id AS STRING) AS Project_Id,
        key AS Project_Code,
        COALESCE(name, 'Unknown') AS Project_Name,
        COALESCE(projectTypeKey, 'Unknown') AS Project_Type,
        COALESCE(style, 'Unknown') AS Project_Style,
        COALESCE(CAST(CAST(isPrivate AS boolean) AS STRING), 'false') AS Is_Private,
        lead_accountId AS Lead_Account_Id,
        COALESCE(lead_displayName, 'Unassigned') AS Lead_Name,
        COALESCE(CAST(CAST(lead_active AS boolean) AS STRING), 'false') AS Lead_Active
    FROM Silver.jira.projects
""")

fmt.merge(spark, df, fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_project",
    table_type="dim",
    merge_fields=["Project_Id"],
    key_column="Project_Key",
))

print("Dim_Project built")
