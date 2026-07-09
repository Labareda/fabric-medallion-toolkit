# Fabric notebook source
# "Gold - Dim_IssueType" — carries hierarchy_level, the field driving the
# parent-child Gantt structure via Fact_Issue.parent_issue_key.
# Attach: Silver + Gold lakehouses + env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
df = spark.sql(f"""
    SELECT
        CAST(issue_type_id AS STRING) AS IssueType_Id,
        COALESCE(issue_type_name, 'Unknown') AS IssueType_Name,
        description AS Description,
        COALESCE(CAST(CAST(is_subtask AS boolean) AS STRING), 'false') AS Is_Subtask,
        hierarchy_level AS Hierarchy_Level
    FROM Silver.jira.issuetypes
""")

fmt.merge(spark, df, fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue_type",
    table_type="dim",
    merge_fields=["IssueType_Id"],
    key_column="IssueType_Key",
))

print("Dim_IssueType built")
