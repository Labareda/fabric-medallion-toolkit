# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Project
# Project_Code is also the root prefix Dim_Issue's Sort_Path uses, so this
# table must be built BEFORE Dim_Issue -- enrich_issue_hierarchy reads it via
# root_prefix_lookup.
#
# TOTAL_ISSUE_COUNT from the Jira payload is deliberately NOT carried here.
# It is a Jira-computed count taken at extract time and will disagree with
# COUNT() over Fact_Issue. Two issue counts that differ is worse than one.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_project",
    table_type="dim",
    key_column="Project_Key",
    columns={
        "Project_Id":     {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Project_Code":   {"type": "string", "default": "Unknown"},
        "Project_Name":   {"type": "string", "default": "Unknown"},
        "Project_Type":   {"type": "string", "default": "Unknown"},
        "Project_Style":  {"type": "string", "default": "Unknown"},
        "Project_Lead":   {"type": "string", "default": "Unassigned"},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT
        p.id              AS Project_Id,
        p.key             AS Project_Code,
        p.name            AS Project_Name,
        p.projectTypeKey  AS Project_Type,
        p.style           AS Project_Style,
        p.lead_displayName AS Project_Lead
    FROM Silver.jira.projects p
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Project built successfully")
