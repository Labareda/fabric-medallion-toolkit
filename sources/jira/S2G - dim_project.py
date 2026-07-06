# Fabric notebook source
# "S2G - dim_project" — attach Silver + Gold lakehouses, plus env_medallion_toolkit.
#
# Built from jira.projects (Jira's dedicated projects list), NOT from
# distinct project_key values in jira.issues -- if it were built from
# issues, every project a fact references would trivially already be in
# the dimension, defeating the point of the Unknown-member fallback below
# (a task could reference a project that hasn't synced into the projects
# list yet, was renamed, or was deleted).

# CELL ********************
import fabric_medallion_toolkit as fmt

# CELL ********************
df_dim_project = spark.sql("""
    SELECT DISTINCT project_key, project_name
    FROM jira.projects
    WHERE project_key IS NOT NULL
""")

fmt.merge(spark, df_dim_project, fmt.TableSchema(
    table_name="gold.dim_project",
    table_type="dim",
    merge_fields=["project_key"],
    key_column="Project_sk",
    include_unknown_member=True,  # fact_task looks this up -- fall back cleanly on an unmatched project
))

# CELL ********************
print("S2G - dim_project complete.")
