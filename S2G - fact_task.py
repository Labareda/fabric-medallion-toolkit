# Fabric notebook source
# "S2G - fact_task" — the Gantt/resource-tracking fact table. Run this AFTER
# dim_project and dim_resource (it looks their keys up). Attach Silver and
# Gold lakehouses.

# CELL ********************
%pip install /lakehouse/default/Files/libs/fabric_medallion_toolkit-0.2.1-py3-none-any.whl

# CELL ********************
from fabric_medallion_toolkit.gold import merge_fact, lookup_dimension_key

# CELL ********************
df_fact_task = spark.sql("""
    SELECT
        ticket_key,
        summary,
        status,
        issue_type,
        parent_key,
        assignee,
        project_key,
        created_at,
        updated_at,
        COALESCE(start_date, CAST(created_at AS DATE)) AS gantt_start_date,
        COALESCE(due_date, DATE_ADD(COALESCE(start_date, CAST(created_at AS DATE)), 1)) AS gantt_end_date,
        CASE WHEN due_date IS NULL THEN true ELSE false END AS is_date_estimated,
        CASE
            WHEN status = 'Done' THEN 100.0
            WHEN status = 'In Progress' THEN 50.0
            ELSE 0.0
        END AS percent_complete,
        original_estimate_seconds,
        time_spent_seconds
    FROM jira.issues
""")

# display(df_fact_task)

# Resolve natural keys into the dimensions' GUID keys before merging.
df_fact_task = lookup_dimension_key(
    spark, df_fact_task,
    dim_table_name="gold.dim_project",
    dim_natural_key_column="project_key",
    dim_key_column="project_key_sk",
    fact_join_column="project_key",
)
df_fact_task = lookup_dimension_key(
    spark, df_fact_task,
    dim_table_name="gold.dim_resource",
    dim_natural_key_column="resource_name",
    dim_key_column="resource_key",
    fact_join_column="assignee",
    # as_of_column="created_at",  # uncomment for point-in-time resolution
    # once dim_resource has real tracked attributes worth being historically
    # accurate about -- leaving it out always links to the CURRENT version.
)

merge_fact(
    spark, df_fact_task,
    table_name="gold.fact_task",
    merge_fields=["ticket_key"],
    surrogate_key_column="task_key",
)

# CELL ********************
print("S2G - fact_task complete.")
