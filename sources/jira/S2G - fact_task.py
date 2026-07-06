# Fabric notebook source
# "S2G - fact_task" — the Gantt/resource-tracking fact table. Run AFTER
# dim_project and dim_resource. Attach Silver + Gold lakehouses, plus
# env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt

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

df_fact_task = fmt.lookup_key(
    spark, df_fact_task,
    dim_table_name="gold.dim_project",
    dim_natural_key_column="project_key",
    dim_key_column="Project_sk",
    fact_join_column="project_key",
    output_column="Project_sk",
    default_to_unknown=True,
)
df_fact_task = fmt.lookup_key(
    spark, df_fact_task,
    dim_table_name="gold.dim_resource",
    dim_natural_key_column="resource_name",
    dim_key_column="Resource_key",
    fact_join_column="assignee",
    output_column="Resource_key",
    default_to_unknown=True,
    # as_of_column="created_at",  # uncomment for point-in-time resolution
    # once dim_resource has real tracked attributes.
)

fmt.merge(spark, df_fact_task, fmt.TableSchema(
    table_name="gold.fact_task",
    table_type="fact",
    merge_fields=["ticket_key"],
    key_column="Task_key",
))

# CELL ********************
print("S2G - fact_task complete.")
