# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# One row per issue x assignee x WORKING day (Mon-Fri, weekends excluded)
# across the issue's start_date -> due_date range, with the original
# estimate spread evenly across those working days. This is the
# "planned" half of resourcing -- Fact_Worklog is the "actual" half.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_resource_allocation",
    table_type="fact",
    key_column="Allocation_Key",
    columns={
        "Issue_Key":       {"type": "string", "merge_field": True},
        "Allocation_Date": {"type": "date", "merge_field": True},
        "Allocated_Hours": {"type": "double", "default": 0.0},
    },
)

# MARKDOWN ********************

# ## Build the daily-spread allocation from Silver

# CELL ********************
# WEEKENDS ARE EXCLUDED from the spread -- an assumption, not a fact from
# the data. If you'd rather spread evenly across ALL calendar days
# (including weekends), remove the "WHERE dayofweek(...) NOT IN (1, 7)"
# filter below and recompute accordingly. Spark's dayofweek(): 1=Sunday,
# 7=Saturday.
df = spark.sql("""
    WITH issue_range AS (
        SELECT
            key AS Issue_Key,
            fields_assignee_accountId AS assignee_account_id,
            CAST(fields_start_date AS date) AS range_start,
            CAST(fields_duedate AS date) AS range_end,
            fields_timeoriginalestimate / 3600.0 AS total_estimate_hours
        FROM Silver.jira.issues
        WHERE fields_start_date IS NOT NULL
          AND fields_duedate IS NOT NULL
          AND fields_assignee_accountId IS NOT NULL
          AND fields_timeoriginalestimate IS NOT NULL
    ),
    exploded AS (
        SELECT
            Issue_Key, assignee_account_id, total_estimate_hours,
            explode(sequence(range_start, range_end, interval 1 day)) AS Allocation_Date
        FROM issue_range
    ),
    working_days_only AS (
        SELECT * FROM exploded
        WHERE dayofweek(Allocation_Date) NOT IN (1, 7)
    ),
    with_day_count AS (
        SELECT *, COUNT(*) OVER (PARTITION BY Issue_Key) AS total_working_days
        FROM working_days_only
    )
    SELECT
        Issue_Key,
        assignee_account_id,
        Allocation_Date,
        total_estimate_hours / total_working_days AS Allocated_Hours
    FROM with_day_count
""")

# MARKDOWN ********************

# ## Resolve foreign keys

# CELL ********************
df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_resource",
                     dim_natural_key_column="Resource_Account_Id", dim_key_column="Resource_Key",
                     fact_join_column="assignee_account_id", output_column="Resource_Key", default_to_unknown=True)

df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_date",
                     dim_natural_key_column="date", dim_key_column="date_key",
                     fact_join_column="Allocation_Date", output_column="Date_Key", default_to_unknown=True)

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Fact_ResourceAllocation built successfully")
