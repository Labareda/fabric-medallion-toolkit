# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_worklog",
    table_type="fact",
    key_column="Worklog_Key",
    columns={
        "Worklog_Id":  {"type": "string", "merge_field": True},
        "Hours_Spent": {"type": "double", "default": 0.0},
    },
)

# MARKDOWN ********************

# ## Build the fact from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        worklog_id AS Worklog_Id,
        issue_key AS Issue_Key,
        author_account_id,
        CAST(started AS date) AS logged_date,
        time_spent_seconds / 3600.0 AS Hours_Spent
    FROM Silver.jira.worklogs
""")

# MARKDOWN ********************

# ## Resolve foreign keys

# CELL ********************
df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_resource",
                     dim_natural_key_column="Resource_Account_Id", dim_key_column="Resource_Key",
                     fact_join_column="author_account_id", output_column="Resource_Key", default_to_unknown=True)

df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_date",
                     dim_natural_key_column="date", dim_key_column="date_key",
                     fact_join_column="logged_date", output_column="Date_Key", default_to_unknown=True)

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Fact_Worklog built successfully")
