# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Purely the many-to-many "people involved" relationship -- the "Lead"
# role was dropped since it's redundant with Fact_Issue.Assignee_Key
# (a one-to-many relationship, already correctly handled there).
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_people_involved",
    table_type="fact",
    key_column="Issue_Resource_Key",
    columns={
        "Issue_Key":          {"type": "string", "merge_field": True},
        "Resource_Account_Id": {"type": "string", "merge_field": True},
    },
)

# MARKDOWN ********************

# ## Build the bridge from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        issue_key AS Issue_Key,
        person_account_id AS Resource_Account_Id
    FROM Silver.jira.issue_people_involved
    WHERE person_account_id IS NOT NULL
""").distinct()

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Bridge_IssuePeopleInvolved built successfully")
