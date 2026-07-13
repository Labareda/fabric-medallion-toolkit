# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Same id/code/key pattern as Dim_Project: Issue_Id (Silver's numeric id)
# is the merge field, Issue_Code (Silver's "key", e.g. "DGPR-1037") is a
# plain attribute, Issue_Key is the generated surrogate -- no naming
# collision between the natural business code and the surrogate.
#
# Parent_Issue_Id deliberately has NO default -- null here is a real,
# meaningful value (this issue has no parent, i.e. it's a root-level item
# in the Gantt hierarchy), not a missing value to paper over.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue",
    table_type="dim",
    key_column="Issue_Key",
    columns={
        "Issue_Id":       {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Code":     {"type": "string", "default": "Unknown"},
        "Summary":        {"type": "string", "default": "No summary"},
        "Parent_Issue_Id": {"type": "string"},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        id AS Issue_Id,
        key AS Issue_Code,
        fields_summary AS Summary,
        fields_parent_id AS Parent_Issue_Id
    FROM Silver.jira.issues
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_Issue built successfully")
