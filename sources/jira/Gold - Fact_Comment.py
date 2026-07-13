# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Issue_Code (the plain Jira business code, e.g. "PROJ-123") is a regular
# attribute; Issue_Key is now a REAL resolved foreign key to Dim_Issue's
# surrogate, joined below on Silver's issue_id (which comments already
# carries) -- not just the business code passed through misleadingly
# named "Issue_Key" the way it was before.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_comment",
    table_type="fact",
    key_column="Comment_Key",
    columns={
        "Comment_Id":   {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Code":   {"type": "string", "default": "Unknown"},
        "Comment_Body": {"type": "string", "default": ""},
        "Is_Public":    {"type": "string", "default": "true"},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Author_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_resource",
                "natural_key_column": "Resource_Account_Id",
                "key_column": "Resource_Key",
                "unknown_value": "Unknown",
            },
        },
        "Update_Author_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_resource",
                "natural_key_column": "Resource_Account_Id",
                "key_column": "Resource_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# MARKDOWN ********************

# ## Build the fact from Silver, joining every dimension directly

# CELL ********************
# Plain JOINs -- nothing hidden, check them here directly. Any row where
# a join doesn't find a match comes back null for that key, and
# lookup_missing_from (declared above) fills it in automatically.
df = spark.sql(f"""
    SELECT
        c.comment_id AS Comment_Id,
        c.issue_key AS Issue_Code,
        dim_issue.Issue_Key AS Issue_Key,
        c.comment_body AS Comment_Body,
        c.is_public AS Is_Public,
        c.created AS Created,
        c.updated AS Updated,
        author.Resource_Key AS Author_Key,
        update_author.Resource_Key AS Update_Author_Key
    FROM Silver.jira.comments c
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON c.issue_id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource author
        ON c.author_account_id = author.Resource_Account_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource update_author
        ON c.update_author_account_id = update_author.Resource_Account_Id
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Fact_Comment built successfully")
