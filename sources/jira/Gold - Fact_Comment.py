# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: ONE ROW PER COMMENT. Full history, not "latest per issue" -- a
# report can compute "latest" from this via DAX, the reverse isn't true.
# Losing the history to save one column isn't worth it.
#
# Two role-playing dates: Created_Date drives the primary Dim_Date
# relationship ("when was this said"); Updated_Date is inactive, reached via
# USERELATIONSHIP only if someone specifically asks "when was it edited".
#
# Two authors on the row -- original author and last editor -- BOTH point at
# Dim_Resource, so one has to be inactive. Author_Key is the active one
# ("who said this"); Update_Author_Key is inactive.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_comment",
    table_type="fact",
    key_column="Comment_Key",
    columns={
        "Comment_Id":   {"type": "string", "merge_field": True},
        "Issue_Code":   {"type": "string", "default": "Unknown"},
        "Comment_Body": {"type": "string", "default": ""},
        "Is_Public":    {"type": "boolean", "default": True},
        "Comment_Count":{"type": "int", "default": 1},

        "Created_Date": {"type": "date"},
        "Updated_Date": {"type": "date"},
        "Created_At":   {"type": "timestamp"},
        "Updated_At":   {"type": "timestamp"},

        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Author_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
        "Update_Author_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build from Silver

# CELL ********************
df = spark.sql(f"""
    SELECT
        c.comment_id AS Comment_Id,
        c.issue_key AS Issue_Code,
        c.comment_body AS Comment_Body,
        COALESCE(c.is_public, TRUE) AS Is_Public,
        1 AS Comment_Count,

        CAST(c.created AS timestamp) AS Created_At,
        CAST(c.updated AS timestamp) AS Updated_At,
        CAST(c.created AS date) AS Created_Date,
        CAST(c.updated AS date) AS Updated_Date,

        dim_issue.Issue_Key,
        author.Resource_Key AS Author_Key,
        updater.Resource_Key AS Update_Author_Key
    FROM Silver.jira.comments c
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue ON c.issue_id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource author
        ON c.author_account_id = author.Resource_Account_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource updater
        ON c.update_author_account_id = updater.Resource_Account_Id
""")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Fact_Comment built successfully")
