# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Author_Key/Update_Author_Key: you write the plain JOIN yourself below
# (so you can see/check it), and lookup_missing_from handles the "join
# found nothing, use Dim_Resource's own Unknown row's key instead of
# leaving null" part automatically -- no COALESCE/subquery to write by hand.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_comment",
    table_type="fact",
    key_column="Comment_Key",
    columns={
        "Comment_Id":   {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Key":    {"type": "string", "default": "Unknown"},
        "Comment_Body": {"type": "string", "default": ""},
        "Is_Public":    {"type": "string", "default": "true"},
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

# ## Build the fact from Silver, joining Dim_Resource directly

# CELL ********************
# Plain JOINs -- nothing hidden, check them here directly. Any row where
# a join doesn't find a match comes back null for that key, and
# lookup_missing_from (declared above) fills it in automatically.
df = spark.sql(f"""
    SELECT
        c.comment_id AS Comment_Id,
        c.issue_key AS Issue_Key,
        c.comment_body AS Comment_Body,
        c.is_public AS Is_Public,
        c.created AS Created,
        c.updated AS Updated,
        author.Resource_Key AS Author_Key,
        update_author.Resource_Key AS Update_Author_Key
    FROM Silver.jira.comments c
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
