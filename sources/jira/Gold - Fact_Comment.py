# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Comment_Id is already unique on its own -- no composite merge field
# needed here, unlike the bridge/explosion tables. Issue_Key stays a
# plain attribute (not resolved through lookup_key) since there's no
# separate Dim_Issue -- it relates to Fact_Issue directly via that
# shared business key.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_comment",
    table_type="fact",
    key_column="Comment_Key",
    columns={
        "Comment_Id":   {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Key":    {"type": "string", "default": "Unknown"},
        "Comment_Body": {"type": "string", "default": ""},
        "Is_Public":    {"type": "string", "default": "true"},
    },
)

# MARKDOWN ********************

# ## Build the fact from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        comment_id AS Comment_Id,
        issue_key AS Issue_Key,
        comment_body AS Comment_Body,
        is_public AS Is_Public,
        created AS Created,
        updated AS Updated,
        author_account_id,
        update_author_account_id
    FROM Silver.jira.comments
""")

# MARKDOWN ********************

# ## Resolve author foreign keys against Dim_User

# CELL ********************
df = fmt.lookup_key(
    spark, df,
    dim_table_name=f"{GOLD_SCHEMA}.dim_resource",
    dim_natural_key_column="User_Account_Id",
    dim_key_column="User_Key",
    fact_join_column="author_account_id",
    output_column="Author_Key",
    default_to_unknown=True,
)

df = fmt.lookup_key(
    spark, df,
    dim_table_name=f"{GOLD_SCHEMA}.dim_resource",
    dim_natural_key_column="User_Account_Id",
    dim_key_column="User_Key",
    fact_join_column="update_author_account_id",
    output_column="Update_Author_Key",
    default_to_unknown=True,
)

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Fact_Comment built successfully")
