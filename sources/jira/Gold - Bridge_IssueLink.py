# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Silver's issue_links has EITHER outward_issue_key OR inward_issue_key
# populated per row (never both) -- COALESCE'd into one Linked_Issue_Key
# below, with Direction preserving which side it came from. Composite
# merge field (not just link_id) since the SAME underlying Jira link
# relationship is recorded on BOTH issues involved -- Issue_Key +
# Linked_Issue_Key + Link_Type is what actually guarantees uniqueness at
# the grain this table needs (one row per issue-to-linked-issue
# relationship), regardless of how link_id itself behaves across the two
# sides.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_link",
    table_type="fact",
    key_column="Issue_Link_Key",
    columns={
        "Issue_Key":        {"type": "string", "merge_field": True},
        "Linked_Issue_Key": {"type": "string", "merge_field": True},
        "Link_Type":        {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Direction":        {"type": "string", "default": "Unknown"},
    },
)

# MARKDOWN ********************

# ## Build the bridge from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        issue_key AS Issue_Key,
        COALESCE(outward_issue_key, inward_issue_key) AS Linked_Issue_Key,
        link_type AS Link_Type,
        CASE WHEN outward_issue_key IS NOT NULL THEN 'Outward' ELSE 'Inward' END AS Direction
    FROM Silver.jira.issue_links
    WHERE COALESCE(outward_issue_key, inward_issue_key) IS NOT NULL
""").distinct()

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Bridge_IssueLink built successfully")
