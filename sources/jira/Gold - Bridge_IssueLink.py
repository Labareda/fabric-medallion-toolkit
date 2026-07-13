# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Issue_Key/Linked_Issue_Key (the resolved surrogates, produced by the two
# joins below) are used directly as merge fields -- no separate
# Issue_Code/Linked_Issue_Code needed, since nobody browses a bridge
# table directly; a report author gets the human-readable code by
# relating through Dim_Issue itself.
#
# lookup_missing_from is STILL declared on both, even though they're
# merge fields -- it runs before merge()'s null-check validation, so if
# either join fails to find a match, this falls back gracefully to
# Dim_Issue's Unknown row's key instead of hard-erroring on a null merge
# field.
#
# ALL FOUR columns are still merge fields together (confirmed necessary
# by real data: the same Issue_Key/Linked_Issue_Key/Link_Type combination
# can legitimately appear twice with different Direction -- e.g.
# TRAN-2104 has BOTH an inward and an outward "Duplicate" link to
# TRAN-2083, two genuinely distinct link objects Jira allows to coexist).
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_link",
    table_type="fact",
    key_column="Issue_Link_Key",
    columns={
        "Issue_Key": {
            "type": "string",
            "merge_field": True,
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Linked_Issue_Key": {
            "type": "string",
            "merge_field": True,
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Link_Type": {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Direction": {"type": "string", "merge_field": True, "missing": "Unknown"},
    },
)

# MARKDOWN ********************

# ## Build the bridge from Silver, joining Dim_Issue on both sides

# CELL ********************
# The owning issue resolves via issue_id (available directly); the LINKED
# issue only has its business code available in Silver (no id), so that
# side resolves by joining on Dim_Issue.Issue_Code instead.
# lookup_missing_from's fallback still works correctly either way, since
# it only needs Dim_Issue's OWN natural key column name (Issue_Id) to
# find its Unknown row, regardless of which column the actual join used.
df = spark.sql(f"""
    SELECT
        owning.Issue_Key AS Issue_Key,
        linked.Issue_Key AS Linked_Issue_Key,
        l.link_type AS Link_Type,
        CASE WHEN l.outward_issue_key IS NOT NULL THEN 'Outward' ELSE 'Inward' END AS Direction
    FROM Silver.jira.issue_links l
    LEFT JOIN {GOLD_SCHEMA}.dim_issue owning
        ON l.issue_id = owning.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue linked
        ON COALESCE(l.outward_issue_key, l.inward_issue_key) = linked.Issue_Code
    WHERE COALESCE(l.outward_issue_key, l.inward_issue_key) IS NOT NULL
""").distinct()

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Bridge_IssueLink built successfully")
