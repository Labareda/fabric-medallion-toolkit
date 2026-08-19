# Fabric notebook source

# MARKDOWN ********************

# ## Bridge_Issue_Blocks -- normalised "is blocked by" relationships
# Grain: ONE ROW PER (BLOCKED ISSUE, BLOCKING ISSUE) PAIR.
#
# Dim_Issue.Predecessor_Issue_Code already carries this same information,
# but as a single comma-joined text column (built for the Gantt's
# dependency-line affordance, not for reporting). A "list the work items
# that block X" report needs one row per relationship, not a string to
# parse in DAX -- that's what this table is for.
#
# Same dual-direction handling as Fact_Test's parent_issues and Fact_Test_
# Coverage's covering_sets: Silver.jira.issue_links has no linked_issue_id,
# only inward_issue_key/outward_issue_key (strings), and link_type is just
# 'Blocks' (the outward/inward LABELS are 'blocks'/'is blocked by', not
# link_type values). A link is captured from whichever side Jira recorded
# it on -- both directions normalised to the same (Blocked_Issue_Code,
# Blocking_Issue_Code) shape, deduplicated with UNION (not UNION ALL) for
# the same reason covering_sets needed it: the Jira REST API attaches each
# link to BOTH linked issues' own issuelinks field, so a per-issue Silver
# ingestion captures the same link twice.
#
#   outward_issue_key populated -> issue_key "blocks" outward_issue_key
#     => issue_key is the BLOCKER, outward_issue_key is BLOCKED
#   inward_issue_key populated  -> issue_key "is blocked by" inward_issue_key
#     => issue_key is BLOCKED, inward_issue_key is the BLOCKER

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_blocks",
    table_type="fact",
    key_column="Issue_Block_Key",
    columns={
        "Blocked_Issue_Code":  {"type": "string", "merge_field": True},
        "Blocking_Issue_Code": {"type": "string", "merge_field": True},
        "Blocked_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Code",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Blocking_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Code",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    WITH blocks AS (
        SELECT issue_key AS Blocking_Issue_Code, outward_issue_key AS Blocked_Issue_Code
        FROM Silver.jira.issue_links
        WHERE link_type = 'Blocks' AND outward_issue_key IS NOT NULL

        UNION

        SELECT inward_issue_key AS Blocking_Issue_Code, issue_key AS Blocked_Issue_Code
        FROM Silver.jira.issue_links
        WHERE link_type = 'Blocks' AND inward_issue_key IS NOT NULL
    )
    SELECT
        b.Blocked_Issue_Code,
        b.Blocking_Issue_Code,
        blocked_di.Issue_Key  AS Blocked_Issue_Key,
        blocking_di.Issue_Key AS Blocking_Issue_Key
    FROM blocks b
    LEFT JOIN {GOLD_SCHEMA}.dim_issue blocked_di  ON blocked_di.Issue_Code = b.Blocked_Issue_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_issue blocking_di ON blocking_di.Issue_Code = b.Blocking_Issue_Code
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Bridge_Issue_Blocks built successfully")
