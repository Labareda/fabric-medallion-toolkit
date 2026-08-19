# Fabric notebook source

# MARKDOWN ********************

# ## Bridge_Issue_Link -- every linked-work-item relationship, any type
# Grain: ONE ROW PER ISSUE PER LINK RECORD, from that issue's OWN
# perspective -- i.e. this mirrors Jira's own "Linked work items" panel
# (see the client's screenshots): for issue A "is blocked by" B, A gets a
# row here saying so, AND B gets its own separate row saying it "blocks" A.
# Both rows are correct and wanted, not duplicates to collapse -- unlike
# Bridge_Issue_Blocks (which normalises "Blocks" specifically down to one
# semantic pair per relationship), this table is general-purpose: every
# link type, both directions, so a report can filter to whichever relation
# the client actually wants (Blocked by, Tests, Relates to, Implements,
# Duplicates, ...) instead of having a bridge per link type.
#
# Silver.jira.issue_links has no linked_issue_id column -- only
# inward_issue_key/outward_issue_key (strings) -- see Bridge_Issue_Blocks
# and Fact Test.py for the same finding. Each row already has EITHER
# outward_issue_key OR inward_issue_key populated (never both), so this
# is a straight COALESCE, not the UNION-of-two-directions pattern those
# other notebooks needed -- there's no separate "requirement side" vs
# "test side" to normalise into one shape here; the anchor is just
# whichever issue that Silver row's own issue_key/issue_id is.
#
# Link_Label is whichever of inward_label/outward_label matches the
# populated side -- "is blocked by", "blocks", "tests", "relates to", etc.
# -- the human-readable phrase Jira itself shows in the Linked work items
# panel. Dim_Link_Type carries both labels per type for anyone who wants
# the type's OTHER label too.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_link",
    table_type="fact",
    key_column="Issue_Link_Key",
    columns={
        "Issue_Code":        {"type": "string", "merge_field": True},
        "Linked_Issue_Code": {"type": "string", "merge_field": True},
        "Link_Type_Name":    {"type": "string", "merge_field": True},
        "Direction":         {"type": "string", "merge_field": True},
        "Link_Label":        {"type": "string", "default": "Unknown"},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Code",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Linked_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Code",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Link_Type_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_link_type",
                "natural_key_column": "Link_Type_Name",
                "key_column": "Link_Type_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    SELECT DISTINCT
        il.issue_key AS Issue_Code,
        COALESCE(il.outward_issue_key, il.inward_issue_key) AS Linked_Issue_Code,
        il.link_type AS Link_Type_Name,
        CASE WHEN il.outward_issue_key IS NOT NULL THEN 'Outward' ELSE 'Inward' END AS Direction,
        COALESCE(il.outward_label, il.inward_label) AS Link_Label,
        di.Issue_Key,
        linked_di.Issue_Key AS Linked_Issue_Key,
        lt.Link_Type_Key
    FROM Silver.jira.issue_links il
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di        ON di.Issue_Code = il.issue_key
    LEFT JOIN {GOLD_SCHEMA}.dim_issue linked_di ON linked_di.Issue_Code = COALESCE(il.outward_issue_key, il.inward_issue_key)
    LEFT JOIN {GOLD_SCHEMA}.dim_link_type lt    ON lt.Link_Type_Name = il.link_type
    WHERE COALESCE(il.outward_issue_key, il.inward_issue_key) IS NOT NULL
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Bridge_Issue_Link built successfully")
