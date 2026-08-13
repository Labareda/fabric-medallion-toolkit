# Fabric notebook source

# MARKDOWN ********************

# ## Bridge_Issue_Link
# Grain: ONE ROW PER ISSUE PER LINKED ISSUE PER DIRECTION.
#
# BOTH DIRECTIONS ARE STORED. Jira records a link once, from one side. If
# Gold stored it once too, then "show me what blocks this" would work and
# "show me what this blocks" would not, and every report would need
# bidirectional DAX to compensate. Storing the mirror row is cheap and makes
# both questions a plain filter.
#
# This is the traceability backbone. From it you get, with no new structures:
#   * Requirement -> Story -> Test Set -> Test -> Defect coverage chains
#   * requirements with no test coverage (the go-live number that matters)
#   * stories with no parent requirement (scope creep)
#   * cross-workstream blockers -- 'Blocks' links whose two ends have
#     different Workstream_Label values, which is usually the highest-value
#     visual on the whole programme and nobody has asked for it yet

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_link",
    table_type="fact",
    key_column="Issue_Link_Key",
    columns={
        "Link_Id":          {"type": "string", "merge_field": True},
        "Issue_Id":         {"type": "string", "merge_field": True},
        "Direction":        {"type": "string", "merge_field": True},
        "Issue_Code":       {"type": "string", "default": "Unknown"},
        "Linked_Issue_Code":{"type": "string", "default": "Unknown"},
        "Link_Type_Name":   {"type": "string", "default": "Unknown"},
        "Link_Label":       {"type": "string", "default": ""},
        "Link_Count":       {"type": "int", "default": 1},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Linked_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Code", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    WITH links AS (
        SELECT l.link_id AS Link_Id, l.issue_id AS Issue_Id, l.issue_key AS Issue_Code,
               l.outward_issue_key AS Linked_Issue_Code, l.link_type AS Link_Type_Name,
               l.outward_label AS Link_Label, 'Outward' AS Direction
        FROM Silver.jira.issue_links l
        WHERE l.outward_issue_key IS NOT NULL

        UNION ALL

        SELECT l.link_id, l.issue_id, l.issue_key, l.inward_issue_key, l.link_type,
               l.inward_label, 'Inward'
        FROM Silver.jira.issue_links l
        WHERE l.inward_issue_key IS NOT NULL
    )
    SELECT k.*, 1 AS Link_Count,
           src.Issue_Key AS Issue_Key,
           tgt.Issue_Key AS Linked_Issue_Key
    FROM links k
    LEFT JOIN {GOLD_SCHEMA}.dim_issue src ON k.Issue_Id = src.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue tgt ON k.Linked_Issue_Code = tgt.Issue_Code
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Bridge_Issue_Link built successfully")
