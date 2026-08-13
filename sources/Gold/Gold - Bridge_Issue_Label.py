# Fabric notebook source

# MARKDOWN ********************

# ## Bridge_Issue_Label
# Grain: ONE ROW PER ISSUE PER LABEL.
#
# No Dim_Label. Label_Name sits here as a degenerate attribute -- a separate
# two-column dimension joined to a two-column bridge earns nothing. Give
# labels their own dimension only if the client wants them grouped into
# themes (a Label_Category column), which is the point at which it pays.
#
# Labels are the client's own ad-hoc taxonomy, so this is where recurring
# blocker themes and informal groupings surface. In the semantic model set
# the Dim_Issue side of this relationship to BOTH directions -- otherwise a
# label slicer cannot filter issues.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_label",
    table_type="fact",
    key_column="Issue_Label_Key",
    columns={
        "Issue_Id":   {"type": "string", "merge_field": True},
        "Label_Name": {"type": "string", "merge_field": True},
        "Issue_Code": {"type": "string", "default": "Unknown"},
        "Label_Count":{"type": "int", "default": 1},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    SELECT l.issue_id AS Issue_Id, l.label_name AS Label_Name, l.issue_key AS Issue_Code,
           1 AS Label_Count, dim_issue.Issue_Key
    FROM Silver.jira.issue_labels l
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue ON l.issue_id = dim_issue.Issue_Id
    WHERE l.label_name IS NOT NULL
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Bridge_Issue_Label built successfully")
