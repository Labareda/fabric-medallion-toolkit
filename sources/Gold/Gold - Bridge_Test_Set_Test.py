# Fabric notebook source

# MARKDOWN ********************

# ## Bridge_Test_Set_Test
# Grain: ONE ROW PER TEST SET PER TEST.
#
# This is the table that makes the client's requested testing hierarchy
# possible: Requirement -> Test Set -> Test -> Test Run. Test Sets and Tests
# are both Jira issues, so both ends resolve into Dim_Issue and the whole
# structure reuses the existing dimension.
#
# WHY A BRIDGE AND NOT A PARENT COLUMN: in Xray a Test can belong to more
# than one Test Set. Run the check in the validation notebook. If no test
# belongs to two sets, this is really 1:many and Test_Set_Code can be
# flattened onto Dim_Issue for a genuine expand/collapse hierarchy -- which
# is the nicer outcome and what the client pictured. If tests DO span sets,
# keep the bridge, and be explicit in the report that a test counted under
# two sets means set-level totals will not sum to the programme total.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_test_set_test",
    table_type="fact",
    key_column="Test_Set_Test_Key",
    columns={
        "Test_Set_Issue_Id": {"type": "string", "merge_field": True},
        "Test_Issue_Id":     {"type": "string", "merge_field": True},
        "Test_Set_Code":     {"type": "string", "default": "Unknown"},
        "Test_Set_Summary":  {"type": "string", "default": ""},
        "Test_Code":         {"type": "string", "default": "Unknown"},
        "Test_Set_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    SELECT
        ts.test_set_issue_id  AS Test_Set_Issue_Id,
        ts.test_issue_id      AS Test_Issue_Id,
        ts.test_set_issue_key AS Test_Set_Code,
        ts.test_set_summary   AS Test_Set_Summary,
        ts.test_issue_key     AS Test_Code,
        set_issue.Issue_Key   AS Test_Set_Issue_Key,
        test_issue.Issue_Key  AS Test_Issue_Key
    FROM Silver.xray.test_sets ts
    LEFT JOIN {GOLD_SCHEMA}.dim_issue set_issue  ON ts.test_set_issue_id = set_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue test_issue ON ts.test_issue_id = test_issue.Issue_Id
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Bridge_Test_Set_Test built successfully")
