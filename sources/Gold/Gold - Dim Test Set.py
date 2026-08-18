# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Test_Set
# One row per Xray Test Set.
# A Test Set is a Jira issue, but we surface it as its own dimension here so
# the test report needs no knowledge of Dim_Issue or its hierarchy machinery.
#
# SIMPLIFIED -- Workstream, Release, Status_Name, Lead_Name, Has_No_Lead
# removed. They were denormalised copies of Dim_Issue / Fact_Issue columns
# reachable via Test_Set_Id = Dim_Issue.Issue_Id, and duplicating them here
# risked drifting from the source of truth. If the test report needs to
# slice by workstream/release/status/lead, relate this table to Dim_Issue on
# Test_Set_Id = Issue_Id instead.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_test_set",
    table_type="dim",
    key_column="Test_Set_Key",
    columns={
        "Test_Set_Id":      {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Test_Set_Code":    {"type": "string", "default": "Unknown"},
        "Test_Set_Name":    {"type": "string", "default": "Unknown"},
        "Test_Set_Label":   {"type": "string", "default": "Unknown"},
        "Issue_URL":        {"type": "string", "default": ""},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT DISTINCT
        test_set_issue_id                                       AS Test_Set_Id,
        test_set_issue_key                                      AS Test_Set_Code,
        test_set_summary                                        AS Test_Set_Name,
        CONCAT(test_set_issue_key, ': ', test_set_summary)       AS Test_Set_Label,
        CONCAT('https://esshtransform.atlassian.net/browse/', test_set_issue_key) AS Issue_URL
    FROM Silver.xray.test_sets
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Test_Set built successfully")
