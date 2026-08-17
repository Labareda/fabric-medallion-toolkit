# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Test_Set
# One row per Xray Test Set.
# A Test Set is a Jira issue, but we surface it as its own dimension here so
# the test report needs no knowledge of Dim_Issue or its hierarchy machinery.
# The fields the client needs for filtering (workstream, release, status,
# lead) are all carried directly -- no cross-table lookup required in the report.

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
        # Context columns carried directly so the report needs no Dim_Issue join
        "Workstream":       {"type": "string", "default": "Unknown"},
        "Release":          {"type": "string", "default": "Unknown"},
        "Status_Name":      {"type": "string", "default": "Unknown"},
        "Lead_Name":        {"type": "string", "default": "Unassigned"},
        "Has_No_Lead":      {"type": "boolean", "default": True},
        "Issue_URL":        {"type": "string", "default": ""},
    },
)

# CELL ********************
df = spark.sql("""
    WITH sets AS (
        SELECT DISTINCT test_set_issue_id, test_set_issue_key, test_set_summary
        FROM Silver.xray.test_sets
    )
    SELECT
        s.test_set_issue_id                         AS Test_Set_Id,
        s.test_set_issue_key                        AS Test_Set_Code,
        s.test_set_summary                          AS Test_Set_Name,
        CONCAT(s.test_set_issue_key, ': ', s.test_set_summary) AS Test_Set_Label,
        -- Carry context from the Jira issue so the test report
        -- needs no relationship to Dim_Issue
        COALESCE(di.Workstream_Label, 'Unknown')    AS Workstream,
        COALESCE(di.Release_Label, 'Unknown')       AS Release,
        COALESCE(fi_status.Status_Name, 'Unknown')  AS Status_Name,
        COALESCE(di.Lead_Name, 'Unassigned')        AS Lead_Name,
        di.Has_No_Lead,
        CONCAT('https://esshtransform.atlassian.net/browse/', s.test_set_issue_key) AS Issue_URL
    FROM sets s
    LEFT JOIN Gold.gold.dim_issue di
           ON s.test_set_issue_id = di.Issue_Id
    LEFT JOIN Gold.gold.fact_issue fi
           ON fi.Issue_Key = di.Issue_Key
    LEFT JOIN Gold.gold.dim_status fi_status
           ON fi.Status_Key = fi_status.Status_Key
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Test_Set built successfully")
