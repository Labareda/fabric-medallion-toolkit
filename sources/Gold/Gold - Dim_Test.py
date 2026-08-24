# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Test -- ALL descriptive attributes of a Test, in ONE table
# Grain: ONE ROW PER TEST (a Jira issue whose issuetype = 'Test').
#
# WHY THIS EXISTS: a Test IS a Jira issue (issuetype='Test'), so its Jira
# attributes live in Silver.jira.issues, while its Xray-specific attributes
# (test type, steps) live in Silver.xray.tests. Before this table those were
# scattered -- Jira attrs reachable only via Dim_Issue/Fact_Issue, test_type
# stranded on the run-grain Fact_Test, steps never surfaced in Gold at all.
# This table brings every DESCRIPTIVE attribute of a test together in one
# place, which is exactly what the reporting requirement asked for.
#
# DESCRIPTIVE ONLY -- NO relationship columns. "Tested by", "tests",
# "blocks", "is blocked by", parent/requirement, test-set membership are
# RELATIONSHIPS, not attributes of the test, and live in Bridge_Issue_Link.
# Run/execution results (latest status, pass/fail, executor, run date) are a
# different GRAIN and live in Fact_Test. This table is the test's own
# standing description, nothing else.
#
# RELATES 1:1 TO Dim_Issue on Test_Code = Issue_Code -- a test is still an
# issue, so it stays in Dim_Issue (the universal navigation hub the bridge
# and every fact point at). Dim_Test is the descriptive extension for the
# subset of issues that are tests. Selecting a test in the Dim_Issue slicer
# filters Dim_Test through that 1:1.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_test",
    table_type="dim",
    key_column="Test_Key",
    columns={
        "Test_Id":              {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Test_Code":            {"type": "string", "default": "Unknown"},
        "Test_Summary":         {"type": "string", "default": ""},
        "Description":          {"type": "string", "default": ""},
        "Test_Status":          {"type": "string", "default": "Unknown"},
        "Priority":             {"type": "string", "default": "Unknown"},
        "Project":              {"type": "string", "default": "Unknown"},
        "Assignee":             {"type": "string", "default": "Unassigned"},
        "Reporter":             {"type": "string", "default": "Unknown"},
        "Acceptance_Criteria":  {"type": "string", "default": ""},
        # Xray-specific, from Silver.xray.tests.
        "Test_Type":            {"type": "string", "default": "Unknown"},
        # Steps land as the raw JSON string (as in Silver) -- nothing needs
        # them exploded to one-row-per-step yet; parse downstream if a report
        # ever needs step-level detail.
        "Steps":                {"type": "string", "default": ""},
        "Created_Date":         {"type": "date"},
        "Updated_Date":         {"type": "date"},
    },
)

# CELL ********************
df = spark.sql(f"""
    SELECT
        i.id                          AS Test_Id,
        i.key                         AS Test_Code,
        i.fields_summary              AS Test_Summary,
        i.fields_description          AS Description,
        i.fields_status_name          AS Test_Status,
        i.fields_priority_name        AS Priority,
        proj.Project_Name             AS Project,
        i.fields_assignee_displayName AS Assignee,
        i.fields_reporter_displayName AS Reporter,
        i.fields_acceptance_criteria  AS Acceptance_Criteria,
        x.test_type                   AS Test_Type,
        x.steps_json                  AS Steps,
        CAST(i.fields_created AS date) AS Created_Date,
        CAST(i.fields_updated AS date) AS Updated_Date
    FROM Silver.jira.issues i
    -- issuetype = 'Test' -- the individual Xray tests. NOT Test Set / Test
    -- Plan / Test Execution (those are containers, navigable via the
    -- bridge, not descriptive tests in their own right).
    LEFT JOIN Silver.xray.tests x     ON x.test_issue_id = i.id
    LEFT JOIN {GOLD_SCHEMA}.dim_project proj ON proj.Project_Id = i.fields_project_id
    WHERE i.fields_issuetype_name = 'Test'
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Test built successfully")
