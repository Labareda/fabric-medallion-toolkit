# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: ONE ROW PER (TEST, REQUIREMENT) PAIR. "Requirement" here means
# whatever issue the Test is linked to via the "Tests" link type -- in
# practice mostly Stories, sometimes Bugs or Epics; nothing in Xray or Jira
# restricts the other side to a specific issue type.
#
# WHY THIS TABLE EXISTS SEPARATELY FROM Bridge_IssueLink, WHEN THE DATA IS
# ALREADY IN THERE: Bridge_IssueLink is generic -- Issue_Key / Linked_Issue_Key
# / Direction, for EVERY link type Jira has (Blocks, Duplicates, Relates,
# Tests...). It does not say which side is the Test. A report author using
# it for coverage would need to know, every time, "if Link_Type = 'Tests' and
# Direction = 'Outward', then Issue_Key is the Test" -- exactly the kind of
# hidden convention this model has been built to avoid elsewhere. This table
# resolves that ONCE, so Test_Issue_Key and Requirement_Issue_Key mean
# exactly what their names say, with no direction logic for anyone to
# remember.
#
# No new extraction: issue links already land via the ordinary Jira issues
# pull (issuelinks is part of fields=*all) and are already flattened to
# Silver.jira.issue_links. This notebook is Gold-only.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_test_coverage",
    table_type="fact",
    key_column="Test_Coverage_Key",
    columns={
        "Test_Issue_Key": {
            "type": "string",
            "merge_field": True,
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Requirement_Issue_Key": {
            "type": "string",
            "merge_field": True,
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build: normalize both sides of the "Tests" link into Test/Requirement columns

# CELL ********************
# Jira issue links are stored per-issue -- if issue A has an OUTWARD "Tests"
# link to B, that JSON row means "A tests B" (A is the Test, B is the
# Requirement). If A has an INWARD "Tests" link to B, that means "A is
# tested by B" (A is the Requirement, B is the Test). Both the Test and the
# Requirement carry their own copy of this relationship in their own
# issuelinks array, so Silver.jira.issue_links contains it TWICE -- once
# from each issue's perspective. The UNION ALL below reads both directions
# and normalizes each to the same (Test, Requirement) shape; the final
# DISTINCT collapses the resulting duplicate pair back to one row.
df = spark.sql(f"""
    WITH outward_as_test AS (
        -- issue_id has an OUTWARD "Tests" link -> issue_id is the Test,
        -- the linked issue is the Requirement.
        SELECT
            l.issue_id AS test_issue_id,
            l.outward_issue_key AS requirement_issue_code
        FROM Silver.jira.issue_links l
        WHERE l.link_type = 'Tests' AND l.outward_issue_key IS NOT NULL
    ),
    inward_as_requirement AS (
        -- issue_id has an INWARD "Tests" link -> issue_id is the
        -- Requirement, the linked issue is the Test.
        SELECT
            l.inward_issue_key AS test_issue_code,
            l.issue_id AS requirement_issue_id
        FROM Silver.jira.issue_links l
        WHERE l.link_type = 'Tests' AND l.inward_issue_key IS NOT NULL
    ),
    normalized AS (
        SELECT test.Issue_Key AS Test_Issue_Key, req.Issue_Key AS Requirement_Issue_Key
        FROM outward_as_test o
        LEFT JOIN {GOLD_SCHEMA}.dim_issue test ON o.test_issue_id = test.Issue_Id
        LEFT JOIN {GOLD_SCHEMA}.dim_issue req  ON o.requirement_issue_code = req.Issue_Code

        UNION ALL

        SELECT test.Issue_Key AS Test_Issue_Key, req.Issue_Key AS Requirement_Issue_Key
        FROM inward_as_requirement i
        LEFT JOIN {GOLD_SCHEMA}.dim_issue test ON i.test_issue_code = test.Issue_Code
        LEFT JOIN {GOLD_SCHEMA}.dim_issue req  ON i.requirement_issue_id = req.Issue_Id
    )
    SELECT DISTINCT Test_Issue_Key, Requirement_Issue_Key
    FROM normalized
    WHERE Test_Issue_Key IS NOT NULL AND Requirement_Issue_Key IS NOT NULL
""")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Bridge_TestCoverage built successfully")
