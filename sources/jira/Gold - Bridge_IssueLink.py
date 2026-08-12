# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: ONE ROW PER (FROM_ISSUE, TO_ISSUE, LINK_TYPE). The single bridge
# for BOTH:
#   Link_Type = 'Coverage'         -- Test  covers Requirement
#                                     (from = Test, to = Requirement)
#   Link_Type = 'TestSetMember'    -- Test Set contains Test
#                                     (from = Test Set, to = Test)
# Combined rather than two separate bridges because they have the SAME
# structural shape (one issue related to another, with a type label), and
# collapsing them keeps the model simpler for the report author without
# losing any question the client wants answered -- filtering by Link_Type
# gives back either view.
#
# BOTH sides point at Dim_Issue, so ONE relationship in Power BI has to be
# inactive. To_Issue_Key is the active one -- that's the "child" side in
# both usages (the Requirement being covered, the Test in the Set), and
# it's the side the client filters on ("show me tests for this
# requirement", "show me tests in this set"). From_Issue_Key is inactive,
# reached via USERELATIONSHIP when needed.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_link",
    table_type="fact",
    key_column="Issue_Link_Key",
    columns={
        "From_Issue_Id": {"type": "string", "merge_field": True},
        "To_Issue_Id":   {"type": "string", "merge_field": True},
        "Link_Type":     {"type": "string", "merge_field": True, "default": "Unknown"},
        "Link_Count":    {"type": "int", "default": 1},
        "From_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "To_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build the Coverage rows from jira.issue_links

# CELL ********************
# Xray coverage uses the 'Test' link type in Jira (confirmed against real
# data -- link_type='Test', with outward='tests' and inward='is tested by').
# Read ONLY the outward side (from = Test, to = Requirement) to avoid
# duplicating every link twice from both perspectives.
#
# Requires both issues to resolve to real Issue_Ids in Dim_Issue -- an issue
# linked to something outside the extract scope would give a null one side
# and land on Dim_Issue's Unknown row, which is fine (no crash) but the
# report author would see it. Kept a note here so if that ever spikes, it's
# not a mystery.
coverage_df = spark.sql(f"""
    SELECT
        l.issue_id AS From_Issue_Id,
        to_issue.id AS To_Issue_Id,
        'Coverage' AS Link_Type,
        1 AS Link_Count
    FROM Silver.jira.issue_links l
    LEFT JOIN Silver.jira.issues to_issue
        ON l.outward_issue_key = to_issue.key
    WHERE l.link_type = 'Test'
      AND l.outward_issue_key IS NOT NULL
      AND to_issue.id IS NOT NULL
""")

# MARKDOWN ********************

# ## Build the TestSetMember rows from Silver.xray.test_sets

# CELL ********************
# Requires Silver.xray.test_sets to exist (built by B2S - Xray from the new
# getTestSets extraction in S2B - Xray). If it doesn't yet, this whole branch
# is skipped and only Coverage rows land -- the bridge still works, just
# without the test-set-membership half. That way this notebook doesn't error
# out on a fresh pipeline where Xray Bronze/Silver haven't run.
if spark.catalog.tableExists("Silver.xray.test_sets"):
    membership_df = spark.sql("""
        SELECT
            test_set_issue_id AS From_Issue_Id,
            test_issue_id     AS To_Issue_Id,
            'TestSetMember'   AS Link_Type,
            1 AS Link_Count
        FROM Silver.xray.test_sets
        WHERE test_set_issue_id IS NOT NULL
          AND test_issue_id     IS NOT NULL
    """)
    df = coverage_df.unionByName(membership_df)
    print(f"Coverage rows: {coverage_df.count()}, TestSetMember rows: {membership_df.count()}")
else:
    print("Silver.xray.test_sets doesn't exist yet -- only Coverage rows will land.")
    df = coverage_df

# MARKDOWN ********************

# ## Resolve keys via a join to Dim_Issue

# CELL ********************
df.createOrReplaceTempView("_bridge_staged")
df = spark.sql(f"""
    SELECT
        s.From_Issue_Id, s.To_Issue_Id, s.Link_Type, s.Link_Count,
        di_from.Issue_Key AS From_Issue_Key,
        di_to.Issue_Key   AS To_Issue_Key
    FROM _bridge_staged s
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di_from ON s.From_Issue_Id = di_from.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di_to   ON s.To_Issue_Id   = di_to.Issue_Id
""")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Bridge_IssueLink built successfully")
