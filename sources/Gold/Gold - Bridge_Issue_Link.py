# Fabric notebook source

# MARKDOWN ********************

# ## Bridge_Issue_Link -- every relationship an issue has to another issue,
# any TYPE, any SOURCE -- Jira-native links AND Xray-native containment.
# Grain: ONE ROW PER ISSUE PER RELATIONSHIP RECORD, from that issue's OWN
# perspective -- i.e. this mirrors Jira's own "Linked work items" panel
# (see the client's screenshots): for issue A "is blocked by" B, A gets a
# row here saying so, AND B gets its own separate row saying it "blocks" A.
# Both rows are correct and wanted, not duplicates to collapse -- this
# table is general-purpose: every relationship type, both directions, so a
# report can filter to whichever relation the client actually wants
# (Blocked by, Tests, Relates to, Test Set, Test Plan, Test Execution...)
# rather than needing a bridge per relationship type. (An earlier version
# of this model had a Blocks-only Bridge_Issue_Blocks alongside this one
# -- removed again immediately, it was pure duplication: filter this table
# to Link_Type_Name = 'Blocks' AND Direction = 'Inward' for the exact same
# rows.)
#
# ALSO ABSORBS Bridge_Test_Membership (a separate table for one turn, then
# folded in here) -- a Test/Test Set/Test Plan/Test Execution is, underneath,
# just a Jira ISSUE (issuetype = Test/Test Set/etc, same Dim_Issue_Type
# every other issue uses), so "this Test Set contains this Test" is the
# SAME KIND of fact as "this issue blocks that issue" -- both are just
# "issue A relates to issue B, this way, for this reason." Splitting them
# into two tables forced a report to stitch together two mini-tables that
# each needed their own version of a "related issue" column, just because
# the two relationship kinds happened to come from different SOURCE
# systems (Jira issue_links vs Xray's native test_sets/test_plans/
# test_runs) -- a Gold-layer/lineage distinction the report shouldn't have
# to care about. One bridge, one Linked Issue Code column, one Link Type
# Name slicer covering everything.
#
# Silver.jira.issue_links has no linked_issue_id column -- only
# inward_issue_key/outward_issue_key (strings) -- see Fact Test.py for the
# same finding. Each row already has EITHER
# outward_issue_key OR inward_issue_key populated (never both), so this
# is a straight COALESCE, not the UNION-of-two-directions pattern the
# Xray-membership branches below need (those genuinely have to synthesize
# both directions, since test_sets/test_plans/test_runs only ever record
# the containment one way round).
#
# Link_Label is whichever of inward_label/outward_label matches the
# populated side for a Jira link -- "is blocked by", "blocks", "tests",
# "relates to", etc. -- the human-readable phrase Jira itself shows in the
# Linked work items panel. For the Xray-membership rows, Link_Label is
# hand-set to "contains" / "belongs to". Dim_Link_Type carries both labels
# per JIRA link type for anyone who wants the type's OTHER label too --
# it has no rows for the Xray container types, so those resolve to
# Link_Type_Key = Unknown, which is harmless (nothing currently reads
# Link_Type_Key besides the relationship itself).

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
# Built as separate DataFrames, unioned in Python -- NOT one big embedded
# SQL string -- so the Xray-membership branches can be skipped gracefully
# (spark.catalog.tableExists) if Xray hasn't run yet. Spark validates every
# table reference in a SQL string at ANALYSIS time, before any row is
# returned -- so if the Xray branches were embedded in the same query as
# issue_link_rows, a missing Silver.xray table would fail the WHOLE query,
# taking the Jira-only relationships down with it. This table used to be
# Jira-only and Xray-independent; absorbing Bridge_Test_Membership's job
# must not regress that independence.
issue_link_rows = spark.sql("""
    SELECT
        il.issue_key AS Issue_Code,
        COALESCE(il.outward_issue_key, il.inward_issue_key) AS Linked_Issue_Code,
        il.link_type AS Link_Type_Name,
        CASE WHEN il.outward_issue_key IS NOT NULL THEN 'Outward' ELSE 'Inward' END AS Direction,
        COALESCE(il.outward_label, il.inward_label) AS Link_Label
    FROM Silver.jira.issue_links il
    WHERE COALESCE(il.outward_issue_key, il.inward_issue_key) IS NOT NULL
""")
all_rows = issue_link_rows

# CELL ********************
# Xray-native containment, synthesized both directions -- test_sets/
# test_plans/test_runs only ever record "this test is in this container",
# never the reverse, unlike Jira's own issue_links.
if spark.catalog.tableExists("Silver.xray.test_sets"):
    test_set_rows = spark.sql("""
        SELECT test_set_issue_key AS Issue_Code, test_issue_key AS Linked_Issue_Code,
               'Test Set' AS Link_Type_Name, 'Outward' AS Direction, 'contains' AS Link_Label
        FROM Silver.xray.test_sets WHERE test_issue_key IS NOT NULL

        UNION ALL

        SELECT test_issue_key AS Issue_Code, test_set_issue_key AS Linked_Issue_Code,
               'Test Set' AS Link_Type_Name, 'Inward' AS Direction, 'belongs to' AS Link_Label
        FROM Silver.xray.test_sets WHERE test_issue_key IS NOT NULL
    """)
    all_rows = all_rows.unionByName(test_set_rows)
else:
    print("No Silver.xray.test_sets -- skipping Test Set containment rows this run.")

if spark.catalog.tableExists("Silver.xray.test_plans"):
    test_plan_rows = spark.sql("""
        SELECT test_plan_issue_key AS Issue_Code, test_issue_key AS Linked_Issue_Code,
               'Test Plan' AS Link_Type_Name, 'Outward' AS Direction, 'contains' AS Link_Label
        FROM Silver.xray.test_plans WHERE test_issue_key IS NOT NULL

        UNION ALL

        SELECT test_issue_key AS Issue_Code, test_plan_issue_key AS Linked_Issue_Code,
               'Test Plan' AS Link_Type_Name, 'Inward' AS Direction, 'belongs to' AS Link_Label
        FROM Silver.xray.test_plans WHERE test_issue_key IS NOT NULL
    """)
    all_rows = all_rows.unionByName(test_plan_rows)
else:
    print("No Silver.xray.test_plans -- skipping Test Plan containment rows this run.")

if spark.catalog.tableExists("Silver.xray.test_runs"):
    # DISTINCT: test_runs is RUN grain (many runs per test under one
    # execution) -- this only needs the (execution, test) pair once.
    test_execution_rows = spark.sql("""
        SELECT DISTINCT execution_issue_key AS Issue_Code, test_issue_key AS Linked_Issue_Code,
               'Test Execution' AS Link_Type_Name, 'Outward' AS Direction, 'contains' AS Link_Label
        FROM Silver.xray.test_runs WHERE test_issue_key IS NOT NULL

        UNION

        SELECT DISTINCT test_issue_key AS Issue_Code, execution_issue_key AS Linked_Issue_Code,
               'Test Execution' AS Link_Type_Name, 'Inward' AS Direction, 'belongs to' AS Link_Label
        FROM Silver.xray.test_runs WHERE test_issue_key IS NOT NULL
    """)
    all_rows = all_rows.unionByName(test_execution_rows)
else:
    print("No Silver.xray.test_runs -- skipping Test Execution containment rows this run.")

# CELL ********************
all_rows.createOrReplaceTempView("all_relationship_rows")
df = spark.sql(f"""
    SELECT DISTINCT
        a.Issue_Code, a.Linked_Issue_Code, a.Link_Type_Name, a.Direction, a.Link_Label,
        di.Issue_Key,
        linked_di.Issue_Key AS Linked_Issue_Key,
        lt.Link_Type_Key
    FROM all_relationship_rows a
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di        ON di.Issue_Code = a.Issue_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_issue linked_di ON linked_di.Issue_Code = a.Linked_Issue_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_link_type lt    ON lt.Link_Type_Name = a.Link_Type_Name
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Bridge_Issue_Link built successfully")
