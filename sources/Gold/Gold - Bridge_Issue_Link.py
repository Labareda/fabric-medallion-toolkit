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
        # Link_Label = the phrase for THIS row's direction (reads correctly
        # from the anchor's side: "is blocked by" / "blocks" / "tests" ...).
        "Link_Label":        {"type": "string", "default": "Unknown"},
        # BOTH directional names of the link TYPE, on every row -- so a
        # report can show either phrasing regardless of which side it's
        # viewing from, and it's never hardcoded to "blocks". Jira gives
        # both on every issue_links row; the Xray containment types get
        # "belongs to" (inward) / "contains" (outward).
        "Inward_Label":      {"type": "string", "default": ""},
        "Outward_Label":     {"type": "string", "default": ""},
        # DENORMALISED linked-issue attributes -- this is what makes the
        # bridge a drag-and-drop fact with NO measures for the "select an
        # issue, see what's linked to it" view. The bridge relates to
        # Dim_Issue on the ANCHOR (Issue_Code) side; Power BI allows only
        # one active relationship to Dim_Issue, so the LINKED issue's own
        # attributes can't come through a relationship -- they're flattened
        # onto the row here at build time instead.
        "Linked_Issue_Summary": {"type": "string", "default": ""},
        # Latest test status of the linked issue, IF it's a test (else "").
        # This is the column that answers "select a Test Set, see its
        # member tests' RESULTS" -- the whole reason the client needed the
        # bridge to carry status.
        "Linked_Test_Status": {"type": "string", "default": ""},
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
        # FK to the LINKED issue's latest test status, so the client can
        # SLICE the bridge by status ("show links where the linked test is
        # Blocked"). Non-test linked issues have no runs -> falls to the
        # Unknown member, harmless (you'd filter by Link Type anyway).
        "Test_Status_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_test_status",
                "natural_key_column": "Test_Status_Name",
                "key_column": "Test_Status_Key",
                "unknown_value": "Unknown",
            },
        },
        # Anchor issue's project/team, so a project or team slicer filters
        # the relationships in one hop -- matches every other fact.
        "Project_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_project",
                "natural_key_column": "Project_Id",
                "key_column": "Project_Key",
                "unknown_value": "Unknown",
            },
        },
        "Team_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_team",
                "natural_key_column": "Team_Name",
                "key_column": "Team_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# CELL ********************
# ONE spark.sql query -- the spark.sql parser is the only thing that resolves
# 3-part Fabric names (Lakehouse.schema.table); spark.table / spark.catalog
# throw REQUIRES_SINGLE_PART_NAMESPACE on them. No existence checks / Python
# unions: Xray (S2B/B2S - Xray) always runs before Gold in orchestration and
# the Xray tables exist, so the earlier graceful-degradation scaffolding was
# unnecessary complexity.
#
# all_rows: every issue-to-issue relationship, one row per (issue, linked
#   issue, type, direction) --
#   * Jira issue_links -- one row already has EITHER outward OR inward
#     populated (never both), so a straight COALESCE, no direction synthesis.
#   * Xray containment (Test Set / Test Plan / Test Execution) -- test_sets/
#     test_plans/test_runs only record "test is in container" one way round,
#     so both directions are synthesized (contains / belongs to). Test
#     Execution uses DISTINCT since test_runs is run grain (many runs per
#     (execution, test) pair).
# latest_status_per_test: each test's latest run status, for the
#   denormalised Linked_Test_Status / Test_Status_Key on the linked side.
df = spark.sql(f"""
    WITH all_rows AS (
        SELECT
            il.issue_key AS Issue_Code,
            COALESCE(il.outward_issue_key, il.inward_issue_key) AS Linked_Issue_Code,
            il.link_type AS Link_Type_Name,
            CASE WHEN il.outward_issue_key IS NOT NULL THEN 'Outward' ELSE 'Inward' END AS Direction,
            COALESCE(il.outward_label, il.inward_label) AS Link_Label,
            -- both directional names of the type, carried on every row
            il.inward_label  AS Inward_Label,
            il.outward_label AS Outward_Label
        FROM Silver.jira.issue_links il
        WHERE COALESCE(il.outward_issue_key, il.inward_issue_key) IS NOT NULL

        UNION ALL
        SELECT test_set_issue_key, test_issue_key, 'Test Set', 'Outward', 'contains', 'belongs to', 'contains'
        FROM Silver.xray.test_sets WHERE test_issue_key IS NOT NULL
        UNION ALL
        SELECT test_issue_key, test_set_issue_key, 'Test Set', 'Inward', 'belongs to', 'belongs to', 'contains'
        FROM Silver.xray.test_sets WHERE test_issue_key IS NOT NULL

        UNION ALL
        SELECT test_plan_issue_key, test_issue_key, 'Test Plan', 'Outward', 'contains', 'belongs to', 'contains'
        FROM Silver.xray.test_plans WHERE test_issue_key IS NOT NULL
        UNION ALL
        SELECT test_issue_key, test_plan_issue_key, 'Test Plan', 'Inward', 'belongs to', 'belongs to', 'contains'
        FROM Silver.xray.test_plans WHERE test_issue_key IS NOT NULL

        UNION ALL
        SELECT DISTINCT execution_issue_key, test_issue_key, 'Test Execution', 'Outward', 'contains', 'belongs to', 'contains'
        FROM Silver.xray.test_runs WHERE test_issue_key IS NOT NULL
        UNION ALL
        SELECT DISTINCT test_issue_key, execution_issue_key, 'Test Execution', 'Inward', 'belongs to', 'belongs to', 'contains'
        FROM Silver.xray.test_runs WHERE test_issue_key IS NOT NULL
    ),
    latest_status_per_test AS (
        SELECT Test_Code, Test_Status_Name, Test_Status_Key
        FROM (
            SELECT
                r.test_issue_key AS Test_Code,
                dts.Test_Status_Name,
                dts.Test_Status_Key,
                ROW_NUMBER() OVER (PARTITION BY r.test_issue_key ORDER BY r.started_on DESC NULLS LAST) AS rn
            FROM Silver.xray.test_runs r
            LEFT JOIN {GOLD_SCHEMA}.dim_test_status dts ON LOWER(dts.Test_Status_Name) = LOWER(r.status_name)
            WHERE r.test_issue_key IS NOT NULL
        ) x
        WHERE rn = 1
    )
    SELECT DISTINCT
        a.Issue_Code, a.Linked_Issue_Code, a.Link_Type_Name, a.Direction, a.Link_Label,
        a.Inward_Label, a.Outward_Label,
        linked_di.Summary       AS Linked_Issue_Summary,
        lsp.Test_Status_Name    AS Linked_Test_Status,
        di.Issue_Key,
        linked_di.Issue_Key     AS Linked_Issue_Key,
        lt.Link_Type_Key,
        lsp.Test_Status_Key,
        proj.Project_Key,
        team.Team_Key
    FROM all_rows a
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di        ON di.Issue_Code = a.Issue_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_issue linked_di ON linked_di.Issue_Code = a.Linked_Issue_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_link_type lt    ON lt.Link_Type_Name = a.Link_Type_Name
    LEFT JOIN latest_status_per_test lsp        ON lsp.Test_Code = a.Linked_Issue_Code
    -- anchor issue's own project/team, via Silver.jira.issues
    LEFT JOIN Silver.jira.issues ai             ON ai.key = a.Issue_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_project proj     ON proj.Project_Id = ai.fields_project_id
    LEFT JOIN {GOLD_SCHEMA}.dim_team team        ON team.Team_Name = ai.fields_team_name
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Bridge_Issue_Link built successfully")
