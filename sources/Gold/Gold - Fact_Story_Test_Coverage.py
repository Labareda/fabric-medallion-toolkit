# Fabric notebook source

# MARKDOWN ********************
# ## Fact_Story_Test_Coverage -- the Story -> Test traceability tree
#
# Purpose:
#   Let the client sit on a Story and expand a single hierarchy that shows
#   every Test that covers it, grouped by the Test's Test Set / Test Plan,
#   and then the issues that are BLOCKING each test.
#
#   Story
#     -> Test Set / Test Plan   (or "Direct" when the test is in no set)
#          -> Test              (linked to the Story via "is tested by")
#               -> Blocker      (Bug / Task the test "is blocked by")
#
# Why this table exists:
#   Bridge_Issue_Link stores only ADJACENT pairs
#   (Story<->Test, Test<->Test Set, Test<->Blocker).  There is no single
#   place to sit on a Story and roll up all of its tests.  This table
#   resolves the full path once, in Gold, so Power BI only has to display a
#   matrix hierarchy -- no cross-table DAX, no ambiguous relationships.
#
# Grain:
#   ONE ROW PER (Story, Container, Test, Blocker)
#
#   Container is the Test Set / Test Plan the test belongs to, or "Direct".
#   Blocker is empty when the test has no blocking issue.
#
# Relationship to Dim_Issue:
#   Active on the anchor Story (Story_Issue_Key).  Every other issue's
#   attributes (test, container, blocker) are denormalised onto the row,
#   exactly like Bridge_Issue_Link, because Power BI can only use one
#   active relationship to Dim_Issue at a time.
# CELL ********************

import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# Replace this with the client's actual Jira base URL (no trailing slash).
#
# Example:
# JIRA_BASE_URL = "https://client.atlassian.net"
# ---------------------------------------------------------------------------

JIRA_BASE_URL = "https://yourcompany.atlassian.net"


# MARKDOWN ********************
# ## Declare the table schema
# CELL ********************

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_story_test_coverage",

    table_type="fact",

    key_column="Story_Test_Coverage_Key",

    columns={

        # -------------------------------------------------------------------
        # Grain (all four resolved together define the row)
        # -------------------------------------------------------------------

        "Story_Code": {
            "type": "string",
            "merge_field": True
        },

        "Container_Code": {
            "type": "string",
            "merge_field": True,
            "default": "Direct"
        },

        "Test_Code": {
            "type": "string",
            "merge_field": True
        },

        "Blocker_Code": {
            "type": "string",
            "merge_field": True,
            "default": "None"
        },


        # -------------------------------------------------------------------
        # Hierarchy display attributes (denormalised for the tree)
        # -------------------------------------------------------------------

        "Story_Summary": {
            "type": "string",
            "default": "Unknown"
        },

        "Coverage_Path": {
            "type": "string",
            "default": "Direct"
        },

        # Both directional labels of the Story <-> Test link, e.g.
        # "is tested by / tests". "Unknown" when Jira supplies no label.
        "Test_Link_Label": {
            "type": "string",
            "default": "Unknown"
        },

        # Both directional labels of the Test <-> Blocker link, e.g.
        # "is blocked by / blocks". "None" when there is no blocker,
        # "Unknown" when a blocker exists but Jira supplies no label.
        "Blocker_Link_Label": {
            "type": "string",
            "default": "None"
        },

        "Container_Type": {
            "type": "string",
            "default": "Direct"
        },

        "Container_Summary": {
            "type": "string",
            "default": "Unknown"
        },

        "Test_Summary": {
            "type": "string",
            "default": "Unknown"
        },

        "Test_Status": {
            "type": "string",
            "default": "Unknown"
        },

        "Blocker_Type": {
            "type": "string",
            "default": "None"
        },

        "Blocker_Summary": {
            "type": "string",
            "default": "None"
        },

        "Blocker_Status": {
            "type": "string",
            "default": "None"
        },

        "Test_URL": {
            "type": "string",
            "default": "Unknown"
        },


        # -------------------------------------------------------------------
        # Foreign keys
        # -------------------------------------------------------------------

        # Active relationship to Dim_Issue is on the anchor Story.
        "Story_Issue_Key": {
            "type": "string",

            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Code",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },

        "Test_Issue_Key": {
            "type": "string",

            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Code",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },

        # FK to the latest status of the Test.
        "Test_Status_Key": {
            "type": "string",

            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_test_status",
                "natural_key_column": "Test_Status_Name",
                "key_column": "Test_Status_Key",
                "unknown_value": "Unknown",
            },
        },

        # Story's own project / team.
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


# MARKDOWN ********************
# ## Resolve the Story -> Test -> Container / Blocker tree
#
# story_test:
#   A Story is covered by a Test through the Jira "Test" link.
#   Captured from BOTH perspectives so it survives whichever side Jira
#   exported:
#       Story row : inward_label  = 'is tested by' -> inward_issue is Test
#       Test  row : outward_label = 'tests'        -> outward_issue is Story
#
# test_container:
#   The Test Set / Test Plan a Test belongs to (Xray containment).
#   A test may sit in several sets, which correctly expands the tree.
#
# test_blocker:
#   The issue a Test "is blocked by" (Jira "Blocks" link), captured from
#   both perspectives.
#
# latest_status_per_test:
#   Most recent Xray result per Test (same rule as Bridge_Issue_Link).
# CELL ********************

df = spark.sql(f"""

    WITH

    # --------------------------------------------------------------------
    # Canonical label pair per link type.
    #
    # inward_label / outward_label are properties of the link TYPE, so we
    # take one representative pair per link_type by aggregation. This is
    # what lets us show BOTH directional labels regardless of which row a
    # given direction was exported on -- and it means we never filter or
    # orient on the label text, only on link_type + which issue key the row
    # carries. Robust to instances that rename the labels
    # (e.g. "is verified by / verifies").
    # --------------------------------------------------------------------

    link_labels AS (

        SELECT
            link_type,
            MAX(inward_label)  AS inward_label,
            MAX(outward_label) AS outward_label
        FROM Silver.jira.issue_links
        GROUP BY link_type
    ),


    # --------------------------------------------------------------------
    # Story <-> Test.
    #
    # Orientation comes from which issue key the row carries, NOT the label:
    #   inward  row : issue_key is the Story, inward_issue_key is the Test
    #   outward row : outward_issue_key is the Story, issue_key is the Test
    # --------------------------------------------------------------------

    story_test AS (

        SELECT
            il.issue_key        AS Story_Code,
            il.inward_issue_key AS Test_Code
        FROM Silver.jira.issue_links il
        WHERE il.link_type = 'Test'
          AND il.inward_issue_key IS NOT NULL

        UNION

        SELECT
            il.outward_issue_key AS Story_Code,
            il.issue_key         AS Test_Code
        FROM Silver.jira.issue_links il
        WHERE il.link_type = 'Test'
          AND il.outward_issue_key IS NOT NULL
    ),


    test_container AS (

        SELECT
            test_issue_key     AS Test_Code,
            test_set_issue_key AS Container_Code,
            'Test Set'         AS Container_Type
        FROM Silver.xray.test_sets
        WHERE test_issue_key IS NOT NULL
          AND test_set_issue_key IS NOT NULL

        UNION

        SELECT
            test_issue_key      AS Test_Code,
            test_plan_issue_key AS Container_Code,
            'Test Plan'         AS Container_Type
        FROM Silver.xray.test_plans
        WHERE test_issue_key IS NOT NULL
          AND test_plan_issue_key IS NOT NULL
    ),


    # --------------------------------------------------------------------
    # Test <-> Blocker.
    #
    # Same key-driven orientation:
    #   inward  row : issue_key is the Test, inward_issue_key is the Blocker
    #   outward row : outward_issue_key is the Test, issue_key is the Blocker
    # --------------------------------------------------------------------

    test_blocker AS (

        SELECT
            il.issue_key        AS Test_Code,
            il.inward_issue_key AS Blocker_Code
        FROM Silver.jira.issue_links il
        WHERE il.link_type = 'Blocks'
          AND il.inward_issue_key IS NOT NULL

        UNION

        SELECT
            il.outward_issue_key AS Test_Code,
            il.issue_key         AS Blocker_Code
        FROM Silver.jira.issue_links il
        WHERE il.link_type = 'Blocks'
          AND il.outward_issue_key IS NOT NULL
    ),


    latest_status_per_test AS (

        SELECT
            Test_Code,
            Test_Status_Name,
            Test_Status_Key

        FROM (
            SELECT
                r.test_issue_key AS Test_Code,
                dts.Test_Status_Name,
                dts.Test_Status_Key,
                ROW_NUMBER() OVER (
                    PARTITION BY r.test_issue_key
                    ORDER BY
                        r.started_on DESC NULLS LAST,
                        r.finished_on DESC NULLS LAST,
                        r.run_id DESC
                ) AS rn
            FROM Silver.xray.test_runs r
            LEFT JOIN {GOLD_SCHEMA}.dim_test_status dts
                ON LOWER(dts.Test_Status_Name) = LOWER(r.status_name)
            WHERE r.test_issue_key IS NOT NULL
        ) x

        WHERE rn = 1
    )


    SELECT DISTINCT

        # ================================================================
        # Grain
        # ================================================================

        st.Story_Code,

        COALESCE(tc.Container_Code, 'Direct') AS Container_Code,

        st.Test_Code,

        COALESCE(tb.Blocker_Code, 'None')     AS Blocker_Code,


        # ================================================================
        # Story
        # ================================================================

        COALESCE(story.Summary, 'Unknown') AS Story_Summary,


        # ================================================================
        # Container (Test Set / Test Plan / Direct)
        # ================================================================

        CASE
            WHEN tc.Container_Type = 'Test Set'  THEN 'Via Test Set'
            WHEN tc.Container_Type = 'Test Plan' THEN 'Via Test Plan'
            ELSE 'Direct'
        END AS Coverage_Path,

        # CONCAT returns NULL if either label is missing, so wrap it to keep
        # the no-blanks / no-nulls rule.
        COALESCE(
            CONCAT(test_ll.inward_label, ' / ', test_ll.outward_label),
            'Unknown'
        ) AS Test_Link_Label,

        CASE
            WHEN tb.Blocker_Code IS NOT NULL THEN
                COALESCE(
                    CONCAT(blk_ll.inward_label, ' / ', blk_ll.outward_label),
                    'Unknown'
                )
            ELSE 'None'
        END AS Blocker_Link_Label,

        COALESCE(tc.Container_Type, 'Direct') AS Container_Type,

        COALESCE(container.Summary, 'Unknown') AS Container_Summary,


        # ================================================================
        # Test
        # ================================================================

        COALESCE(test.Summary, 'Unknown')         AS Test_Summary,

        COALESCE(lsp.Test_Status_Name, 'Unknown') AS Test_Status,


        # ================================================================
        # Blocker
        # ================================================================

        CASE WHEN tb.Blocker_Code IS NULL THEN 'None'
             ELSE COALESCE(blocker.Issue_Type_Name, 'Unknown') END AS Blocker_Type,

        CASE WHEN tb.Blocker_Code IS NULL THEN 'None'
             ELSE COALESCE(blocker.Summary, 'Unknown') END AS Blocker_Summary,

        CASE WHEN tb.Blocker_Code IS NULL THEN 'None'
             ELSE COALESCE(blocker.Status_Name, 'Unknown') END AS Blocker_Status,


        # ================================================================
        # Test URL
        # ================================================================

        CONCAT('{JIRA_BASE_URL}', '/browse/', st.Test_Code) AS Test_URL,


        # ================================================================
        # Foreign keys
        # ================================================================

        story.Issue_Key AS Story_Issue_Key,

        test.Issue_Key  AS Test_Issue_Key,

        lsp.Test_Status_Key,

        proj.Project_Key,

        team.Team_Key


    FROM story_test st


    # --------------------------------------------------------------------
    # Test's container(s) -- kept even when the test is in no set/plan
    # --------------------------------------------------------------------

    LEFT JOIN test_container tc
        ON tc.Test_Code = st.Test_Code


    # --------------------------------------------------------------------
    # Test's blocker(s) -- kept even when the test has no blocker
    # --------------------------------------------------------------------

    LEFT JOIN test_blocker tb
        ON tb.Test_Code = st.Test_Code


    # --------------------------------------------------------------------
    # Issue attributes
    # --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_issue story
        ON story.Issue_Code = st.Story_Code

    LEFT JOIN {GOLD_SCHEMA}.dim_issue test
        ON test.Issue_Code = st.Test_Code

    LEFT JOIN {GOLD_SCHEMA}.dim_issue container
        ON container.Issue_Code = tc.Container_Code

    LEFT JOIN {GOLD_SCHEMA}.dim_issue blocker
        ON blocker.Issue_Code = tb.Blocker_Code


    # --------------------------------------------------------------------
    # Canonical directional labels for each edge (by link type, not text)
    # --------------------------------------------------------------------

    LEFT JOIN link_labels test_ll
        ON test_ll.link_type = 'Test'

    LEFT JOIN link_labels blk_ll
        ON blk_ll.link_type = 'Blocks'


    # --------------------------------------------------------------------
    # Latest test status
    # --------------------------------------------------------------------

    LEFT JOIN latest_status_per_test lsp
        ON lsp.Test_Code = st.Test_Code


    # --------------------------------------------------------------------
    # Story's own project / team
    # --------------------------------------------------------------------

    LEFT JOIN Silver.jira.issues si
        ON si.key = st.Story_Code

    LEFT JOIN {GOLD_SCHEMA}.dim_project proj
        ON proj.Project_Id = si.fields_project_id

    LEFT JOIN {GOLD_SCHEMA}.dim_team team
        ON team.Team_Name = si.fields_team_name

""")


# MARKDOWN ********************
# ## Merge into Gold
# CELL ********************

fmt.merge(
    spark,
    df,
    schema
)

print(
    "Fact_Story_Test_Coverage built successfully"
)
