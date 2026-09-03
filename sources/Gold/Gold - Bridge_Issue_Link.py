# Fabric notebook source

# MARKDOWN ********************
# ## Bridge_Issue_Link
#
# Every issue-to-issue link, both directions.
#
# Source:
#   * Jira issue_links -- the ONLY source of links between issues.
#     Xray does NOT add issue links; it only enriches Tests with a result
#     STATUS (see Linked_Test_Status / Issue_Test_Status below), pulled from
#     Silver.xray.test_runs. Test Set / Plan / Execution containment is NOT
#     a Jira issue link and is intentionally not treated as one here.
#
# Grain:
#   ONE ROW PER ISSUE PER RELATIONSHIP RECORD
#
# Jira stores each link from BOTH issues' perspectives, so both directions
# are already present in issue_links, e.g.:
#
#   TRAN-2151 is blocked by TF-18118   (Inward,  from the test's row)
#   TF-18118  blocks        TRAN-2151  (Outward, from the blocker's row)
#
# Both directional label names (inward_label AND outward_label) are kept on
# every row so the report can filter either way.
#
# The bridge also denormalises attributes of the LINKED issue:
#
#   Linked_Issue_Type
#   Linked_Issue_Summary
#   Linked_Issue_Acceptance_Criteria
#   Linked_Issue_URL
#   Linked_Test_Status
#
# This is intentional. The active relationship to Dim_Issue is on the
# ANCHOR Issue_Code. Power BI cannot simultaneously use another active
# relationship to Dim_Issue for the linked issue's attributes. Therefore
# those linked attributes are flattened onto the bridge during Gold
# processing.
# CELL ********************

import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# Replace this with the client's actual Jira base URL.
#
# Example:
# JIRA_BASE_URL = "https://client.atlassian.net"
#
# Do not include a trailing slash.
# ---------------------------------------------------------------------------

JIRA_BASE_URL = "https://yourcompany.atlassian.net"


# MARKDOWN ********************
# ## Declare the table schema
# CELL ********************

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_link",

    table_type="fact",

    key_column="Issue_Link_Key",

    columns={

        # -------------------------------------------------------------------
        # Anchor relationship
        # -------------------------------------------------------------------
        "Issue_Code": {
            "type": "string",
            "merge_field": True
        },

        "Linked_Issue_Code": {
            "type": "string",
            "merge_field": True
        },

        "Link_Type_Name": {
            "type": "string",
            "merge_field": True
        },

        "Direction": {
            "type": "string",
            "merge_field": True
        },

        # One label per row, matching the row's direction -- holds every
        # label value ("is blocked by", "blocks", "tests", "is tested by",
        # "relates to", ...) so the client filters whichever they need.
        "Link_Label": {
            "type": "string",
            "default": "Unknown"
        },

        # -------------------------------------------------------------------
        # LINKED ISSUE ATTRIBUTES
        #
        # These are deliberately denormalised from Dim_Issue.
        # -------------------------------------------------------------------

        "Linked_Issue_Type": {
            "type": "string",
            "default": "Unknown"
        },

        "Linked_Issue_Summary": {
            "type": "string",
            "default": "Unknown"
        },

        # Jira workflow status of the linked issue (e.g. Draft / To Do /
        # Done). This is the linked issue's OWN status -- drives the
        # "Is Blocked By Status" column on the blocked-tests report.
        "Linked_Issue_Status": {
            "type": "string",
            "default": "Unknown"
        },

        "Linked_Issue_Acceptance_Criteria": {
            "type": "string",
            "default": "Unknown"
        },

        "Linked_Issue_URL": {
            "type": "string",
            "default": "Unknown"
        },

        # -------------------------------------------------------------------
        # Latest test status for the linked issue.
        #
        # Populated when Linked_Issue_Code is an Xray Test.
        # -------------------------------------------------------------------

        "Linked_Test_Status": {
            "type": "string",
            "default": "Unknown"
        },

        # Latest test result of the ANCHOR issue (when it is a Test);
        # "N/A" for non-test anchors.
        "Issue_Test_Status": {
            "type": "string",
            "default": "N/A"
        },

        # TRUE if the anchor issue has an "is blocked by" link. FALSE with
        # Issue_Test_Status = 'Blocked' = blocked but no blocker recorded.
        "Has_Blocker_Link": {
            "type": "boolean",
            "default": False
        },

        # -------------------------------------------------------------------
        # Foreign keys
        # -------------------------------------------------------------------

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

        # -------------------------------------------------------------------
        # FK to latest status of linked Test.
        # -------------------------------------------------------------------

        "Test_Status_Key": {
            "type": "string",

            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_test_status",
                "natural_key_column": "Test_Status_Name",
                "key_column": "Test_Status_Key",
                "unknown_value": "Unknown",
            },
        },

        # -------------------------------------------------------------------
        # Anchor issue's project/team.
        # -------------------------------------------------------------------

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
# ## Build all issue relationships
#
# all_rows:
#   Every link comes from Jira issue_links. Each row already carries either
#   outward_issue_key or inward_issue_key (COALESCE picks the populated one),
#   plus both label names. No Xray containment is treated as a link.
#
# latest_status_per_test:
#   Gets the most recent Xray result for every Test -- the only thing Xray
#   contributes here (a status, not a link).
# CELL ********************

df = spark.sql(f"""

    WITH all_rows AS (

        -- ================================================================
        -- Jira-native issue links
        -- ================================================================

        SELECT

            il.issue_key AS Issue_Code,

            COALESCE(
                il.outward_issue_key,
                il.inward_issue_key
            ) AS Linked_Issue_Code,

            il.link_type AS Link_Type_Name,

            CASE
                WHEN il.outward_issue_key IS NOT NULL
                    THEN 'Outward'
                ELSE 'Inward'
            END AS Direction,

            -- The label MUST follow the DIRECTION, not a COALESCE of labels.
            -- Both inward_label and outward_label are populated on every row
            -- (they are properties of the link TYPE), so COALESCE would
            -- always pick outward ("blocks") -- wrong for an inward row that
            -- is actually "is blocked by". So: an outward row (outward_issue
            -- populated) uses the outward label, an inward row uses the
            -- inward label. One Label column then holds every label value,
            -- and the client filters whichever they want.
            CASE
                WHEN il.outward_issue_key IS NOT NULL
                    THEN il.outward_label
                ELSE il.inward_label
            END AS Link_Label

        FROM Silver.jira.issue_links il

        WHERE COALESCE(
            il.outward_issue_key,
            il.inward_issue_key
        ) IS NOT NULL


        UNION ALL


        -- ================================================================
        -- Issues with NO links -- one placeholder row each, so every issue
        -- still appears in the report (as "No links") even when nothing is
        -- linked to it. Sourced from Dim_Issue so it matches the report's
        -- Dim_Issue relationship. NOT EXISTS is null-safe (unlike NOT IN).
        -- ================================================================

        SELECT
            di.Issue_Code AS Issue_Code,
            '(none)'   AS Linked_Issue_Code,
            'None'     AS Link_Type_Name,
            'None'     AS Direction,
            'No links' AS Link_Label

        FROM {GOLD_SCHEMA}.dim_issue di

        WHERE di.Issue_Code <> 'Unknown'
          AND NOT EXISTS (
              SELECT 1
              FROM Silver.jira.issue_links il
              WHERE il.issue_key = di.Issue_Code
          )
    ),


    -- ====================================================================
    -- Latest Xray result per Test
    --
    -- The original model used Started_On only.
    --
    -- We now use:
    --   1. Started_On DESC
    --   2. Finished_On DESC
    --   3. Run_Id DESC
    --
    -- This makes the selection deterministic when two runs have identical
    -- Started_On values.
    -- ====================================================================

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
                ON LOWER(dts.Test_Status_Name)
                   =
                   LOWER(r.status_name)

            WHERE r.test_issue_key IS NOT NULL
        ) x

        WHERE rn = 1
    ),


    -- ====================================================================
    -- Issues that HAVE an "is blocked by" link (an inward Blocks link).
    -- Used to flag the gap the client cares about: a test that is BLOCKED
    -- but has no blocker recorded -- whether it has no links at all, or has
    -- links but none of them is "is blocked by".
    -- ====================================================================

    blocker_links AS (

        SELECT DISTINCT
            il.issue_key AS Issue_Code

        FROM Silver.jira.issue_links il

        WHERE il.link_type = 'Blocks'
          AND il.inward_issue_key IS NOT NULL
    )


    -- ====================================================================
    -- Final bridge
    -- ====================================================================

    SELECT DISTINCT

        -- ----------------------------------------------------------------
        -- Relationship
        -- ----------------------------------------------------------------
        a.Issue_Code,

        a.Linked_Issue_Code,

        a.Link_Type_Name,

        a.Direction,

        a.Link_Label,


        -- ----------------------------------------------------------------
        -- Linked issue attributes
        -- ----------------------------------------------------------------

        COALESCE(
            linked_di.Issue_Type_Name,
            'Unknown'
        ) AS Linked_Issue_Type,

        COALESCE(
            linked_di.Summary,
            'Unknown'
        ) AS Linked_Issue_Summary,

        COALESCE(
            linked_di.Status_Name,
            'Unknown'
        ) AS Linked_Issue_Status,

        COALESCE(
            linked_di.Acceptance_Criteria,
            'Unknown'
        ) AS Linked_Issue_Acceptance_Criteria,

        CONCAT(
            '{JIRA_BASE_URL}',
            '/browse/',
            a.Linked_Issue_Code
        ) AS Linked_Issue_URL,


        -- ----------------------------------------------------------------
        -- Latest result of linked Test
        -- ----------------------------------------------------------------

        COALESCE(
            lsp.Test_Status_Name,
            'Unknown'
        ) AS Linked_Test_Status,


        -- ----------------------------------------------------------------
        -- Latest result of the ANCHOR issue when it is a Test -- so a
        -- blocked-tests report can show each test's own result status
        -- alongside what it is blocked by.
        -- ----------------------------------------------------------------

        COALESCE(
            lsp_anchor.Test_Status_Name,
            'N/A'
        ) AS Issue_Test_Status,


        -- ----------------------------------------------------------------
        -- Does the anchor issue have an "is blocked by" link at all?
        -- FALSE + Issue_Test_Status = 'Blocked' is the client's gap:
        -- blocked but no blocker recorded.
        -- ----------------------------------------------------------------

        CASE
            WHEN bl.Issue_Code IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS Has_Blocker_Link,


        -- ----------------------------------------------------------------
        -- Foreign keys
        -- ----------------------------------------------------------------

        di.Issue_Key,

        linked_di.Issue_Key AS Linked_Issue_Key,

        lt.Link_Type_Key,

        lsp.Test_Status_Key,

        proj.Project_Key,

        team.Team_Key


    FROM all_rows a


    -- --------------------------------------------------------------------
    -- Anchor issue
    -- --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_issue di

        ON di.Issue_Code = a.Issue_Code


    -- --------------------------------------------------------------------
    -- Linked issue
    --
    -- This is the source for:
    --   Linked_Issue_Type
    --   Linked_Issue_Summary
    --   Linked_Issue_Acceptance_Criteria
    --   Linked_Issue_URL
    -- --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_issue linked_di

        ON linked_di.Issue_Code = a.Linked_Issue_Code


    -- --------------------------------------------------------------------
    -- Link type
    -- --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_link_type lt

        ON lt.Link_Type_Name = a.Link_Type_Name


    -- --------------------------------------------------------------------
    -- Latest linked Test status
    -- --------------------------------------------------------------------

    LEFT JOIN latest_status_per_test lsp

        ON lsp.Test_Code = a.Linked_Issue_Code


    -- --------------------------------------------------------------------
    -- Latest anchor Test status (anchor issue's own result)
    -- --------------------------------------------------------------------

    LEFT JOIN latest_status_per_test lsp_anchor

        ON lsp_anchor.Test_Code = a.Issue_Code


    -- --------------------------------------------------------------------
    -- Whether the anchor issue has any "is blocked by" link
    -- --------------------------------------------------------------------

    LEFT JOIN blocker_links bl

        ON bl.Issue_Code = a.Issue_Code


    -- --------------------------------------------------------------------
    -- Anchor issue's own project/team
    -- --------------------------------------------------------------------

    LEFT JOIN Silver.jira.issues ai

        ON ai.key = a.Issue_Code


    LEFT JOIN {GOLD_SCHEMA}.dim_project proj

        ON proj.Project_Id = ai.fields_project_id


    LEFT JOIN {GOLD_SCHEMA}.dim_team team

        ON team.Team_Name = ai.fields_team_name

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
    "Bridge_Issue_Link built successfully"
)