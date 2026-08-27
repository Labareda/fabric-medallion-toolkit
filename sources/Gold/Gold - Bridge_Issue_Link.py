# Fabric notebook source

# MARKDOWN ********************
# ## Bridge_Issue_Link
#
# Every relationship an issue has to another issue, any TYPE, any SOURCE.
#
# Sources:
#   * Jira-native issue links
#   * Xray Test Sets
#   * Xray Test Plans
#   * Xray Test Executions
#
# Grain:
#   ONE ROW PER ISSUE PER RELATIONSHIP RECORD
#
# This table intentionally stores BOTH directions of a relationship where
# the source system only provides one direction.
#
# Example:
#
#   Test Set PTP-10 contains Test PTP-1
#
# produces:
#
#   PTP-10 -> PTP-1   Outward   contains
#   PTP-1  -> PTP-10  Inward    belongs to
#
# This mirrors Jira/Xray from the perspective of each issue and makes the
# table suitable for client-facing traceability reports.
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
# Jira issue_links:
#   The source already contains either outward_issue_key or
#   inward_issue_key, so COALESCE is sufficient.
#
# Xray:
#   Test Set / Test Plan / Test Execution relationships only record the
#   containment direction in Silver, so both directions are generated.
#
# Test Execution:
#   test_runs is run-grain, so DISTINCT is required to avoid producing
#   multiple relationship rows for the same Execution/Test combination.
#
# latest_status_per_test:
#   Gets the most recent Xray result for every Test.
# CELL ********************

df = spark.sql(f"""

    WITH all_rows AS (

        # ================================================================
        # Jira-native issue links
        # ================================================================

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

            COALESCE(
                il.outward_label,
                il.inward_label
            ) AS Link_Label

        FROM Silver.jira.issue_links il

        WHERE COALESCE(
            il.outward_issue_key,
            il.inward_issue_key
        ) IS NOT NULL


        UNION ALL


        # ================================================================
        # Xray Test Set
        # ================================================================

        SELECT
            test_set_issue_key AS Issue_Code,
            test_issue_key AS Linked_Issue_Code,
            'Test Set' AS Link_Type_Name,
            'Outward' AS Direction,
            'contains' AS Link_Label

        FROM Silver.xray.test_sets

        WHERE test_issue_key IS NOT NULL


        UNION ALL


        SELECT
            test_issue_key AS Issue_Code,
            test_set_issue_key AS Linked_Issue_Code,
            'Test Set' AS Link_Type_Name,
            'Inward' AS Direction,
            'belongs to' AS Link_Label

        FROM Silver.xray.test_sets

        WHERE test_issue_key IS NOT NULL


        UNION ALL


        # ================================================================
        # Xray Test Plan
        # ================================================================

        SELECT
            test_plan_issue_key AS Issue_Code,
            test_issue_key AS Linked_Issue_Code,
            'Test Plan' AS Link_Type_Name,
            'Outward' AS Direction,
            'contains' AS Link_Label

        FROM Silver.xray.test_plans

        WHERE test_issue_key IS NOT NULL


        UNION ALL


        SELECT
            test_issue_key AS Issue_Code,
            test_plan_issue_key AS Linked_Issue_Code,
            'Test Plan' AS Link_Type_Name,
            'Inward' AS Direction,
            'belongs to' AS Link_Label

        FROM Silver.xray.test_plans

        WHERE test_issue_key IS NOT NULL


        UNION ALL


        # ================================================================
        # Xray Test Execution
        #
        # DISTINCT because a Test Execution can contain multiple Test Runs
        # for the same Test.
        # ================================================================

        SELECT DISTINCT
            execution_issue_key AS Issue_Code,
            test_issue_key AS Linked_Issue_Code,
            'Test Execution' AS Link_Type_Name,
            'Outward' AS Direction,
            'contains' AS Link_Label

        FROM Silver.xray.test_runs

        WHERE test_issue_key IS NOT NULL


        UNION ALL


        SELECT DISTINCT
            test_issue_key AS Issue_Code,
            execution_issue_key AS Linked_Issue_Code,
            'Test Execution' AS Link_Type_Name,
            'Inward' AS Direction,
            'belongs to' AS Link_Label

        FROM Silver.xray.test_runs

        WHERE test_issue_key IS NOT NULL
    ),


    # ====================================================================
    # Latest Xray result per Test
    #
    # The original model used Started_On only.
    #
    # We now use:
    #   1. Started_On DESC
    #   2. Finished_On DESC
    #   3. Run_Id DESC
    #
    # This makes the selection deterministic when two runs have identical
    # Started_On values.
    # ====================================================================

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
    )


    # ====================================================================
    # Final bridge
    # ====================================================================

    SELECT DISTINCT

        # ----------------------------------------------------------------
        # Relationship
        # ----------------------------------------------------------------
        a.Issue_Code,

        a.Linked_Issue_Code,

        a.Link_Type_Name,

        a.Direction,

        a.Link_Label,


        # ----------------------------------------------------------------
        # Linked issue attributes
        # ----------------------------------------------------------------

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


        # ----------------------------------------------------------------
        # Latest result of linked Test
        # ----------------------------------------------------------------

        COALESCE(
            lsp.Test_Status_Name,
            'Unknown'
        ) AS Linked_Test_Status,


        # ----------------------------------------------------------------
        # Latest result of the ANCHOR issue when it is a Test -- so a
        # blocked-tests report can show each test's own result status
        # alongside what it is blocked by.
        # ----------------------------------------------------------------

        COALESCE(
            lsp_anchor.Test_Status_Name,
            'N/A'
        ) AS Issue_Test_Status,


        # ----------------------------------------------------------------
        # Foreign keys
        # ----------------------------------------------------------------

        di.Issue_Key,

        linked_di.Issue_Key AS Linked_Issue_Key,

        lt.Link_Type_Key,

        lsp.Test_Status_Key,

        proj.Project_Key,

        team.Team_Key


    FROM all_rows a


    # --------------------------------------------------------------------
    # Anchor issue
    # --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_issue di

        ON di.Issue_Code = a.Issue_Code


    # --------------------------------------------------------------------
    # Linked issue
    #
    # This is the source for:
    #   Linked_Issue_Type
    #   Linked_Issue_Summary
    #   Linked_Issue_Acceptance_Criteria
    #   Linked_Issue_URL
    # --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_issue linked_di

        ON linked_di.Issue_Code = a.Linked_Issue_Code


    # --------------------------------------------------------------------
    # Link type
    # --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_link_type lt

        ON lt.Link_Type_Name = a.Link_Type_Name


    # --------------------------------------------------------------------
    # Latest linked Test status
    # --------------------------------------------------------------------

    LEFT JOIN latest_status_per_test lsp

        ON lsp.Test_Code = a.Linked_Issue_Code


    # --------------------------------------------------------------------
    # Latest anchor Test status (anchor issue's own result)
    # --------------------------------------------------------------------

    LEFT JOIN latest_status_per_test lsp_anchor

        ON lsp_anchor.Test_Code = a.Issue_Code


    # --------------------------------------------------------------------
    # Anchor issue's own project/team
    # --------------------------------------------------------------------

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