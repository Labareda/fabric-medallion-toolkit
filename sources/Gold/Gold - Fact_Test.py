# Fabric notebook source

# MARKDOWN ********************
# ## Fact_Test -- the ONE test fact
#
# Grain:
#   ONE ROW PER XRAY TEST RUN.
#
# This table consolidates Test Run information into one fact table.
#
# Is_Latest:
#   TRUE for the most recent run for each Test.
#
#   Use Is_Latest = TRUE for:
#       - current test status
#       - client-facing test reports
#       - current test dashboards
#
#   Remove the Is_Latest filter when displaying:
#       - complete execution history
#       - previous test results
#       - execution trends
#
# Test Acceptance Criteria:
#   Stored on Dim_Issue.Acceptance_Criteria because it belongs to the Test
#   issue itself rather than to a specific execution/run.
#
# Test Coverage / Linked Issues:
#   Stored in Bridge_Issue_Link because coverage and relationships are
#   issue-to-issue relationships.
#
# Project_Key / Team_Key:
#   The project/team of the Test issue itself.
#
# Test_Status_Name:
#   Not stored directly in Fact_Test.
#   Dim_Test_Status provides the formatted status via Test_Status_Key.
# CELL ********************

import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"


# MARKDOWN ********************
# ## Declare the table schema
# CELL ********************

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test",

    table_type="fact",

    key_column="Test_Fact_Key",

    columns={

        # -------------------------------------------------------------------
        # Run identity
        # -------------------------------------------------------------------

        "Run_Id": {
            "type": "string",
            "merge_field": True
        },


        # -------------------------------------------------------------------
        # Test result measures / flags
        # -------------------------------------------------------------------

        "Is_Passed": {
            "type": "int",
            "default": 0
        },

        "Is_Failed": {
            "type": "int",
            "default": 0
        },

        "Is_Final": {
            "type": "int",
            "default": 0
        },

        "Run_Count": {
            "type": "int",
            "default": 1
        },


        # -------------------------------------------------------------------
        # Current/latest flag
        #
        # TRUE for the most recent run of each Test.
        # -------------------------------------------------------------------

        "Is_Latest": {
            "type": "boolean",
            "default": False
        },


        # -------------------------------------------------------------------
        # Execution dates
        # -------------------------------------------------------------------

        "Started_On": {
            "type": "timestamp"
        },

        "Finished_On": {
            "type": "timestamp"
        },

        "Executed_Date": {
            "type": "date"
        },


        # -------------------------------------------------------------------
        # Test issue
        #
        # Test_Issue_Key links the Test Run to Dim_Issue.
        # -------------------------------------------------------------------

        "Test_Issue_Key": {
            "type": "string",

            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },


        # -------------------------------------------------------------------
        # Xray Test Execution issue
        # -------------------------------------------------------------------

        "Execution_Issue_Key": {
            "type": "string",

            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },


        # -------------------------------------------------------------------
        # Test status
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
        # Executor
        # -------------------------------------------------------------------

        "Executed_By_Key": {
            "type": "string",

            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_resource",
                "natural_key_column": "Resource_Id",
                "key_column": "Resource_Key",
                "unknown_value": "Unknown",
            },
        },


        # -------------------------------------------------------------------
        # Test issue's project/team
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
# ## Build Test Run fact
#
# One row per Xray Test Run.
#
# The latest run is determined using:
#
#   1. Started_On DESC
#   2. Finished_On DESC
#   3. Run_Id DESC
#
# This ensures that Is_Latest is deterministic if two runs have the same
# Started_On timestamp.
# CELL ********************

df = spark.sql(f"""

    SELECT

        # ================================================================
        # Run identity
        # ================================================================

        r.run_id AS Run_Id,


        # ================================================================
        # Result flags
        # ================================================================

        CASE
            WHEN UPPER(r.status_name) = 'PASSED'
                THEN 1
            ELSE 0
        END AS Is_Passed,


        CASE
            WHEN UPPER(r.status_name) = 'FAILED'
                THEN 1
            ELSE 0
        END AS Is_Failed,


        CASE
            WHEN UPPER(r.status_name) IN ('PASSED', 'FAILED')
                THEN 1
            ELSE 0
        END AS Is_Final,


        1 AS Run_Count,


        # ================================================================
        # Latest run flag
        #
        # More deterministic than using Started_On alone.
        # ================================================================

        ROW_NUMBER() OVER (

            PARTITION BY r.test_issue_id

            ORDER BY
                r.started_on DESC NULLS LAST,
                r.finished_on DESC NULLS LAST,
                r.run_id DESC

        ) = 1 AS Is_Latest,


        # ================================================================
        # Dates
        # ================================================================

        r.started_on AS Started_On,

        r.finished_on AS Finished_On,

        CAST(
            r.finished_on AS date
        ) AS Executed_Date,


        # ================================================================
        # Test issue
        # ================================================================

        test_issue.Issue_Key AS Test_Issue_Key,


        # ================================================================
        # Test Execution issue
        # ================================================================

        exec_issue.Issue_Key AS Execution_Issue_Key,


        # ================================================================
        # Test status
        #
        # LOWER() makes this case-insensitive because Xray normally returns
        # uppercase values such as PASSED/FAILED while the Gold dimension
        # contains display values such as Passed/Failed.
        # ================================================================

        status.Test_Status_Key,


        # ================================================================
        # Executor
        # ================================================================

        resource.Resource_Key AS Executed_By_Key,


        # ================================================================
        # Test project's project/team
        # ================================================================

        proj.Project_Key,

        team.Team_Key


    FROM Silver.xray.test_runs r


    # --------------------------------------------------------------------
    # Test issue
    # --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_issue test_issue

        ON test_issue.Issue_Id = r.test_issue_id


    # --------------------------------------------------------------------
    # Test Execution issue
    # --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_issue exec_issue

        ON exec_issue.Issue_Id = r.execution_issue_id


    # --------------------------------------------------------------------
    # Test status
    # --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_test_status status

        ON LOWER(status.Test_Status_Name)
           =
           LOWER(r.status_name)


    # --------------------------------------------------------------------
    # Executing resource
    # --------------------------------------------------------------------

    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource

        ON resource.Resource_Id = r.executed_by_id


    # --------------------------------------------------------------------
    # Test issue's own Jira project/team
    # --------------------------------------------------------------------

    LEFT JOIN Silver.jira.issues ti

        ON ti.id = r.test_issue_id


    LEFT JOIN {GOLD_SCHEMA}.dim_project proj

        ON proj.Project_Id = ti.fields_project_id


    LEFT JOIN {GOLD_SCHEMA}.dim_team team

        ON team.Team_Name = ti.fields_team_name

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
    "Fact_Test built successfully"
)