# Fabric notebook source

# MARKDOWN ********************
# ## Dim_Issue -- the work-item dimension
# One row per Jira issue at every tier (Programme, Release, Initiative,
# Workstream, Epic, Task, Sub-task are all issues, distinguished by type and
# place in the parent chain).
#
# Xray Tests are Jira issues as well, so their Xray-specific attributes
# (Test_Type and Test_Steps) are integrated into this dimension.
#
# Acceptance_Criteria also belongs here because it is an attribute of the
# Test ISSUE itself, not of an individual Test Run.
#
# Jira_URL is generated from the configured Jira base URL and the issue key.
#
# ONLY THE DENSE WALK IS BUILT -- Level_1..7, depth-placed, contiguous, drives
# the xViz Gantt tree. Leave "Hide Blanks" OFF; trailing nulls are normal.
#
# There is no typed (Programme/Release/...) walk: no report in scope sums or
# groups by workstream/release across the whole programme, only the Gantt
# drill-down, which Level_1..7 plus Sort_Path already cover.
#
# Never filter on the dense columns for cross-project grouping, for the same
# reason: a tier skipped in one project shifts everything up a slot.
#
# LEAD_NAME / RESOURCE_NAMES -- these remain denormalised text because most
# Gantt visuals bind their Resource field from the same table as Task/Start/End.
# Fact_Resource_Allocation remains the source of truth for detailed resourcing
# analysis.
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


# ---------------------------------------------------------------------------
# FINAL PERSISTED COLUMN LIST
# ---------------------------------------------------------------------------
FINAL_COLUMNS = [
    "Issue_Id",
    "Issue_Code",
    "Summary",
    "Display_Label",
    "Is_Milestone",
    "Acceptance_Criteria",
    "Jira_URL",
    "Issue_Type_Name",
    "Status_Name",
    "Priority_Name",
    "Description",
    "Created_Date",
    "Updated_Date",
    "Test_Type",
    "Test_Steps",
    "Parent_Issue_Id",
    "Sort_Path",
    "Level_1",
    "Level_2",
    "Level_3",
    "Level_4",
    "Level_5",
    "Level_6",
    "Level_7",
    "Lead_Name",
    "Resource_Names",
    "Predecessor_Issue_Code",
]


# MARKDOWN ********************
# ## Declare the table schema
# CELL ********************

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue",
    table_type="dim",
    key_column="Issue_Key",

    # MERGE (default), NOT overwrite.
    #
    # Silver full-replaces its tables each run (current snapshot only), so if
    # Gold overwrote too, any issue that leaves Silver -- deleted, or aged out
    # of the extract -- would vanish from Gold.
    #
    # Merge accumulates: rows that drop out of Silver still persist here.

    columns={

        # -------------------------------------------------------------------
        # Core issue identifiers
        # -------------------------------------------------------------------
        "Issue_Id": {
            "type": "string",
            "merge_field": True,
            "missing": "Unknown"
        },

        "Issue_Code": {
            "type": "string",
            "default": "Unknown"
        },

        "Summary": {
            "type": "string",
            "default": "No summary"
        },

        "Display_Label": {
            "type": "string",
            "default": "Unknown"
        },

        "Is_Milestone": {
            "type": "boolean",
            "default": False
        },

        # -------------------------------------------------------------------
        # Xray Acceptance Criteria
        #
        # This belongs to the Jira/Xray Test issue itself, rather than
        # Fact_Test, because it does not change between individual executions.
        # -------------------------------------------------------------------
        "Acceptance_Criteria": {
            "type": "string",
            "default": ""
        },

        # -------------------------------------------------------------------
        # Direct Jira URL
        # -------------------------------------------------------------------
        "Jira_URL": {
            "type": "string",
            "default": ""
        },

        # -------------------------------------------------------------------
        # Current descriptive issue attributes
        # -------------------------------------------------------------------
        "Issue_Type_Name": {
            "type": "string",
            "default": "Unknown"
        },

        "Status_Name": {
            "type": "string",
            "default": "Unknown"
        },

        "Priority_Name": {
            "type": "string",
            "default": "Unknown"
        },

        "Description": {
            "type": "string",
            "default": ""
        },

        "Created_Date": {
            "type": "date"
        },

        "Updated_Date": {
            "type": "date"
        },

        # -------------------------------------------------------------------
        # Xray test-specific attributes
        #
        # Populated only for issues whose type is Test.
        # -------------------------------------------------------------------
        "Test_Type": {
            "type": "string",
            "default": ""
        },

        "Test_Steps": {
            "type": "string",
            "default": ""
        },

        # -------------------------------------------------------------------
        # Hierarchy / working columns
        # -------------------------------------------------------------------
        "Parent_Issue_Id": {
            "type": "string"
        },

        "Sort_Path": {
            "type": "string",
            "default": ""
        },

        "Level_1": {"type": "string"},
        "Level_2": {"type": "string"},
        "Level_3": {"type": "string"},
        "Level_4": {"type": "string"},
        "Level_5": {"type": "string"},
        "Level_6": {"type": "string"},
        "Level_7": {"type": "string"},

        # -------------------------------------------------------------------
        # Gantt resource display
        # -------------------------------------------------------------------
        "Lead_Name": {
            "type": "string",
            "default": "Unassigned"
        },

        "Resource_Names": {
            "type": "string",
            "default": ""
        },

        # -------------------------------------------------------------------
        # Dependency
        # -------------------------------------------------------------------
        "Predecessor_Issue_Code": {
            "type": "string",
            "default": ""
        },
    },
)


# MARKDOWN ********************
# ## Build the flat base from Silver
#
# One row per Jira issue.
# CELL ********************

df = spark.sql(f"""
    WITH predecessors AS (
        SELECT
            issue_id,
            ARRAY_JOIN(
                ARRAY_SORT(
                    COLLECT_SET(inward_issue_key)
                ),
                ','
            ) AS Predecessor_Issue_Code

        FROM Silver.jira.issue_links

        WHERE
            link_type IN ('Blocks')
            AND inward_issue_key IS NOT NULL

        GROUP BY issue_id
    ),

    involved_people AS (
        SELECT
            issue_id,
            ARRAY_SORT(
                COLLECT_SET(person_name)
            ) AS involved_arr

        FROM Silver.jira.issue_people_involved

        WHERE person_name IS NOT NULL

        GROUP BY issue_id
    )

    SELECT

        # -------------------------------------------------------------------
        # Issue identity
        # -------------------------------------------------------------------
        i.id AS Issue_Id,

        i.key AS Issue_Code,

        COALESCE(
            i.fields_summary,
            'No summary'
        ) AS Summary,

        CONCAT(
            i.key,
            ': ',
            COALESCE(
                i.fields_summary,
                'No summary'
            )
        ) AS Display_Label,

        # -------------------------------------------------------------------
        # Milestone
        # -------------------------------------------------------------------
        LOWER(it.name) = 'milestone' AS Is_Milestone,

        # -------------------------------------------------------------------
        # Xray Acceptance Criteria
        #
        # Different Jira/Xray configurations may expose the field under
        # different Silver column names, hence the fallback.
        # -------------------------------------------------------------------
        COALESCE(
            i.fields_customfield_acceptance_criteria,
            i.fields_acceptance_criteria,
            ''
        ) AS Acceptance_Criteria,

        # -------------------------------------------------------------------
        # Jira URL
        # -------------------------------------------------------------------
        CONCAT(
            '{JIRA_BASE_URL}',
            '/browse/',
            i.key
        ) AS Jira_URL,

        # -------------------------------------------------------------------
        # Current issue attributes
        # -------------------------------------------------------------------
        COALESCE(
            it.name,
            'Unknown'
        ) AS Issue_Type_Name,

        COALESCE(
            i.fields_status_name,
            'Unknown'
        ) AS Status_Name,

        COALESCE(
            i.fields_priority_name,
            'Unknown'
        ) AS Priority_Name,

        COALESCE(
            i.fields_description,
            ''
        ) AS Description,

        CAST(
            i.fields_created AS date
        ) AS Created_Date,

        CAST(
            i.fields_updated AS date
        ) AS Updated_Date,

        # -------------------------------------------------------------------
        # Hierarchy working columns
        # -------------------------------------------------------------------
        i.fields_rank AS Rank,

        i.fields_parent_id AS Parent_Issue_Id,

        i.fields_project_id AS Project_Id,

        it.hierarchylevel AS Hierarchy_Level,

        # -------------------------------------------------------------------
        # Resources
        # -------------------------------------------------------------------
        COALESCE(
            i.fields_assignee_displayName,
            'Unassigned'
        ) AS Lead_Name,

        ARRAY_JOIN(
            ARRAY_DISTINCT(
                ARRAY_COMPACT(
                    CONCAT(
                        ARRAY(i.fields_assignee_displayName),
                        COALESCE(
                            p.involved_arr,
                            ARRAY(CAST(NULL AS STRING))
                        )
                    )
                )
            ),
            ', '
        ) AS Resource_Names,

        # -------------------------------------------------------------------
        # Dependencies
        # -------------------------------------------------------------------
        COALESCE(
            pre.Predecessor_Issue_Code,
            ''
        ) AS Predecessor_Issue_Code

    FROM Silver.jira.issues i

    LEFT JOIN Silver.jira.issuetypes it
        ON i.fields_issuetype_id = it.id

    LEFT JOIN predecessors pre
        ON i.id = pre.issue_id

    LEFT JOIN involved_people p
        ON i.id = p.issue_id
""")


# MARKDOWN ********************
# ## Add the hierarchy
# CELL ********************

df = fmt.enrich_issue_hierarchy(
    df,

    id_column="Issue_Id",

    parent_id_column="Parent_Issue_Id",

    rank_column="Rank",

    type_rank_column="Hierarchy_Level",

    typed_code_column="Display_Label",

    dense_code_column="Display_Label",

    root_prefix_lookup=spark.sql(
        f"""
        SELECT
            Project_Id,
            Project_Code
        FROM {GOLD_SCHEMA}.dim_project
        """
    ),

    root_prefix_join_column="Project_Id",

    label_column="Issue_Code",

    # Dense walk -- Level_1..7 by depth, for the Gantt tree.
    build_dense_levels=True,

    # No typed walk.
    # The dense hierarchy is positional and is only intended for the Gantt.
)


# MARKDOWN ********************
# ## Integrate Xray test attributes
#
# A Test is a Jira issue, so Xray-specific test attributes belong on
# Dim_Issue rather than in a separate Dim_Test.
# CELL ********************

from pyspark.sql import functions as F


def _table_exists(name: str) -> bool:
    """
    Check whether a Fabric three-part table exists.

    spark.sql(DESCRIBE TABLE ...) is used because Fabric's SQL parser
    correctly resolves the three-part Lakehouse.schema.table name.
    """
    try:
        spark.sql(
            f"DESCRIBE TABLE {name}"
        )
        return True

    except Exception:
        return False


if _table_exists("Silver.xray.tests"):

    xray_tests = spark.sql("""
        SELECT
            test_issue_id AS Issue_Id,
            test_type AS Test_Type,
            steps_json AS Test_Steps

        FROM Silver.xray.tests

        WHERE test_issue_id IS NOT NULL
    """)

    df = df.join(
        xray_tests,
        on="Issue_Id",
        how="left"
    )

else:

    print(
        "No Silver.xray.tests -- "
        "Test_Type/Test_Steps left empty this run."
    )

    df = (
        df
        .withColumn(
            "Test_Type",
            F.lit("")
        )
        .withColumn(
            "Test_Steps",
            F.lit("")
        )
    )


# MARKDOWN ********************
# ## Trim to the final persisted shape
#
# Drops working columns such as Rank, Depth and Project_Id.
# CELL ********************

df = df.select(
    *FINAL_COLUMNS
)


# MARKDOWN ********************
# ## Merge into Gold
# CELL ********************

fmt.merge(
    spark,
    df,
    schema
)

print(
    "Dim_Issue built successfully"
)