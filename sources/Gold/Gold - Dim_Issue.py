# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Issue -- the work-item dimension
# One row per Jira issue at every tier (Programme, Release, Initiative,
# Workstream, Epic, Task, Sub-task are all issues, distinguished by type and
# place in the parent chain). The wheel adds the hierarchy columns from the
# flat base query below.
#
# ONLY THE DENSE WALK IS BUILT -- Level_1..7, depth-placed, contiguous, drives
# the xViz Gantt tree. Leave "Hide Blanks" OFF; trailing nulls are normal.
# There is no typed (Programme/Release/...) walk: no report in scope sums or
# groups by workstream/release across the whole programme, only the Gantt
# drill-down, which Level_1..7 plus Sort_Path already cover. If a workstream
# rollup report is needed later, re-add the typed walk (see enrich_issue_
# hierarchy's typed_level_names/rank_to_level params) rather than trying to
# fake it from the dense columns -- they're positional (by depth) and mixing
# projects on a Level_N filter silently regroups issues under the wrong tier.
#
# Never filter on the dense columns for cross-project grouping, for the same
# reason: a tier skipped in one project shifts everything up a slot.
#
# SIMPLIFIED -- removed from this table (see below for where the info still
# lives):
#   Rank, Depth               -- Sort_Path already orders the tree; Depth is
#                                 just a count of populated Level_N columns.
#   Project_Id                -- was a snowflake link (Dim_Project ->
#                                 Dim_Issue). Project_Key now sits directly on
#                                 every fact table instead, so a project
#                                 slicer filters facts in one hop, star-schema
#                                 style.
#   Parent_Issue_Key           -- Parent_Issue_Id is kept (Fact_Issue's date
#                                 rollup joins on it); the surrogate key added
#                                 no reporting value nothing else used.
#   Hierarchy_Level_Name, Has_Children, Is_Leaf, Has_No_Lead
#                              -- all derivable from columns already here
#                                 (Level_N non-null count, Lead_Name IS NULL)
#                                 or from Dim_IssueType.Hierarchy_Level via
#                                 Fact_Issue.
#   Resource_Count, Has_No_Lead
#                              -- derivable (Resource_Names being blank/non-
#                                 blank, or via Fact_Resource_Allocation).
#                                 Lead_Name and Resource_Names themselves ARE
#                                 kept -- see note below.
#   Programme_Label, Release_Label, Initiative_Label, Workstream_Label,
#   Epic_Label                 -- the typed tier walk (see above).
#   Connector_Type              -- was a hardcoded 'FS' literal on every row,
#                                 not computed from anything. Zero information
#                                 as stored data; a report-level default if
#                                 the Gantt visual needs the field present.
#
# LEAD_NAME / RESOURCE_NAMES -- back on this table, not just via
# Fact_Resource_Allocation. Most Gantt visuals (xViz included) bind their
# Resource field from the SAME table as Task/Start/End, not through a
# separate fact relationship, so the properly-modelled path
# (Fact_Resource_Allocation -> Dim_Resource -> Dim_Resource_Role, filter Role='Lead')
# doesn't reach the Gantt visual directly. These stay denormalised text for
# that reason; Fact_Resource_Allocation is still the source of truth for any
# resourcing analysis that isn't the Gantt itself (headcount, effort by
# person/role, etc).

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# The final, persisted column list -- everything else the SQL/wheel produce
# along the way (Rank, Depth, Project_Id, Parent_Issue_Key, Hierarchy_Level,
# ...) is a working column, not part of the Gold table.
FINAL_COLUMNS = [
    "Issue_Id", "Issue_Code", "Summary", "Display_Label", "Is_Milestone",
    "Acceptance_Criteria",
    "Issue_Type_Name", "Status_Name", "Priority_Name", "Description",
    "Created_Date", "Updated_Date",
    "Test_Type", "Test_Steps",
    "Parent_Issue_Id",
    "Sort_Path",
    "Level_1", "Level_2", "Level_3", "Level_4", "Level_5", "Level_6", "Level_7",
    "Lead_Name", "Resource_Names",
    "Predecessor_Issue_Code",
]

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue",
    table_type="dim",
    key_column="Issue_Key",
    # MERGE (default), NOT overwrite. Silver full-replaces its tables each run
    # (current snapshot only), so if Gold overwrote too, any issue that leaves
    # Silver -- deleted, or aged out of the extract -- would vanish from Gold.
    # Merge accumulates: rows that drop out of Silver still persist here.
    columns={
        "Issue_Id":         {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Code":       {"type": "string", "default": "Unknown"},
        "Summary":          {"type": "string", "default": "No summary"},
        "Display_Label":    {"type": "string", "default": "Unknown"},
        "Is_Milestone":     {"type": "boolean", "default": False},
        # Only populated on Test-type issues (Xray's Acceptance Criteria
        # field); every other issue gets the default. Lives here rather than
        # on Fact_Test because it's an attribute of the test ISSUE itself --
        # Fact_Test already relates to Dim_Issue via Parent_Issue_Code for
        # display, and any test-level fact can pull it through a relationship
        # instead of carrying its own copy.
        "Acceptance_Criteria": {"type": "string", "default": ""},
        # Descriptive attributes every issue has -- carried here so "select
        # an issue, see what it IS" needs no hop through Fact_Issue. These
        # are the issue's CURRENT values (a point-in-time snapshot); trend/
        # history still lives in Fact_Issue_History. Status/Priority/Type are
        # ALSO available as proper dimensions off Fact_Issue for measure
        # slicing -- these text copies are for display on the issue itself.
        "Issue_Type_Name":  {"type": "string", "default": "Unknown"},
        "Status_Name":      {"type": "string", "default": "Unknown"},
        "Priority_Name":    {"type": "string", "default": "Unknown"},
        "Description":      {"type": "string", "default": ""},
        "Created_Date":     {"type": "date"},
        "Updated_Date":     {"type": "date"},
        # Xray test-specific attributes -- populated ONLY for issuetype=Test
        # (null for every other issue), joined in below from Silver.xray.tests.
        # A test IS an issue, so its Xray attributes are integrated into the
        # ONE issue dimension rather than snowflaked into a separate Dim_Test:
        # "the Test table" is simply this dimension filtered to Issue_Type_Name
        # = 'Test', and every fact still links straight to Dim_Issue (star).
        "Test_Type":        {"type": "string", "default": ""},
        "Test_Steps":       {"type": "string", "default": ""},
        # Kept for Fact_Issue's date-rollup join -- not a reporting column.
        "Parent_Issue_Id":  {"type": "string"},
        "Sort_Path":        {"type": "string", "default": ""},
        # Dense levels -- drive the xViz Gantt Task Name. Depth-placed,
        # contiguous, each shows "KEY: Summary".
        "Level_1": {"type": "string"}, "Level_2": {"type": "string"},
        "Level_3": {"type": "string"}, "Level_4": {"type": "string"},
        "Level_5": {"type": "string"}, "Level_6": {"type": "string"},
        "Level_7": {"type": "string"},
        # Gantt resource display -- see header note.
        "Lead_Name":      {"type": "string", "default": "Unassigned"},
        "Resource_Names": {"type": "string", "default": ""},
        # Dependency affordance for the Gantt
        "Predecessor_Issue_Code": {"type": "string", "default": ""},
    },
)

# MARKDOWN ********************

# ## Build the flat base from Silver (one row per issue)

# CELL ********************
df = spark.sql("""
    WITH predecessors AS (
        SELECT issue_id,
               ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(inward_issue_key)), ',') AS Predecessor_Issue_Code
        FROM Silver.jira.issue_links
        WHERE link_type IN ('Blocks') AND inward_issue_key IS NOT NULL
        GROUP BY issue_id
    ),
    involved_people AS (
        SELECT issue_id, ARRAY_SORT(COLLECT_SET(person_name)) AS involved_arr
        FROM Silver.jira.issue_people_involved
        WHERE person_name IS NOT NULL
        GROUP BY issue_id
    )
    SELECT
        i.id AS Issue_Id,
        i.key AS Issue_Code,
        COALESCE(i.fields_summary, 'No summary') AS Summary,
        CONCAT(i.key, ': ', COALESCE(i.fields_summary, 'No summary')) AS Display_Label,
        LOWER(it.name) = 'milestone' AS Is_Milestone,
        COALESCE(i.fields_customfield_acceptance_criteria,
                 i.fields_acceptance_criteria, '') AS Acceptance_Criteria,
        COALESCE(it.name, 'Unknown')               AS Issue_Type_Name,
        COALESCE(i.fields_status_name, 'Unknown')   AS Status_Name,
        COALESCE(i.fields_priority_name, 'Unknown') AS Priority_Name,
        COALESCE(i.fields_description, '')          AS Description,
        CAST(i.fields_created AS date)              AS Created_Date,
        CAST(i.fields_updated AS date)             AS Updated_Date,
        i.fields_rank AS Rank,
        i.fields_parent_id AS Parent_Issue_Id,
        i.fields_project_id AS Project_Id,
        it.hierarchylevel AS Hierarchy_Level,
        COALESCE(i.fields_assignee_displayName, 'Unassigned') AS Lead_Name,
        ARRAY_JOIN(ARRAY_DISTINCT(ARRAY_COMPACT(CONCAT(
            ARRAY(i.fields_assignee_displayName),
            COALESCE(p.involved_arr, ARRAY(CAST(NULL AS STRING)))))), ', ') AS Resource_Names,
        COALESCE(pre.Predecessor_Issue_Code, '') AS Predecessor_Issue_Code
    FROM Silver.jira.issues i
    LEFT JOIN Silver.jira.issuetypes it ON i.fields_issuetype_id = it.id
    LEFT JOIN predecessors pre          ON i.id = pre.issue_id
    LEFT JOIN involved_people p         ON i.id = p.issue_id
""")

# MARKDOWN ********************

# ## Add the hierarchy (dense levels, Sort_Path)

# CELL ********************
df = fmt.enrich_issue_hierarchy(
    df,
    # Every column-name parameter below is REQUIRED by the wheel -- it has no
    # Jira-specific (or any other) default. This notebook is the one place
    # that says what its own columns are called; the wheel never guesses.
    id_column="Issue_Id",
    parent_id_column="Parent_Issue_Id",
    rank_column="Rank",
    type_rank_column="Hierarchy_Level",
    typed_code_column="Display_Label",
    dense_code_column="Display_Label",
    root_prefix_lookup=spark.sql(f"SELECT Project_Id, Project_Code FROM {GOLD_SCHEMA}.dim_project"),
    root_prefix_join_column="Project_Id",
    label_column="Issue_Code",
    # Dense walk -- Level_1..7 by depth, for the Gantt tree.
    build_dense_levels=True,
    # No typed walk (typed_level_names omitted) -- see header note.
)

# MARKDOWN ********************

# ## Integrate Xray test attributes (Test_Type, Test_Steps)
# A test IS an issue, so its Xray-specific attributes belong on the ONE
# issue dimension -- not a separate Dim_Test (that would be a dimension-to-
# dimension snowflake, not a star). Guarded: Silver.xray.tests is optional
# (Xray may not have run), and Dim_Issue is a required Jira dimension that
# must still build without it -- so the join only happens if the table
# exists; otherwise these columns stay their defaults (empty).

# CELL ********************
from pyspark.sql import functions as F

def _table_exists(name: str) -> bool:
    # spark.sql (DESCRIBE), not spark.catalog/spark.table -- only the SQL
    # parser resolves 3-part Fabric names (Lakehouse.schema.table).
    try:
        spark.sql(f"DESCRIBE TABLE {name}")
        return True
    except Exception:
        return False

if _table_exists("Silver.xray.tests"):
    xray_tests = spark.sql("""
        SELECT test_issue_id AS Issue_Id,
               test_type     AS Test_Type,
               steps_json    AS Test_Steps
        FROM Silver.xray.tests
        WHERE test_issue_id IS NOT NULL
    """)
    df = df.join(xray_tests, on="Issue_Id", how="left")
else:
    print("No Silver.xray.tests -- Test_Type/Test_Steps left empty this run.")
    df = df.withColumn("Test_Type", F.lit("")).withColumn("Test_Steps", F.lit(""))

# Trim to the final persisted shape -- drops Rank, Depth, Project_Id,
# Parent_Issue_Key (all working columns the wheel/base query needed but the
# Gold table does not).
df = df.select(*FINAL_COLUMNS)

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Issue built successfully")
