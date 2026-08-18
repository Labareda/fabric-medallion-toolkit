# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Issue -- the work-item dimension
# One row per Jira issue at every tier (Programme, Release, Initiative,
# Workstream, Epic, Task, Sub-task are all issues, distinguished by type and
# place in the parent chain). The wheel adds the hierarchy columns from the
# flat base query below.
#
# TWO HIERARCHY WALKS, BOTH KEPT --
#   Level_1..7 (DENSE, by depth)      -> xViz Gantt nesting. Leave "Hide
#                                        Blanks" OFF; trailing nulls are normal.
#   Programme/Release/... (TYPED)     -> slicers, grouping, the workstream
#                                        scorecard. Ragged by design: an Epic
#                                        hanging straight off a Programme
#                                        genuinely has no Release. NOT
#                                        redundant with Level_1..7: the dense
#                                        walk answers "this item and everything
#                                        under it" (drill-down), the typed walk
#                                        answers "sum story points BY
#                                        workstream" (grouping) -- a prefix
#                                        filter on Sort_Path can only do the
#                                        former.
#
# Never filter on the dense columns. A tier skipped in one project shifts
# everything up a slot and would silently regroup issues under the wrong
# workstream -- which looks like real data, not like an error.
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
#   Resource_Names, Lead_Name, Resource_Count
#                              -- denormalised text duplicating the properly
#                                 modelled Fact_Resource_Allocation ->
#                                 Dim_Resource / Dim_Role relationship. Slice
#                                 "who's assigned" through that fact with
#                                 Role = 'Lead' / 'Involved' instead.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# Jira issuetype hierarchy levels -> business tier:
#   5 Programme  4 Release  3 Initiative  2 Workstream  1 Epic  0 Task  -1 Sub-task
# Workstream sits at level 1 or 2 depending on the project, which is exactly
# why the TYPED walk is placed by the issue's own type rank rather than by its
# depth: the same tier lands in the same column whichever project it is in.
RANK_TO_LEVEL = {5: 1, 4: 2, 3: 3, 2: 4, 1: 5, 0: 6, -1: 7}

TYPED_LEVEL_NAMES = {
    "Level_1": "Programme_Label",
    "Level_2": "Release_Label",
    "Level_3": "Initiative_Label",
    "Level_4": "Workstream_Label",
    "Level_5": "Epic_Label",
}
# Task and Sub-task typed columns are omitted on purpose: an issue's own
# label already tells you that, so they would just repeat Display_Label.

# The final, persisted column list -- everything else the SQL/wheel produce
# along the way (Rank, Depth, Project_Id, Parent_Issue_Key, Hierarchy_Level,
# ...) is a working column, not part of the Gold table.
FINAL_COLUMNS = [
    "Issue_Id", "Issue_Code", "Summary", "Display_Label", "Is_Milestone",
    "Parent_Issue_Id",
    "Sort_Path",
    "Level_1", "Level_2", "Level_3", "Level_4", "Level_5", "Level_6", "Level_7",
    "Programme_Label", "Release_Label", "Initiative_Label", "Workstream_Label", "Epic_Label",
    "Predecessor_Issue_Code", "Connector_Type",
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
        # Kept for Fact_Issue's date-rollup join -- not a reporting column.
        "Parent_Issue_Id":  {"type": "string"},
        "Sort_Path":        {"type": "string", "default": ""},
        # Dense levels -- drive the xViz Gantt Task Name. Depth-placed,
        # contiguous, each shows "KEY: Summary".
        "Level_1": {"type": "string"}, "Level_2": {"type": "string"},
        "Level_3": {"type": "string"}, "Level_4": {"type": "string"},
        "Level_5": {"type": "string"}, "Level_6": {"type": "string"},
        "Level_7": {"type": "string"},
        # Typed tiers -- slicers and grouping. Ragged by design.
        "Programme_Label":  {"type": "string"},
        "Release_Label":    {"type": "string"},
        "Initiative_Label": {"type": "string"},
        "Workstream_Label": {"type": "string"},
        "Epic_Label":       {"type": "string"},
        # Dependency affordances for the Gantt
        "Predecessor_Issue_Code": {"type": "string", "default": ""},
        "Connector_Type":         {"type": "string", "default": "FS"},
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
    )
    SELECT
        i.id AS Issue_Id,
        i.key AS Issue_Code,
        COALESCE(i.fields_summary, 'No summary') AS Summary,
        CONCAT(i.key, ': ', COALESCE(i.fields_summary, 'No summary')) AS Display_Label,
        LOWER(it.name) = 'milestone' AS Is_Milestone,
        i.fields_rank AS Rank,
        i.fields_parent_id AS Parent_Issue_Id,
        i.fields_project_id AS Project_Id,
        it.hierarchylevel AS Hierarchy_Level,
        COALESCE(pre.Predecessor_Issue_Code, '') AS Predecessor_Issue_Code,
        'FS' AS Connector_Type
    FROM Silver.jira.issues i
    LEFT JOIN Silver.jira.issuetypes it ON i.fields_issuetype_id = it.id
    LEFT JOIN predecessors pre          ON i.id = pre.issue_id
""")

# MARKDOWN ********************

# ## Add the hierarchy (dense levels, typed tiers, Sort_Path)

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
    # Typed walk -- by the issue type's own tier, for slicers and grouping.
    # rank_to_level is mandatory whenever typed_level_names is supplied.
    rank_to_level=RANK_TO_LEVEL,
    typed_level_names=TYPED_LEVEL_NAMES,
)

# Trim to the final persisted shape -- drops Rank, Depth, Project_Id,
# Parent_Issue_Key (all working columns the wheel/base query needed but the
# Gold table does not).
df = df.select(*FINAL_COLUMNS)

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Issue built successfully")
