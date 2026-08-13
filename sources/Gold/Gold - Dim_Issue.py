# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Issue -- the work-item dimension
# One row per Jira issue at every tier (Programme, Release, Initiative,
# Workstream, Epic, Task, Sub-task are all issues, distinguished by type and
# place in the parent chain). The wheel adds the hierarchy columns from the
# flat base query below.
#
# CHANGED FROM THE PREVIOUS VERSION -- typed tier columns are back on.
# They were switched off on the grounds that Hierarchy_Level_Name plus a
# Sort_Path prefix filter covers tier reporting. That is true for DRILL-DOWN
# ("this item and everything under it") but not for GROUPING. The Workstream
# Health dashboard needs "sum story points BY workstream" across every
# descendant at once -- a prefix filter answers one workstream at a time and
# cannot produce that column. So both walks run now, exactly as the wheel's
# own docstring intends:
#
#   Level_1..7 (DENSE, by depth)      -> xViz Gantt nesting. Leave "Hide
#                                        Blanks" OFF; trailing nulls are normal.
#   Programme/Release/... (TYPED)     -> slicers, grouping, the workstream
#                                        scorecard. Ragged by design: an Epic
#                                        hanging straight off a Programme
#                                        genuinely has no Release.
#
# Never filter on the dense columns. A tier skipped in one project shifts
# everything up a slot and would silently regroup issues under the wrong
# workstream -- which looks like real data, not like an error.

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
        "Hierarchy_Level_Name": {"type": "string", "default": "Unknown"},
        # Structure
        "Parent_Issue_Id":  {"type": "string"},
        "Parent_Issue_Key": {"type": "string"},
        "Is_Leaf":          {"type": "boolean", "default": True},
        "Has_Children":     {"type": "boolean", "default": False},
        "Sort_Path":        {"type": "string", "default": ""},
        "Rank":             {"type": "string", "default": ""},
        # Dense levels -- drive the xViz Gantt Task Name. Depth-placed,
        # contiguous, each shows "KEY: Summary".
        "Level_1": {"type": "string"}, "Level_2": {"type": "string"},
        "Level_3": {"type": "string"}, "Level_4": {"type": "string"},
        "Level_5": {"type": "string"}, "Level_6": {"type": "string"},
        "Level_7": {"type": "string"},
        "Depth":   {"type": "int", "default": 1},
        # Typed tiers -- slicers and grouping. Ragged by design.
        "Programme_Label":  {"type": "string"},
        "Release_Label":    {"type": "string"},
        "Initiative_Label": {"type": "string"},
        "Workstream_Label": {"type": "string"},
        "Epic_Label":       {"type": "string"},
        # Gantt display / dependency affordances
        "Resource_Names":         {"type": "string", "default": ""},
        "Lead_Name":              {"type": "string", "default": "Unassigned"},
        "Resource_Count":         {"type": "int", "default": 0},
        "Has_No_Lead":            {"type": "boolean", "default": True},
        "Predecessor_Issue_Code": {"type": "string", "default": ""},
        "Connector_Type":         {"type": "string", "default": "FS"},
        # Project_Id kept: it's the join key for the direct Dim_Project ->
        # Dim_Issue relationship, so a project slicer filters issues directly.
        "Project_Id":       {"type": "string", "default": "Unknown"},
    },
)

# MARKDOWN ********************

# ## Build the flat base from Silver (one row per issue)

# CELL ********************
df = spark.sql("""
    WITH involved_people AS (
        SELECT issue_id, ARRAY_SORT(COLLECT_SET(person_name)) AS involved_arr
        FROM Silver.jira.issue_people_involved
        WHERE person_name IS NOT NULL
        GROUP BY issue_id
    ),
    predecessors AS (
        SELECT issue_id,
               ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(inward_issue_key)), ',') AS Predecessor_Issue_Code
        FROM Silver.jira.issue_links
        WHERE link_type IN ('Blocks') AND inward_issue_key IS NOT NULL
        GROUP BY issue_id
    ),
    children AS (
        SELECT DISTINCT fields_parent_id AS parent_id
        FROM Silver.jira.issues
        WHERE fields_parent_id IS NOT NULL
    )
    SELECT
        i.id AS Issue_Id,
        i.key AS Issue_Code,
        COALESCE(i.fields_summary, 'No summary') AS Summary,
        CONCAT(i.key, ': ', COALESCE(i.fields_summary, 'No summary')) AS Display_Label,
        LOWER(it.name) = 'milestone' AS Is_Milestone,
        CASE it.hierarchylevel
            WHEN 5 THEN 'Programme' WHEN 4 THEN 'Release' WHEN 3 THEN 'Initiative'
            WHEN 2 THEN 'Workstream' WHEN 1 THEN 'Epic' WHEN 0 THEN 'Task'
            WHEN -1 THEN 'Sub-task' ELSE 'Unknown'
        END AS Hierarchy_Level_Name,
        i.fields_rank AS Rank,
        i.fields_parent_id AS Parent_Issue_Id,
        i.fields_project_id AS Project_Id,
        it.hierarchylevel AS Hierarchy_Level,
        c.parent_id IS NOT NULL AS Has_Children,
        c.parent_id IS NULL AS Is_Leaf,
        i.fields_assignee_displayName AS Lead_Name,
        i.fields_assignee_accountId IS NULL AS Has_No_Lead,
        ARRAY_JOIN(ARRAY_DISTINCT(ARRAY_COMPACT(CONCAT(
            ARRAY(i.fields_assignee_displayName),
            COALESCE(p.involved_arr, ARRAY(CAST(NULL AS STRING)))))), ', ') AS Resource_Names,
        SIZE(ARRAY_DISTINCT(ARRAY_COMPACT(CONCAT(
            ARRAY(i.fields_assignee_displayName),
            COALESCE(p.involved_arr, ARRAY(CAST(NULL AS STRING))))))) AS Resource_Count,
        COALESCE(pre.Predecessor_Issue_Code, '') AS Predecessor_Issue_Code,
        'FS' AS Connector_Type
    FROM Silver.jira.issues i
    LEFT JOIN Silver.jira.issuetypes it ON i.fields_issuetype_id = it.id
    LEFT JOIN involved_people p         ON i.id = p.issue_id
    LEFT JOIN predecessors pre          ON i.id = pre.issue_id
    LEFT JOIN children c                ON i.id = c.parent_id
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

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Issue built successfully")
