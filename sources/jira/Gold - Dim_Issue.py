# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Issue -- the work-item dimension
# One row per Jira issue at every tier (Programme, Release, Epic, Task,
# Sub-task are all issues, distinguished by type and place in the parent
# chain). The wheel adds all the hierarchy columns -- typed tiers for
# slicers AND the Gantt hierarchy, plus the Sort_Path that
# orders the whole tree -- from the flat base query below.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# Maps this Jira instance's issuetype hierarchy levels to the named business
# tiers. Add an entry if the client introduces a new issue type -- the build
# fails loudly on an unmapped rank rather than dropping those issues silently.
#   5 Programme  4 Release  3 Initiative  2 Workstream  1 Epic  0 Task  -1 Sub-task
# Maps this Jira instance's issuetype hierarchy levels to the named business
# tiers. All 7 levels are named so EVERY issue -- down to Sub-task -- gets its
# own row in the Gantt hierarchy, each showing "KEY: Summary". Add an entry if
# the client introduces a new issue type -- the build fails loudly on an
# unmapped rank rather than dropping those issues silently.
#   5 Programme  4 Release  3 Initiative  2 Workstream  1 Epic  0 Task  -1 Sub-task
RANK_TO_LEVEL = {5: 1, 4: 2, 3: 3, 2: 4, 1: 5, 0: 6, -1: 7}
TYPED_TIERS   = {"Level_1": "Programme", "Level_2": "Release",
                 "Level_3": "Initiative", "Level_4": "Workstream", "Level_5": "Epic",
                 "Level_6": "Task", "Level_7": "Sub_Task"}

# The single readable tier label per issue -- "Programme", "Release", etc. --
# so a report can filter "show me all Programmes" or drive a portfolio-children
# view (select a level, then Sort_Path gives everything beneath it). Keyed by
# the issue's own Hierarchy_Level (the it.hierarchylevel from Silver).
LEVEL_NAMES = {5: "Programme", 4: "Release", 3: "Initiative", 2: "Workstream",
               1: "Epic", 0: "Task", -1: "Sub-task"}

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue",
    table_type="dim",
    key_column="Issue_Key",
    write_mode="overwrite",   # rebuilt in full every run -- cheaper than MERGE
    columns={
        "Issue_Id":         {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Code":       {"type": "string", "default": "Unknown"},
        "Display_Label":    {"type": "string", "default": "Unknown"},
        "Is_Milestone":     {"type": "boolean", "default": False},
        # The one readable tier label -- filter "all Programmes", etc.
        "Hierarchy_Level_Name": {"type": "string", "default": "Unknown"},
        # Structure
        "Parent_Issue_Id":  {"type": "string"},
        "Parent_Issue_Key": {"type": "string"},
        "Is_Leaf":          {"type": "boolean", "default": True},
        "Has_Children":     {"type": "boolean", "default": False},
        "Sort_Path":        {"type": "string", "default": ""},
        "Rank":             {"type": "string", "default": ""},
        # Typed ancestors -- drive the Gantt hierarchy AND slicers. Each shows
        # "KEY: Summary". All 7 tiers so every issue down to Sub-task gets a row.
        "Programme":        {"type": "string"},
        "Release":          {"type": "string"},
        "Initiative":       {"type": "string"},
        "Workstream":       {"type": "string"},
        "Epic":             {"type": "string"},
        "Task":             {"type": "string"},
        "Sub_Task":         {"type": "string"},
        # Gantt display / dependency affordances
        "Resource_Names":         {"type": "string", "default": ""},
        "Lead_Name":              {"type": "string", "default": "Unassigned"},
        "Resource_Count":         {"type": "int", "default": 0},
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

# ## Add the hierarchy (typed tiers, Sort_Path)

# CELL ********************
df = fmt.enrich_issue_hierarchy(
    df,
    rank_to_level=RANK_TO_LEVEL,
    typed_level_names=TYPED_TIERS,
    root_prefix_lookup=spark.sql(f"SELECT Project_Id, Project_Code FROM {GOLD_SCHEMA}.dim_project"),
    root_prefix_join_column="Project_Id",
    label_column="Issue_Code",
)

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Issue built successfully")
