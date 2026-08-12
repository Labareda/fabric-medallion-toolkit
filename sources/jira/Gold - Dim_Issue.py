# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
from pyspark.sql import functions as F
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# THE work item dimension. One row per Jira issue, every type -- Programme,
# Epic, Task, Test, Test Set, Test Execution, all of them. Modelling them as
# separate tables would break the first time the client adds a tier.
#
# ---------------------------------------------------------------------------
# THIS DIMENSION IS DELIBERATELY WIDE. That's not sloppy modelling -- Silver
# is the normalized layer (issuetypes, statuses, priorities all exist as
# separate tables there); Gold is the reporting layer, where the client's
# actual questions -- "show me every Programme's timeline" or "filter by
# Workstream" -- benefit from a single row carrying every attribute they'd
# slice by. Anything they've asked to slice on lives here as a plain column.
#
# THREE SETS OF HIERARCHY COLUMNS. All three are different, and it's worth
# understanding why each exists rather than assuming any is redundant.
#
# 1. BUSINESS TIERS: Programme, Initiative, Workstream, Release, Sub_Epic,
#    Work_Package. Sourced DIRECTLY from custom fields on the issue itself
#    (fields_workstream, fields_release, etc.) -- NOT walked from the parent
#    chain. The client tags issues explicitly with these business dimensions
#    so a Task can belong to "Payments" workstream regardless of what its
#    Jira parent happens to be. Each becomes a slicer.
#
# 2. Level_1 .. Level_7: DENSE, for the xViz Gantt.
#    Placed by DEPTH IN THE PARENT CHAIN. An issue two levels down fills
#    Level_1, Level_2, Level_3 contiguously; only TRAILING levels are null.
#    xViz nests these left to right, stops at a null. This is what draws
#    the tree in the visual. Leave "Hide Blanks" OFF.
#
# 3. Parent_Issue_Key: THE structural key linking a child to its parent in
#    Jira. What Power BI's PATH()/PATHITEM() functions read for a native
#    parent-child hierarchy, if the report author wants that.
#
# Business tiers filter. Level_1..7 draw the visual. Parent_Issue_Key defines
# the structural relationship. None replaces the others.
# ---------------------------------------------------------------------------
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue",
    table_type="dim",
    key_column="Issue_Key",
    columns={
        "Issue_Id":         {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Code":       {"type": "string", "default": "Unknown"},
        "Summary":          {"type": "string", "default": "No summary"},
        "Description":      {"type": "string", "default": ""},
        "Acceptance_Criteria": {"type": "string", "default": ""},
        "Display_Label":    {"type": "string", "default": "Unknown"},

        # Issue type, folded onto the dim rather than kept as a separate Dim_IssueType.
        # These are pure attributes (no measures behind them), and reports slice on
        # them constantly -- one join fewer for every visual that filters by type.
        "Issue_Type_Name":  {"type": "string", "default": "Unknown"},
        "Hierarchy_Level":  {"type": "int", "default": 0},
        "Is_Milestone":     {"type": "boolean", "default": False},
        "Is_Test":          {"type": "boolean", "default": False},
        "Is_Test_Set":      {"type": "boolean", "default": False},
        "Is_Test_Execution":{"type": "boolean", "default": False},

        # Structure
        "Parent_Issue_Id":  {"type": "string"},
        "Parent_Issue_Key": {"type": "string"},
        "Depth":            {"type": "int", "default": 1},
        "Is_Leaf":          {"type": "boolean", "default": True},
        "Has_Children":     {"type": "boolean", "default": False},
        "Sort_Path":        {"type": "string", "default": ""},
        "Rank":             {"type": "string", "default": ""},

        # Business tiers -- direct from custom fields, slicers.
        "Programme":        {"type": "string"},
        "Initiative":       {"type": "string"},
        "Workstream":       {"type": "string"},
        "Release":          {"type": "string"},
        "Sub_Epic":         {"type": "string"},
        "Work_Package":     {"type": "string"},

        # Dense levels -- xViz Gantt only.
        "Level_1": {"type": "string"},
        "Level_2": {"type": "string"},
        "Level_3": {"type": "string"},
        "Level_4": {"type": "string"},
        "Level_5": {"type": "string"},
        "Level_6": {"type": "string"},
        "Level_7": {"type": "string"},

        # Denormalised affordances -- for the row label / tooltip on the Gantt.
        # Resource_Names is what xViz displays as the data label; can only read
        # a column that sits on the row being drawn, not a many-to-many.
        "Resource_Names":   {"type": "string", "default": ""},
        "Lead_Name":        {"type": "string", "default": "Unassigned"},
        "Resource_Count":   {"type": "int", "default": 0},

        "Project_Id":       {"type": "string", "default": "Unknown"},
    },
)

# MARKDOWN ********************

# ## Build from Silver

# CELL ********************
# involved_people collapses People Involved to ONE array per issue before
# joining, so the join stays 1:1 with the issue row. Exploding into the main
# SELECT would multiply issue rows and break the merge field's uniqueness.
#
# has_children_map is a small precomputed lookup so Has_Children/Is_Leaf come
# from ONE query rather than a subselect per row.
#
# Resource_Names puts the LEAD FIRST, then everyone else, deduped -- so
# "Ana leads and is also involved" reads "Ana, Rupert, Diogo", not
# "Ana, Ana, Rupert, Diogo". ARRAY_COMPACT drops the null that
# ARRAY(fields_assignee_displayName) leaves behind when unassigned.
df = spark.sql("""
    WITH involved_people AS (
        SELECT issue_id, ARRAY_SORT(COLLECT_SET(person_name)) AS involved_arr
        FROM Silver.jira.issue_people_involved
        WHERE person_name IS NOT NULL
        GROUP BY issue_id
    ),
    children AS (
        SELECT DISTINCT fields_parent_id AS parent_id
        FROM Silver.jira.issues
        WHERE fields_parent_id IS NOT NULL
    )
    SELECT
        i.id                              AS Issue_Id,
        i.key                             AS Issue_Code,
        i.fields_summary                  AS Summary,
        COALESCE(i.fields_description, '')       AS Description,
        COALESCE(i.fields_acceptance_criteria, '') AS Acceptance_Criteria,
        CONCAT(i.key, ': ', COALESCE(i.fields_summary, 'No summary')) AS Display_Label,

        i.fields_issuetype_name           AS Issue_Type_Name,
        COALESCE(CAST(i.fields_issuetype_hierarchyLevel AS INT), 0) AS Hierarchy_Level,
        LOWER(i.fields_issuetype_name) = 'milestone'    AS Is_Milestone,
        LOWER(i.fields_issuetype_name) = 'test'          AS Is_Test,
        LOWER(i.fields_issuetype_name) = 'test set'      AS Is_Test_Set,
        LOWER(i.fields_issuetype_name) = 'test execution' AS Is_Test_Execution,

        i.fields_parent_id                AS Parent_Issue_Id,
        c.parent_id IS NOT NULL           AS Has_Children,
        c.parent_id IS NULL               AS Is_Leaf,
        i.fields_rank                     AS Rank,

        -- Business tiers, straight from custom fields
        i.fields_programme                AS Programme,
        i.fields_initiative               AS Initiative,
        i.fields_workstream               AS Workstream,
        i.fields_release                  AS Release,
        i.fields_sub_epic                 AS Sub_Epic,
        i.fields_work_package             AS Work_Package,

        i.fields_assignee_displayName     AS Lead_Name,
        ARRAY_JOIN(
            ARRAY_DISTINCT(ARRAY_COMPACT(CONCAT(
                ARRAY(i.fields_assignee_displayName),
                COALESCE(p.involved_arr, ARRAY(CAST(NULL AS STRING)))
            ))), ', ') AS Resource_Names,
        SIZE(ARRAY_DISTINCT(ARRAY_COMPACT(CONCAT(
                ARRAY(i.fields_assignee_displayName),
                COALESCE(p.involved_arr, ARRAY(CAST(NULL AS STRING)))
        )))) AS Resource_Count,

        i.fields_project_id               AS Project_Id
    FROM Silver.jira.issues i
    LEFT JOIN involved_people p ON i.id = p.issue_id
    LEFT JOIN children c        ON i.id = c.parent_id
""")

# MARKDOWN ********************

# ## Parent surrogate key

# CELL ********************
# Root issues keep a NULL parent key. Hashing a null would give every root
# the same meaningless GUID, so only real parents are hashed. The hash is a
# pure function of its input, so hashing Parent_Issue_Id here yields the
# IDENTICAL key that hashing Issue_Id yields for the same issue -- no lookup
# needed.
df = fmt.add_guid_key(df, ["Parent_Issue_Id"], "Parent_Key_Raw")
df = df.withColumn(
    "Parent_Issue_Key",
    F.when(F.col("Parent_Issue_Id").isNotNull(), F.col("Parent_Key_Raw")).otherwise(None),
).drop("Parent_Key_Raw")

# MARKDOWN ********************

# ## Dense levels (xViz Gantt) -- contiguous, trailing nulls only

# CELL ********************
# DEPTH-based, deliberately. An earlier iteration in this pipeline used typed
# placement (Task always at the Task tier); it produced a RAGGED hierarchy
# with interior gaps, which xViz can't render -- its "Hide Blanks" setting
# DELETED every row containing a blank instead of skipping the gap. Depth-
# based has no interior gaps, only trailing nulls; xViz just stops nesting.
# Leave "Hide Blanks" OFF.
#
# code_column="Display_Label" so the tree reads "PSP-145: PSP Project
# Initiation" rather than a bare "PSP-145". Only used as the stored label --
# matching runs off id/parent_id, sorting off Rank -- so it can't restructure
# anything.
levels_df = fmt.build_hierarchy_levels(
    df,
    id_column="Issue_Id", parent_id_column="Parent_Issue_Id",
    code_column="Display_Label", max_depth=7,
)
df = df.join(levels_df, on="Issue_Id", how="left")

# Depth = how many dense levels are actually populated. Drives Is_Leaf checks
# in DAX and lets a report author collapse the visual to N tiers.
level_cols = [F.col(f"Level_{n}").isNotNull().cast("int") for n in range(1, 8)]
df = df.withColumn("Depth", sum(level_cols[1:], level_cols[0]))

# MARKDOWN ********************

# ## Sort_Path -- the single column that orders the whole tree

# CELL ********************
# Jira's Rank orders every issue against every OTHER issue globally, so a
# child can sort nowhere near its parent. Sort_Path concatenates each
# ancestor's rank from the root down, so a parent's path is a literal PREFIX
# of its children's -- a plain ascending sort reproduces the tree exactly.
# Roots are prefixed with Project_Code so projects group rather than
# interleave. Sort by Sort_Path ASC in the visual. That is the ONLY sort
# needed.
project_codes = spark.sql(f"SELECT Project_Id, Project_Code FROM {GOLD_SCHEMA}.dim_project")
paths = fmt.build_sort_path(
    df.join(project_codes, on="Project_Id", how="left"),
    id_column="Issue_Id", parent_id_column="Parent_Issue_Id",
    rank_column="Rank", root_prefix_column="Project_Code",
)
df = df.join(paths, on="Issue_Id", how="left")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Dim_Issue built successfully")
