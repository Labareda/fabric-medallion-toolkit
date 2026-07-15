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
# THE work item dimension. One row per Jira issue, at every tier -- Programme,
# Release, Epic, Task, Sub-task are all just issues with different types and
# different places in the parent chain. Modelling them as separate tables would
# break the first time the client adds a tier in Jira.
#
# ---------------------------------------------------------------------------
# TWO SETS OF HIERARCHY COLUMNS. They look similar and they are NOT
# interchangeable. This is the single most important thing to understand about
# this table.
#
# 1. Level_1 .. Level_7  -- DENSE, for the xViz Gantt ONLY.
#    Placed by DEPTH IN THE PARENT CHAIN. An issue two levels down fills
#    Level_1, Level_2, Level_3 contiguously; only TRAILING levels are null.
#    xViz nests these left to right, and it stops nesting when it hits a null.
#
#    This replaces an earlier design that placed each issue at its TYPE's tier
#    (a Task always at the Task tier, like Jira's own timeline). That produced
#    a RAGGED hierarchy -- a Task parented straight to a Release left Level_3
#    and Level_4 blank IN THE MIDDLE of the chain -- and xViz cannot render
#    that. It drew a phantom empty row for every interior gap, and switching on
#    its "Hide Blanks" setting emptied the visual completely, because that
#    setting DELETES ANY ROW CONTAINING A BLANK rather than skipping the gap.
#    Dense placement has no interior gaps, so the phantom rows disappear.
#    Leave "Hide Blanks" OFF -- trailing nulls are fine and expected.
#
# 2. Programme / Release / Initiative / Workstream / Epic -- TYPED, for slicers.
#    Placed by the issue TYPE's own hierarchy level, so "Programme" always
#    means a programme regardless of chain depth. These are ragged (an issue
#    with no programme ancestor has a null Programme) and that is CORRECT --
#    a slicer with blanks is normal; a tree with blanks is not.
#
# Use Level_N for the visual. Use the named columns for filtering and grouping.
# ---------------------------------------------------------------------------
#
# NO STATUS/PRIORITY/ISSUETYPE HERE -- NEITHER AS TEXT NOR AS KEYS.
# An earlier pass put Status_Key/Priority_Key/IssueType_Key on THIS table,
# resolving them against Dim_Status/Dim_Priority/Dim_IssueType. That's wrong
# in a star schema: foreign keys into dimension tables belong on FACT
# tables, not on other dimensions -- a dimension holding keys into other
# dimensions is snowflaking, which the brief explicitly asks to minimise.
# Status/Priority/IssueType keys live on Fact_Issue instead (see that
# notebook) -- Dim_Issue carries none of these attributes, in any form.
# A report gets Status/Priority/Type by relating Dim_Status/Dim_Priority/
# Dim_IssueType -> Fact_Issue -> Dim_Issue, exactly as a star schema expects.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue",
    table_type="dim",
    key_column="Issue_Key",
    columns={
        "Issue_Id":         {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Code":       {"type": "string", "default": "Unknown"},
        "Summary":          {"type": "string", "default": "No summary"},
        "Display_Label":    {"type": "string", "default": "Unknown"},

        "Is_Milestone":     {"type": "boolean", "default": False},

        # Structure
        "Parent_Issue_Id":  {"type": "string"},
        "Parent_Issue_Key": {"type": "string"},
        "Depth":            {"type": "int", "default": 1},
        "Is_Leaf":          {"type": "boolean", "default": True},
        "Has_Children":     {"type": "boolean", "default": False},
        "Sort_Path":        {"type": "string", "default": ""},
        "Rank":             {"type": "string", "default": ""},

        # Typed ancestors -- slicers
        "Programme":        {"type": "string"},
        "Release":          {"type": "string"},
        "Initiative":       {"type": "string"},
        "Workstream":       {"type": "string"},
        "Epic":             {"type": "string"},

        # Dense levels -- xViz Gantt
        "Level_1": {"type": "string"},
        "Level_2": {"type": "string"},
        "Level_3": {"type": "string"},
        "Level_4": {"type": "string"},
        "Level_5": {"type": "string"},
        "Level_6": {"type": "string"},
        "Level_7": {"type": "string"},

        # Gantt display / dependency affordances
        "Resource_Names":         {"type": "string", "default": ""},
        "Lead_Name":              {"type": "string", "default": "Unassigned"},
        "Resource_Count":         {"type": "int", "default": 0},
        "Predecessor_Issue_Code": {"type": "string", "default": ""},
        "Connector_Type":         {"type": "string", "default": "FS"},

        "Project_Id":       {"type": "string", "default": "Unknown"},
    },
)

# MARKDOWN ********************

# ## Build the base from Silver

# CELL ********************
# involved_people and predecessors are each collapsed to ONE row per issue
# before joining, so the joins below stay 1:1 with the issue row. Exploding
# them inline would multiply issue rows and break the merge key's uniqueness.
#
# Predecessors: only "Blocks" links are real SCHEDULING dependencies. Relates
# to / Duplicates / Clones say nothing about order. Only the INWARD side is
# read -- a row on issue X with inward_issue_key = Y means "Y blocks X", so Y
# is X's predecessor. The mirror row on Y says the same thing backwards and
# would double every arrow.
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
        i.fields_summary AS Summary,
        CONCAT(i.key, ': ', COALESCE(i.fields_summary, 'No summary')) AS Display_Label,
        LOWER(it.name) = 'milestone' AS Is_Milestone,
        i.fields_rank AS Rank,
        i.fields_parent_id AS Parent_Issue_Id,
        i.fields_project_id AS Project_Id,
        it.hierarchylevel AS Hierarchy_Level,
        c.parent_id IS NOT NULL AS Has_Children,
        c.parent_id IS NULL AS Is_Leaf,
        i.fields_assignee_displayName AS Lead_Name,
        ARRAY_JOIN(
            ARRAY_DISTINCT(ARRAY_COMPACT(CONCAT(
                ARRAY(i.fields_assignee_displayName),
                COALESCE(p.involved_arr, ARRAY(CAST(NULL AS STRING)))
            ))), ', ') AS Resource_Names,
        SIZE(ARRAY_DISTINCT(ARRAY_COMPACT(CONCAT(
                ARRAY(i.fields_assignee_displayName),
                COALESCE(p.involved_arr, ARRAY(CAST(NULL AS STRING)))
        )))) AS Resource_Count,
        COALESCE(pre.Predecessor_Issue_Code, '') AS Predecessor_Issue_Code,
        'FS' AS Connector_Type
    FROM Silver.jira.issues i
    LEFT JOIN Silver.jira.issuetypes it ON i.fields_issuetype_id = it.id
    LEFT JOIN involved_people p         ON i.id = p.issue_id
    LEFT JOIN predecessors pre          ON i.id = pre.issue_id
    LEFT JOIN children c                ON i.id = c.parent_id
""")

# MARKDOWN ********************

# ## Sanity check: one row per issue, before anything downstream assumes it

# CELL ********************
# build_typed_hierarchy_levels, build_hierarchy_levels, and build_sort_path
# below all assume EXACTLY ONE ROW PER Issue_Id. If Silver.jira.issues ever
# has more than one row for the same issue id (duplicate ingestion, a bad
# Bronze-to-Silver merge key, etc.), none of the CTEs above can cause it --
# involved_people/predecessors are GROUP BY'd and children is DISTINCT, so
# none of them can multiply rows on their own. A duplicate here means it
# came in from Silver.jira.issues itself.
#
# Left uncaught, that duplicate doesn't fail cleanly -- it fans out
# MULTIPLICATIVELY through three different self-join/recursive-walk
# functions in a row, which looks like a notebook that never finishes (or
# finishes after 30+ minutes) rather than a clear error. Checking here,
# on the small base df, is nearly free and fails immediately with the
# actual offending issue(s) instead.
duplicate_issues = (
    df.groupBy("Issue_Id", "Issue_Code").count().filter("count > 1")
)
duplicate_count = duplicate_issues.limit(1).count()
if duplicate_count > 0:
    examples = [r["Issue_Code"] for r in duplicate_issues.select("Issue_Code").limit(10).collect()]
    raise ValueError(
        f"Dim_Issue: Silver.jira.issues has more than one row for at least one issue id "
        f"(examples: {examples}). Fix the duplicate at the Silver layer before re-running -- "
        f"do not dedupe here, since silently picking one row would hide whichever Bronze/Silver "
        f"bug produced the duplicate in the first place."
    )

# MARKDOWN ********************

# ## Parent surrogate key

# CELL ********************
# Root issues keep a NULL parent key. Hashing a null would give every root the
# same meaningless GUID, so only real parents are hashed. The hash is a pure
# function of its input, so hashing Parent_Issue_Id here yields the IDENTICAL
# key that hashing Issue_Id yields for that same issue -- no lookup needed.
df = fmt.add_guid_key(df, ["Parent_Issue_Id"], "Parent_Key_Raw")
df = df.withColumn(
    "Parent_Issue_Key",
    F.when(F.col("Parent_Issue_Id").isNotNull(), F.col("Parent_Key_Raw")).otherwise(None),
).drop("Parent_Key_Raw")

# MARKDOWN ********************

# ## Typed ancestors (slicers) -- ragged is fine here

# CELL ********************
# RANK_TO_LEVEL maps this instance's issuetypes.hierarchylevel values to the
# named business tiers. Add a rank here if the client introduces a new issue
# type -- the build FAILS LOUDLY on an unmapped rank rather than silently
# dropping those issues out of the hierarchy.
#   5 Programme   3 Initiative   1 Epic/Requirement   -1 Sub-task
#   4 Release     2 Workstream   0 Task/Story/Bug/Milestone
RANK_TO_LEVEL = {5: 1, 4: 2, 3: 3, 2: 4, 1: 5, 0: 6, -1: 7}
TYPED_NAMES = {"Level_1": "Programme", "Level_2": "Release", "Level_3": "Initiative",
               "Level_4": "Workstream", "Level_5": "Epic"}

typed = fmt.build_typed_hierarchy_levels(
    df, id_column="Issue_Id", parent_id_column="Parent_Issue_Id",
    code_column="Summary", type_rank_column="Hierarchy_Level",
    rank_to_level=RANK_TO_LEVEL,
)
# Level_6/Level_7 from the typed walk are the issue's own Task/Sub-task name --
# already on the row as Summary, so they're dropped rather than duplicated.
typed = typed.drop("Level_6", "Level_7")
for old, new in TYPED_NAMES.items():
    typed = typed.withColumnRenamed(old, new)
df = df.join(typed, on="Issue_Id", how="left")

# MARKDOWN ********************

# ## Dense levels (xViz Gantt) -- contiguous, trailing nulls only

# CELL ********************
# DEPTH-based, deliberately. See the long note on the schema above for why
# typed placement cannot drive this visual.
dense = fmt.build_hierarchy_levels(
    df, id_column="Issue_Id", parent_id_column="Parent_Issue_Id",
    code_column="Display_Label", max_depth=7,
)
df = df.join(dense, on="Issue_Id", how="left")

# Depth = how many dense levels are actually populated. Drives Is_Leaf checks
# in DAX and lets a report author collapse the visual to N tiers.
level_cols = [F.col(f"Level_{n}").isNotNull().cast("int") for n in range(1, 8)]
df = df.withColumn("Depth", sum(level_cols[1:], level_cols[0]))

# MARKDOWN ********************

# ## Sort_Path -- the single column that orders the whole tree

# CELL ********************
# Jira's Rank orders every issue against every OTHER issue globally, so a child
# can sort nowhere near its parent. Sort_Path concatenates each ancestor's rank
# from the root down, so a parent's path is a literal PREFIX of its children's
# -- a plain ascending sort then reproduces the tree exactly. Roots are
# prefixed with Project_Code so projects group rather than interleave.
#
# Sort by Sort_Path ASC in the visual. That is the ONLY sort needed.
project_codes = spark.sql(f"SELECT Project_Id, Project_Code FROM {GOLD_SCHEMA}.dim_project")
paths = fmt.build_sort_path(
    df.join(project_codes, on="Project_Id", how="left"),
    id_column="Issue_Id", parent_id_column="Parent_Issue_Id",
    rank_column="Rank", root_prefix_column="Project_Code",
)
# build_sort_path() also returns its own "Depth" (0-indexed parent-chain
# depth, used internally while walking the tree) -- take ONLY Sort_Path from
# it here. df already has the Depth this table actually documents and
# exposes (the Level_N-populated-count computed above, used for Is_Leaf
# checks and "collapse to N tiers"). Joining paths' Depth in as well would
# put two differently-defined columns on the DataFrame under the identical
# name "Depth" -- an ambiguous reference the moment anything (including
# merge() resolving the schema below) tries to read it.
df = df.join(paths.select("Issue_Id", "Sort_Path"), on="Issue_Id", how="left").drop("Hierarchy_Level")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Dim_Issue built successfully")
