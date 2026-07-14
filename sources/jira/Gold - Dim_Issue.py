# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Same id/code/key pattern as Dim_Project: Issue_Id (Silver's numeric id)
# is the merge field, Issue_Code (Silver's "key", e.g. "DGPR-1037") is a
# plain attribute, Issue_Key is the generated surrogate -- no naming
# collision between the natural business code and the surrogate.
#
# Display_Label ("PSP-145: PSP Project Initiation") is what a report
# author actually shows to end users -- the parent-child hierarchy (via
# Parent_Issue_Key below) and Level_1-7 (for xViz) both use plain
# Issue_Code internally for matching/dependency-resolution, kept separate
# from what's displayed.
#
# Parent_Issue_Key is Parent_Issue_Id resolved to the SAME surrogate key
# type as Issue_Key itself -- specifically so Power BI's native
# parent-child hierarchy DAX pattern (PATH()/PATHITEM()) works directly
# off Issue_Key/Parent_Issue_Key, with no need to drag seven separate
# Level_N columns into a visual by hand.
#
# This does NOT use lookup_missing_from (there's nothing to join against
# yet -- this table doesn't exist until this very build completes, so it
# can't look itself up mid-build). Instead it computes the SAME
# deterministic hash add_guid_key uses for Issue_Key itself, directly on
# Parent_Issue_Id -- since the hash is a pure function of the input value
# alone, hashing "35025" via Parent_Issue_Id produces the IDENTICAL GUID
# that hashing "35025" via Issue_Id would, with no lookup needed at all.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue",
    table_type="dim",
    key_column="Issue_Key",
    columns={
        "Issue_Id":         {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Code":       {"type": "string", "default": "Unknown"},
        "Summary":          {"type": "string", "default": "No summary"},
        "Display_Label":    {"type": "string", "default": "Unknown"},
        "Rank":             {"type": "string", "default": ""},
        "Sort_Path":        {"type": "string", "default": ""},
        "Depth":            {"type": "int", "default": 0},
        "Issue_Type_Id":    {"type": "string", "default": "Unknown"},
        "Project_Id":       {"type": "string", "default": "Unknown"},
        "Parent_Issue_Id":  {"type": "string"},
        "Parent_Issue_Key": {"type": "string"},
        "Level_1": {"type": "string"},
        "Level_2": {"type": "string"},
        "Level_3": {"type": "string"},
        "Level_4": {"type": "string"},
        "Level_5": {"type": "string"},
        "Level_6": {"type": "string"},
        "Level_7": {"type": "string"},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        id AS Issue_Id,
        key AS Issue_Code,
        fields_summary AS Summary,
        CONCAT(key, ': ', COALESCE(fields_summary, 'No summary')) AS Display_Label,
        fields_rank AS Rank,
        fields_parent_id AS Parent_Issue_Id,
        fields_issuetype_id AS Issue_Type_Id,
        fields_project_id AS Project_Id
    FROM Silver.jira.issues
""")

# MARKDOWN ********************

# ## Compute Parent_Issue_Key -- same deterministic hash as Issue_Key, applied to Parent_Issue_Id

# CELL ********************
# Root issues (Parent_Issue_Id is null) must stay null here too, NOT get
# hashed into a real-but-meaningless GUID (hashing null/empty would
# otherwise give every root issue the SAME nonsense key) -- the CASE WHEN
# below keeps null as null, only hashing where a real parent exists.
from pyspark.sql import functions as F

df_with_hashed_parent = fmt.add_guid_key(df, ["Parent_Issue_Id"], "Parent_Issue_Key_Raw")
df = df_with_hashed_parent.withColumn(
    "Parent_Issue_Key",
    F.when(F.col("Parent_Issue_Id").isNotNull(), F.col("Parent_Issue_Key_Raw")).otherwise(None),
).drop("Parent_Issue_Key_Raw")

# MARKDOWN ********************

# ## Compute the flattened hierarchy levels for xViz

# CELL ********************
# Placement is by ISSUE TYPE, not by parent-chain depth -- this is what
# Jira's own timeline does, and it's the difference between a tree that
# matches the client's Jira view and one that doesn't. A Task always
# renders at the Task tier (Level_6) whether its parent is an Epic or a
# Release; under depth-based placement that same Task would land at
# whatever depth its chain happened to reach, which is why the earlier
# Gantt buried top-level items five levels deep.
#
# RANK_TO_LEVEL maps this Jira instance's Hierarchy_Level values (from
# Gold.gold.dim_issue_type) to their Level_N slots:
#   5 Programme -> Level_1      1 Epic/Requirement -> Level_5
#   4 Release   -> Level_2      0 Task/Story/Bug/Milestone/... -> Level_6
#   3 Initiative-> Level_3     -1 Sub-task -> Level_7
#   2 Workstream-> Level_4
# If a new issue type appears with a rank not listed here, the build
# FAILS LOUDLY rather than silently dropping those issues out of the
# hierarchy -- add the rank here when that happens.
#
# Issues whose type skips tiers leave those Level_N columns blank (a
# "ragged" hierarchy) -- that's correct, not a defect. Enable xViz's
# "Filter blank" setting so those gaps don't render as empty rows.
RANK_TO_LEVEL = {5: 1, 4: 2, 3: 3, 2: 4, 1: 5, 0: 6, -1: 7}

issue_types = spark.sql("""
    SELECT
        id AS Type_Id,
        hierarchylevel AS Hierarchy_Level
    FROM Silver.jira.issuetypes
""")
df_typed = df.join(
    issue_types,
    df["Issue_Type_Id"] == issue_types["Type_Id"],
    "left",
).drop("Type_Id")

levels_df = fmt.build_typed_hierarchy_levels(
    df_typed,
    id_column="Issue_Id",
    parent_id_column="Parent_Issue_Id",
    code_column="Issue_Code",
    type_rank_column="Hierarchy_Level",
    rank_to_level=RANK_TO_LEVEL,
)
df = df.join(levels_df, on="Issue_Id", how="left")

# MARKDOWN ********************

# ## Compute Sort_Path -- the single column that orders the whole tree

# CELL ********************
# Rank alone does NOT order a tree correctly: it ranks every issue against
# every OTHER issue globally, so a child can sort nowhere near its parent
# (this is why PSP-2/PSP-3/PSP-4 ended up stranded at the bottom of the
# Gantt instead of near the top). Sort_Path concatenates each ancestor's
# rank from the root down, so a parent's path is a literal prefix of its
# children's -- a plain ascending sort then reproduces the tree exactly:
# children immediately after their parent, siblings in rank order.
#
# Roots are prefixed with their Project_Code so separate projects group
# together rather than interleaving. Children inherit it via the path.
#
# Sort by Sort_Path ASC in the visual. That is the ONLY sort needed.
project_codes = spark.sql(f"SELECT Project_Id, Project_Code FROM {GOLD_SCHEMA}.dim_project")
df_with_project = df.join(project_codes, on="Project_Id", how="left")

paths_df = fmt.build_sort_path(
    df_with_project,
    id_column="Issue_Id",
    parent_id_column="Parent_Issue_Id",
    rank_column="Rank",
    root_prefix_column="Project_Code",
)
df = df.join(paths_df, on="Issue_Id", how="left")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_Issue built successfully")
