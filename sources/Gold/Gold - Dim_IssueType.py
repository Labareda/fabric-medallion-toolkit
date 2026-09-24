# Fabric notebook source

# MARKDOWN ********************

# ## Dim_IssueType
# FIXED: the previous version selected issue_type_id / issue_type_name /
# is_subtask / hierarchy_level, which are the POWER BI CONNECTOR's column
# names. This Silver comes from the Jira REST API via auto_standardize, so
# the columns are the API's own: id, name, description, subtask,
# hierarchyLevel. Gold - Dim_Issue.py was already joining on it.id / it.name /
# it.hierarchylevel, so the two notebooks disagreed and this one would fail.
#
# Hierarchy_Level is the tier driver: 5 Programme, 4 Release, 3 Initiative,
# 2 Workstream, 1 Epic, 0 Task, -1 Sub-task. Kept as int -- Dim_Issue's
# rank_to_level map compares it numerically.
#
# SIMPLIFIED -- Is_Subtask and Tier_Name removed: both are fully derivable
# from Hierarchy_Level (Is_Subtask = Hierarchy_Level = -1; Tier_Name is the
# same CASE mapping Dim_Issue already applies to build its typed columns).

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue_type",
    table_type="dim",
    key_column="IssueType_Key",
    columns={
        "IssueType_Id":    {"type": "string", "merge_field": True, "missing": "Unknown"},
        # IssueType_Name is normalised in the query below: "Subtask" and
        # "Sub-task" are the same type, so both are stored as "Sub-task". Every
        # other name is kept exactly as Jira has it -- the several rows that
        # share a name (different type IDs / descriptions) collapse to one value
        # on their own in any slicer, so no extra "clean" column is needed.
        "IssueType_Name":  {"type": "string", "default": "Unknown"},
        "Description":     {"type": "string", "default": "Unknown"},
        "Hierarchy_Level": {"type": "int", "default": 0},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT
        it.id             AS IssueType_Id,
        -- Normalise the one real inconsistency: "Subtask" and "Sub-task" are the
        -- same type, so store both as "Subtask". Every other name is kept
        -- exactly as Jira has it. (The multiple rows that share a name -- same
        -- name, different type id/description -- collapse to one value on their
        -- own in any slicer, so nothing else needs cleaning here.)
        CASE WHEN LOWER(TRIM(it.name)) IN ('subtask', 'sub-task') THEN 'Subtask' ELSE it.name END AS IssueType_Name,
        it.description    AS Description,
        it.hierarchyLevel AS Hierarchy_Level
    FROM Silver.jira.issuetypes it
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_IssueType built successfully")
