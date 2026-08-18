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
        "IssueType_Name":  {"type": "string", "default": "Unknown"},
        "Description":     {"type": "string", "default": "Unknown"},
        "Hierarchy_Level": {"type": "int", "default": 0},
        # The reporting grouping that lets ONE model serve delivery, RAID,
        # requirements, governance and testing dashboards without separate
        # structures. Everything in this Jira is an issue; this says which
        # kind of report it belongs to.
        "Issue_Category":  {"type": "string", "default": "Other"},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT
        it.id             AS IssueType_Id,
        it.name           AS IssueType_Name,
        it.description    AS Description,
        it.hierarchyLevel AS Hierarchy_Level,
        CASE
            WHEN it.name IN ('Programme','Initiative','Release')            THEN 'Programme'
            WHEN it.name IN ('Bug','Defect')                                THEN 'Defect'
            WHEN it.name IN ('Requirement','User Story','Story')            THEN 'Requirement'
            WHEN it.name IN ('Risk','Issue','Assumption','Dependency')      THEN 'RAID'
            WHEN it.name IN ('Decision','Action','Key Design Decision')     THEN 'Governance'
            WHEN it.name IN ('Policy')                                      THEN 'Policy'
            WHEN it.name LIKE 'Test%'                                       THEN 'Test'
            WHEN it.name IN ('Epic','Task','Sub-task','Subtask','Milestone') THEN 'Delivery'
            ELSE 'Other'
        END AS Issue_Category
    FROM Silver.jira.issuetypes it
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_IssueType built successfully")
