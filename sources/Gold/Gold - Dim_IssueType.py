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
        # ONE canonical name per REAL issue type. Jira lets anyone create types,
        # so the same real type turns up under several spellings/casings (and
        # sometimes different descriptions). Reports and slicers should group by
        # THIS column, not the raw IssueType_Name -- every raw type ID still gets
        # its own dim row (FK integrity), but they collapse to one clean value.
        # Unmapped names fall back to a trimmed, title-cased version, which alone
        # merges pure casing/whitespace duplicates; add true synonyms to the
        # CASE in the query below.
        "Clean_Type_Name": {"type": "string", "default": "Unknown"},
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
        -- Canonical clean name. The ELSE tidies every unmapped name (trim +
        -- title-case), which already collapses "Change request" / "CHANGE
        -- REQUEST" / " Change Request " into one value. The WHENs above it merge
        -- real SYNONYMS (different words, same meaning). Match on
        -- LOWER(TRIM(...)) so casing/spacing never matters. Add a WHEN line per
        -- synonym you find in the data -- this is the one place to maintain it.
        CASE LOWER(TRIM(it.name))
            WHEN 'epic'                THEN 'Epic'
            WHEN 'task'                THEN 'Task'
            WHEN 'sub-task'            THEN 'Sub-task'
            WHEN 'subtask'             THEN 'Sub-task'
            WHEN 'milestone'           THEN 'Milestone'
            WHEN 'new feature'         THEN 'New Feature'
            WHEN 'improvement'         THEN 'Improvement'
            WHEN 'story'               THEN 'Story'
            WHEN 'user story'          THEN 'Story'
            WHEN 'requirement'         THEN 'Requirement'
            WHEN 'bug'                 THEN 'Bug'
            WHEN 'defect'              THEN 'Bug'
            WHEN 'change request'      THEN 'Change Request'
            WHEN 'change item'         THEN 'Change Item'
            WHEN 'risk'                THEN 'Risk'
            WHEN 'issue'               THEN 'Issue'
            WHEN 'assumption'          THEN 'Assumption'
            WHEN 'dependencies'        THEN 'Dependency'
            WHEN 'dependency'          THEN 'Dependency'
            WHEN 'constraints'         THEN 'Constraint'
            WHEN 'constraint'          THEN 'Constraint'
            WHEN 'decision'            THEN 'Decision'
            WHEN 'key design decision' THEN 'Key Design Decision'
            WHEN 'action'              THEN 'Action'
            WHEN 'lesson'              THEN 'Lesson'
            WHEN 'meeting'             THEN 'Meeting'
            WHEN 'programme'           THEN 'Programme'
            WHEN 'initiative'          THEN 'Initiative'
            WHEN 'release'             THEN 'Release'
            WHEN 'workstream'          THEN 'Workstream'
            WHEN 'policy initiative'   THEN 'Policy Initiative'
            WHEN 'policy'              THEN 'Policy'
            WHEN 'cost item'           THEN 'Cost Item'
            WHEN 'integration'         THEN 'Integration'
            WHEN 'test'                THEN 'Test'
            WHEN 'test case'           THEN 'Test'
            WHEN 'test set'            THEN 'Test Set'
            WHEN 'test plan'           THEN 'Test Plan'
            WHEN 'test execution'      THEN 'Test Execution'
            WHEN 'precondition'        THEN 'Precondition'
            ELSE INITCAP(TRIM(it.name))
        END AS Clean_Type_Name,
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
