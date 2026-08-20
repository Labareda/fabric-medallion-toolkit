# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Worklog
# Grain: ONE ROW PER WORKLOG ENTRY.
#
# CHECK COVERAGE BEFORE PROMISING ANYTHING FROM THIS. Worklogs only exist
# where teams actually log time, and on a programme this size that is usually
# some workstreams and not others. Run the coverage query in the validation
# notebook first -- a utilisation dashboard built on 20% coverage is worse
# than no dashboard, because it looks complete.
#
# Note also: this gives hours SPENT. Utilisation is spent / available, and
# Jira holds no availability at all. Fact_Capacity (external feed) is
# required before any utilisation % can be computed.
#
# RESOURCE_ROLE_KEY -- who logged the time, relative to the ISSUE they
# logged it against. Role isn't a property of the worklog row itself (Jira
# doesn't record one); it's derived the same way Fact_Resource_Allocation
# derives it: the author is 'Lead' if they're that issue's assignee,
# 'Involved' if they're on that issue's People Involved list, otherwise
# they logged time without a formal role on the issue at all (falls to
# Dim_Resource_Role's Unknown member) -- which is itself a real,
# worth-seeing number, not an error.
# A person can be Lead on one issue and Involved (or nothing) on another,
# so this has to be computed per worklog row, not looked up from a
# person-level attribute.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_worklog",
    table_type="fact",
    key_column="Worklog_Fact_Key",
    columns={
        "Worklog_Id":       {"type": "string", "merge_field": True},
        "Issue_Code":       {"type": "string", "default": "Unknown"},
        "Started_At":       {"type": "timestamp"},
        "Started_Date":     {"type": "date"},
        "Time_Spent_Hours": {"type": "double", "default": 0.0},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Author_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
        "Resource_Role_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resourcerole",
                                     "natural_key_column": "Role_Name", "key_column": "Resource_Role_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    WITH involved_pairs AS (
        SELECT DISTINCT issue_id, person_account_id
        FROM Silver.jira.issue_people_involved
        WHERE person_account_id IS NOT NULL
    )
    -- DISTINCT: Silver.jira.worklogs can carry the exact same worklog_id
    -- twice with identical data -- same landing-duplication pattern seen
    -- in issue_links and issue_people_involved elsewhere in this pipeline.
    -- Deduped here rather than left to collide against Worklog_Id's
    -- merge_field uniqueness check downstream.
    SELECT DISTINCT
        w.worklog_id AS Worklog_Id,
        w.issue_key  AS Issue_Code,
        w.started    AS Started_At,
        CAST(w.started AS date) AS Started_Date,
        w.time_spent_seconds / 3600.0 AS Time_Spent_Hours,
        dim_issue.Issue_Key,
        res.Resource_Key AS Author_Key,
        role.Resource_Role_Key
    FROM Silver.jira.worklogs w
    LEFT JOIN Silver.jira.issues i               ON w.issue_id = i.id
    LEFT JOIN involved_pairs ip                  ON w.issue_id = ip.issue_id
                                                 AND w.author_account_id = ip.person_account_id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue  ON w.issue_id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res     ON w.author_account_id = res.Resource_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resourcerole role ON role.Role_Name = COALESCE(
        CASE WHEN w.author_account_id = i.fields_assignee_accountId THEN 'Lead' END,
        CASE WHEN ip.person_account_id IS NOT NULL THEN 'Involved' END
    )
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Worklog built successfully")
