# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: ONE ROW PER ISSUE x PERSON. Not per day.
#
# An earlier version exploded this to issue x person x working day, spreading
# the estimate evenly across the range. That required inventing two rules that
# aren't in the data (weekends don't count; effort is flat across the task) and
# it multiplied a ~12,000-row table into millions. Utilisation over time does
# NOT need pre-exploded days -- it's an "events in progress" measure against
# Dim_Date, and the DAX for it is in the semantic model notes. Keep the fact
# small and honest.
#
# THE PERSON POPULATION IS PEOPLE INVOLVED + THE LEAD (assignee), unioned and
# deduped. Somebody who both leads and works a task is ONE row with Role =
# 'Lead & Involved', not two rows -- two rows would double them in every
# headcount and every utilisation measure.
#
# THIS IS THE ONLY TABLE Dim_Resource TOUCHES -- relate Dim_Resource directly
# to THIS fact in the semantic model (Resource_Key -> Resource_Key). There is
# no separate Bridge_IssueResource table: it used to exist purely as a
# read-only projection of this exact table at this exact grain, which meant
# two tables doing one job. A fact CAN serve as its own bridge in a star
# schema -- that's what this is. One path from a person to everything else,
# no deactivated relationships, no USERELATIONSHIP measures, one fewer table
# for a report author to ever need to know exists. It's only possible because
# the client doesn't use Jira worklogs -- if they ever start, Fact_Worklog
# gives Dim_Resource a second path and this simplicity is gone.
#
# Start/End are the ISSUE's planned dates, carried here so the allocation fact
# can answer "who was busy in March" without joining back to Fact_Issue.
#
# Status_Key: added so a resource/workload view can filter by task status
# DIRECTLY (project / resource / status all filter this fact without a trip
# back through Fact_Issue). Same lookup pattern Fact_Issue itself uses.
#
# Task_Hours can be blank (no Jira estimate yet) -- see the default + the
# Is_Task_Hours_Estimated flag in the build cell below.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_resource_allocation",
    table_type="fact",
    key_column="Allocation_Key",
    columns={
        "Issue_Code":          {"type": "string", "merge_field": True},
        "Resource_Account_Id": {"type": "string", "merge_field": True},
        "Role":                {"type": "string",  "default": "Involved"},
        "Is_Lead":             {"type": "boolean", "default": False},
        "Is_Involved":         {"type": "boolean", "default": False},
        "Resource_Count":      {"type": "int",     "default": 1},
        "Start_Date":          {"type": "date"},
        "End_Date":            {"type": "date"},
        # Estimate split across everyone on the task, so SUM() still reconciles
        # to the project's total planned effort. Task_Hours is the same number
        # UNSPLIT, for "how loaded is this person" questions where what matters
        # is that the task occupies them, not what share is nominally theirs.
        "Allocated_Hours":     {"type": "double"},
        "Task_Hours":          {"type": "double"},
        # TRUE when Task_Hours came from the DEFAULT below, not a real Jira
        # estimate -- lets a report caveat/exclude placeholder numbers, and
        # flip to accurate automatically once the client fills in real
        # estimates in Jira (no rebuild needed -- just stops defaulting).
        "Is_Task_Hours_Estimated": {"type": "boolean", "default": False},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Resource_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
        # So the resource view (filter by project / resource / task status)
        # can filter on status DIRECTLY, without traversing back through
        # Fact_Issue -- same lookup pattern Fact_Issue itself uses.
        "Status_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_status",
                                     "natural_key_column": "Status_Id", "key_column": "Status_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build the union of leads and people involved

# CELL ********************
# MAX(CAST(flag AS INT)) = 1 rather than MAX(boolean): makes "did this person
# appear as a lead ANYWHERE in the group" explicit, and avoids depending on
# Spark's boolean ordering semantics.
df = spark.sql(f"""
    WITH resource_union AS (
        SELECT i.id AS Issue_Id, i.fields_assignee_accountId AS Resource_Account_Id,
               1 AS lead_flag, 0 AS involved_flag
        FROM Silver.jira.issues i
        WHERE i.fields_assignee_accountId IS NOT NULL

        UNION ALL

        SELECT p.issue_id, p.person_account_id, 0, 1
        FROM Silver.jira.issue_people_involved p
        WHERE p.person_account_id IS NOT NULL
    ),
    roles AS (
        SELECT Issue_Id, Resource_Account_Id,
               MAX(lead_flag) = 1     AS Is_Lead,
               MAX(involved_flag) = 1 AS Is_Involved
        FROM resource_union
        GROUP BY Issue_Id, Resource_Account_Id
    ),
    counts AS (
        SELECT Issue_Id, CAST(COUNT(*) AS INT) AS Resource_Count
        FROM roles GROUP BY Issue_Id
    )
    SELECT
        i.key AS Issue_Code,
        r.Resource_Account_Id,
        dim_issue.Issue_Key,
        resource.Resource_Key,
        status.Status_Key,
        r.Is_Lead,
        r.Is_Involved,
        CASE WHEN r.Is_Lead AND r.Is_Involved THEN 'Lead & Involved'
             WHEN r.Is_Lead                   THEN 'Lead'
             ELSE                                  'Involved' END AS Role,
        c.Resource_Count,
        CAST(i.fields_start_date AS date) AS Start_Date,
        CAST(i.fields_duedate AS date)    AS End_Date,
        COALESCE(
            i.fields_timeoriginalestimate / 3600.0,
            CASE dim_issue.Hierarchy_Level_Name
                WHEN 'Task'     THEN 8.0
                WHEN 'Sub-task' THEN 4.0
                ELSE NULL
            END
        ) / c.Resource_Count AS Allocated_Hours,
        COALESCE(
            i.fields_timeoriginalestimate / 3600.0,
            CASE dim_issue.Hierarchy_Level_Name
                WHEN 'Task'     THEN 8.0
                WHEN 'Sub-task' THEN 4.0
                ELSE NULL
            END
        ) AS Task_Hours,
        (i.fields_timeoriginalestimate IS NULL
            AND dim_issue.Hierarchy_Level_Name IN ('Task', 'Sub-task')) AS Is_Task_Hours_Estimated
    FROM roles r
    INNER JOIN counts c            ON r.Issue_Id = c.Issue_Id
    INNER JOIN Silver.jira.issues i ON r.Issue_Id = i.id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON r.Issue_Id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource
        ON r.Resource_Account_Id = resource.Resource_Account_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_status status
        ON i.fields_status_id = status.Status_Id
    -- Task_Hours: real Jira estimate where it exists. Where blank, a
    -- PLACEHOLDER default by hierarchy tier -- purely so the client has
    -- something to look at before Jira estimates are filled in, not a real
    -- number (Task=8h, Sub-task=4h are a starting assumption for the client
    -- to confirm or correct, not derived from their data). Epic and above
    -- deliberately left NULL -- those aren't usually individually worked, so
    -- defaulting them would invent effort with no real person behind it.
    -- Is_Task_Hours_Estimated marks every defaulted row so reports can
    -- caveat/exclude them, and stops firing automatically once a real
    -- estimate is entered in Jira.
""")

# MARKDOWN ********************

# ## Report people missing from the user directory

# CELL ********************
# Deactivated and app/bot accounts don't come back from /users/search, so they
# legitimately resolve to Dim_Resource's Unknown row. A SPIKE here means the
# users extract is incomplete, not that people left.
unresolved = df.filter("Resource_Key IS NULL").count()
if unresolved > 0:
    print(f"NOTE: {unresolved} allocation(s) reference an account id not in Silver.jira.users "
          f"-- these resolve to Dim_Resource's Unknown row.")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Fact_ResourceAllocation built successfully")
