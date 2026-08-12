# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: ONE ROW PER ISSUE x PERSON. This IS the many-to-many between
# Dim_Issue and Dim_Resource -- a fact serving as its own bridge (in
# Kimball's terms, a "factless fact"), which is the standard alternative to
# a separate Bridge_IssueResource. The relationship IS the business event.
#
# PERSON POPULATION = PEOPLE INVOLVED + THE LEAD (assignee), unioned and
# deduped. Someone who both leads and works a task gets ONE row with
# Role='Lead & Involved', not two rows that would double them in every
# headcount.
#
# NO daily spread. An earlier iteration exploded this to issue x person x
# WORKING DAY, spreading estimates evenly across the range. That required
# inventing two rules that aren't in the data (weekends don't count; effort
# is flat) and multiplied the fact into millions of rows. Utilisation over
# time is a DAX "events in progress" measure against Dim_Date; the fact
# stays small and honest.
#
# THIS IS THE ONLY TABLE Dim_Resource TOUCHES. That's what keeps the model
# unambiguous: one path from a person to everything else, no deactivated
# relationships. Only possible because worklogs aren't extracted.
#
# The Dim_Issue relationship needs to be set to BIDIRECTIONAL in Power BI
# so a resource slicer filters the Gantt. That's the one non-obvious wiring
# decision in the whole model, and it's load-bearing.
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
        "Allocation_Count":    {"type": "int",     "default": 1},
        "Start_Date":          {"type": "date"},
        "End_Date":            {"type": "date"},
        # Estimate split across everyone on the task, so SUM() reconciles to
        # the total planned effort. Task_Hours is the same number UNSPLIT,
        # for "how loaded is this person" questions where what matters is
        # that the task occupies them, not what share is nominally theirs.
        "Allocated_Hours":     {"type": "double"},
        "Task_Hours":          {"type": "double"},
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
    },
)

# MARKDOWN ********************

# ## Build the union of leads and people involved

# CELL ********************
# MAX(CAST(flag AS INT)) = 1 rather than MAX(boolean): makes "did this
# person appear as a lead ANYWHERE in the group" explicit, and avoids
# depending on Spark's boolean ordering semantics.
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
        r.Is_Lead,
        r.Is_Involved,
        CASE WHEN r.Is_Lead AND r.Is_Involved THEN 'Lead & Involved'
             WHEN r.Is_Lead                   THEN 'Lead'
             ELSE                                  'Involved' END AS Role,
        c.Resource_Count,
        1 AS Allocation_Count,
        CAST(i.fields_start_date AS date) AS Start_Date,
        CAST(i.fields_target_end AS date) AS End_Date,
        (i.fields_timeoriginalestimate / 3600.0) / c.Resource_Count AS Allocated_Hours,
        (i.fields_timeoriginalestimate / 3600.0)                    AS Task_Hours
    FROM roles r
    INNER JOIN counts c            ON r.Issue_Id = c.Issue_Id
    INNER JOIN Silver.jira.issues i ON r.Issue_Id = i.id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON r.Issue_Id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource
        ON r.Resource_Account_Id = resource.Resource_Account_Id
""")

# MARKDOWN ********************

# ## Report people missing from the user directory

# CELL ********************
# Deactivated and app/bot accounts don't come back from /users/search, so
# they legitimately resolve to Dim_Resource's Unknown row. A SPIKE here
# means the users extract is incomplete, not that people left.
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
