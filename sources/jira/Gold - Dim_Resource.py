# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Renamed from Dim_User -- "Resource" is the natural project-planning
# term (matches the reference Gantt tool's own "resource workload"
# language). Population is the user directory UNIONED with everyone who
# appears as an assignee or in People Involved (see the build cell), so
# nobody who works an issue is missing from resource filtering.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_resource",
    table_type="dim",
    key_column="Resource_Key",
    columns={
        "Resource_Account_Id": {"type": "string", "merge_field": True},
        "Resource_Name":        {"type": "string", "default": "Unknown"},
        "Email":                {"type": "string", "default": "Unknown"},
        "Is_Active":            {"type": "string", "default": "false"},
        "Account_Type":         {"type": "string", "default": "Unknown"},
        "Time_Zone":            {"type": "string", "default": "Unknown"},
        # HARDCODED placeholder until the client provides real capacity data
        # (see build cell). SUPPLY, not demand -- do not confuse with
        # Fact_ResourceAllocation.Allocated_Hours.
        "Daily_Capacity_Hours":  {"type": "double", "default": 5.0},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
# The user DIRECTORY (Silver.jira.users) is the primary source -- it's the
# only place with email, active flag, account type, timezone. But it is NOT
# guaranteed to contain everyone who appears on an issue: deactivated users,
# external collaborators, and people who are only ever in "People Involved"
# (never a project member) are routinely absent from /users/search.
#
# If Dim_Resource were built from the directory ALONE, every such person would
# fail to resolve in Fact_ResourceAllocation and collapse onto the single
# "Unknown" row -- so a report author filtering "issues assigned to me" would
# find their own name missing even though it shows in Dim_Issue.Resource_Names
# (which uses the display-name TEXT and doesn't depend on the directory).
#
# So the population is the UNION of:
#   1. the full user directory (rich attributes), and
#   2. every account id that appears as an assignee or in People Involved
#      (name only -- other attributes default to Unknown).
# Directory rows WIN over involvement-only rows for the same account id, so
# nobody with real attributes is downgraded. Now every person on any issue has
# a real Dim_Resource row, and filtering the Gantt by resource works for
# everyone -- lead, assignee, or involved.
df = spark.sql("""
    WITH directory AS (
        SELECT
            accountId                    AS Resource_Account_Id,
            CAST(displayName AS STRING)  AS Resource_Name,
            CAST(emailAddress AS STRING) AS Email,
            CAST(active AS STRING)       AS Is_Active,
            CAST(accountType AS STRING)  AS Account_Type,
            CAST(timeZone AS STRING)     AS Time_Zone
        FROM Silver.jira.users
        WHERE accountId IS NOT NULL
    ),
    -- Everyone who appears on an issue, from either people-involved or as an
    -- assignee, with whatever display name travels with them there.
    involved AS (
        SELECT CAST(person_account_id AS STRING) AS Resource_Account_Id,
               CAST(person_name AS STRING)        AS Resource_Name
        FROM Silver.jira.issue_people_involved
        WHERE person_account_id IS NOT NULL
        UNION
        SELECT CAST(fields_assignee_accountId AS STRING),
               CAST(fields_assignee_displayName AS STRING)
        FROM Silver.jira.issues
        WHERE fields_assignee_accountId IS NOT NULL
    ),
    -- Involvement-only people: those NOT already in the directory. One row per
    -- account id (a person can appear on many issues), name picked arbitrarily
    -- but deterministically via MAX so the merge key stays stable run to run.
    involved_only AS (
        SELECT
            i.Resource_Account_Id,
            MAX(i.Resource_Name) AS Resource_Name,
            CAST(NULL AS STRING) AS Email,
            CAST(NULL AS STRING) AS Is_Active,
            CAST(NULL AS STRING) AS Account_Type,
            CAST(NULL AS STRING) AS Time_Zone
        FROM involved i
        LEFT ANTI JOIN directory d ON i.Resource_Account_Id = d.Resource_Account_Id
        GROUP BY i.Resource_Account_Id
    )
    -- Daily_Capacity_Hours: HARDCODED placeholder (see schema note above the
    -- table -- this is genuinely made up, not derived from anything in Jira).
    -- Same 5.0 for everyone until the client provides real capacity/working-
    -- hours data. Kept as its own column, not reused from Fact_ResourceAllocation
    -- .Allocated_Hours -- that column already means something different (a
    -- task's estimate split across everyone on it, a DEMAND number). This is
    -- SUPPLY: how many hours a day this person is assumed available. Mixing
    -- the two would silently corrupt both.
    SELECT *, CAST(5.0 AS DOUBLE) AS Daily_Capacity_Hours FROM directory
    UNION ALL
    SELECT *, CAST(5.0 AS DOUBLE) AS Daily_Capacity_Hours FROM involved_only
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_Resource built successfully")
