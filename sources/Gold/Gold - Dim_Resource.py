# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Resource -- the people
# UNION of two sources, deliberately: Silver.jira.users is the account
# directory, but People Involved can name someone who never appears there
# (deactivated, or a customer-scoped account). Taking users alone silently
# drops those rows from Fact_Resource_Allocation.
#
# RESTRICTED to people who actually appear on an issue (assignee or People
# Involved). The full Jira user directory includes accounts that never touch
# this programme's work (other staff, service accounts) -- those don't belong
# in a resourcing report and would just be noise in a Resource_Name slicer.
#
# This is a THIN dimension on purpose. The client's question is "who leads
# this and who is involved", not "show me a user list" -- so the analytical
# weight sits in Fact_Resource_Allocation and Dim_Resource_Role, not here.
#
# Daily_Capacity_Hours: hardcoded 5.0 for everyone for now -- a placeholder
# the resource report's capacity/utilisation measures can divide by. Replace
# with a real per-person value (e.g. from a config table) if the client wants
# per-person FTE/part-time capacity rather than one flat number.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_resource",
    table_type="dim",
    key_column="Resource_Key",
    columns={
        "Resource_Id":   {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Resource_Name":         {"type": "string", "default": "Unassigned"},
        "Email":                 {"type": "string", "default": ""},
        "Is_Active":             {"type": "boolean", "default": True},
        "Daily_Capacity_Hours":  {"type": "double", "default": 5.0},
    },
)

# CELL ********************
df = spark.sql("""
    WITH
    -- Everyone who is an assignee or named in People Involved, anywhere.
    involved_ids AS (
        SELECT DISTINCT person_account_id AS Resource_Id
        FROM Silver.jira.issue_people_involved
        WHERE person_account_id IS NOT NULL

        UNION

        SELECT DISTINCT fields_assignee_accountId
        FROM Silver.jira.issues
        WHERE fields_assignee_accountId IS NOT NULL
    ),
    directory AS (
        SELECT u.accountId AS Resource_Id, u.displayName AS Resource_Name,
               u.emailAddress AS Email, u.active AS Is_Active
        FROM Silver.jira.users u
        WHERE u.accountId IS NOT NULL
    ),
    -- Involved OR assigned people who never made it into the user
    -- directory (deactivated, or a customer-scoped account). BUG FIX: this
    -- used to only scan issue_people_involved -- an assignee who is
    -- deactivated/customer-scoped AND never separately listed in People
    -- Involved fell through both branches and silently vanished from
    -- Dim_Resource entirely. issues.fields_assignee_* carries the same
    -- displayName/emailAddress/active info the assignee object gives, so
    -- both sources are unioned here BEFORE the anti-join and GROUP BY --
    -- one dedup point, so a person who is both an assignee on one issue
    -- and Involved on another (and missing from the directory either way)
    -- still gets exactly one row, not two.
    missing_people_raw AS (
        SELECT person_account_id AS Resource_Id, person_name AS Resource_Name,
               person_email AS Email, person_active AS Is_Active
        FROM Silver.jira.issue_people_involved
        WHERE person_account_id IS NOT NULL

        UNION ALL

        SELECT fields_assignee_accountId, fields_assignee_displayName,
               fields_assignee_emailAddress, fields_assignee_active
        FROM Silver.jira.issues
        WHERE fields_assignee_accountId IS NOT NULL
    ),
    missing_from_directory AS (
        SELECT m.Resource_Id,
               MAX(m.Resource_Name) AS Resource_Name,
               MAX(m.Email)         AS Email,
               MAX(m.Is_Active)     AS Is_Active
        FROM missing_people_raw m
        LEFT ANTI JOIN directory d ON m.Resource_Id = d.Resource_Id
        GROUP BY m.Resource_Id
    )
    SELECT *, CAST(5.0 AS DOUBLE) AS Daily_Capacity_Hours
    FROM (
        -- Directory rows, but only for people who are actually involved or
        -- assigned -- this is what keeps unrelated Jira users out.
        SELECT d.Resource_Id, d.Resource_Name, d.Email, d.Is_Active
        FROM directory d
        INNER JOIN involved_ids i ON d.Resource_Id = i.Resource_Id

        UNION ALL

        SELECT Resource_Id, Resource_Name, Email, Is_Active
        FROM missing_from_directory
    )
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Resource built successfully")
