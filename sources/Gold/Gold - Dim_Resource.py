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
    -- Involved/assigned people who never made it into the user directory
    -- (deactivated, or a customer-scoped account). One row per account id --
    -- MAX picks a value deterministically so the merge key stays stable.
    involved_only AS (
        SELECT p.person_account_id AS Resource_Id,
               MAX(p.person_name)   AS Resource_Name,
               MAX(p.person_email)  AS Email,
               MAX(p.person_active) AS Is_Active
        FROM Silver.jira.issue_people_involved p
        LEFT ANTI JOIN directory d ON p.person_account_id = d.Resource_Id
        WHERE p.person_account_id IS NOT NULL
        GROUP BY p.person_account_id
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
        FROM involved_only
    )
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Resource built successfully")
