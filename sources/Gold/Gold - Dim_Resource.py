# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Resource -- the people
# UNION of two sources, deliberately: Silver.jira.users is the account
# directory, but People Involved can name someone who never appears there
# (deactivated, or a customer-scoped account). Taking users alone silently
# drops those rows from Fact_Resource_Allocation.
#
# This is a THIN dimension on purpose. The client's question is "who leads
# this and who is involved", not "show me a user list" -- so the analytical
# weight sits in Fact_Resource_Allocation and Dim_Role, not here.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_resource",
    table_type="dim",
    key_column="Resource_Key",
    columns={
        "Resource_Account_Id": {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Resource_Name":       {"type": "string", "default": "Unassigned"},
        "Email":               {"type": "string", "default": ""},
        "Account_Type":        {"type": "string", "default": "Unknown"},
        "Is_Active":           {"type": "boolean", "default": True},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT u.accountId AS Resource_Account_Id, u.displayName AS Resource_Name,
           u.emailAddress AS Email, u.accountType AS Account_Type, u.active AS Is_Active
    FROM Silver.jira.users u
    WHERE u.accountId IS NOT NULL

    UNION

    SELECT DISTINCT p.person_account_id, p.person_name, p.person_email, 'atlassian', p.person_active
    FROM Silver.jira.issue_people_involved p
    WHERE p.person_account_id IS NOT NULL
      AND p.person_account_id NOT IN (SELECT accountId FROM Silver.jira.users WHERE accountId IS NOT NULL)
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Resource built successfully")
