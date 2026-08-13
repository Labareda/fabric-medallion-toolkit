# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Resource_Allocation
# Grain: ONE ROW PER ISSUE PER PERSON PER ROLE.
#
# This is the ONLY path from Dim_Resource into issue data. Fact_Issue
# deliberately has no Assignee_Key, so there is one relationship, one answer,
# and no deactivated-relationship gymnastics. "Issues led by X" and "issues X
# is involved in" are the same table with a Role slicer.
#
# Allocation_Weight is what stops double counting. Lead = 1.0, Involved = 0.0.
# Involved people count towards HEADCOUNT and ENGAGEMENT but not towards
# EFFORT, so summing story points across this fact does not multiply an
# issue's work by the number of names attached to it. If the client later
# wants Involved to carry a share, change the weight -- nothing else moves.
#
# A person who is BOTH assignee and named in People Involved is a Lead only.
# Counting them twice would overstate the size of every team.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_resource_allocation",
    table_type="fact",
    key_column="Resource_Allocation_Key",
    columns={
        "Issue_Id":          {"type": "string", "merge_field": True},
        "Resource_Account_Id": {"type": "string", "merge_field": True},
        "Role_Name":         {"type": "string", "merge_field": True},
        "Issue_Code":        {"type": "string", "default": "Unknown"},
        "Allocation_Weight": {"type": "double", "default": 0.0},
        "Allocation_Count":  {"type": "int", "default": 1},
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
        "Role_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_role",
                                     "natural_key_column": "Role_Name", "key_column": "Role_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    WITH allocations AS (
        SELECT i.id AS Issue_Id, i.key AS Issue_Code,
               i.fields_assignee_accountId AS Resource_Account_Id,
               'Lead' AS Role_Name, 1.0 AS Allocation_Weight
        FROM Silver.jira.issues i
        WHERE i.fields_assignee_accountId IS NOT NULL

        UNION ALL

        SELECT p.issue_id, p.issue_key, p.person_account_id, 'Involved', 0.0
        FROM Silver.jira.issue_people_involved p
        JOIN Silver.jira.issues i2 ON i2.id = p.issue_id
        WHERE p.person_account_id IS NOT NULL
          -- a Lead is not also counted as Involved
          AND (i2.fields_assignee_accountId IS NULL
               OR i2.fields_assignee_accountId <> p.person_account_id)
    )
    SELECT a.Issue_Id, a.Issue_Code, a.Resource_Account_Id, a.Role_Name,
           a.Allocation_Weight, 1 AS Allocation_Count,
           dim_issue.Issue_Key, res.Resource_Key, role.Role_Key
    FROM allocations a
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue ON a.Issue_Id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res    ON a.Resource_Account_Id = res.Resource_Account_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_role role       ON a.Role_Name = role.Role_Name
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Resource_Allocation built successfully")
