# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Replaces Bridge_IssuePeopleInvolved. Grain: ONE ROW PER ISSUE x PERSON,
# regardless of HOW that person is linked to the issue -- not one row per
# link type. A person who is both the lead (assignee) AND in People
# Involved (the common case -- e.g. Ana leading a task she's also working
# on) gets ONE row with both flags set, not two rows. Two rows would
# double-count that person in every headcount and every hours measure.
#
# Is_Lead / Is_Involved carry the distinction the client asked for, so
# both questions are answerable off ONE table:
#   "who leads this task"        -> Is_Lead = true
#   "who is linked to this task" -> every row
#   "show me Rupert's tasks"     -> slice Dim_Resource, any row
#
# Role_Label is the same information as one friendly slicer field, so a
# report author doesn't have to build a DAX measure just to show
# "Lead & Involved" in a legend.
#
# THIS TABLE IS THE SINGLE RESOURCE PATH IN THE SEMANTIC MODEL. Fact_
# ResourceAllocation relates to Dim_Resource THROUGH this bridge (via
# Issue_Resource_Key), not directly -- see that notebook, and the model
# wiring notes in the README. That's what keeps Power BI from seeing two
# different paths from Dim_Resource to the allocation fact and refusing
# to make the bridge bidirectional.
#
# Issue_Key (the resolved surrogate) is the merge field -- no Issue_Code
# needed here, since nobody browses a bridge directly; the readable code
# comes from relating through Dim_Issue. lookup_missing_from is still
# declared on it even though it's a merge field: it runs BEFORE merge()'s
# null-check validation, so an unmatched join falls back to Dim_Issue's
# Unknown row rather than hard-erroring on a null merge field.
#
# Resource_Key is properly resolved here -- the old
# Bridge_IssuePeopleInvolved stored only the raw account id, which meant
# no Unknown-member fallback for people who aren't in Silver.jira.users
# (deactivated accounts and app/bot users routinely aren't) and an
# inconsistent relationship shape versus every other fact in the model.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_resource",
    table_type="fact",
    key_column="Issue_Resource_Key",
    columns={
        "Issue_Key": {
            "type": "string",
            "merge_field": True,
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Resource_Account_Id": {"type": "string", "merge_field": True},
        "Resource_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
        "Is_Lead":     {"type": "boolean", "default": False},
        "Is_Involved": {"type": "boolean", "default": False},
        "Role_Label":  {"type": "string", "default": "Involved"},
    },
)

# MARKDOWN ********************

# ## Build the union of leads and involved people, one row per issue x person

# CELL ********************
# UNION ALL then GROUP BY, rather than a FULL OUTER JOIN: a person can
# arrive from either side (or both), and grouping is what collapses the
# both-sides case into the single row with two flags set.
#
# MAX(CAST(... AS INT)) = 1 rather than MAX(boolean) -- Spark will accept
# max() on a boolean, but casting to int makes the "did this person
# appear as a lead ANYWHERE in the group" intent explicit and avoids
# depending on boolean ordering semantics.
df = spark.sql(f"""
    WITH resource_union AS (
        SELECT
            i.id  AS Issue_Id,
            i.fields_assignee_accountId AS Resource_Account_Id,
            1 AS is_lead_flag,
            0 AS is_involved_flag
        FROM Silver.jira.issues i
        WHERE i.fields_assignee_accountId IS NOT NULL

        UNION ALL

        SELECT
            p.issue_id AS Issue_Id,
            p.person_account_id AS Resource_Account_Id,
            0 AS is_lead_flag,
            1 AS is_involved_flag
        FROM Silver.jira.issue_people_involved p
        WHERE p.person_account_id IS NOT NULL
    ),
    resource_roles AS (
        SELECT
            Issue_Id,
            Resource_Account_Id,
            MAX(is_lead_flag)     = 1 AS Is_Lead,
            MAX(is_involved_flag) = 1 AS Is_Involved
        FROM resource_union
        GROUP BY Issue_Id, Resource_Account_Id
    )
    SELECT
        dim_issue.Issue_Key AS Issue_Key,
        r.Resource_Account_Id,
        resource.Resource_Key AS Resource_Key,
        r.Is_Lead,
        r.Is_Involved,
        CASE
            WHEN r.Is_Lead AND r.Is_Involved THEN 'Lead & Involved'
            WHEN r.Is_Lead                   THEN 'Lead'
            ELSE                                  'Involved'
        END AS Role_Label
    FROM resource_roles r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON r.Issue_Id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource
        ON r.Resource_Account_Id = resource.Resource_Account_Id
""")

# MARKDOWN ********************

# ## Report people who aren't in the Jira user directory

# CELL ********************
# Not a failure -- deactivated users and app/bot accounts genuinely don't
# come back from /users/search, so they legitimately land on Dim_Resource's
# Unknown row via lookup_missing_from. Printed rather than silently
# swallowed, because a SPIKE here usually means the users extract is
# incomplete rather than that a few people left the company.
unresolved = df.filter("Resource_Key IS NULL").count()
if unresolved > 0:
    print(f"NOTE: {unresolved} issue-resource link(s) reference an account id that isn't in "
          f"Silver.jira.users -- these resolve to Dim_Resource's Unknown row. Deactivated or "
          f"app accounts are the usual explanation; a large number here suggests the users "
          f"extract is incomplete.")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Bridge_IssueResource built successfully")
