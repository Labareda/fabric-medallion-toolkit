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
#
# PROJECTED FROM Fact_ResourceAllocation, NOT RE-DERIVED FROM SILVER.
# The Lead/Involved union used to be computed independently here AND in
# Gold - Fact_ResourceAllocation.py -- identical logic, duplicated in two
# places, one silent-divergence risk away from the bridge and the fact
# disagreeing about who's on an issue. Fact_ResourceAllocation is already
# at this exact grain (issue x person) and already resolves both keys, so
# this notebook now just selects the bridge's columns off it. This
# notebook must therefore run AFTER Fact_ResourceAllocation -- see the
# added dependency in Orchestration - Jira.py.
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

# ## Project the bridge straight off Fact_ResourceAllocation

# CELL ********************
# Fact_ResourceAllocation already computed the Lead/Involved union at
# EXACTLY this grain (issue x person) and already resolved both surrogate
# keys with the same Unknown-member fallback. Re-deriving that union here
# from Silver a second time was the duplication risk this refactor removes
# -- a future change to the "who counts as involved" rule now only has to
# happen in one place (Gold - Fact_ResourceAllocation.py) to stay correct
# everywhere. DISTINCT guards against this notebook being re-run before
# that dependency in a misconfigured orchestration.
df = spark.sql(f"""
    SELECT DISTINCT
        Issue_Key,
        Resource_Account_Id,
        Resource_Key,
        Is_Lead,
        Is_Involved,
        CASE
            WHEN Is_Lead AND Is_Involved THEN 'Lead & Involved'
            WHEN Is_Lead                 THEN 'Lead'
            ELSE                               'Involved'
        END AS Role_Label
    FROM {GOLD_SCHEMA}.fact_resource_allocation
""")

# MARKDOWN ********************

# ## Report people who aren't in the Jira user directory

# CELL ********************
# Not a failure -- deactivated users and app/bot accounts genuinely don't
# come back from /users/search, so they legitimately land on Dim_Resource's
# Unknown row. This should never actually fire now (Fact_ResourceAllocation
# already reports the same condition when it builds), but it's kept as a
# cheap sanity check on the assumption that source table is complete.
unresolved = df.filter("Resource_Key IS NULL").count()
if unresolved > 0:
    print(f"NOTE: {unresolved} issue-resource link(s) resolve to Dim_Resource's Unknown row "
          f"-- check Gold - Fact_ResourceAllocation.py's own build log for the underlying cause.")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Bridge_IssueResource built successfully")
