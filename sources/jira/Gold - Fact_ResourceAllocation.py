# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# GRAIN CHANGE (was: issue x ASSIGNEE x working day).
# Now: one row per ISSUE x RESOURCE x WORKING DAY, where "resource" means
# EVERYONE linked to the issue -- the lead (assignee) AND everyone in
# People Involved -- exactly the population Bridge_IssueResource carries.
#
# The old version spread hours across the single assignee only. That meant
# an issue with three people involved gave 100% of its hours to whichever
# one was the assignee, and an issue with People Involved but NO assignee
# produced no allocation rows at all (the WHERE clause dropped it). The
# client's definition of "resource used per task" is People Involved, so
# the old table could not answer the question it was built for.
#
# Resource_Account_Id is now PART OF THE MERGE KEY, and that is not
# optional: merge() hard-errors on duplicate merge-field combinations, so
# without it this notebook would FAIL outright the moment any issue had a
# second person on it -- not produce wrong numbers, but crash.
#
# --- TWO HOURS MEASURES, deliberately ---
# The split-vs-replicate question doesn't have one right answer, so both
# land and the report author picks per visual, with no rebuild needed if
# the client changes their mind:
#
#   Allocated_Hours = estimate / working_days / resource_count
#       Splits the estimate across the people on the task. SUM() over any
#       set of issues still reconciles to the total original estimate --
#       use this for "what is this project's planned effort" and for a
#       resource-histogram that isn't inflated.
#
#   Task_Hours      = estimate / working_days
#       The task's FULL daily load, repeated for each person. SUM() across
#       people over-counts by design -- use it for "is Rupert's week full",
#       where what matters is that the task occupies his day, not what
#       fraction of it is nominally his.
#
# Resource_Count is carried so either measure can be derived from the
# other in DAX without re-reading Silver.
#
# --- RELATIONSHIP SHAPE ---
# Issue_Resource_Key relates this fact to BRIDGE_ISSUE_RESOURCE, NOT to
# Dim_Resource directly. That is what makes the model unambiguous: the
# bridge needs to filter Dim_Issue bidirectionally (so a Dim_Resource
# slicer reaches the Gantt), and if Dim_Resource ALSO joined straight to
# this fact, Power BI would see two paths from Dim_Resource to it and
# refuse the bidirectional relationship. One path, through the bridge.
# Resource_Key is still carried as an attribute for convenience, but it
# should NOT be given an active relationship to Dim_Resource.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_resource_allocation",
    table_type="fact",
    key_column="Allocation_Key",
    columns={
        "Issue_Code":          {"type": "string", "merge_field": True},
        "Resource_Account_Id": {"type": "string", "merge_field": True},
        "Allocation_Date":     {"type": "date",   "merge_field": True},
        "Allocated_Hours":     {"type": "double",  "default": 0.0},
        "Task_Hours":          {"type": "double",  "default": 0.0},
        "Resource_Count":      {"type": "int",     "default": 1},
        "Is_Lead":             {"type": "boolean", "default": False},
        "Is_Involved":         {"type": "boolean", "default": False},
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
        # FK to Bridge_IssueResource -- the model's single resource path.
        "Issue_Resource_Key":  {"type": "string"},
    },
)

# MARKDOWN ********************

# ## Report source data problems before building

# CELL ********************
# Issues where start_date is AFTER due_date are excluded entirely (see the
# WHERE in issue_range below) -- a genuine source data quality problem this
# notebook can't meaningfully resolve, since there's no valid ascending
# range to spread hours across. Reported so it's visible, not silently
# dropped.
bad_range_count = spark.sql("""
    SELECT COUNT(*) AS cnt FROM Silver.jira.issues
    WHERE fields_start_date IS NOT NULL AND fields_duedate IS NOT NULL
      AND CAST(fields_start_date AS date) > CAST(fields_duedate AS date)
""").collect()[0]["cnt"]
if bad_range_count > 0:
    print(f"WARNING: {bad_range_count} issue(s) have a start_date AFTER due_date -- excluded from "
          f"resource allocation entirely (see Silver.jira.issues to identify and fix these)")

# An issue with dates and an estimate but NOBODY linked to it produces no
# allocation rows -- correct (there's no resource to allocate to), but
# worth surfacing, since it's usually a Jira hygiene problem rather than a
# deliberate state.
unresourced_count = spark.sql("""
    SELECT COUNT(*) AS cnt
    FROM Silver.jira.issues i
    LEFT JOIN (SELECT DISTINCT issue_id FROM Silver.jira.issue_people_involved
               WHERE person_account_id IS NOT NULL) p
        ON i.id = p.issue_id
    WHERE i.fields_start_date IS NOT NULL
      AND i.fields_duedate IS NOT NULL
      AND i.fields_timeoriginalestimate IS NOT NULL
      AND i.fields_assignee_accountId IS NULL
      AND p.issue_id IS NULL
""").collect()[0]["cnt"]
if unresourced_count > 0:
    print(f"NOTE: {unresourced_count} scheduled, estimated issue(s) have NO lead and NO people "
          f"involved -- they contribute no allocation rows. Not an error, but they'll be invisible "
          f"in any resource view.")

# MARKDOWN ********************

# ## Build the daily spread: issue x resource x working day

# CELL ********************
# WEEKENDS ARE EXCLUDED from the spread -- an assumption, not a fact from
# the data. To spread across ALL calendar days instead, remove the
# "WHERE dayofweek(...) NOT IN (1, 7)" filter in working_days_only.
# Spark's dayofweek(): 1 = Sunday, 7 = Saturday.
#
# issue_resources is the SAME lead-union-involved logic Bridge_IssueResource
# uses -- deliberately recomputed here from Silver rather than read back
# out of the bridge, so this notebook's numbers don't silently change shape
# if the bridge's own definition is ever edited. The bridge is still joined
# at the end, but only to pick up its surrogate key, not to define the
# population.
#
# Allocation_Date always comes from a real generated day inside the issue's
# own start/due range, so it's never null by construction -- no COALESCE
# needed the way Fact_Issue/Fact_Worklog need one for raw source dates.
#
# total_working_days and resource_count are both counted PER ISSUE (not per
# issue x resource): every person on the task shares the same date range,
# so a PARTITION BY that included the resource would give an identical
# answer at more cost, and would quietly hide it if that ever stopped being
# true.
spread_df = spark.sql("""
    WITH issue_range AS (
        SELECT
            id AS issue_id,
            key AS Issue_Code,
            CAST(fields_start_date AS date) AS range_start,
            CAST(fields_duedate AS date) AS range_end,
            fields_timeoriginalestimate / 3600.0 AS total_estimate_hours
        FROM Silver.jira.issues
        WHERE fields_start_date IS NOT NULL
          AND fields_duedate IS NOT NULL
          AND fields_timeoriginalestimate IS NOT NULL
          AND CAST(fields_start_date AS date) <= CAST(fields_duedate AS date)
    ),
    resource_union AS (
        SELECT i.id AS issue_id, i.fields_assignee_accountId AS Resource_Account_Id,
               1 AS is_lead_flag, 0 AS is_involved_flag
        FROM Silver.jira.issues i
        WHERE i.fields_assignee_accountId IS NOT NULL

        UNION ALL

        SELECT p.issue_id, p.person_account_id,
               0 AS is_lead_flag, 1 AS is_involved_flag
        FROM Silver.jira.issue_people_involved p
        WHERE p.person_account_id IS NOT NULL
    ),
    issue_resources AS (
        SELECT
            issue_id,
            Resource_Account_Id,
            MAX(is_lead_flag)     = 1 AS Is_Lead,
            MAX(is_involved_flag) = 1 AS Is_Involved
        FROM resource_union
        GROUP BY issue_id, Resource_Account_Id
    ),
    resource_counts AS (
        SELECT issue_id, COUNT(*) AS resource_count
        FROM issue_resources
        GROUP BY issue_id
    ),
    exploded AS (
        SELECT
            r.issue_id,
            r.Issue_Code,
            r.total_estimate_hours,
            explode(sequence(r.range_start, r.range_end, interval 1 day)) AS Allocation_Date
        FROM issue_range r
    ),
    working_days_only AS (
        SELECT * FROM exploded
        WHERE dayofweek(Allocation_Date) NOT IN (1, 7)
    ),
    with_day_count AS (
        SELECT *, COUNT(*) OVER (PARTITION BY issue_id) AS total_working_days
        FROM working_days_only
    )
    SELECT
        d.issue_id,
        d.Issue_Code,
        ir.Resource_Account_Id,
        d.Allocation_Date,
        ir.Is_Lead,
        ir.Is_Involved,
        CAST(rc.resource_count AS INT) AS Resource_Count,
        d.total_estimate_hours / d.total_working_days / rc.resource_count AS Allocated_Hours,
        d.total_estimate_hours / d.total_working_days                     AS Task_Hours
    FROM with_day_count d
    INNER JOIN issue_resources ir
        ON d.issue_id = ir.issue_id
    INNER JOIN resource_counts rc
        ON d.issue_id = rc.issue_id
""")
spread_df.createOrReplaceTempView("spread_allocation")

# MARKDOWN ********************

# ## Resolve the surrogate keys

# CELL ********************
# Issue_Resource_Key is joined out of the bridge on the bridge's own
# natural key (Issue_Key + Resource_Account_Id) rather than re-deriving the
# hash here. Re-deriving would work -- the GUID is a pure function of its
# inputs -- but it would depend on the bridge's merge-field ORDER never
# changing, which is exactly the kind of invisible coupling that breaks
# silently a year later. A join can't drift.
df = spark.sql(f"""
    SELECT
        s.Issue_Code,
        s.Resource_Account_Id,
        s.Allocation_Date,
        s.Allocated_Hours,
        s.Task_Hours,
        s.Resource_Count,
        s.Is_Lead,
        s.Is_Involved,
        dim_issue.Issue_Key AS Issue_Key,
        resource.Resource_Key AS Resource_Key,
        bridge.Issue_Resource_Key AS Issue_Resource_Key
    FROM spread_allocation s
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON s.issue_id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource
        ON s.Resource_Account_Id = resource.Resource_Account_Id
    LEFT JOIN {GOLD_SCHEMA}.bridge_issue_resource bridge
        ON dim_issue.Issue_Key = bridge.Issue_Key
       AND s.Resource_Account_Id = bridge.Resource_Account_Id
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Fact_ResourceAllocation built successfully")
