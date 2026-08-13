# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Issue_Daily -- the snapshot
# Grain: ONE ROW PER OPEN ISSUE PER DAY, plus a month-end row for closed ones.
#
# RUN THIS DAILY, AND START RUNNING IT NOW. It is the only table here that
# cannot be rebuilt after the fact -- every other Gold table can be
# regenerated from Silver at any time, but a snapshot you never took is gone.
#
# It matters more than usual on this programme BECAUSE sprints are barely
# used. With sprints, Jira's own boards give burndown for free. Without them
# this table is the single source of every trend:
#   * scope burndown / burnup against a Release
#   * scope ADDED or REMOVED after a baseline (the honest version of
#     "commitment vs delivery")
#   * ageing WIP over time rather than just today
#   * risk exposure burndown
#   * any period-over-period comparison, for any measure
#
# VOLUME: restricting to open issues plus a month-end row for closed ones
# keeps this roughly an order of magnitude below issues x 365. Partition by
# Snapshot_Date.
#
# The Workstream / Release labels are copied ONTO the snapshot deliberately
# rather than reached through Dim_Issue. Going through the dimension gives
# the issue's workstream TODAY; these columns preserve where it sat on the
# day in question. If an issue moves workstream, only these tell the truth --
# and "why did last quarter's total change" is otherwise unanswerable.

# CELL ********************
import fabric_medallion_toolkit as fmt
from pyspark.sql import functions as F

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_issue_daily",
    table_type="fact",
    key_column="Issue_Daily_Key",
    columns={
        "Snapshot_Date":    {"type": "date", "merge_field": True},
        "Issue_Id":         {"type": "string", "merge_field": True},
        "Issue_Code":       {"type": "string", "default": "Unknown"},
        "Programme_Label":  {"type": "string"},
        "Release_Label":    {"type": "string"},
        "Workstream_Label": {"type": "string"},
        "Status_Name":      {"type": "string", "default": "Unknown"},
        "Status_Group":     {"type": "string", "default": "Unknown"},
        "Lead_Name":        {"type": "string", "default": "Unassigned"},
        "Story_Points":     {"type": "double"},
        "Remaining_Estimate_Hours": {"type": "double"},
        "Risk_Total_Score": {"type": "double"},
        "Is_Open":          {"type": "boolean", "default": True},
        "Is_Blocked":       {"type": "boolean", "default": False},
        "Is_Overdue":       {"type": "boolean", "default": False},
        "Issue_Count":      {"type": "int", "default": 1},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# CELL ********************
# Merge on (Snapshot_Date, Issue_Id) so a re-run on the same day corrects that
# day rather than duplicating it.
df = spark.sql(f"""
    SELECT
        CURRENT_DATE() AS Snapshot_Date,
        f.Issue_Id,
        f.Issue_Code,
        di.Programme_Label,
        di.Release_Label,
        di.Workstream_Label,
        ds.Status_Name,
        ds.Status_Group,
        di.Lead_Name,
        f.Story_Points,
        f.Remaining_Estimate_Hours,
        f.Risk_Total_Score,
        NOT f.Is_Done AS Is_Open,
        ds.Is_Blocked,
        f.Is_Overdue,
        1 AS Issue_Count,
        f.Issue_Key
    FROM {GOLD_SCHEMA}.fact_issue f
    JOIN {GOLD_SCHEMA}.dim_issue di ON f.Issue_Key = di.Issue_Key
    JOIN {GOLD_SCHEMA}.dim_status ds ON f.Status_Key = ds.Status_Key
    WHERE NOT f.Is_Done
       OR CURRENT_DATE() = LAST_DAY(CURRENT_DATE())
""")

# CELL ********************
fmt.merge(spark, df, schema)
print(f"Fact_Issue_Daily snapshot written for {spark.sql('SELECT CURRENT_DATE()').collect()[0][0]}")
