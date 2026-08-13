# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Status_History
# Grain: ONE ROW PER STATUS TRANSITION.
#
# This notebook exists because the earlier conclusion -- that the changelog
# had no usable timestamp -- was based on history_items alone. It doesn't
# have one; Silver.jira.histories does (`created`, a real timestamp mapped in
# B2S), and the two join on history_id. That single join unlocks everything
# time-in-state:
#
#   * time in each status, per issue
#   * flow efficiency = Active time / total elapsed
#   * reopen rate (Done -> not Done)
#   * blocked days accumulated
#   * ageing WIP, cycle-time percentiles, Monte Carlo delivery forecasting
#
# Which matters more than usual here BECAUSE the client barely uses sprints.
# With no sprints there is no velocity and no burndown, so flow metrics from
# this table are the substitute -- and for a programme running to release
# dates rather than a two-week cadence they are the better measure anyway.
#
# Secs_In_From_Status is the gap back to the previous status change on the
# same issue; for the first transition it measures from issue creation.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_status_history",
    table_type="fact",
    key_column="Status_History_Key",
    columns={
        "History_Id":  {"type": "string", "merge_field": True},
        "Issue_Id":    {"type": "string", "merge_field": True},
        "Issue_Code":  {"type": "string", "default": "Unknown"},
        "From_Status_Name": {"type": "string", "default": "Unknown"},
        "To_Status_Name":   {"type": "string", "default": "Unknown"},
        "Changed_At":       {"type": "timestamp"},
        "Changed_Date":     {"type": "date"},
        "Secs_In_From_Status":  {"type": "long"},
        "Hours_In_From_Status": {"type": "double"},
        "Days_In_From_Status":  {"type": "double"},
        "Is_Reopen":       {"type": "boolean", "default": False},
        "Transition_Count":{"type": "int", "default": 1},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Author_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
        # To_Status resolved against Dim_Status BY NAME, not id -- the
        # changelog records status names, not ids. Statuses are project-scoped
        # so a name can match several rows; the DISTINCT-on-name view below
        # picks one deterministically. Good enough for flow reporting, and the
        # alternative (fanning out) would be worse.
        "To_Status_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_status",
                                     "natural_key_column": "Status_Id", "key_column": "Status_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    WITH status_by_name AS (
        SELECT Status_Name, MIN(Status_Id) AS Status_Id, MIN(Status_Key) AS Status_Key,
               MAX(Is_Done) AS Is_Done
        FROM {GOLD_SCHEMA}.dim_status
        GROUP BY Status_Name
    ),
    transitions AS (
        SELECT
            h.history_id AS History_Id,
            hi.issue_id  AS Issue_Id,
            hi.issue_key AS Issue_Code,
            h.author_account_id AS Author_Account_Id,
            hi.old_value_formatted AS From_Status_Name,
            hi.new_value_formatted AS To_Status_Name,
            h.created AS Changed_At,
            LAG(h.created) OVER (PARTITION BY hi.issue_id ORDER BY h.created) AS Prev_Changed_At,
            hi.issue_created AS Issue_Created_At
        FROM Silver.jira.history_items hi
        JOIN Silver.jira.histories h ON hi.history_id = h.history_id
        WHERE hi.field_name = 'status'
    )
    SELECT
        t.History_Id, t.Issue_Id, t.Issue_Code,
        t.From_Status_Name, t.To_Status_Name,
        t.Changed_At,
        CAST(t.Changed_At AS date) AS Changed_Date,
        BIGINT(t.Changed_At) - BIGINT(COALESCE(t.Prev_Changed_At, t.Issue_Created_At)) AS Secs_In_From_Status,
        (BIGINT(t.Changed_At) - BIGINT(COALESCE(t.Prev_Changed_At, t.Issue_Created_At))) / 3600.0 AS Hours_In_From_Status,
        (BIGINT(t.Changed_At) - BIGINT(COALESCE(t.Prev_Changed_At, t.Issue_Created_At))) / 86400.0 AS Days_In_From_Status,
        COALESCE(sf.Is_Done, false) AND NOT COALESCE(st.Is_Done, false) AS Is_Reopen,
        1 AS Transition_Count,
        dim_issue.Issue_Key,
        res.Resource_Key AS Author_Key,
        st.Status_Key AS To_Status_Key
    FROM transitions t
    LEFT JOIN status_by_name sf ON sf.Status_Name = t.From_Status_Name
    LEFT JOIN status_by_name st ON st.Status_Name = t.To_Status_Name
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue ON t.Issue_Id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res    ON t.Author_Account_Id = res.Resource_Account_Id
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Status_History built successfully")
