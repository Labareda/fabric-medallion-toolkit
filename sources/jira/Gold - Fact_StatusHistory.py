# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: one row per issue per STATUS TRANSITION.
#
# Jira has no "actual start date" field. It only has a changelog. This table
# turns that changelog into a fact, and it is the ONLY place actual dates can
# come from -- Fact_Issue reads Actual_Start back out of here. That's why this
# notebook must run BEFORE Fact_Issue (declared in the orchestrator).
#
# It also earns its place on its own: time-in-status, cycle time and lead time
# all come from here, and they're the first things a client asks for once they
# can see a timeline.
#
# Days_In_Status is the gap to the NEXT transition on the same issue. The most
# recent transition has no "next", so its value is NULL, not 0 -- an issue
# sitting in "In Progress" right now has an OPEN duration, and writing 0 there
# would silently understate every in-flight item in a cycle-time average.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_status_history",
    table_type="fact",
    key_column="Status_History_Key",
    columns={
        "History_Id":     {"type": "string", "merge_field": True},
        "Issue_Code":     {"type": "string", "default": "Unknown"},
        "Changed_At":     {"type": "timestamp"},
        "Changed_Date":   {"type": "date"},
        "From_Status":    {"type": "string", "default": "None"},
        "To_Status":      {"type": "string", "default": "Unknown"},
        "Days_In_Status": {"type": "double"},
        "Is_Latest":      {"type": "boolean", "default": False},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Changed_By_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build from the changelog

# CELL ********************
# Silver.jira.history_items already holds every changelog line for every issue.
# field_name = 'status' filters it to status transitions only -- the same table
# also carries assignee changes, summary edits, etc.
#
# LEAD() over the issue's own transitions gives each row the timestamp of the
# next one, which is what turns a list of events into a list of DURATIONS.
df = spark.sql("""
    WITH status_changes AS (
        SELECT
            h.history_id AS History_Id,
            h.issue_id,
            h.issue_key AS Issue_Code,
            h.author_account_id AS Changed_By_Account_Id,
            h.created AS Changed_At,
            h.old_value_formatted AS From_Status,
            h.new_value_formatted AS To_Status
        FROM Silver.jira.history_items h
        WHERE h.field_name = 'status'
          AND h.new_value_formatted IS NOT NULL
    ),
    with_next AS (
        SELECT
            *,
            LEAD(Changed_At) OVER (PARTITION BY issue_id ORDER BY Changed_At) AS Next_Changed_At
        FROM status_changes
    )
    SELECT
        History_Id,
        issue_id,
        Issue_Code,
        Changed_By_Account_Id,
        Changed_At,
        CAST(Changed_At AS date) AS Changed_Date,
        From_Status,
        To_Status,
        CASE WHEN Next_Changed_At IS NULL THEN NULL
             ELSE (UNIX_TIMESTAMP(Next_Changed_At) - UNIX_TIMESTAMP(Changed_At)) / 86400.0
        END AS Days_In_Status,
        Next_Changed_At IS NULL AS Is_Latest
    FROM with_next
""")

# MARKDOWN ********************

# ## Resolve dimension keys

# CELL ********************
df = df.createOrReplaceTempView("status_changes_staged")
df = spark.sql(f"""
    SELECT
        s.History_Id, s.Issue_Code, s.Changed_At, s.Changed_Date,
        s.From_Status, s.To_Status, s.Days_In_Status, s.Is_Latest,
        dim_issue.Issue_Key AS Issue_Key,
        resource.Resource_Key AS Changed_By_Key
    FROM status_changes_staged s
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON s.issue_id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource
        ON s.Changed_By_Account_Id = resource.Resource_Account_Id
""")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Fact_StatusHistory built successfully")
