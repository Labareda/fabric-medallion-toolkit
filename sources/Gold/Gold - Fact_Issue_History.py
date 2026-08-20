# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Issue_History -- point-in-time state, rebuilt from the changelog
# Grain: ONE ROW PER ISSUE PER STATE PERIOD, with Valid_From / Valid_To.
#
# REPLACES BOTH Fact_Issue_Daily AND Fact_Status_History.
#
#   * vs Fact_Issue_Daily -- no scheduled job, no risk of a gap if a run
#     fails, and no "we only have history from the day we switched it on".
#     This rebuilds from scratch every run like every other Gold table.
#   * vs Fact_Status_History -- a period row carries everything a transition
#     row did (From_Status_Name, the author, Is_Reopen, the duration) PLUS
#     both endpoints, so it can answer "what was true on 30 June", which an
#     event table cannot without windowing in DAX. Strictly richer, so the
#     event table is redundant.
#
# WHAT A PERIOD IS
# A new period starts whenever a TRACKED field changes -- currently status
# and story points. Values carry forward until something changes them, so
# every row states the issue's full tracked state for that stretch of time.
# Adding another tracked field means adding it to the events CTE; nothing
# else in the table changes shape.
#
# THE SEED ROW
# The changelog records CHANGES, not the starting state. So each issue gets
# a seed period beginning at its creation, whose status is the OLD value of
# its first status change (that old value IS the state it was created in).
# An issue that never changed status keeps its current status for the whole
# period. Without the seed, every issue would be invisible for the stretch
# between creation and its first transition -- which on a programme with a
# lot of untouched backlog is most of the data.
#
# WHAT THE CHANGELOG WON'T GIVE YOU
# Only fields Jira logs have history. Risk scores are custom fields and are
# very unlikely to be logged, so risk burndown over time is NOT available
# from this table -- Fact_Issue holds the current score and the previous one
# (Risk_Previous_Score), and that is the honest limit. Say so before
# dashboard 5's "risk burndown" is promised.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_issue_history",
    table_type="fact",
    key_column="Issue_History_Key",
    columns={
        "Issue_Id":   {"type": "string", "merge_field": True},
        "Valid_From": {"type": "timestamp", "merge_field": True},
        "Issue_Code": {"type": "string", "default": "Unknown"},
        "Valid_To":   {"type": "timestamp"},          # NULL = still current
        "Valid_From_Date": {"type": "date"},
        "Valid_To_Date":   {"type": "date"},
        "Is_Current":      {"type": "boolean", "default": False},
        # State in force during this period
        "Status_Name":      {"type": "string", "default": "Unknown"},
        "From_Status_Name": {"type": "string"},
        "Story_Points":     {"type": "double"},
        "Is_Done":          {"type": "boolean", "default": False},
        # Transition metadata -- this is what makes Fact_Status_History
        # unnecessary. Describes the change that STARTED this period.
        "Is_Reopen":        {"type": "boolean", "default": False},
        "Is_Seed_Period":   {"type": "boolean", "default": False},
        "Days_In_Period":   {"type": "double"},
        "Hours_In_Period":  {"type": "double"},
        "Period_Count":     {"type": "int", "default": 1},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        # Project_Key/Team_Key sit directly on every fact table so a project
        # or team slicer filters in one hop -- matches Fact_Issue.
        "Project_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_project",
                                     "natural_key_column": "Project_Id", "key_column": "Project_Key",
                                     "unknown_value": "Unknown"},
        },
        "Team_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_team",
                                     "natural_key_column": "Team_Name", "key_column": "Team_Key",
                                     "unknown_value": "Unknown"},
        },
        "Status_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_status",
                                     "natural_key_column": "Status_Id", "key_column": "Status_Key",
                                     "unknown_value": "Unknown"},
        },
        "Changed_By_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build the periods
# histories carries the timestamp, history_items carries the field detail;
# they join on history_id. That join is the whole basis of this table.

# CELL ********************
df = spark.sql(f"""
    WITH
    -- Statuses collapsed to one row per NAME. The changelog records status
    -- names, not ids, and Jira statuses are project-scoped so a name can map
    -- to several ids. Picking one deterministically is right here: the
    -- alternative fans every period out into duplicates.
    status_by_name AS (
        SELECT Status_Name, MIN(Status_Id) AS Status_Id, MIN(Status_Key) AS Status_Key,
               MAX(Is_Done) AS Is_Done
        FROM {GOLD_SCHEMA}.dim_status
        GROUP BY Status_Name
    ),

    status_events AS (
        SELECT hi.issue_id, hi.issue_key, h.created AS changed_at,
               hi.new_value_formatted AS status,
               hi.old_value_formatted AS from_status,
               CAST(NULL AS DOUBLE) AS story_points,
               h.author_account_id
        FROM Silver.jira.history_items hi
        JOIN Silver.jira.histories h ON hi.history_id = h.history_id
        WHERE hi.field_name = 'status'
    ),

    -- Both story point fields are in play on this instance, so match either.
    point_events AS (
        SELECT hi.issue_id, hi.issue_key, h.created AS changed_at,
               CAST(NULL AS STRING) AS status,
               CAST(NULL AS STRING) AS from_status,
               TRY_CAST(hi.new_value_formatted AS DOUBLE) AS story_points,
               CAST(NULL AS STRING) AS author_account_id
        FROM Silver.jira.history_items hi
        JOIN Silver.jira.histories h ON hi.history_id = h.history_id
        WHERE hi.field_name LIKE 'Story%oint%'
    ),

    -- The state each issue was CREATED in = the old value of its first change.
    first_status AS (
        SELECT issue_id, MIN_BY(from_status, changed_at) AS initial_status
        FROM status_events GROUP BY issue_id
    ),
    first_points AS (
        SELECT hi.issue_id,
               MIN_BY(TRY_CAST(hi.old_value_formatted AS DOUBLE), h.created) AS initial_points
        FROM Silver.jira.history_items hi
        JOIN Silver.jira.histories h ON hi.history_id = h.history_id
        WHERE hi.field_name LIKE 'Story%oint%'
        GROUP BY hi.issue_id
    ),

    seed AS (
        SELECT i.id AS issue_id, i.key AS issue_key,
               CAST(i.fields_created AS TIMESTAMP) AS changed_at,
               -- never changed status -> it is still in the one it started in
               COALESCE(fs.initial_status, i.fields_status_name) AS status,
               CAST(NULL AS STRING) AS from_status,
               COALESCE(fp.initial_points,
                        i.fields_story_point_estimate, i.fields_story_points) AS story_points,
               CAST(NULL AS STRING) AS author_account_id
        FROM Silver.jira.issues i
        LEFT JOIN first_status fs ON i.id = fs.issue_id
        LEFT JOIN first_points fp ON i.id = fp.issue_id
    ),

    -- Several fields can change in one Jira action, landing at the same
    -- instant. Collapse those to a single event so they become ONE period
    -- rather than a zero-length one followed by the real one.
    combined AS (
        SELECT issue_id, issue_key, changed_at,
               MAX(status)       AS status,
               MAX(from_status)  AS from_status,
               MAX(story_points) AS story_points,
               MAX(author_account_id) AS author_account_id,
               MAX(CASE WHEN is_seed THEN 1 ELSE 0 END) = 1 AS is_seed
        FROM (
            SELECT *, TRUE  AS is_seed FROM seed
            UNION ALL SELECT *, FALSE FROM status_events
            UNION ALL SELECT *, FALSE FROM point_events
        )
        GROUP BY issue_id, issue_key, changed_at
    ),

    -- Carry every value forward until something changes it. LAST(x, true)
    -- ignores nulls, so a story-point-only change keeps the status it was in
    -- and vice versa -- which is what makes each row a full state, not a diff.
    filled AS (
        SELECT
            issue_id, issue_key, changed_at, is_seed, from_status, author_account_id,
            LAST(status, true) OVER (
                PARTITION BY issue_id ORDER BY changed_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS status,
            LAST(story_points, true) OVER (
                PARTITION BY issue_id ORDER BY changed_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS story_points,
            LEAD(changed_at) OVER (
                PARTITION BY issue_id ORDER BY changed_at) AS valid_to
        FROM combined
    )

    SELECT
        f.issue_id  AS Issue_Id,
        f.issue_key AS Issue_Code,
        f.changed_at AS Valid_From,
        f.valid_to   AS Valid_To,
        CAST(f.changed_at AS DATE) AS Valid_From_Date,
        CAST(f.valid_to  AS DATE)  AS Valid_To_Date,
        f.valid_to IS NULL         AS Is_Current,
        f.status                   AS Status_Name,
        f.from_status              AS From_Status_Name,
        f.story_points             AS Story_Points,
        COALESCE(st.Is_Done, false) AS Is_Done,
        -- came from a done status into a not-done one
        COALESCE(sf.Is_Done, false) AND NOT COALESCE(st.Is_Done, false) AS Is_Reopen,
        f.is_seed                  AS Is_Seed_Period,
        (BIGINT(COALESCE(f.valid_to, CURRENT_TIMESTAMP())) - BIGINT(f.changed_at)) / 86400.0 AS Days_In_Period,
        (BIGINT(COALESCE(f.valid_to, CURRENT_TIMESTAMP())) - BIGINT(f.changed_at)) / 3600.0  AS Hours_In_Period,
        1 AS Period_Count,
        di.Issue_Key,
        project.Project_Key,
        team.Team_Key,
        st.Status_Key,
        res.Resource_Key AS Changed_By_Key
    FROM filled f
    LEFT JOIN status_by_name st ON st.Status_Name = f.status
    LEFT JOIN status_by_name sf ON sf.Status_Name = f.from_status
    LEFT JOIN Silver.jira.issues i           ON f.issue_id = i.id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di     ON f.issue_id = di.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_project project ON i.fields_project_id = project.Project_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_team team    ON i.fields_team_name = team.Team_Name
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res ON f.author_account_id = res.Resource_Id
""")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Issue_History built successfully")
