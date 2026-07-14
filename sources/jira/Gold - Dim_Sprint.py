# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Sprint is a CUSTOM field in this Jira instance (customfield_10020), not the
# built-in fields.sprint -- B2S already unpacks that into Silver.jira.issue_sprints.
#
# That Silver table is at ISSUE x SPRINT grain (a sprint repeats on every issue
# that touched it), so DISTINCT collapses it to one row per sprint here. The
# issue-to-sprint links live in Bridge_IssueSprint, deliberately in their own
# notebook -- a bridge is fact-shaped and belongs in the fact phase, not bolted
# onto the bottom of a dimension build.
#
# Board_Key is resolved rather than carrying the raw Board_Id, so sprints can be
# sliced by board -- which in this Jira instance is the closest thing to a TEAM,
# and the brief asks for team filtering.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_sprint",
    table_type="dim",
    key_column="Sprint_Key",
    columns={
        "Sprint_Id":     {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Sprint_Name":   {"type": "string", "default": "Unknown"},
        "Sprint_State":  {"type": "string", "default": "Unknown"},
        "Sprint_Goal":   {"type": "string", "default": ""},
        "Start_Date":    {"type": "date"},
        "End_Date":      {"type": "date"},
        "Complete_Date": {"type": "date"},
        "Board_Id":      {"type": "string", "default": "Unknown"},
        "Board_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_board",
                                     "natural_key_column": "Board_Id", "key_column": "Board_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql(f"""
    SELECT DISTINCT
        CAST(s.sprint_id AS string)   AS Sprint_Id,
        s.sprint_name                 AS Sprint_Name,
        s.state                       AS Sprint_State,
        s.goal                        AS Sprint_Goal,
        CAST(s.start_date AS date)    AS Start_Date,
        CAST(s.end_date AS date)      AS End_Date,
        CAST(s.complete_date AS date) AS Complete_Date,
        CAST(s.board_id AS string)    AS Board_Id,
        board.Board_Key               AS Board_Key
    FROM Silver.jira.issue_sprints s
    LEFT JOIN {GOLD_SCHEMA}.dim_board board
        ON CAST(s.board_id AS string) = board.Board_Id
    WHERE s.sprint_id IS NOT NULL
""")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Dim_Sprint built successfully")
