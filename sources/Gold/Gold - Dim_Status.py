# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Status
# Jira statuses are PROJECT-SCOPED: the same name ("To Do") exists several
# times with different ids and a scope_project_id. Every row is kept -- the
# fact joins on Status_Id, so dropping duplicates would orphan rows -- but
# Status_Name is what reports group by, so a slicer shows one "To Do".
#
# Status_Group and Flow_State are the two derived groupings the dashboards
# need. Flow_State (Queue/Active/Wait/Done) is what makes flow efficiency
# computable: Active time over total elapsed. Edit the CASE lists below as
# the client's workflow changes -- this is the one place they live.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_status",
    table_type="dim",
    key_column="Status_Key",
    columns={
        "Status_Id":       {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Status_Name":     {"type": "string", "default": "Unknown"},
        "Description":     {"type": "string", "default": "Unknown"},
        "Status_Category": {"type": "string", "default": "Unknown"},
        "Status_Group":    {"type": "string", "default": "Unknown"},
        "Flow_State":      {"type": "string", "default": "Unknown"},
        "Is_Done":         {"type": "boolean", "default": False},
        "Is_Blocked":      {"type": "boolean", "default": False},
        "Sort_Order":      {"type": "int", "default": 99},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT
        s.id                       AS Status_Id,
        s.name                     AS Status_Name,
        s.description              AS Description,
        s.statusCategory_name      AS Status_Category,
        CASE
            WHEN LOWER(s.name) LIKE '%block%' OR LOWER(s.name) LIKE '%hold%' THEN 'Blocked'
            WHEN LOWER(s.name) LIKE '%review%' OR LOWER(s.name) LIKE '%approv%' THEN 'In Review'
            WHEN LOWER(s.name) LIKE '%cancel%' OR LOWER(s.name) LIKE '%reject%' THEN 'Cancelled'
            WHEN s.statusCategory_key = 'new'  THEN 'Not Started'
            WHEN s.statusCategory_key = 'done' THEN 'Done'
            ELSE 'In Progress'
        END AS Status_Group,
        CASE
            WHEN LOWER(s.name) LIKE '%block%' OR LOWER(s.name) LIKE '%hold%'
              OR LOWER(s.name) LIKE '%wait%'  OR LOWER(s.name) LIKE '%pending%' THEN 'Wait'
            WHEN s.statusCategory_key = 'new'  THEN 'Queue'
            WHEN s.statusCategory_key = 'done' THEN 'Done'
            ELSE 'Active'
        END AS Flow_State,
        s.statusCategory_key = 'done' AS Is_Done,
        (LOWER(s.name) LIKE '%block%' OR LOWER(s.name) LIKE '%hold%') AS Is_Blocked,
        -- Fine-grained LIFECYCLE rank, so a status axis reads left-to-right in
        -- true workflow order (Draft -> Reviewed -> Approved -> In Build ->
        -- Tested -> Released -> Done) instead of alphabetically. Jira's
        -- statusCategory only has three buckets (new/indeterminate/done), which
        -- is far too coarse -- it collapsed every mid-flow status onto one
        -- value. This maps the status NAME to a stage instead. First match
        -- wins, so the WHENs are ordered most-specific/terminal first. Every
        -- row with the same Status_Name gets the same number (required for a
        -- Power BI Sort-By-Column). THIS IS THE ONE PLACE TO TUNE THE ORDER --
        -- eyeball the resulting axis and adjust the keyword->number map to the
        -- client's actual status names. Gaps of 5-10 leave room to insert.
        CASE
            WHEN LOWER(s.name) LIKE '%cancel%' OR LOWER(s.name) LIKE '%reject%'
              OR LOWER(s.name) LIKE '%won%t%'  OR LOWER(s.name) LIKE '%abandon%' THEN 95
            WHEN LOWER(s.name) LIKE '%done%'    OR LOWER(s.name) LIKE '%close%'
              OR LOWER(s.name) LIKE '%resolv%'  OR LOWER(s.name) LIKE '%complete%' THEN 90
            WHEN LOWER(s.name) LIKE '%releas%'  OR LOWER(s.name) LIKE '%deploy%'
              OR LOWER(s.name) LIKE '%live%'                                        THEN 80
            WHEN LOWER(s.name) LIKE '%test%'    OR LOWER(s.name) LIKE '%qa%'
              OR LOWER(s.name) LIKE '%uat%'                                         THEN 70
            WHEN LOWER(s.name) LIKE '%block%'   OR LOWER(s.name) LIKE '%hold%'
              OR LOWER(s.name) LIKE '%wait%'    OR LOWER(s.name) LIKE '%pending%'   THEN 55
            WHEN LOWER(s.name) LIKE '%build%'   OR LOWER(s.name) LIKE '%develop%'
              OR LOWER(s.name) LIKE '%progress%' OR LOWER(s.name) LIKE '%implement%' THEN 50
            WHEN LOWER(s.name) LIKE '%approv%'                                       THEN 40
            WHEN LOWER(s.name) LIKE '%review%'                                       THEN 35
            WHEN LOWER(s.name) LIKE '%refin%'   OR LOWER(s.name) LIKE '%ready%'      THEN 30
            WHEN LOWER(s.name) LIKE '%draft%'                                        THEN 20
            WHEN LOWER(s.name) IN ('to do','open','new') OR LOWER(s.name) LIKE 'to do%' THEN 15
            WHEN LOWER(s.name) LIKE '%backlog%'                                      THEN 10
            -- Fallbacks for names the map does not recognise: keep them roughly
            -- in the right third by Jira category rather than dumping at 99.
            WHEN s.statusCategory_key = 'new'  THEN 5
            WHEN s.statusCategory_key = 'done' THEN 92
            ELSE 60
        END AS Sort_Order
    FROM Silver.jira.statuses s
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Status built successfully")
