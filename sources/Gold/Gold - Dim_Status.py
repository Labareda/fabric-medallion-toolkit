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
        # Per-NAME lifecycle rank from the explicit STAGES map below. Unique and
        # increasing across the whole workflow, so Status_Name sorts in true
        # order on any axis (Status_Name sortByColumn = Sort_Order).
        "Sort_Order":      {"type": "int", "default": 9999},
        # Custom lifecycle STAGE (Open / To Do / Draft / ... / Done / Cancelled)
        # -- a business-meaningful grouping, finer and better ordered than Jira's
        # 3-bucket Status_Category. Status_Stage sortByColumn = Stage_Sort.
        "Status_Stage":    {"type": "string", "default": "Unknown"},
        "Stage_Sort":      {"type": "int", "default": 999},
        # Orders the THREE Jira status categories (To Do -> In Progress -> Done)
        # for the native-category view that still uses Status_Category.
        "Category_Sort":   {"type": "int", "default": 4},
    },
)

# MARKDOWN ********************

# ## Explicit lifecycle map (THE one place to tune the order)
# Buckets are listed TOP-TO-BOTTOM in workflow order; names within a bucket in
# the order they should appear on an axis. Each name gets a UNIQUE Sort_Order
# (its position in the whole sequence) and its bucket's Status_Stage +
# Stage_Sort. To reorder: move a name between lists, or move a whole bucket, and
# rerun. Matching is case-insensitive + trimmed. Any status name NOT listed here
# falls back by Jira category AND is printed, so gaps are easy to spot and add.

# CELL ********************
from pyspark.sql import functions as F

STAGES = [
    ("Open", [
        "Open", "New",
    ]),
    ("To Do", [
        "To Do", "Triage", "Triage (GoJ)", "Triage (P247)",
        "Backlog", "Proposed", "Selected for Development",
        "Ratify Need", "Reopened", "Needs a CR", "Defect with Configuration",
    ]),
    ("Draft", [
        "Draft", "Drafting",
    ]),
    ("Scoping & ROM", [
        "Workshop Review", "Identify Datasets", "Assign Integration Pattern",
        "Write Business Reqs Spec", "Write Technical Specification",
        "Awaiting ROM", "ROM Review", "ROM complete", "ROM Complete",
        "Awaiting Refined Cost", "Refined Cost Complete", "Awaiting Tech Spec Approval",
    ]),
    ("Review & Approval", [
        "In BA Review", "Review", "In Review", "Under Review", "More info needed",
        "Awaiting Bus Reqs Product Team Approval", "Awaiting Bus reqs vendor approval",
        "ROM Approved", "Refined cost approved", "Approved",
    ]),
    ("Ready for Development", [
        "Approved for Development", "Ready for Dev Design", "In Dev Design",
        "Ready for Development",
    ]),
    ("Development", [
        "In Development", "In Progress", "Building",
        "Defect with developer", "Fix awaiting deployment", "Development done",
    ]),
    ("Testing & UAT", [
        "Ready for Test", "Ready to test", "System Testing", "In Test Review",
        "Testing Review", "REACT Re-tests", "Blocked",
        "Testing complete", "System Testing complete", "Testing Feedback Complete",
        "Ready for UAT", "UAT", "UAT Testing", "UAT Complete",
    ]),
    ("Launch / Release", [
        "Ready for Launch", "Ready / Live", "Launched",
    ]),
    ("Done", [
        "Done", "Closed", "Complete", "Resolved",
    ]),
    ("Cancelled / Dropped", [
        "CR Rejected", "Rejected", "Descoped", "Out of Scope",
        "Duplicate/Dead Record", "Retired / Replaced", "Not a KDD",
    ]),
]

# Flatten to (name_lc, Status_Stage, Stage_Sort, Name_Sort). Stage_Sort in tens;
# Name_Sort unique + increasing across the whole list (gaps of 10 to insert).
# Matching is on the LOWERCASED name, so two names differing only in case (e.g.
# "ROM Complete" vs "ROM complete") are the same key -- keep the FIRST listing
# and skip the rest, so the ref never has a duplicate key (which would fan a
# status row out into duplicates and fail the merge).
_rows, _n, _seen = [], 0, set()
for _si, (_stage, _names) in enumerate(STAGES):
    for _name in _names:
        _lc = _name.strip().lower()
        if _lc in _seen:
            continue
        _seen.add(_lc)
        _n += 10
        _rows.append((_lc, _stage, (_si + 1) * 10, _n))
stage_ref = spark.createDataFrame(_rows, ["name_lc", "Status_Stage", "Stage_Sort", "Name_Sort"])

# CELL ********************
base = spark.sql("""
    SELECT
        s.id                       AS Status_Id,
        s.name                     AS Status_Name,
        s.description              AS Description,
        s.statusCategory_name      AS Status_Category,
        s.statusCategory_key       AS _cat_key,
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
        (LOWER(s.name) LIKE '%block%' OR LOWER(s.name) LIKE '%hold%') AS Is_Blocked
    FROM Silver.jira.statuses s
""")

df = base.join(F.broadcast(stage_ref),
               F.lower(F.trim(F.col("Status_Name"))) == F.col("name_lc"), "left")

# Report names that weren't in STAGES BEFORE filling, so the lists can be completed.
_unmapped = sorted({r["Status_Name"] for r in
                    df.filter(F.col("Status_Stage").isNull()).select("Status_Name").distinct().collect()})
if _unmapped:
    print(f"UNMAPPED status names ({len(_unmapped)}) -- add them to STAGES; "
          f"fell back by Jira category for now: {_unmapped}")
else:
    print("All status names are mapped in STAGES.")

# Fallbacks for unmapped names: keep them roughly in the right third by Jira
# category and sorted AFTER the mapped ones, rather than dumping at a sentinel.
df = (df
    .withColumn("Status_Stage", F.coalesce(F.col("Status_Stage"),
        F.when(F.col("_cat_key") == "new", F.lit("To Do"))
         .when(F.col("_cat_key") == "done", F.lit("Done"))
         .otherwise(F.lit("In Progress"))))
    .withColumn("Stage_Sort", F.coalesce(F.col("Stage_Sort"),
        F.when(F.col("_cat_key") == "new", F.lit(15))
         .when(F.col("_cat_key") == "done", F.lit(105))
         .otherwise(F.lit(65))))
    .withColumn("Sort_Order", F.coalesce(F.col("Name_Sort"),
        F.when(F.col("_cat_key") == "new", F.lit(9000))
         .when(F.col("_cat_key") == "done", F.lit(9990))
         .otherwise(F.lit(9500))))
    .withColumn("Category_Sort",
        F.when(F.col("_cat_key") == "new", 1)
         .when(F.col("_cat_key") == "indeterminate", 2)
         .when(F.col("_cat_key") == "done", 3).otherwise(4))
    .drop("name_lc", "Name_Sort", "_cat_key"))

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Status built successfully")
