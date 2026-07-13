# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Same id/code/key pattern as Dim_Project: Issue_Id (Silver's numeric id)
# is the merge field, Issue_Code (Silver's "key", e.g. "DGPR-1037") is a
# plain attribute, Issue_Key is the generated surrogate -- no naming
# collision between the natural business code and the surrogate.
#
# Rank lives here, not on Fact_Issue -- it's a structural/descriptive
# property of the issue (its position among siblings), not a measure,
# same category as Summary/Parent_Issue_Id. Keeping it alongside those
# also means sorting the Gantt by Rank needs no extra calculated column
# or cross-table RELATED() -- it's already in the same table as
# Issue_Code/Summary.
#
# Parent_Issue_Id deliberately has NO default -- null here is a real,
# meaningful value (this issue has no parent, i.e. it's a root-level item
# in the Gantt hierarchy), not a missing value to paper over.
#
# Level_1 through Level_7 are for xViz Gantt Chart specifically -- it
# expects a flattened, per-level hierarchy (separate columns per level)
# rather than the recursive Issue_Id/Parent_Issue_Id pattern Power BI's
# own native hierarchy visual understands directly. 7 levels matches the
# real depth you described: Programme, Release, Initiative, Workstream,
# Epic, Task, Sub-task.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue",
    table_type="dim",
    key_column="Issue_Key",
    columns={
        "Issue_Id":        {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Issue_Code":      {"type": "string", "default": "Unknown"},
        "Summary":         {"type": "string", "default": "No summary"},
        "Rank":            {"type": "string", "default": ""},
        "Parent_Issue_Id": {"type": "string"},
        "Level_1": {"type": "string"},
        "Level_2": {"type": "string"},
        "Level_3": {"type": "string"},
        "Level_4": {"type": "string"},
        "Level_5": {"type": "string"},
        "Level_6": {"type": "string"},
        "Level_7": {"type": "string"},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        id AS Issue_Id,
        key AS Issue_Code,
        fields_summary AS Summary,
        fields_rank AS Rank,
        fields_parent_id AS Parent_Issue_Id
    FROM Silver.jira.issues
""")

# MARKDOWN ********************

# ## Compute the flattened hierarchy levels for xViz

# CELL ********************
levels_df = fmt.build_hierarchy_levels(
    df, id_column="Issue_Id", parent_id_column="Parent_Issue_Id",
    code_column="Issue_Code", max_depth=7,
)
df = df.join(levels_df, on="Issue_Id", how="left")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_Issue built successfully")
