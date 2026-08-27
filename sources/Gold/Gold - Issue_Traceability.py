# Fabric notebook source

# MARKDOWN ********************
# ## Issue_Traceability -- ONE self-contained table for the client
#
# Pick any issue (issue/task/bug/test) and see everything it is linked to,
# and what THOSE are linked to, and so on down to the last link -- with each
# linked item's Summary, Link Type, Acceptance Criteria and (for tests) the
# latest Test Status.
#
# Deliberately NOT a star schema. Everything the report needs is denormalised
# onto this ONE table, so in Power BI it is: drop the table on a page, add a
# slicer on Root_Code, done -- no relationships to build.
#
# Grain: ONE ROW PER (Root, reachable issue). The reach is resolved here in
# Gold because Power BI relationships can only ever follow ONE hop live.
# Traversal is breadth-first, first visit only, so each issue appears once
# per root (no duplicate/cartesian rows) and cycles stop themselves.
#
# Reads Gold.gold.bridge_issue_link (all links, both directions, Jira + Xray)
# and Gold.gold.dim_issue, plus Silver.xray.test_runs for the latest test
# status. Fully rebuilt each run -> write_mode="overwrite".
# CELL ********************

import fabric_medallion_toolkit as fmt
from functools import reduce
from pyspark.sql import functions as F

GOLD_SCHEMA = "Gold.gold"

# Replace with the client's Jira base URL (no trailing slash).
JIRA_BASE_URL = "https://yourcompany.atlassian.net"

# How many hops deep to expand. First-visit already guarantees termination;
# this bounds depth (and is the number of dense Level_* indent columns).
MAX_DEPTH = 8

# Don't broadcast an edge list bigger than this (broadcasting a huge table
# stalls the driver); Spark shuffle-joins instead.
BROADCAST_EDGE_LIMIT = 300000


# MARKDOWN ********************
# ## Schema -- one flat, denormalised table
# CELL ********************

schema_columns = {

    # Grain.
    "Path_String": {"type": "string", "merge_field": True},

    # The issue the user picks (the top of this row's chain).
    "Root_Code": {"type": "string", "default": "Unknown"},

    # Where this linked issue sits in the chain.
    "Level":       {"type": "int", "default": 0},
    "Parent_Code": {"type": "string", "default": "Unknown"},

    # The link from the parent to this issue.
    "Link_Type_Name": {"type": "string", "default": "None"},
    "Link_Label":     {"type": "string", "default": "None"},

    # The linked issue itself, everything the client asked to see.
    "Node_Code":                {"type": "string", "default": "Unknown"},
    "Node_Issue_Type":          {"type": "string", "default": "Unknown"},
    "Node_Summary":             {"type": "string", "default": "Unknown"},
    "Node_Acceptance_Criteria": {"type": "string", "default": "None"},
    "Node_Test_Status":         {"type": "string", "default": "N/A"},
    "Node_URL":                 {"type": "string", "default": "Unknown"},

    # Friendly name for how far down the chain this issue sits (Level 1 =
    # directly linked, Level 2 = sub-linked, ...).
    "Node_Relationship":        {"type": "string", "default": "Linked"},
}

# Dense indent columns for an optional expandable matrix (trailing nulls).
for _k in range(1, MAX_DEPTH + 1):
    schema_columns[f"Level_{_k}"] = {"type": "string"}

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.issue_traceability",
    table_type="fact",
    key_column="Issue_Traceability_Key",
    columns=schema_columns,
    write_mode="overwrite",
)


# MARKDOWN ********************
# ## Edges, node attributes, latest test status
# CELL ********************

edges = spark.sql(f"""
    SELECT
        Issue_Code        AS src,
        Linked_Issue_Code AS dst,
        Link_Label,
        Link_Type_Name
    FROM {GOLD_SCHEMA}.bridge_issue_link
    WHERE Linked_Issue_Code IS NOT NULL
      AND Linked_Issue_Code <> Issue_Code
""").persist()
edge_count = edges.count()
print(f"edges: {edge_count} rows")

# All issues, with the display attributes (used as roots AND as nodes).
issues = spark.sql(f"""
    SELECT
        Issue_Code          AS code,
        Issue_Type_Name,
        Summary,
        Acceptance_Criteria
    FROM {GOLD_SCHEMA}.dim_issue
""")

# Latest test status per test, straight from the raw run status (no
# dependency on the optional Dim_Test_Status).
test_status = spark.sql(f"""
    SELECT Test_Code, Status_Name
    FROM (
        SELECT
            r.test_issue_key AS Test_Code,
            CONCAT(UPPER(SUBSTRING(r.status_name, 1, 1)), LOWER(SUBSTRING(r.status_name, 2))) AS Status_Name,
            ROW_NUMBER() OVER (
                PARTITION BY r.test_issue_key
                ORDER BY r.started_on DESC NULLS LAST, r.finished_on DESC NULLS LAST, r.run_id DESC
            ) AS rn
        FROM Silver.xray.test_runs r
        WHERE r.test_issue_key IS NOT NULL
    ) x
    WHERE rn = 1
""")


# MARKDOWN ********************
# ## Walk breadth-first, first visit per (root, node)
# CELL ********************

edges_b = F.broadcast(edges) if edge_count <= BROADCAST_EDGE_LIMIT else edges

# Level 0 seed: each issue as its own root (NOT emitted -- the root is the
# filter, not a descendant). Seeds go into `visited` so a link back to the
# root is dropped.
seed = issues.select(
    F.col("code").alias("Root_Code"),
    F.col("code").alias("Node_Code"),
    F.array(F.col("code")).alias("Path"),
).persist()
seed.count()

visited = seed.select("Root_Code", "Node_Code").persist()
visited.count()

frontier = seed
levels = []

for depth in range(1, MAX_DEPTH + 1):
    candidates = (
        frontier.alias("f")
        .join(edges_b.alias("e"), F.col("f.Node_Code") == F.col("e.src"), "inner")
        .select(
            F.col("f.Root_Code").alias("Root_Code"),
            F.col("e.dst").alias("Node_Code"),
            F.col("f.Node_Code").alias("Parent_Code"),
            F.lit(depth).alias("Level"),
            F.concat(F.col("f.Path"), F.array(F.col("e.dst"))).alias("Path"),
            F.col("e.Link_Label").alias("Link_Label"),
            F.col("e.Link_Type_Name").alias("Link_Type_Name"),
        )
        .dropDuplicates(["Root_Code", "Node_Code"])
    )
    nxt = (
        candidates.alias("c")
        .join(visited.alias("v"),
              (F.col("c.Root_Code") == F.col("v.Root_Code")) & (F.col("c.Node_Code") == F.col("v.Node_Code")),
              "left_anti")
        .persist()
    )
    if nxt.limit(1).count() == 0:
        nxt.unpersist()
        break
    levels.append(nxt)
    visited = visited.unionByName(nxt.select("Root_Code", "Node_Code")).persist()
    visited.count()
    frontier = nxt.select("Root_Code", "Node_Code", "Path")

walk = reduce(lambda a, b: a.unionByName(b), levels)


# MARKDOWN ********************
# ## Flatten path + denormalise everything the report shows
# CELL ********************

for k in range(1, MAX_DEPTH + 1):
    walk = walk.withColumn(
        f"Level_{k}",
        F.when(F.size(F.col("Path")) >= k, F.col("Path").getItem(k - 1)).otherwise(F.lit(None).cast("string")),
    )
walk = walk.withColumn("Path_String", F.array_join(F.col("Path"), " > "))

level_cols = [f"Level_{k}" for k in range(1, MAX_DEPTH + 1)]

df = (
    walk.alias("w")
    .join(issues.alias("n"), F.col("w.Node_Code") == F.col("n.code"), "left")
    .join(test_status.alias("ts"), F.col("w.Node_Code") == F.col("ts.Test_Code"), "left")
    .select(
        F.col("w.Path_String"),
        F.col("w.Root_Code"),
        F.col("w.Level"),
        F.col("w.Parent_Code"),
        F.col("w.Link_Type_Name"),
        F.col("w.Link_Label"),
        F.col("w.Node_Code"),
        F.coalesce(F.col("n.Issue_Type_Name"), F.lit("Unknown")).alias("Node_Issue_Type"),
        F.coalesce(F.col("n.Summary"), F.lit("Unknown")).alias("Node_Summary"),
        F.coalesce(F.col("n.Acceptance_Criteria"), F.lit("None")).alias("Node_Acceptance_Criteria"),
        F.coalesce(F.col("ts.Status_Name"), F.lit("N/A")).alias("Node_Test_Status"),
        F.concat(F.lit(f"{JIRA_BASE_URL}/browse/"), F.col("w.Node_Code")).alias("Node_URL"),
        F.when(F.col("w.Level") == 1, F.lit("Linked"))
         .when(F.col("w.Level") == 2, F.lit("Sub-linked"))
         .when(F.col("w.Level") == 3, F.lit("Sub-sub-linked"))
         .otherwise(F.concat(F.lit("Linked (level "), F.col("w.Level").cast("string"), F.lit(")")))
         .alias("Node_Relationship"),
        *[F.col(f"w.{c}").alias(c) for c in level_cols],
    )
)


# MARKDOWN ********************
# ## Merge into Gold
# CELL ********************

fmt.merge(spark, df, schema)

edges.unpersist()

print("Issue_Traceability built successfully")
