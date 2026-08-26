# Fabric notebook source

# MARKDOWN ********************
# ## Bridge_Issue_Traceability -- the issue-to-issue relationship model
#
# The middle of a three-table model:
#
#     Dim Issue  --(Root_Issue_Key)-->  Bridge_Issue_Traceability  <--(Node_Issue_Key)--  Linked Issue
#
# One row per (Root, reachable descendant Node). Select a Root in the report
# (via Dim Issue) and every issue reachable from it appears -- its DIRECT
# links at Level 1, THEIR links at Level 2, and so on down to the leaves.
# This is what lets "select TRAN-3574 -> see TRAN-2411..2425 AND the tests
# linked to those tests" work: Power BI relationships only ever resolve ONE
# hop live, so the multi-hop reach is resolved here in Gold instead.
#
# Traversal: breadth-first, FIRST VISIT ONLY -- each descendant is placed
# once per root, at its shortest path. Cheap, and cycle-safe (a link back to
# an ancestor is already visited, so it is not expanded again).
#
# Edges come from Bridge_Issue_Link (both directions of every Jira link plus
# Xray containment). Fully rebuilt each run -> write_mode="overwrite".
# CELL ********************

import fabric_medallion_toolkit as fmt
from functools import reduce
from pyspark.sql import functions as F

GOLD_SCHEMA = "Gold.gold"

# How many hops deep to walk. First-visit already guarantees termination;
# this just bounds depth. Also the number of dense Level_* code columns.
MAX_DEPTH = 6

# Don't broadcast an edge list bigger than this (broadcasting a large table
# stalls the driver); Spark shuffle-joins instead.
BROADCAST_EDGE_LIMIT = 300000


# MARKDOWN ********************
# ## Declare the table schema
# CELL ********************

schema_columns = {

    # Grain -- one row per distinct root->node path.
    "Path_String": {"type": "string", "merge_field": True},

    # The selected top of the tree, the descendant, and its immediate parent.
    "Root_Code":   {"type": "string", "default": "Unknown"},
    "Node_Code":   {"type": "string", "default": "Unknown"},
    "Parent_Code": {"type": "string", "default": "Unknown"},

    # Hop count from the root (1 = a direct link of the root).
    "Level": {"type": "int", "default": 0},

    # The link from Parent to Node.
    "Link_Type_Name": {"type": "string", "default": "None"},
    "Link_Label":     {"type": "string", "default": "None"},

    # Keys for the relationship model.
    "Root_Issue_Key": {
        "type": "string",
        "lookup_missing_from": {
            "table": f"{GOLD_SCHEMA}.dim_issue",
            "natural_key_column": "Issue_Code", "key_column": "Issue_Key",
            "unknown_value": "Unknown",
        },
    },
    "Node_Issue_Key": {
        "type": "string",
        "lookup_missing_from": {
            "table": f"{GOLD_SCHEMA}.dim_issue",
            "natural_key_column": "Issue_Code", "key_column": "Issue_Key",
            "unknown_value": "Unknown",
        },
    },
    "Link_Type_Key": {
        "type": "string",
        "lookup_missing_from": {
            "table": f"{GOLD_SCHEMA}.dim_link_type",
            "natural_key_column": "Link_Type_Name", "key_column": "Link_Type_Key",
            "unknown_value": "Unknown",
        },
    },
}

# Dense Level_1..Level_MAX_DEPTH code columns (trailing nulls) -- optional, for
# a nested matrix. The flat Root/Parent/Node/Level columns above drive the
# plain table. NO default (trailing nulls are expected in a hierarchy).
for _k in range(1, MAX_DEPTH + 1):
    schema_columns[f"Level_{_k}"] = {"type": "string"}

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_traceability",
    table_type="fact",
    key_column="Issue_Traceability_Key",
    columns=schema_columns,
    write_mode="overwrite",
)


# MARKDOWN ********************
# ## Edges and node keys
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

issue_keys = spark.sql(f"""
    SELECT Issue_Code AS code, Issue_Key FROM {GOLD_SCHEMA}.dim_issue
""")


# MARKDOWN ********************
# ## Walk breadth-first, first visit per (root, node)
# CELL ********************

edges_b = F.broadcast(edges) if edge_count <= BROADCAST_EDGE_LIMIT else edges

# Level 0 seed: each issue as its own root (NOT emitted -- the root is the
# filter, not a descendant). Seeds go into `visited` so a link back to the
# root is dropped.
seed = issue_keys.select(
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
# ## Flatten path, resolve keys, assemble
# CELL ********************

for k in range(1, MAX_DEPTH + 1):
    walk = walk.withColumn(
        f"Level_{k}",
        F.when(F.size(F.col("Path")) >= k, F.col("Path").getItem(k - 1)).otherwise(F.lit(None).cast("string")),
    )
walk = walk.withColumn("Path_String", F.array_join(F.col("Path"), " > "))

level_cols = [f"Level_{k}" for k in range(1, MAX_DEPTH + 1)]

link_types = spark.sql(f"""
    SELECT Link_Type_Name AS lt_name, Link_Type_Key FROM {GOLD_SCHEMA}.dim_link_type
""")

df = (
    walk.alias("w")
    .join(issue_keys.alias("rt"), F.col("w.Root_Code") == F.col("rt.code"), "left")
    .withColumnRenamed("Issue_Key", "Root_Issue_Key").drop("code")
    .alias("w2")
    .join(issue_keys.alias("nd"), F.col("w2.Node_Code") == F.col("nd.code"), "left")
    .withColumnRenamed("Issue_Key", "Node_Issue_Key").drop("code")
    .alias("w3")
    .join(link_types.alias("lt"), F.col("w3.Link_Type_Name") == F.col("lt.lt_name"), "left")
    .select(
        "Path_String", "Root_Code", "Node_Code", "Parent_Code", "Level",
        "Link_Type_Name", "Link_Label",
        "Root_Issue_Key", "Node_Issue_Key",
        F.col("Link_Type_Key"),
        *level_cols,
    )
)


# MARKDOWN ********************
# ## Merge into Gold
# CELL ********************

fmt.merge(spark, df, schema)

edges.unpersist()

print("Bridge_Issue_Traceability built successfully")
