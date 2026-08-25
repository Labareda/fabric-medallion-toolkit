# Fabric notebook source

# MARKDOWN ********************
# ## Fact_Issue_Traceability -- the full recursive link tree
#
# Purpose:
#   Let the client sit on ANY issue and expand a single tree that keeps
#   drilling through every link, of every type, down to the leaves:
#
#     TRAN-3574 (Task)
#       -> TRAN-2425 (Test)        [Blocks]
#            -> TRAN-2827 (Exec)   [Belongs to]
#            -> TRAN-2770 (Set)    [Belongs to]
#            -> TRAN-3574 (Task)   [Blocks]   <- loops back: shown once, not re-expanded
#       -> TRAN-2415 (Test)        [Blocks]
#       -> ...
#
#   This is arbitrary-depth graph traversal, which Power BI cannot do in a
#   matrix and Spark SQL cannot express as a recursive CTE, so it is resolved
#   here in Gold by iteratively walking the Bridge and flattened into dense
#   Level_1..Level_N columns for the report's matrix.
#
# Design (confirmed with the client):
#   * ROOT      = every issue can be the top of a tree; the report picks one
#                 via a slicer on Root_Code.
#   * LINKS     = every link type / direction (read straight from
#                 Bridge_Issue_Link, which already holds both directions of
#                 every Jira link plus Xray containment).
#   * TRAVERSAL = breadth-first, FIRST VISIT ONLY. Each node is placed once
#                 per root, at its shortest path. This keeps the table small
#                 and the build fast, and handles cycles for free: a link
#                 back to an ancestor is already-visited, so it is not
#                 expanded again (and the reciprocal link is not repeated).
#   * MAX_DEPTH = a safety cap on top of the first-visit rule.
#
# Grain:
#   ONE ROW PER (Root, Node). A node reachable several ways is shown once,
#   under its shortest route -- not duplicated once per route. (If the
#   client later needs every distinct route, that is the slower all-paths
#   variant; ask before switching back.)
#
# Reads Gold.gold.bridge_issue_link (a fact) -- declared as a dependency in
# the orchestrator (fact-reads-fact, like Fact_Resource_Day_Allocation).
# CELL ********************

import fabric_medallion_toolkit as fmt
from functools import reduce
from pyspark.sql import functions as F

GOLD_SCHEMA = "Gold.gold"

# ---------------------------------------------------------------------------
# Replace with the client's actual Jira base URL (no trailing slash).
# ---------------------------------------------------------------------------
JIRA_BASE_URL = "https://yourcompany.atlassian.net"

# Safety cap. The cycle rule already guarantees termination (a path can never
# revisit a node), so this only bounds how deep the tree is allowed to go.
# Traceability chains here are shallow (Task -> Test -> Execution/Set); raise
# it if a genuinely deeper chain gets cut off.
MAX_DEPTH = 6

# Number of dense Level_* columns emitted (fixed, matches the semantic model
# and the report matrix). Kept separate from MAX_DEPTH so the traversal depth
# can be tuned without changing the table shape -- levels beyond the deepest
# path are simply trailing nulls. MAX_DEPTH must never exceed this.
LEVEL_COLUMNS = 8

# Above this edge count, don't broadcast the edge list (broadcasting a large
# table collects it to the driver and stalls); Spark picks a shuffle join.
BROADCAST_EDGE_LIMIT = 300000


# MARKDOWN ********************
# ## Declare the table schema
#
# Level_1..Level_{LEVEL_COLUMNS} are the DENSE, depth-placed hierarchy columns the
# matrix nests on. They are contiguous with only TRAILING nulls -- this is
# the same convention as Dim_Issue's Gantt Level columns, and the deliberate
# exception to the no-blanks rule (a hierarchy needs trailing nulls to know
# where to stop nesting; leave the visual's "Hide Blanks" OFF).
# CELL ********************

schema_columns = {

    # Grain -- one row per distinct root->node path.
    "Path_String": {"type": "string", "merge_field": True},

    # The top of this row's tree.
    "Root_Code":   {"type": "string", "default": "Unknown"},

    # Depth of this node (1 = the root itself).
    "Level":       {"type": "int", "default": 0},

    # This node and how it hangs off its parent.
    "Node_Code":      {"type": "string", "default": "Unknown"},
    "Parent_Code":    {"type": "string", "default": "None"},
    "Link_Label":     {"type": "string", "default": "None"},
    "Link_Type_Name": {"type": "string", "default": "None"},

    # Denormalised attributes of this node.
    "Node_Issue_Type": {"type": "string", "default": "Unknown"},
    "Node_Summary":    {"type": "string", "default": "Unknown"},
    "Node_Status":     {"type": "string", "default": "Unknown"},
    "Node_URL":        {"type": "string", "default": "Unknown"},

    # Active relationship is on the ROOT, so selecting an issue elsewhere in
    # the model scopes the tree to that issue's root rows.
    "Root_Issue_Key": {
        "type": "string",
        "lookup_missing_from": {
            "table": f"{GOLD_SCHEMA}.dim_issue",
            "natural_key_column": "Issue_Code",
            "key_column": "Issue_Key",
            "unknown_value": "Unknown",
        },
    },

    "Node_Issue_Key": {
        "type": "string",
        "lookup_missing_from": {
            "table": f"{GOLD_SCHEMA}.dim_issue",
            "natural_key_column": "Issue_Code",
            "key_column": "Issue_Key",
            "unknown_value": "Unknown",
        },
    },
}

# Dense hierarchy levels -- string, nullable, NO default (trailing nulls are
# expected and required for the matrix to stop nesting).
for _k in range(1, LEVEL_COLUMNS + 1):
    schema_columns[f"Level_{_k}"] = {"type": "string"}

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_issue_traceability",
    table_type="fact",
    key_column="Issue_Traceability_Key",
    columns=schema_columns,
    # Fully recomputed from the graph every run -- there is nothing to
    # preserve, so replace the whole table instead of MERGE-comparing every
    # rebuilt row against the old one (MERGE here is pure overhead).
    write_mode="overwrite",
)


# MARKDOWN ********************
# ## Build the edge list and the node attributes
#
# Edges come from the Bridge, which already carries BOTH directions of every
# Jira link plus Xray containment, with the label and type on each edge --
# so the walk never has to reason about direction itself.
# CELL ********************

# The Bridge is already deduplicated (its own final SELECT is DISTINCT), so
# no dropDuplicates here -- that would be a redundant shuffle, and the walk
# collapses duplicate edges per (root, node) anyway. This cell is then just a
# 4-column, column-pruned read of the Bridge: as cheap as the read gets.
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

nodes = spark.sql(f"""
    SELECT
        Issue_Code      AS code,
        Issue_Key,
        Issue_Type_Name,
        Summary,
        Status_Name
    FROM {GOLD_SCHEMA}.dim_issue
""")


# MARKDOWN ********************
# ## Walk the graph breadth-first, one visit per (root, node)
#
# FIRST-VISIT BFS (not all-paths). Each node is recorded ONCE per root, at
# its shortest path from that root. This is what keeps the table small and
# the build fast:
#   * a node reachable by several routes is not duplicated once per route;
#   * a link back to an ancestor is simply already-visited, so it is not
#     expanded again -- cycles handled with no per-path array scan at all.
# Going strictly level by level means the first time a node is reached IS its
# shortest path, so the retained parent/label is the shortest-route one.
#
# `edges` is broadcast: it is small relative to the growing frontier, and
# broadcasting turns each level into a map-side join with no shuffle.
# CELL ********************

# Broadcast only when the edge list is small enough; otherwise let Spark
# shuffle-join (broadcasting a large edge set stalls the driver).
edges_b = F.broadcast(edges) if edge_count <= BROADCAST_EDGE_LIMIT else edges

# Level 1: every issue is the root of its own tree.
frontier = nodes.select(
    F.col("code").alias("Root_Code"),
    F.col("code").alias("Node_Code"),
    F.lit(1).alias("Level"),
    F.array(F.col("code")).alias("Path"),
    F.lit(None).cast("string").alias("Parent_Code"),
    F.lit(None).cast("string").alias("Link_Label"),
    F.lit(None).cast("string").alias("Link_Type_Name"),
).persist()
frontier.count()

levels = [frontier]

# Every (root, node) already placed -- used to drop re-visits (which also
# drops cycles, since an ancestor is by definition already visited).
visited = frontier.select("Root_Code", "Node_Code").persist()
visited.count()

for depth in range(2, MAX_DEPTH + 1):
    candidates = (
        frontier.alias("f")
        .join(edges_b.alias("e"), F.col("f.Node_Code") == F.col("e.src"), "inner")
        .select(
            F.col("f.Root_Code").alias("Root_Code"),
            F.col("e.dst").alias("Node_Code"),
            F.lit(depth).alias("Level"),
            F.concat(F.col("f.Path"), F.array(F.col("e.dst"))).alias("Path"),
            F.col("f.Node_Code").alias("Parent_Code"),
            F.col("e.Link_Label").alias("Link_Label"),
            F.col("e.Link_Type_Name").alias("Link_Type_Name"),
        )
        # one row per (root, node) this level -- collapse multiple parents
        .dropDuplicates(["Root_Code", "Node_Code"])
    )

    # keep only genuinely new (root, node) pairs
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
    frontier = nxt

walk = reduce(lambda a, b: a.unionByName(b), levels)


# MARKDOWN ********************
# ## Flatten the path into dense Level columns + denormalise node attributes
# CELL ********************

# Path array -> Level_1..Level_{LEVEL_COLUMNS} (trailing nulls beyond depth)
for k in range(1, LEVEL_COLUMNS + 1):
    walk = walk.withColumn(
        f"Level_{k}",
        F.when(F.size(F.col("Path")) >= k, F.col("Path").getItem(k - 1)).otherwise(F.lit(None).cast("string")),
    )

walk = walk.withColumn("Path_String", F.array_join(F.col("Path"), " > "))

level_cols = [f"Level_{k}" for k in range(1, LEVEL_COLUMNS + 1)]

df = (
    walk.alias("r")
    .join(nodes.alias("n"), F.col("r.Node_Code") == F.col("n.code"), "left")
    .select(
        F.col("r.Path_String"),
        F.col("r.Root_Code"),
        F.col("r.Level"),
        F.col("r.Node_Code"),
        F.coalesce(F.col("r.Parent_Code"), F.lit("None")).alias("Parent_Code"),
        F.coalesce(F.col("r.Link_Label"), F.lit("None")).alias("Link_Label"),
        F.coalesce(F.col("r.Link_Type_Name"), F.lit("None")).alias("Link_Type_Name"),
        F.coalesce(F.col("n.Issue_Type_Name"), F.lit("Unknown")).alias("Node_Issue_Type"),
        F.coalesce(F.col("n.Summary"), F.lit("Unknown")).alias("Node_Summary"),
        F.coalesce(F.col("n.Status_Name"), F.lit("Unknown")).alias("Node_Status"),
        F.concat(F.lit(f"{JIRA_BASE_URL}/browse/"), F.col("r.Node_Code")).alias("Node_URL"),
        # Root_Issue_Key resolved by lookup_missing_from at merge time; supply
        # the natural key so the fallback has something to resolve.
        F.lit(None).cast("string").alias("Root_Issue_Key"),
        F.col("n.Issue_Key").alias("Node_Issue_Key"),
        *[F.col(f"r.{c}").alias(c) for c in level_cols],
    )
)

# Root_Issue_Key: resolve the root's own key directly (its own dim_issue row).
df = (
    df.alias("d")
    .join(nodes.select(F.col("code").alias("_rc"), F.col("Issue_Key").alias("_rk")).alias("rt"),
          F.col("d.Root_Code") == F.col("rt._rc"), "left")
    .drop("Root_Issue_Key", "_rc")
    .withColumnRenamed("_rk", "Root_Issue_Key")
)


# MARKDOWN ********************
# ## Merge into Gold
# CELL ********************

fmt.merge(spark, df, schema)

edges.unpersist()

print("Fact_Issue_Traceability built successfully")
