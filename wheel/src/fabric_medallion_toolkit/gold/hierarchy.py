"""
Converts a self-referencing parent-child hierarchy into a flattened,
multi-column "levels" representation -- Level_1 (topmost) through
Level_N -- for Power BI custom visuals (e.g. xViz Gantt Chart) that
expect a hierarchy as separate columns per level rather than a recursive
parent/child pair.

Two placement strategies, because they answer different questions:

build_hierarchy_levels()
    Places each row by its DEPTH IN THE PARENT CHAIN. Row with three
    ancestors lands in Level_4, regardless of what it is. Correct for
    hierarchies where depth IS the meaning (folder trees, org charts).

build_typed_hierarchy_levels()
    Places each row by its TYPE'S OWN RANK, ignoring how deep its parent
    chain happens to run. This is what Jira's timeline does: a "Task"
    always renders at the Task tier whether its parent is an Epic or the
    project root. An issue can therefore skip tiers -- a Task parented
    directly to a Release leaves the Initiative/Workstream/Epic levels
    blank -- which is a "ragged" hierarchy and is CORRECT, not a defect.
    Visuals that render these need their blank-row filter enabled.

Both are implemented as a bounded number of self-joins (one per level)
rather than a recursive query, since Spark SQL has no recursive CTE.
Fine for realistic depths -- a handful of levels, not hundreds.
"""

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_hierarchy_levels(df: DataFrame, id_column: str, parent_id_column: str,
                            code_column: str, max_depth: int = 10) -> DataFrame:
    """
    Depth-based placement. See module docstring for when to prefer
    build_typed_hierarchy_levels instead.

    Returns id_column plus Level_1 (topmost ancestor) .. Level_{max_depth}
    (the row's own code), left-aligned by actual chain depth -- shallower
    branches leave trailing Level_N null rather than being padded.
    """
    dup_count = df.groupBy(id_column).count().filter("count > 1").limit(1).count()
    if dup_count > 0:
        raise ValueError(
            f"build_hierarchy_levels: '{id_column}' is not unique in the input DataFrame -- at "
            f"least one id has more than one row. Every one of this function's self-joins "
            f"assumes one row per id; duplicates fan out MULTIPLICATIVELY at each of the "
            f"max_depth={max_depth} levels, which looks like a runaway job rather than a clean "
            f"error. Find and fix the duplicate(s) upstream before calling this."
        )

    result = df.select(
        F.col(id_column).alias("_id0"),
        F.col(code_column).alias("_level0"),
        F.col(parent_id_column).alias("_parent0"),
    )
    lookup = df.select(
        F.col(id_column).alias("_lk_id"),
        F.col(parent_id_column).alias("_lk_parent"),
        F.col(code_column).alias("_lk_code"),
    )

    for i in range(1, max_depth):
        renamed_lookup = lookup.select(
            F.col("_lk_id").alias(f"_id{i}"),
            F.col("_lk_parent").alias(f"_parent{i}"),
            F.col("_lk_code").alias(f"_level{i}"),
        )
        result = result.join(renamed_lookup, result[f"_parent{i - 1}"] == F.col(f"_id{i}"), "left")

    truncated_count = result.filter(F.col(f"_parent{max_depth - 1}").isNotNull()).limit(1).count()
    if truncated_count > 0:
        raise ValueError(
            f"build_hierarchy_levels: max_depth={max_depth} is not deep enough -- at least one "
            f"branch still has an ancestor beyond this depth, which would silently lose that "
            f"branch's root context (showing its deepest levels instead of its topmost ones). "
            f"Increase max_depth to safely cover your data's real maximum hierarchy depth."
        )

    level_cols = [F.col(f"_level{i}") for i in range(max_depth)]
    path_array = F.reverse(F.filter(F.array(*level_cols), lambda x: x.isNotNull()))

    final_cols = [F.col("_id0").alias(id_column)]
    for level_num in range(1, max_depth + 1):
        final_cols.append(F.element_at(path_array, level_num).alias(f"Level_{level_num}"))

    return result.select(*final_cols)


def build_typed_hierarchy_levels(df: DataFrame, id_column: str, parent_id_column: str,
                                  code_column: str, type_rank_column: str,
                                  rank_to_level: Dict[int, int], max_chain_walk: int = 15) -> DataFrame:
    """
    Type-based placement -- each row lands in the Level_N its TYPE maps to,
    not the depth its parent chain happens to reach.

    type_rank_column: a numeric column on df giving each row's type rank
        (e.g. Jira's Hierarchy_Level: Programme=5 .. Task=0, Sub-task=-1).

    rank_to_level: maps each type rank to its 1-based Level_N slot, e.g.
        {5: 1, 4: 2, 3: 3, 2: 4, 1: 5, 0: 6, -1: 7}
        meaning Programme -> Level_1, ... Sub-task -> Level_7. The number
        of levels produced is len(rank_to_level). Ranks in the data that
        are absent from this dict raise, rather than silently vanishing
        from the hierarchy.

    max_chain_walk: how many ancestors to walk while resolving each row's
        ancestry. Must be at least as deep as the longest real parent
        chain -- note this can EXCEED the number of levels, since a chain
        may pass through several issues of the same type. Raises if a
        chain is still unresolved after this many steps.

    An ancestor whose type maps to a level at or below the row's own
    level is ignored for placement purposes (it cannot be an ancestor in
    the type hierarchy even if it is one in the parent chain) -- this is
    what keeps a Task parented to another Task from producing two Task
    levels.
    """
    if not rank_to_level:
        raise ValueError("build_typed_hierarchy_levels: rank_to_level must not be empty")

    dup_count = df.groupBy(id_column).count().filter("count > 1").limit(1).count()
    if dup_count > 0:
        raise ValueError(
            f"build_typed_hierarchy_levels: '{id_column}' is not unique in the input DataFrame "
            f"-- at least one id has more than one row. The chain walk assumes one row per id; "
            f"duplicates fan out MULTIPLICATIVELY at every one of the max_chain_walk="
            f"{max_chain_walk} steps, which looks like a runaway job rather than a clean error. "
            f"Find and fix the duplicate(s) upstream before calling this."
        )

    num_levels = len(rank_to_level)
    levels_present = sorted(rank_to_level.values())
    if levels_present != list(range(1, num_levels + 1)):
        raise ValueError(
            f"build_typed_hierarchy_levels: rank_to_level's values must be exactly the levels "
            f"1..{num_levels} with no gaps or duplicates, got {levels_present}."
        )

    known_ranks = set(rank_to_level.keys())
    actual_ranks = {r[type_rank_column] for r in df.select(type_rank_column).distinct().collect()
                    if r[type_rank_column] is not None}
    unmapped = sorted(actual_ranks - known_ranks)
    if unmapped:
        raise ValueError(
            f"build_typed_hierarchy_levels: the data contains type rank(s) {unmapped} that are "
            f"not in rank_to_level (which covers {sorted(known_ranks)}). Rows with an unmapped "
            f"rank would silently drop out of the hierarchy -- add them to rank_to_level."
        )

    # Map each row's own rank to its level slot, once.
    level_expr = F.lit(None).cast("int")
    for rank, level in rank_to_level.items():
        level_expr = F.when(F.col(type_rank_column) == rank, F.lit(level)).otherwise(level_expr)

    base = df.select(
        F.col(id_column).alias("_id"),
        F.col(code_column).alias("_code"),
        F.col(parent_id_column).alias("_parent"),
        level_expr.alias("_level"),
    )

    lookup = base.select(
        F.col("_id").alias("_lk_id"),
        F.col("_code").alias("_lk_code"),
        F.col("_parent").alias("_lk_parent"),
        F.col("_level").alias("_lk_level"),
    )

    # Walk up the chain, collecting (level, code) pairs for every ancestor
    # AND the row itself. Structs keep each ancestor's level bound to its
    # code, so the final step can drop each one into the right slot
    # regardless of the order they were encountered.
    walk = base.select(
        F.col("_id").alias("_row_id"),
        F.array(F.struct(F.col("_level").alias("lvl"), F.col("_code").alias("code"))).alias("_pairs"),
        F.col("_parent").alias("_ptr"),
    )

    for _ in range(max_chain_walk):
        walk = (
            walk.alias("w")
            .join(lookup.alias("a"), F.col("w._ptr") == F.col("a._lk_id"), "left")
            .select(
                F.col("w._row_id").alias("_row_id"),
                F.when(
                    F.col("a._lk_code").isNotNull(),
                    F.concat(
                        F.col("w._pairs"),
                        F.array(F.struct(F.col("a._lk_level").alias("lvl"), F.col("a._lk_code").alias("code"))),
                    ),
                ).otherwise(F.col("w._pairs")).alias("_pairs"),
                F.col("a._lk_parent").alias("_ptr"),
            )
        )

    unresolved = walk.filter(F.col("_ptr").isNotNull()).limit(1).count()
    if unresolved > 0:
        raise ValueError(
            f"build_typed_hierarchy_levels: max_chain_walk={max_chain_walk} was not enough to "
            f"reach the root of every parent chain. Increase it (note it counts CHAIN STEPS, "
            f"which can exceed the number of type levels when a chain passes through several "
            f"issues of the same type)."
        )

    # Drop each collected pair into its own level column. Taking the FIRST
    # match per level is deliberate: _pairs is built row-first then upward,
    # so if a chain contains two issues of the same type, the one NEAREST
    # the row wins -- the closer ancestor is the more meaningful one.
    final_cols = [F.col("_row_id").alias(id_column)]
    for level_num in range(1, num_levels + 1):
        matches = F.filter(F.col("_pairs"), lambda p: p["lvl"] == F.lit(level_num))
        final_cols.append(F.element_at(matches, 1)["code"].alias(f"Level_{level_num}"))

    return walk.select(*final_cols)
