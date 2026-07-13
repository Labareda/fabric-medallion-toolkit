"""
Converts a self-referencing parent-child hierarchy (one row per entity,
with its own id, parent id, and a business code/name) into a flattened,
multi-column "levels" representation -- Level_1 (the topmost ancestor)
through Level_N (the entity's own code), left-aligned by each row's ACTUAL
depth, not padded to a fixed position.

Not Jira-specific -- any self-referencing parent-child table has this
same shape. Built specifically because some Power BI custom visuals
(e.g. xViz Gantt Chart) expect a hierarchy as separate columns per level,
rather than the recursive parent-child pattern Power BI's own native
hierarchy visual uses -- this is the standard way of bridging the two.

Implemented as a fixed number of self-joins (one per level) rather than a
true recursive query, since Spark SQL doesn't support recursive CTEs the
way some other engines do. This is fine given a small, known max_depth --
each join is a normal, well-optimized operation, not something that scales
badly for realistic hierarchy depths (a handful of levels, not hundreds).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_hierarchy_levels(df: DataFrame, id_column: str, parent_id_column: str,
                            code_column: str, max_depth: int = 10) -> DataFrame:
    """
    df: one row per entity, with id_column/parent_id_column/code_column
        already present (parent_id_column may be null for root entities).
    max_depth: the maximum number of hierarchy levels to support -- must be
        at least as deep as your real data's deepest branch, or that
        branch's deepest levels get silently truncated. Extra depth
        beyond what's actually used just means trailing Level_N columns
        come back null for shallower branches -- harmless, not an error.

    Returns a DataFrame with id_column plus Level_1 (topmost ancestor)
    through Level_{max_depth} (the entity's own code) -- shallower
    branches simply have null in their unused trailing Level_N columns,
    rather than being padded into the wrong position.
    """
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

    # Safety check: if the DEEPEST ancestor this walk reached still has a
    # non-null parent, max_depth ran out before reaching the true root --
    # without this check, that failure is silent and actively misleading:
    # the result shows the BOTTOM of that branch instead of the top (e.g.
    # "Epic > Task > Sub-task" instead of "Programme > Release > Epic"),
    # which loses exactly the context a hierarchy view exists to show.
    # Raising here is much better than a report silently missing the roots
    # of its deepest branches.
    truncated_count = result.filter(F.col(f"_parent{max_depth - 1}").isNotNull()).limit(1).count()
    if truncated_count > 0:
        raise ValueError(
            f"build_hierarchy_levels: max_depth={max_depth} is not deep enough -- at least one "
            f"branch still has an ancestor beyond this depth, which would silently lose that "
            f"branch's root context (showing its deepest levels instead of its topmost ones). "
            f"Increase max_depth to safely cover your data's real maximum hierarchy depth."
        )

    # Each row now has _level0 (its own code) through _level{max_depth-1}
    # (the deepest ancestor this specific branch's walk reached, null
    # beyond that). Collect into an array (bottom-up), drop the nulls
    # (that's what makes shallower branches correctly left-align instead
    # of leaving gaps), reverse to get top-down order, then pull out each
    # position as its own column.
    level_cols = [F.col(f"_level{i}") for i in range(max_depth)]
    path_array = F.reverse(F.filter(F.array(*level_cols), lambda x: x.isNotNull()))

    final_cols = [F.col("_id0").alias(id_column)]
    for level_num in range(1, max_depth + 1):
        final_cols.append(F.element_at(path_array, level_num).alias(f"Level_{level_num}"))

    return result.select(*final_cols)
