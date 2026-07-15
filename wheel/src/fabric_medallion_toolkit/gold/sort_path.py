"""
Sort ordering and date rollup for parent-child hierarchies.

Both solve problems that only appear once a hierarchy is rendered as a
Gantt/timeline, and both are generic to any self-referencing hierarchy --
nothing Jira-specific.

PERFORMANCE NOTE, important for both functions below: .cache() alone does
NOT force Spark to materialize a DataFrame -- it only marks it eligible
for caching; the actual computation still happens lazily on the next
action, and until that happens the full transformation lineage stays
attached. Inside a loop that builds each iteration on the previous one,
skipping an explicit action after each .cache() lets the lineage compound
every pass -- by iteration N, evaluating anything forces Spark to
re-execute the ENTIRE chain of N prior joins/aggregations from scratch,
not just the most recent step. At ~10 iterations over real data (~12,000
rows) this is the difference between seconds and 30+ minutes that never
finishes. Every .cache() below is followed by an explicit .count() for
exactly this reason -- do not remove it as "unnecessary."

SEPARATE PERFORMANCE NOTE: .cache() registers a DataFrame with Spark's
block manager, but reassigning the Python variable it's bound to does NOT
free those blocks -- only an explicit .unpersist() does. A loop that does
`current = (...).cache()` every pass, without unpersisting the PREVIOUS
pass's `current`/`accumulated`, leaves every prior iteration's cached data
sitting in executor memory for the lifetime of the loop. On a small Fabric
pool this fills available memory within a handful of passes and Spark
starts spilling/evicting, which is a second, independent way for this to
get slow -- distinct from the lineage-blowup problem above, and not fixed
by it. Every DataFrame cached inside the loop below is explicitly
unpersisted once it's no longer needed.

ALSO: the default spark.sql.shuffle.partitions (200) is sized for large
data, not a ~12,000-row hierarchy. Every join/union here triggers a
shuffle; 200 tiny partitions per shuffle, times ~2 shuffle stages per
loop pass, times up to max_depth passes, means task-scheduling overhead
alone can dominate wall-clock time on data this small. Both functions
below temporarily lower shuffle partitions for their own duration and
restore the session's original setting afterward -- this only affects
this function's own operations, not any other notebook or cell.
"""

from contextlib import contextmanager
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("gold.sort_path")

# Separator for path segments. Must sort BELOW every character that can
# start a rank value, so that a parent's own path always sorts immediately
# before its children's (whose paths extend it). "!" is ASCII 33 -- below
# digits (48+), letters, and "|" -- so this holds for LexoRank and for
# plain numeric or alphabetic ranks alike. Do not change this to "/" or
# "-" without rechecking that property.
PATH_SEPARATOR = "!"


@contextmanager
def _small_data_shuffle_partitions(spark, row_count: int):
    """
    Temporarily caps shuffle partitions to something sane for a hierarchy
    this size, restoring the session's original value on exit (even if the
    block raises). ~1 partition per 2,000 rows, floor of 8, ceiling of the
    session default -- never INCREASES partitions beyond what the session
    already had configured.
    """
    key = "spark.sql.shuffle.partitions"
    original = spark.conf.get(key)
    target = max(8, min(int(original), (row_count // 2000) + 1))
    spark.conf.set(key, target)
    try:
        yield
    finally:
        spark.conf.set(key, original)


def build_sort_path(df: DataFrame, id_column: str, parent_id_column: str,
                    rank_column: str, max_depth: int = 10,
                    root_prefix_column: Optional[str] = None) -> DataFrame:
    """
    Produces ONE string column whose plain ascending lexical sort
    reproduces the whole tree: every child immediately follows its
    parent, and siblings appear in their own rank order.

    Ranking a hierarchy by rank_column alone does NOT do this -- that
    orders every issue against every other issue globally, so a child can
    sort far away from its parent. Depth alone doesn't do it either.
    Sort_Path is the two combined: it concatenates each ancestor's rank
    from the root down, so a parent's path is a literal prefix of each of
    its children's.

    rank_column may be any value that sorts correctly as text -- Jira's
    LexoRank is designed for exactly this, so no numeric parsing is
    needed. Rows with a null rank fall back to their id, so they still
    get a stable (if arbitrary) position rather than dropping out.

    root_prefix_column: optional column (e.g. Project_Code) prepended to
    ROOT paths only, so that separate trees group together rather than
    interleaving by rank. Children inherit it automatically via their
    parent's path.

    Returns id_column, Sort_Path, and Depth (0 for roots).

    Implemented as a bounded downward walk rather than a recursive CTE
    (Spark SQL has none). Rows whose parent_id points at an id not
    present in df -- e.g. a cross-project parent that was never ingested
    -- are never reached by the walk; they are given a root-level path so
    they still appear, rather than silently vanishing. Stops as soon as a
    pass produces no new rows, rather than always running max_depth times.
    """
    rank_or_fallback = F.coalesce(F.col(rank_column), F.col(id_column))

    dup_count = df.groupBy(id_column).count().filter("count > 1").limit(1).count()
    if dup_count > 0:
        raise ValueError(
            f"build_sort_path: '{id_column}' is not unique in the input DataFrame -- at least "
            f"one id has more than one row. This function's walk assumes exactly one row per "
            f"id; duplicates fan the join out MULTIPLICATIVELY at every depth, which looks like "
            f"a runaway/never-finishing job, not a clean error, until it hits max_depth. Find "
            f"and fix the duplicate(s) upstream (e.g. `df.groupBy('{id_column}').count()."
            f"filter('count > 1')`) before calling this."
        )

    if root_prefix_column is not None:
        seed_path = F.concat(
            F.coalesce(F.col(root_prefix_column), F.lit("~")),
            F.lit(PATH_SEPARATOR),
            rank_or_fallback,
        )
    else:
        seed_path = rank_or_fallback

    spark = df.sparkSession

    base = df.select(
        F.col(id_column).alias("_id"),
        F.col(parent_id_column).alias("_parent"),
        rank_or_fallback.alias("_rank"),
        seed_path.alias("_seed"),
    ).cache()
    row_count = base.count()  # force full materialization now, not lazily later (see module docstring)

    with _small_data_shuffle_partitions(spark, row_count):
        current = base.filter(F.col("_parent").isNull()).select(
            F.col("_id"),
            F.col("_seed").alias("Sort_Path"),
            F.lit(0).alias("Depth"),
        ).cache()
        current.count()

        accumulated = current

        for depth in range(1, max_depth + 1):
            next_current = (
                base.alias("c")
                .join(current.alias("p"), F.col("c._parent") == F.col("p._id"), "inner")
                .select(
                    F.col("c._id").alias("_id"),
                    F.concat(F.col("p.Sort_Path"), F.lit(PATH_SEPARATOR), F.col("c._rank")).alias("Sort_Path"),
                    F.lit(depth).alias("Depth"),
                )
            ).cache()
            next_count = next_current.count()  # forces materialization AND tells us whether to stop

            # This pass's `current` (this depth's frontier) is done being read
            # once it's unioned below -- unpersist it explicitly. Without this,
            # every prior iteration's frontier stays cached in executor memory
            # for the rest of the loop (see module docstring).
            current.unpersist()

            if next_count == 0:
                current = next_current
                break

            previous_accumulated = accumulated
            accumulated = accumulated.unionByName(next_current).cache()
            accumulated.count()
            # Same leak, same fix: the PREVIOUS accumulated union is now fully
            # subsumed by the new one and never read again.
            previous_accumulated.unpersist()

            current = next_current
        else:
            # Loop completed without break -- there may be deeper rows still
            # unreached, which would silently get a root-level path below.
            if current.count() > 0:
                raise ValueError(
                    f"build_sort_path: max_depth={max_depth} was not enough to reach every "
                    f"descendant. Rows deeper than this would be given a misleading root-level "
                    f"path. Increase max_depth."
                )

        result = (
            df.select(F.col(id_column).alias("_bid"), seed_path.alias("_bseed"))
            .join(accumulated.alias("a"), F.col("_bid") == F.col("a._id"), "left")
            .select(
                F.col("_bid").alias(id_column),
                F.coalesce(F.col("a.Sort_Path"), F.col("_bseed")).alias("Sort_Path"),
                F.coalesce(F.col("a.Depth"), F.lit(0)).alias("Depth"),
            )
        ).cache()
        result.count()  # materialize before we unpersist its inputs below

    base.unpersist()
    accumulated.unpersist()
    if current is not accumulated:
        current.unpersist()

    return result


def rollup_hierarchy_dates(df: DataFrame, id_column: str, parent_id_column: str,
                            start_column: str, end_column: str, max_depth: int = 10,
                            out_start: str = "Rollup_Start_Date",
                            out_end: str = "Rollup_End_Date",
                            out_flag: str = "Has_Own_Dates") -> DataFrame:
    """
    Gives every row a drawable date range, the way a Gantt's summary rows
    work: a parent with no dates of its own spans its dated descendants.

    Precedence per row:
      1. its OWN start/end, if present  -> Has_Own_Dates = True
      2. else min(start)/max(end) across ALL its descendants (any depth)
      3. else NULL -- genuinely unscheduled

    Step 3 matters: leaving these NULL rather than substituting a sentinel
    date is what lets a visual render the ROW (in the tree, with its
    label) while drawing NO BAR -- which is what a real Gantt does for an
    unscheduled item. A sentinel like 1900-01-01 instead draws a phantom
    bar a century back and stretches the whole time axis to reach it.

    A row's own dates always win over its children's, even if a child
    falls outside the parent's stated range -- the parent's dates are a
    deliberate statement, not something to be silently overridden.

    Implemented as a bounded upward walk: each pass propagates one more
    generation of descendant min/max up to its parent, so after N passes
    a row has absorbed everything within N levels below it. Stops as soon
    as a pass changes nothing, rather than always running max_depth times
    -- most hierarchies settle in far fewer than 10 passes.
    """
    spark = df.sparkSession

    walk = df.select(
        F.col(id_column).alias("_id"),
        F.col(parent_id_column).alias("_parent"),
        F.col(start_column).alias("_own_start"),
        F.col(end_column).alias("_own_end"),
    ).cache()
    row_count = walk.count()  # force materialization now (see module docstring)

    with _small_data_shuffle_partitions(spark, row_count):
        # Running best-known min/max for each row, seeded with its own dates.
        agg = walk.select(
            F.col("_id"),
            F.col("_parent"),
            F.col("_own_start").alias("_min_start"),
            F.col("_own_end").alias("_max_end"),
        ).cache()
        agg.count()

        for _ in range(max_depth):
            # Each row's children, collapsed to one min/max per parent.
            from_children = (
                agg.filter(F.col("_parent").isNotNull())
                .groupBy(F.col("_parent").alias("_pid"))
                .agg(
                    F.min("_min_start").alias("_child_min_start"),
                    F.max("_max_end").alias("_child_max_end"),
                )
            )

            new_agg = (
                agg.alias("a")
                .join(from_children.alias("c"), F.col("a._id") == F.col("c._pid"), "left")
                .select(
                    F.col("a._id"),
                    F.col("a._parent"),
                    F.least(
                        F.coalesce(F.col("a._min_start"), F.col("c._child_min_start")),
                        F.coalesce(F.col("c._child_min_start"), F.col("a._min_start")),
                    ).alias("_min_start"),
                    F.greatest(
                        F.coalesce(F.col("a._max_end"), F.col("c._child_max_end")),
                        F.coalesce(F.col("c._child_max_end"), F.col("a._max_end")),
                    ).alias("_max_end"),
                )
            ).cache()
            new_agg.count()  # force materialization now, not lazily later

            # Stop as soon as a pass changes nothing -- comparing counts of
            # rows that actually differ from the previous pass is cheap once
            # both sides are already materialized, and avoids running the
            # full max_depth every time regardless of the real hierarchy depth.
            changed = new_agg.alias("n").join(
                agg.alias("o"), F.col("n._id") == F.col("o._id"), "inner"
            ).filter(
                (F.col("n._min_start") != F.col("o._min_start"))
                | (F.col("n._max_end") != F.col("o._max_end"))
            ).limit(1).count()

            # This pass's previous `agg` is fully superseded by new_agg and
            # never read again once we reassign below -- unpersist it
            # explicitly, or it (and every prior pass's copy) stays cached in
            # executor memory for the rest of the loop (see module docstring).
            agg.unpersist()
            agg = new_agg
            if changed == 0:
                break

        result = (
            walk.alias("w")
            .join(agg.alias("g"), F.col("w._id") == F.col("g._id"), "left")
            .select(
                F.col("w._id").alias(id_column),
                # Own dates win outright; otherwise fall back to the rolled-up
                # descendant range; otherwise NULL.
                F.coalesce(F.col("w._own_start"), F.col("g._min_start")).alias(out_start),
                F.coalesce(F.col("w._own_end"), F.col("g._max_end")).alias(out_end),
                (F.col("w._own_start").isNotNull() | F.col("w._own_end").isNotNull()).alias(out_flag),
            )
        ).cache()
        result.count()  # materialize before we unpersist its inputs below

    walk.unpersist()
    agg.unpersist()

    final = df.join(result, on=id_column, how="left")
    result.unpersist()
    return final
