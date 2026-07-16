"""
One high-level function, `enrich_issue_hierarchy()`, that turns a flat
one-row-per-issue DataFrame into a full Gantt-ready issue dimension: parent
surrogate key, typed ancestor columns (for slicers), dense level columns
(for the xViz Gantt), depth, and the single Sort_Path that orders the whole
tree.

Notebooks should call this ONE function rather than orchestrating the four
underlying walks (build_typed_hierarchy_levels, build_hierarchy_levels,
build_sort_path, add_guid_key) and the caching between them by hand. All the
"why" -- the two-different-hierarchies distinction, the ragged-vs-dense
rule, why Sort_Path is a prefix of its children -- lives here and in the
functions this calls, not repeated in every notebook.

Design notes worth keeping near the code:

  TWO SETS OF HIERARCHY COLUMNS, deliberately not interchangeable:
    * Level_1..Level_N (DENSE)  -- placed by DEPTH in the parent chain,
      contiguous with only trailing nulls. This is what the xViz Gantt
      nests; it stops nesting at the first null, so interior gaps (which a
      type-based placement produces) break the visual. Leave the visual's
      "Hide Blanks" OFF -- trailing nulls are expected.
    * Named typed columns (Programme/Release/... via typed_level_names) --
      placed by the issue TYPE's own tier, so "Programme" always means a
      programme regardless of chain depth. Ragged (nulls anywhere) and that
      is correct: a slicer with blanks is normal, a tree with blanks is not.

  SORT_PATH orders the whole tree with a single ascending sort: it
  concatenates each ancestor's rank from the root down, so a parent's path
  is a literal prefix of each child's. Jira's own Rank orders every issue
  globally and would scatter children away from parents; Sort_Path fixes
  that. Sort by Sort_Path ASC in the visual -- it's the only sort needed.
"""

from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fabric_medallion_toolkit.gold.keys import add_guid_key
from fabric_medallion_toolkit.gold.hierarchy import (
    build_hierarchy_levels, build_typed_hierarchy_levels,
)
from fabric_medallion_toolkit.gold.sort_path import build_sort_path
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("gold.issue_hierarchy")


def assert_unique(df: DataFrame, id_column: str, label_column: Optional[str] = None) -> None:
    """
    Raise if id_column is not unique -- every hierarchy walk assumes exactly
    one row per id, and a duplicate fans out multiplicatively (a runaway job,
    not a clean error). Cheap to check up front on the flat base DataFrame.
    label_column, if given, is included in the error's examples so the report
    names the offending business keys, not just internal ids.
    """
    group_cols = [id_column] + ([label_column] if label_column else [])
    dupes = df.groupBy(*group_cols).count().filter("count > 1")
    if dupes.limit(1).count() == 0:
        return
    example_col = label_column or id_column
    examples = [r[example_col] for r in dupes.select(example_col).limit(10).collect()]
    raise ValueError(
        f"{id_column} is not unique -- at least one id has more than one row "
        f"(examples: {examples}). Fix the duplicate at the source before building the "
        f"hierarchy; deduping here would hide whichever upstream bug produced it."
    )


def enrich_issue_hierarchy(
    df: DataFrame,
    *,
    id_column: str = "Issue_Id",
    parent_id_column: str = "Parent_Issue_Id",
    rank_column: str = "Rank",
    type_rank_column: str = "Hierarchy_Level",
    typed_code_column: str = "Display_Label",
    dense_code_column: str = "Display_Label",
    build_dense_levels: bool = False,
    rank_to_level: Optional[Dict[int, int]] = None,
    typed_level_names: Optional[Dict[str, str]] = None,
    root_prefix_lookup: Optional[DataFrame] = None,
    root_prefix_join_column: Optional[str] = None,
    max_depth: int = 7,
    max_chain_walk: int = 9,
    label_column: Optional[str] = None,
) -> DataFrame:
    """
    Add every hierarchy column an issue dimension needs, in one call:
      - Parent_Issue_Key : surrogate key of the parent (null for roots)
      - the typed ancestor columns named in typed_level_names
      - Level_1..Level_{max_depth} : dense, depth-placed, for the Gantt
      - Depth : count of populated dense levels
      - Sort_Path : single column that orders the whole tree ascending

    The returned DataFrame is cached and materialized once -- callers can pass
    it straight to merge() without the hierarchy walks re-running.

    Parameters
    ----------
    rank_to_level : maps each type-rank value to its 1-based Level_N slot,
        e.g. {5:1, 4:2, 3:3, 2:4, 1:5, 0:6, -1:7}. An unmapped rank in the
        data raises rather than silently dropping those issues.
    typed_level_names : renames the typed walk's Level_N outputs to business
        tiers, e.g. {"Level_1":"Programme", "Level_2":"Release", ...}. Any
        typed level NOT named here is dropped (it's the issue's own
        name, already present as a column) rather than kept as a raw Level_N.
    root_prefix_lookup / root_prefix_join_column : optional small DataFrame
        (e.g. dim_project with Project_Id + Project_Code) joined on
        root_prefix_join_column to prefix each ROOT's Sort_Path, so separate
        trees group together instead of interleaving by rank. The prefix
        column is the OTHER column in that DataFrame.
    label_column : optional business-key column (e.g. Issue_Code) used only
        to make the uniqueness-check error message readable.
    """
    # One row per id is assumed by every walk below -- check once, up front.
    assert_unique(df, id_column, label_column)

    # --- Parent surrogate key -------------------------------------------------
    # Hash is a pure function of its input, so hashing the parent id yields the
    # SAME key that hashing the issue id yields for that issue -- no self-join
    # needed. Roots (null parent) keep a null key rather than all hashing to
    # one meaningless shared GUID.
    df = add_guid_key(df, [parent_id_column], "_parent_key_raw")
    df = df.withColumn(
        "Parent_Issue_Key",
        F.when(F.col(parent_id_column).isNotNull(), F.col("_parent_key_raw")).otherwise(None),
    ).drop("_parent_key_raw")

    # --- Typed ancestor columns (slicers, ragged) -----------------------------
    # --- Typed ancestor columns (slicers, ragged) -----------------------------
    # Optional: only built when typed_level_names is provided. When the Gantt is
    # driven by the dense Level_N columns and tier filtering uses a single
    # Hierarchy_Level_Name column, these per-tier columns (Programme, Release,
    # ...) are redundant -- pass typed_level_names=None to skip the walk.
    if typed_level_names:
        if not rank_to_level:
            raise ValueError("enrich_issue_hierarchy: typed_level_names requires rank_to_level")
        typed = build_typed_hierarchy_levels(
            df, id_column=id_column, parent_id_column=parent_id_column,
            code_column=typed_code_column, type_rank_column=type_rank_column,
            rank_to_level=rank_to_level, max_chain_walk=max_chain_walk,
        )
        # Keep only the typed levels that map to a named tier; drop the rest
        # (they're the issue's own name, already a column on df).
        keep = set(typed_level_names)
        for col in typed.columns:
            if col.startswith("Level_") and col not in keep:
                typed = typed.drop(col)
        for old, new in typed_level_names.items():
            typed = typed.withColumnRenamed(old, new)
        df = df.join(typed, on=id_column, how="left")

    # --- Dense level columns (contiguous) + Depth -----------------------------
    # OFF by default. These were built for xViz Gantt nesting, but in practice
    # the TYPED columns (Programme/Release/.../Task) nest correctly in xViz and
    # the dense Level_N did not, so the typed columns drive the Gantt and these
    # are unnecessary. Kept behind a flag for any consumer that genuinely wants
    # depth-placed columns (a different visual, a "collapse to N tiers" control).
    if build_dense_levels:
        dense = build_hierarchy_levels(
            df, id_column=id_column, parent_id_column=parent_id_column,
            code_column=dense_code_column, max_depth=max_depth,
        )
        df = df.join(dense, on=id_column, how="left")
        level_cols = [F.col(f"Level_{n}").isNotNull().cast("int") for n in range(1, max_depth + 1)]
        df = df.withColumn("Depth", sum(level_cols[1:], level_cols[0]))

    # Materialize once before Sort_Path: Sort_Path reads df, and so does the
    # caller's merge(). Without this the two walks above re-run on each read.
    df = df.persist()
    df.count()

    # --- Sort_Path (single tree-ordering column) ------------------------------
    sort_input = df
    if root_prefix_lookup is not None and root_prefix_join_column is not None:
        sort_input = df.join(root_prefix_lookup, on=root_prefix_join_column, how="left")
        prefix_col = next(c for c in root_prefix_lookup.columns if c != root_prefix_join_column)
    else:
        prefix_col = None

    paths = build_sort_path(
        sort_input, id_column=id_column, parent_id_column=parent_id_column,
        rank_column=rank_column, root_prefix_column=prefix_col, max_depth=max_depth,
    )
    enriched = df.join(paths.select(id_column, "Sort_Path"), on=id_column, how="left")

    # Drop the working type-rank column; it was only needed for the typed walk.
    if type_rank_column in enriched.columns:
        enriched = enriched.drop(type_rank_column)

    enriched = enriched.persist()
    enriched.count()
    df.unpersist()

    logger.info("enrich_issue_hierarchy: built Parent_Issue_Key, typed tiers, dense levels, Depth, Sort_Path")
    return enriched
