"""
Generic upsert via a real, visible MERGE INTO statement — used by Bronze,
Silver, and Gold alike. Returns the SQL text it ran, so notebooks can
print/log the actual query for documentation or troubleshooting instead of
the merge being a black-box Python call.
"""

from typing import List, Optional

from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("utils.delta_merge")


def _widen_target_schema(spark, df, target_table: str) -> None:
    """
    MERGE INTO does NOT auto-add columns to an existing target -- if a
    notebook's SELECT gains a new column after the target table already
    exists on disk (from an earlier run, before that column existed), the
    generated UPDATE SET references target.<new_col> and Spark fails with
    DELTA_MERGE_UNRESOLVED_EXPRESSION rather than silently ignoring it or
    adding it. That failure mode is correct to have SOME answer to, but a
    hard stop on every table that's ever had a column added since its
    first run is not the answer.

    Explicitly ALTER TABLE ADD COLUMNS for whatever's missing, rather than
    setting the session-wide spark.databricks.delta.schema.autoMerge.enabled
    flag -- that flag would apply, silently, to every other MERGE running
    in the same session (including ones where an unexpected new column
    showing up SHOULD be investigated, not auto-added). Widening only the
    one target this call actually cares about, only with the columns this
    call's own source df actually has, keeps the effect local and logged.

    Does not handle a column being renamed/retyped/removed -- those need a
    real decision (rename in the target too? backfill? drop?), not
    something to paper over automatically. Only ADDs.
    """
    existing_cols = {f.name for f in spark.table(target_table).schema.fields}
    new_fields = [f for f in df.schema.fields if f.name not in existing_cols]
    if not new_fields:
        return
    add_clause = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in new_fields)
    spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({add_clause})")
    logger.info(f"{target_table}: widened schema, added column(s) {[f.name for f in new_fields]}")


def build_merge_sql(target_table: str, source_view: str, key_cols: List[str],
                     all_cols: List[str]) -> str:
    """
    Builds a standard MERGE INTO ... WHEN MATCHED UPDATE ... WHEN NOT MATCHED
    INSERT ... statement. `all_cols` should be every column in the source
    (key columns included) — update/insert both touch every non-key column.
    """
    non_key_cols = [c for c in all_cols if c not in key_cols]

    on_clause = " AND ".join(f"target.{k} = source.{k}" for k in key_cols)
    update_clause = ", ".join(f"target.{c} = source.{c}" for c in non_key_cols) or None
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"source.{c}" for c in all_cols)

    update_sql = f"WHEN MATCHED THEN UPDATE SET {update_clause}\n" if update_clause else ""

    return (
        f"MERGE INTO {target_table} AS target\n"
        f"USING {source_view} AS source\n"
        f"ON {on_clause}\n"
        f"{update_sql}"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def upsert_delta(spark, df, target_table: str, key_cols: List[str],
                  write_mode: str = "merge") -> Optional[str]:
    """
    Upserts `df` into `target_table` (schema.table, e.g. "bronze.jira_issues").
    Creates the table on first write. Returns the MERGE SQL that was run
    (None on a first-time create, or whenever write_mode="overwrite" -- an
    overwrite has no MERGE statement to return).

    write_mode="merge" (default): incremental upsert. Creates the table on
    first write; MERGE INTO on every write after that.
    write_mode="overwrite": full REPLACE every call, whether or not the
    table already exists -- for a table that's completely rebuilt from
    source each run, where there's nothing to preserve and MERGE's
    row-by-row match-vs-insert comparison is pure overhead.
    "overwriteSchema" is set so a column added/removed/retyped upstream
    doesn't fail the write -- appropriate ONLY because overwrite mode
    always replaces the whole table anyway, so there's no partial-schema
    state to protect against.

    Every table created here gets 'delta.autoOptimize.optimizeWrite' and
    'delta.autoOptimize.autoCompact' set as TABLE PROPERTIES at creation --
    not a session config, which only protects whichever notebook happens to
    set it and stops the moment a different session writes without it. As
    table properties they're permanent and apply no matter which notebook or
    session merges into the table later. This is why: a MERGE-heavy table
    (rebuilt on every pipeline run, as every Gold dim/fact here is) fragments
    into hundreds of small files over repeated runs -- one observed table hit
    201 files for ~12MB of data, and MERGE got slower on every subsequent run
    because of it, not faster once "warm". optimizeWrite coalesces small
    output files before they ever land; autoCompact cleans up what still
    slips through, right after each write. Together they keep a table fast
    indefinitely with no periodic maintenance step required anywhere else in
    the pipeline.
    """
    if write_mode not in ("merge", "overwrite"):
        raise ValueError(f"{target_table}: write_mode must be 'merge' or 'overwrite', got '{write_mode}'")

    if write_mode == "overwrite" or not spark.catalog.tableExists(target_table):
        writer = (
            df.write.format("delta").mode("overwrite")
            .option("delta.autoOptimize.optimizeWrite", "true")
            .option("delta.autoOptimize.autoCompact", "true")
        )
        if write_mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(target_table)
        return None

    _widen_target_schema(spark, df, target_table)

    source_view = f"_src_{target_table.replace('.', '_')}"
    df.createOrReplaceTempView(source_view)

    merge_sql = build_merge_sql(target_table, source_view, key_cols, df.columns)
    spark.sql(merge_sql)
    return merge_sql
