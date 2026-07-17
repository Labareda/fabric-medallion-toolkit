"""
Generic upsert via a real, visible MERGE INTO statement — used by Bronze,
Silver, and Gold alike. Returns the SQL text it ran, so notebooks can
print/log the actual query for documentation or troubleshooting instead of
the merge being a black-box Python call.
"""

from typing import List, Optional


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


def _add_new_columns(spark, target_table: str, df) -> None:
    """
    MERGE INTO does NOT auto-add a column that's new in the source but absent
    from the existing target table -- it fails with
    DELTA_MERGE_UNRESOLVED_EXPRESSION ("Cannot resolve target.X in UPDATE
    clause") instead. This happens every time a Gold notebook adds a column
    to its schema (exactly what happened adding Hierarchy_Level_Name to
    Dim_Issue) -- the notebook's SCHEMA changed, but the physical Delta table
    sitting in Gold from the last run hasn't, and a plain MERGE can't bridge
    that gap on its own.

    Explicit ALTER TABLE ADD COLUMNS, run once before the merge, closes that
    gap: any column present in the incoming DataFrame but absent from the
    existing table gets added (as NULL for all existing rows), and the merge
    below then succeeds. This is deliberately an explicit, visible ALTER
    TABLE rather than a hidden autoMerge session flag -- consistent with this
    module returning real SQL for visibility rather than working invisibly.

    Columns that a schema DROPS are not removed here (or ever, by this
    function) -- they simply stop being written to by future merges and keep
    whatever value they last had. That matches how merge() already treats
    "never deletes" as a deliberate, documented trade-off elsewhere.
    """
    existing_cols = set(spark.table(target_table).columns)
    new_cols = [c for c in df.columns if c not in existing_cols]
    if not new_cols:
        return

    add_clause = ", ".join(f"`{c}` {df.schema[c].dataType.simpleString()}" for c in new_cols)
    spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({add_clause})")


def upsert_delta(spark, df, target_table: str, key_cols: List[str],
                 write_mode: str = "merge") -> Optional[str]:
    """
    Writes `df` into `target_table`.

    write_mode:
      - "merge" (default): MERGE INTO -- updates matched rows, inserts new
        ones. Correct for INCREMENTAL loads where only some rows change and
        existing rows must be preserved. Automatically ALTERs the target
        table to add any column that's new in the source (see
        _add_new_columns) before merging, so a schema change in the notebook
        doesn't require manually dropping and rebuilding the Gold table.
      - "overwrite": replaces the whole table contents. Correct for tables
        REBUILT IN FULL every run (every row recomputed from source), where
        MERGE's per-row match-vs-insert comparison is pure overhead -- there
        is nothing to preserve, so comparing against the old table to decide
        update-vs-insert just doubles the work. For a full-rebuild dimension
        this is dramatically cheaper than MERGE.

    Creates the table on first write regardless of mode. Returns the MERGE
    SQL that was run, or None (first-time create, or an overwrite -- neither
    runs a MERGE statement).
    """
    if write_mode not in ("merge", "overwrite"):
        raise ValueError(f"upsert_delta: write_mode must be 'merge' or 'overwrite', got '{write_mode}'")

    if write_mode == "overwrite":
        # overwriteSchema=true so a column added/removed in the source (e.g. a
        # new hierarchy level) is reflected, rather than failing on schema
        # mismatch the way a plain overwrite would.
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true").saveAsTable(target_table))
        return None

    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return None

    _add_new_columns(spark, target_table, df)

    source_view = f"_src_{target_table.replace('.', '_')}"
    df.createOrReplaceTempView(source_view)

    merge_sql = build_merge_sql(target_table, source_view, key_cols, df.columns)
    spark.sql(merge_sql)
    return merge_sql
