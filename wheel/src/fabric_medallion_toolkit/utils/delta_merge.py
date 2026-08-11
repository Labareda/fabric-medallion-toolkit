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


def upsert_delta(spark, df, target_table: str, key_cols: List[str]) -> Optional[str]:
    """
    Upserts `df` into `target_table` (schema.table, e.g. "bronze.jira_issues").
    Creates the table on first write. Returns the MERGE SQL that was run
    (None on a first-time create, since there's nothing to merge yet).

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
    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").mode("overwrite") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .saveAsTable(target_table)
        return None

    source_view = f"_src_{target_table.replace('.', '_')}"
    df.createOrReplaceTempView(source_view)

    merge_sql = build_merge_sql(target_table, source_view, key_cols, df.columns)
    spark.sql(merge_sql)
    return merge_sql
