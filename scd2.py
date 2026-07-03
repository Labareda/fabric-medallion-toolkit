"""
SCD Type 2 dimension builder: keeps every historical version of a record,
so a fact row always joins to the dimension attributes as they actually
were at that point in time — not retroactively overwritten by whatever the
attribute is today.

Adds these columns to every SCD2 table:
    {surrogate_key_column}   string      deterministic GUID -- see below
    valid_from                timestamp   when this version became current
    valid_to                  timestamp   when this version stopped being current (NULL = still current)
    is_current                boolean     shortcut for "valid_to IS NULL"

Key generation: deterministic GUID of (merge_fields..., tracked-column hash)
-- NOT just merge_fields alone, since merge_fields identify the ENTITY, and
every version of that entity needs a DIFFERENT key. Combining in the change
hash means a version's key is fully determined by "who this is" + "what
their attributes were", so:
  - the same version, recomputed later, always gets the same key back
  - a genuinely new version (different attributes) always gets a new key
  - no max-key lookup or row_number() bookkeeping needed anywhere

Two-step run each time (the standard SCD2 pattern, since a single MERGE
can't both close an old row AND insert a new one for the same entity):
    1. CLOSE OUT: any current row whose tracked columns changed gets
       valid_to = now, is_current = false.
    2. INSERT: a fresh row (new deterministic key) for every entity that's
       either brand new or had its tracked columns change.
"""

from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import functions as F

from fabric_medallion_toolkit.gold.keys import add_guid_key
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("gold.scd2")

_HASH_COL = "_scd_hash"


def _with_hash(df, tracked_columns: List[str]):
    return df.withColumn(_HASH_COL, F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_columns]), 256))


def build_scd2_dimension(spark, source_df, merge_fields: List[str], surrogate_key_column: str,
                          table_name: str, tracked_columns: Optional[List[str]] = None) -> dict:
    """
    source_df: current-state rows from Silver (one row per entity — same
    shape as what you'd feed a "dim" table).
    merge_fields: identifies the ENTITY (e.g. ["resource_name"]) -- used to
        match "is this the same person, a new version of them" across runs.
    tracked_columns: which non-merge-field columns count as "a change".
        None = every column in source_df except merge_fields.
    """
    if tracked_columns is None:
        tracked_columns = [c for c in source_df.columns if c not in merge_fields]

    now = datetime.now(timezone.utc)
    incoming = _with_hash(source_df, tracked_columns)

    if not spark.catalog.tableExists(table_name):
        first_load = (
            add_guid_key(incoming, merge_fields + [_HASH_COL], surrogate_key_column)
            .withColumn("valid_from", F.lit(now))
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )
        first_load.write.format("delta").mode("overwrite").saveAsTable(table_name)
        n = first_load.count()
        logger.info(f"{table_name}: first load, {n} initial versions")
        return {"new": n, "changed": 0, "unchanged": 0}

    current = spark.table(table_name).filter("is_current = true")

    join_cond = [incoming[c] == current[c] for c in merge_fields]
    joined = incoming.join(current.select(*merge_fields, _HASH_COL).withColumnRenamed(_HASH_COL, "_cur_hash"),
                            on=merge_fields, how="left")
    to_version = joined.filter((F.col("_cur_hash").isNull()) | (F.col(_HASH_COL) != F.col("_cur_hash"))).drop("_cur_hash")

    changed_rows = to_version.select(*merge_fields).collect()
    total_incoming = incoming.count()
    unchanged_count = total_incoming - len(changed_rows)

    if changed_rows:
        # Step 1: close out old current versions for entities that changed
        # (brand-new entities simply won't match anything here, which is correct).
        match_clauses = " OR ".join(
            "(" + " AND ".join(f"{c} = {repr(r[c])}" for c in merge_fields) + ")"
            for r in changed_rows
        )
        spark.sql(f"""
            UPDATE {table_name}
            SET valid_to = '{now.isoformat()}', is_current = false
            WHERE is_current = true AND ({match_clauses})
        """)

        # Step 2: insert fresh versions -- key is deterministic from
        # (merge_fields + new attribute hash), no lookup needed.
        new_versions = (
            add_guid_key(to_version, merge_fields + [_HASH_COL], surrogate_key_column)
            .withColumn("valid_from", F.lit(now))
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )
        new_versions.write.format("delta").mode("append").saveAsTable(table_name)

    logger.info(f"{table_name}: {len(changed_rows)} new version(s) written, {unchanged_count} unchanged")
    return {"new_or_changed": len(changed_rows), "unchanged": unchanged_count}
