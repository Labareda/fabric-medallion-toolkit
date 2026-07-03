"""
Tiny control table tracking the last-processed watermark per (source, entity)
so incremental entities only pull new/changed records each run.

This ONE table is a deliberate exception to "Bronze never merges" — it's a
few rows of pipeline metadata, not raw source data, and it needs upsert
semantics (one current value per entity) rather than history.

Table: {bronze_schema}.watermarks
    entity_name, last_watermark (string), updated_at (timestamp)
    (bronze_schema = source_name, so this table already lives in that
    source's own schema/folder — no need to repeat source_name in it)
"""

from datetime import datetime, timezone
from typing import Optional, Iterable, Dict, Any

from fabric_medallion_toolkit.config import EntityConfig
from fabric_medallion_toolkit.utils.json_path import get_by_path
from fabric_medallion_toolkit.utils.delta_merge import upsert_delta

CONTROL_TABLE_SUFFIX = "watermarks"  # lands as {bronze_schema}.watermarks — one row per entity in that source's schema


def get_watermark(spark, source_name: str, entity: EntityConfig, bronze_schema: str = "bronze") -> str:
    """Returns the last-recorded watermark, or the entity's initial_watermark_value if none yet."""
    table = f"{bronze_schema}.{CONTROL_TABLE_SUFFIX}"
    if not spark.catalog.tableExists(table):
        return entity.initial_watermark_value

    row = (
        spark.table(table)
        .filter(spark.table(table)["entity_name"] == entity.entity_name)
        .collect()
    )
    return row[0]["last_watermark"] if row else entity.initial_watermark_value


def save_watermark(spark, source_name: str, entity: EntityConfig, new_watermark: str,
                    bronze_schema: str = "bronze") -> None:
    table = f"{bronze_schema}.{CONTROL_TABLE_SUFFIX}"
    df = spark.createDataFrame([{
        "entity_name": entity.entity_name,
        "last_watermark": new_watermark,
        "updated_at": datetime.now(timezone.utc),
    }])
    upsert_delta(spark, df, table, key_cols=["entity_name"])


def compute_new_watermark(records: Iterable[Dict[str, Any]], entity: EntityConfig,
                           extraction_started_at: datetime) -> str:
    """
    New watermark = max value of entity.incremental_column seen across the
    extracted batch. Falls back to extraction_started_at (as ISO string) if
    the entity has no incremental_column configured, or the batch was empty
    — a small safety margin, since it's better to slightly overlap the next
    run than to silently skip records.
    """
    if not entity.incremental_column:
        return extraction_started_at.isoformat()

    max_val = None
    for rec in records:
        val = get_by_path(rec, entity.incremental_column, default=None)
        if val is not None and (max_val is None or str(val) > str(max_val)):
            max_val = val

    return str(max_val) if max_val is not None else extraction_started_at.isoformat()
