"""
Generic Bronze landing: every source/entity lands in the same shape, so
nothing here is source-specific.

Bronze table: {bronze_schema}.{source_name}_{entity_name}
    primary_key    string     natural key value pulled via EntityConfig.natural_key_field
                                (kept as string regardless of source type; Silver casts it properly)
    raw_data        string     the full record, as JSON — nothing is dropped or flattened here
    source_system  string
    entity          string
    load_id        string     one value per pipeline/notebook run, for traceability
    extracted_at   timestamp
    ingest_date    date       partition column

APPEND-only, full history: Bronze is the source of truth you can always
replay from, so nothing here is ever updated or deleted (a record extracted
5 times over 5 days lands as 5 rows). "What was ingested on a given day" is
just `WHERE ingest_date = '...'`; "what a specific pipeline run pulled in"
is `WHERE load_id = '...'`.

Delta (not raw Parquet-in-Files) even in append mode, because: it's
immediately queryable via the SQL endpoint, Fabric auto-compacts small
files from frequent runs, schema drift is caught rather than silently
accepted, and you get time-travel queries for free — a folder of Parquet
files gives you the append-only history too, just with none of the above.

Bronze will grow unbounded over time — for very high-volume/high-frequency
entities, consider a retention policy later (e.g. periodically archiving
ingest_date partitions older than N days to cheaper storage, or Delta
VACUUM once you're confident you won't need to replay that far back). Not
needed to get started.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Iterable, Dict, Any, Optional

from fabric_medallion_toolkit.config import EntityConfig
from fabric_medallion_toolkit.utils.json_path import get_by_path
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("bronze.landing")


def land_records(spark, records: Iterable[Dict[str, Any]], source_name: str,
                  entity: EntityConfig, bronze_schema: str = "bronze",
                  load_id: Optional[str] = None) -> int:
    """
    Appends `records` to {bronze_schema}.{source_name}_{entity_name}.
    Returns the number of records written (0 if `records` was empty —
    nothing is written in that case, including no watermark update, which
    the calling notebook should honor).
    """
    load_id = load_id or str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    table_name = f"{bronze_schema}.{entity.entity_name}"

    rows = []
    for rec in records:
        natural_key = get_by_path(rec, entity.natural_key_field, default=None)
        rows.append({
            "primary_key": str(natural_key) if natural_key is not None else None,
            "raw_data": json.dumps(rec, default=str),
            "source_system": source_name,
            "entity": entity.entity_name,
            "load_id": load_id,
            "extracted_at": now,
            "ingest_date": now.date(),
        })

    if not rows:
        logger.info(f"No records extracted for {table_name}, nothing to land")
        return 0

    df = spark.createDataFrame(rows)

    # spark.createDataFrame() on an in-memory Python list defaults to a
    # small, fixed number of partitions regardless of how much data is in
    # it. Fine for a small reference entity (priorities, statuses), but for
    # a large entity with rich per-record content (issues, with many
    # custom fields and embedded comments/history), that means too few,
    # too-large partitions -- and Delta's Optimized Write then fails
    # trying to shuffle-rebalance one of those into well-sized files,
    # since the shuffle task itself exceeds Spark's internal RPC message
    # size limit. Repartitioning based on actual row count keeps each
    # partition a sane size regardless of how large this particular
    # entity's records are or how many there are.
    target_rows_per_partition = 2000
    num_partitions = max(1, -(-len(rows) // target_rows_per_partition))  # ceiling division
    df = df.repartition(num_partitions)

    logger.info(f"Appending {len(rows)} records ({num_partitions} partitions) -> {table_name}")
    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .option("mergeSchema", "true")   # tolerate the source adding fields over time
        .saveAsTable(table_name)
    )
    return len(rows)
