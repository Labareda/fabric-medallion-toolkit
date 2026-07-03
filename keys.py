"""
Deterministic surrogate keys: a real GUID (RFC 4122 version 5 — content-
derived, not random), computed from a row's merge fields, so the SAME
merge field values ALWAYS produce the SAME key — this run, next run, in
every environment. That means:

  - No key registry or "what's the next available key" lookup needed.
  - Dimension builds are pure functions of their select_sql — MERGE handles
    matching on merge_fields; the key column just comes along for the ride.
  - SCD2 versions get a new key automatically the moment their tracked
    columns change (since the hash input changed) — no max-key bookkeeping.

Uses uuid.uuid5 with a fixed namespace constant, wrapped as a Spark UDF.
"""

import uuid
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Fixed, arbitrary namespace UUID for this toolkit -- what matters is that
# it never changes, so the same inputs always hash to the same GUID across
# every run and every table. Do not regenerate this value.
_NAMESPACE = uuid.UUID("6f9163ee-6a1e-4b8a-9e2b-2f6a6f6a6f6a")


def _deterministic_guid(*parts) -> str:
    name = "||".join("" if p is None else str(p) for p in parts)
    return str(uuid.uuid5(_NAMESPACE, name))


_guid_udf = F.udf(_deterministic_guid, StringType())


def add_guid_key(df: DataFrame, key_parts_columns: List[str], key_column_name: str = "row_key") -> DataFrame:
    """
    Adds `key_column_name` to df: a deterministic GUID derived from the
    values of `key_parts_columns` in each row. Column order matters (it's
    part of the hash input) — pass them in the same order every time you
    call this for a given table.
    """
    return df.withColumn(key_column_name, _guid_udf(*[F.col(c) for c in key_parts_columns]))
