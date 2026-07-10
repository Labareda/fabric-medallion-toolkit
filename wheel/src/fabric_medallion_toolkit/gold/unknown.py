"""
The "Unknown member" pattern: every dimension that facts will do a lookup
against can have one placeholder row (merge_fields = "Unknown", everything
else null) so that a fact whose natural key doesn't match anything in the
dimension resolves to that row's key instead of NULL. Keeps aggregations
in Power BI from silently dropping unmatched rows or showing a blank
grouping — they show up under "Unknown" instead, which is visible and
investigable.
"""

from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField


def add_unknown_member(df: DataFrame, merge_field_sentinels: Dict[str, Any]) -> DataFrame:
    """
    Appends one row to df where every merge field is set to its own
    sentinel value (merge_field_sentinels[col], already of that column's
    real type -- a string field gets a string like "Unknown", an int
    field gets e.g. -1, a date field gets e.g. date(1900, 1, 1)) and
    everything else is null — UNLESS a row with that exact combination
    already exists in df (idempotent: safe to call every run without
    creating duplicates).

    merge_field_sentinels must have an entry for every merge field --
    merge() is responsible for building this dict (including any
    same-type-coercion fallback) before calling this function.
    """
    spark = df.sparkSession
    merge_fields = list(merge_field_sentinels.keys())

    already_exists = df
    for c in merge_fields:
        already_exists = already_exists.filter(df[c] == merge_field_sentinels[c])
    if already_exists.limit(1).count() > 0:
        return df

    row_values = tuple(
        merge_field_sentinels[field.name] if field.name in merge_fields else None
        for field in df.schema.fields
    )
    # Force every field nullable=True for this one row's schema, regardless
    # of what df.schema itself says -- df.schema can have nullable=False on
    # a field purely because Spark's own inference never happened to see a
    # null in the REAL data sampled so far, which has nothing to do with
    # whether the Unknown row (which deliberately nulls out every
    # non-merge-field column) is actually allowed to. Without this, a
    # column that's simply never been empty in practice makes this whole
    # function fail with an opaque "[CANNOT_BE_NONE] Argument `obj` can not
    # be None" several layers down in PySpark's own type verification, with
    # no indication the real cause is a nullability mismatch at all.
    nullable_schema = StructType([
        StructField(field.name, field.dataType, nullable=True)
        for field in df.schema.fields
    ])
    unknown_df = spark.createDataFrame([row_values], schema=nullable_schema)
    return df.unionByName(unknown_df)
