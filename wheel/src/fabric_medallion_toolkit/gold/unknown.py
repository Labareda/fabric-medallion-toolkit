"""
The "Unknown member" pattern: every dimension that facts will do a lookup
against can have one placeholder row (merge_fields = "Unknown", everything
else null) so that a fact whose natural key doesn't match anything in the
dimension resolves to that row's key instead of NULL. Keeps aggregations
in Power BI from silently dropping unmatched rows or showing a blank
grouping — they show up under "Unknown" instead, which is visible and
investigable.
"""

from typing import List

from pyspark.sql import DataFrame


def add_unknown_member(df: DataFrame, merge_fields: List[str], unknown_value: str = "Unknown") -> DataFrame:
    """
    Appends one row to df where every column in merge_fields is set to
    unknown_value and everything else is null — UNLESS a row with that
    natural key combination already exists in df (idempotent: safe to call
    every run without creating duplicates).

    Assumes merge_fields are string-typed (the common case — a natural key
    is usually a code/name). If one of them isn't a string, cast unknown_value
    to that column's type yourself before calling, or open an issue in your
    own fork if this needs to support mixed types more generally.
    """
    spark = df.sparkSession
    already_exists = df
    for c in merge_fields:
        already_exists = already_exists.filter(df[c] == unknown_value)
    if already_exists.limit(1).count() > 0:
        return df

    row_values = tuple(
        unknown_value if field.name in merge_fields else None
        for field in df.schema.fields
    )
    unknown_df = spark.createDataFrame([row_values], schema=df.schema)
    return df.unionByName(unknown_df)
