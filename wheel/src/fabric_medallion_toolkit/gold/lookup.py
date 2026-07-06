"""
Resolves a fact table's natural key column (e.g. "assignee") into a
dimension's key, adding it to the fact under whatever alias YOU choose
(e.g. "Resource_key"). An explicit alias is required even though the
dimension already has its own name for that column, because a fact pulling
from two or more dimensions needs each one under a distinct name in its
own schema.

Handles both dim and scd2 targets:
  - Plain "dim"/"fact" table -> simple equi-join.
  - "scd2" table (detected by the presence of is_current/valid_from/valid_to)
      as_of_column=None (default): joins to the CURRENT version only.
      as_of_column="some_fact_date_col": point-in-time join -- resolves to
        whichever version was valid on that date, via
        valid_from <= date < COALESCE(valid_to, far future).

default_to_unknown: if the dimension has an "Unknown member" row (see
gold/unknown.py / TableSchema.include_unknown_member), set this True to
have any fact row that doesn't match anything in the dimension resolve to
that row's key instead of NULL.
"""

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_FAR_FUTURE = "9999-12-31 00:00:00"


def lookup_key(spark, fact_df: DataFrame, dim_table_name: str, dim_natural_key_column: str,
               dim_key_column: str, fact_join_column: str, output_column: str,
               as_of_column: Optional[str] = None, default_to_unknown: bool = False,
               unknown_value: str = "Unknown") -> DataFrame:
    """
    fact_df: the fact DataFrame being built (call this BEFORE merge() so the
        resolved key is what gets written).
    dim_table_name: e.g. "gold.dim_resource".
    dim_natural_key_column: the dimension's natural key, e.g. "resource_name".
    dim_key_column: the dimension's key column name, e.g. "Resource_key"
        (whatever TableSchema.key_column was set to for that dimension).
    fact_join_column: the fact's own column holding that natural key value,
        e.g. "assignee". Kept as-is in the output.
    output_column: what to call the resolved key once it's on the fact,
        e.g. "Resource_key" -- can be the same as dim_key_column if you're
        only pulling from one dimension, but give it a distinct name if the
        fact joins to more than one dimension.
    as_of_column: for scd2 dimensions only -- a date/timestamp column on
        fact_df to do a point-in-time join against. Leave None to always
        link to the dimension's current version instead.
    default_to_unknown: resolve unmatched fact rows to the dimension's
        Unknown-member row's key instead of NULL. Raises if the dimension
        doesn't have one (add it via TableSchema.include_unknown_member).
    """
    dim_df = spark.table(dim_table_name)
    is_scd2 = "is_current" in dim_df.columns and "valid_from" in dim_df.columns and "valid_to" in dim_df.columns

    if is_scd2 and as_of_column:
        dim_ready = dim_df.select(
            F.col(dim_natural_key_column).alias("_dim_nk"),
            F.col(dim_key_column).alias(output_column),
            F.col("valid_from").alias("_valid_from"),
            F.coalesce(F.col("valid_to"), F.lit(_FAR_FUTURE).cast("timestamp")).alias("_valid_to"),
        )
        joined = (
            fact_df.alias("f")
            .join(
                dim_ready.alias("d"),
                (F.col(f"f.{fact_join_column}") == F.col("d._dim_nk"))
                & (F.col(f"f.{as_of_column}") >= F.col("d._valid_from"))
                & (F.col(f"f.{as_of_column}") < F.col("d._valid_to")),
                "left",
            )
            .drop("_dim_nk", "_valid_from", "_valid_to")
        )
    else:
        dim_ready = dim_df
        if is_scd2:
            dim_ready = dim_ready.filter("is_current = true")
        dim_ready = dim_ready.select(
            F.col(dim_natural_key_column).alias("_dim_nk"),
            F.col(dim_key_column).alias(output_column),
        )
        joined = (
            fact_df.alias("f")
            .join(dim_ready.alias("d"), F.col(f"f.{fact_join_column}") == F.col("d._dim_nk"), "left")
            .drop("_dim_nk")
        )

    if default_to_unknown:
        unknown_row = (
            dim_df.filter(F.col(dim_natural_key_column) == unknown_value)
            .select(dim_key_column).limit(1).collect()
        )
        if not unknown_row:
            raise ValueError(
                f"default_to_unknown=True but {dim_table_name} has no row where "
                f"{dim_natural_key_column} = '{unknown_value}'. Build that dimension with "
                f"TableSchema(include_unknown_member=True) first."
            )
        unknown_key_value = unknown_row[0][dim_key_column]
        joined = joined.withColumn(output_column, F.coalesce(F.col(output_column), F.lit(unknown_key_value)))

    return joined
