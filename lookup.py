"""
Resolves a fact table's natural key column (e.g. "assignee") into a
dimension's GUID key, adding it to the fact under the SAME column name it
has in the dimension (e.g. joining fact_df.assignee against dim_resource's
resource_name/resource_key adds a "resource_key" column — not "assignee_key"
or anything fact-specific). That's what makes the resulting column an
obvious, unambiguous FK when you look at the fact table's schema.

Handles both dim and scd2 targets:
  - Plain "dim" table -> simple equi-join.
  - "scd2" table (detected by the presence of is_current/valid_from/valid_to)
      as_of_column=None (default): joins to the CURRENT version only.
      as_of_column="some_fact_date_col": point-in-time join -- resolves to
        whichever version was valid on that date, via
        valid_from <= date < COALESCE(valid_to, far future).
"""

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_FAR_FUTURE = "9999-12-31 00:00:00"


def lookup_dimension_key(spark, fact_df: DataFrame, dim_table_name: str,
                          dim_natural_key_column: str, dim_key_column: str,
                          fact_join_column: str, as_of_column: Optional[str] = None) -> DataFrame:
    """
    fact_df: the fact DataFrame being built (before or after merge_fact --
        call this BEFORE merge_fact so the resolved key is what gets written).
    dim_table_name: e.g. "gold.dim_resource".
    dim_natural_key_column: the dimension's natural key, e.g. "resource_name".
    dim_key_column: the dimension's GUID key column name, e.g. "resource_key"
        -- this exact name is what gets added to fact_df.
    fact_join_column: the fact's own column holding that natural key value,
        e.g. "assignee". Kept as-is in the output; only dim_key_column is added.
    as_of_column: for scd2 dimensions only -- a date/timestamp column on
        fact_df to do a point-in-time join against. Leave None to always
        link to the dimension's current version instead.
    """
    dim_df = spark.table(dim_table_name)
    is_scd2 = "is_current" in dim_df.columns and "valid_from" in dim_df.columns and "valid_to" in dim_df.columns

    if is_scd2 and as_of_column:
        dim_ready = dim_df.select(
            F.col(dim_natural_key_column).alias("_dim_nk"),
            F.col(dim_key_column).alias(dim_key_column),
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
        return joined

    dim_ready = dim_df
    if is_scd2:
        dim_ready = dim_ready.filter("is_current = true")
    dim_ready = dim_ready.select(
        F.col(dim_natural_key_column).alias("_dim_nk"),
        F.col(dim_key_column).alias(dim_key_column),
    )
    joined = (
        fact_df.alias("f")
        .join(dim_ready.alias("d"), F.col(f"f.{fact_join_column}") == F.col("d._dim_nk"), "left")
        .drop("_dim_nk")
    )
    return joined
