"""
The alternative to column_mappings: automatically expand EVERY scalar
field in raw_data into its own Silver column, typed correctly, with no
manual mapping list to maintain. Naming is deterministic (dot-path with
underscores) rather than curated -- rename in Gold if you want something
friendlier, per the design choice this exists for.

Correctness of types comes from Spark's own JSON schema inference
(spark.read.json scans actual values across the real data and infers a
proper nested schema), not per-row guessing -- so a field that's
consistently a number infers as a number, consistently a string infers as
a string, etc.

Arrays/maps are kept as their full content, but as a JSON STRING column,
not a native Spark array/struct-typed column. This is a deliberate change
from an earlier version of this function, which kept them as native
complex types -- that works fine in Spark/Delta itself, but Fabric's SQL
Analytics Endpoint (the auto-generated T-SQL view over every Lakehouse
table) cannot represent array or map columns AT ALL, and silently fails to
sync any table containing one ("Columns of the specified data types are
not supported for..."). Storing them as a JSON string keeps the endpoint
working for every table, at the cost of needing get_json_object/from_json
to query into that field's contents when you actually need to -- if you
want one specifically broken into its own child table (e.g. issuelinks),
explode_nested_array is still the right tool for that; this function and
that one solve different problems and are meant to be used together, not
as alternatives.
"""

from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, MapType

from fabric_medallion_toolkit.silver.standardize import dedup_latest
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("silver.auto_standardize")


def _flatten_struct_columns(schema: StructType, root_column: str, prefix: str = "", depth: int = 0,
                             max_depth: int = 5) -> List[str]:
    """
    Returns a list of complete Spark SQL select expressions for every leaf
    field in a (possibly nested) struct schema living under root_column
    (e.g. "_parsed"). Stops recursing into a field once it hits max_depth,
    or immediately for array/map fields -- those are wrapped in to_json()
    rather than flattened further or kept as a native complex type (see
    module docstring for why).
    """
    exprs = []
    for field in schema.fields:
        full_path = f"{prefix}.{field.name}" if prefix else field.name
        alias = full_path.replace(".", "_")
        quoted_path = f"`{root_column}`." + ".".join(f"`{p}`" for p in full_path.split("."))
        if isinstance(field.dataType, StructType) and depth < max_depth:
            exprs.extend(_flatten_struct_columns(field.dataType, root_column, full_path, depth + 1, max_depth))
        elif isinstance(field.dataType, (ArrayType, MapType, StructType)):
            # Array/map, or a struct we've stopped recursing into (too
            # deep) -- to_json handles all three cases correctly.
            exprs.append(f"to_json({quoted_path}) AS `{alias}`")
        else:
            exprs.append(f"{quoted_path} AS `{alias}`")
    return exprs


def auto_standardize(bronze_df: DataFrame, flatten_depth: int = 5) -> DataFrame:
    """
    bronze_df: raw Bronze read (has raw_data, extracted_at, etc.).
    flatten_depth: how many levels of nested objects to flatten into
        dot-path column names before giving up and keeping the rest as a
        nested struct column. 5 is generous for most REST APIs; lower it
        if you want to deliberately leave deeper structure nested.

    Returns every scalar field discovered in raw_data as its own typed
    column, plus extracted_at (kept temporarily for dedup -- dropped
    before the final Silver write, same as flatten_and_standardize).
    """
    parsed_schema = bronze_df.sparkSession.read.json(
        bronze_df.rdd.map(lambda r: r["raw_data"])
    ).schema

    df = bronze_df.withColumn("_parsed", F.from_json(F.col("raw_data"), parsed_schema))
    select_exprs = _flatten_struct_columns(parsed_schema, root_column="_parsed", max_depth=flatten_depth)
    return df.selectExpr(*select_exprs, "extracted_at")


def run_auto_silver_standardize(spark, entity_name: str, natural_key_columns: List[str],
                                 bronze_schema: str = "bronze", silver_schema: str = "silver",
                                 dedup_order_column: str = "extracted_at",
                                 flatten_depth: int = 5) -> None:
    """
    Full auto-expand Silver step for one entity -- the no-column_mappings
    alternative to run_silver_standardize. Same overwrite/dedup semantics,
    just skips maintaining an explicit mapping list: every field in
    raw_data becomes a column, named by its flattened path, typed by
    Spark's own JSON inference.

    natural_key_columns must reference the AUTO-GENERATED flattened names
    (e.g. "key" for a top-level "key" field, "fields_project_key" for a
    nested "fields.project.key" -- run this once and check the resulting
    Silver table's columns if you're not sure what a given field flattened
    to).
    """
    bronze_table = f"{bronze_schema}.{entity_name}"
    silver_table = f"{silver_schema}.{entity_name}"

    bronze_df = spark.table(bronze_table)
    standardized = auto_standardize(bronze_df, flatten_depth=flatten_depth)
    deduped = dedup_latest(standardized, key_cols=natural_key_columns, order_by_col=dedup_order_column)

    if dedup_order_column == "extracted_at" and "extracted_at" in deduped.columns:
        deduped = deduped.drop("extracted_at")

    logger.info(f"Auto Silver standardize (overwrite): {bronze_table} -> {silver_table}")
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
