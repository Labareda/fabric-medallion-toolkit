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
from pyspark.sql.types import StructType, ArrayType, MapType, StringType

from fabric_medallion_toolkit.silver.standardize import dedup_latest
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("silver.auto_standardize")


import re

def _sanitize_alias(segments: List[str]) -> str:
    """
    Builds a clean column name from path segments -- replaces ANY
    character that isn't a letter, digit, or underscore with an
    underscore (not just dots), since some real Jira field names contain
    colons/hyphens too (e.g. app-configuration keys like
    "com.atlassian.jira.plugin.system.customfieldtypes:atlassian-team").
    Collapses repeated underscores that this can produce.
    """
    raw = "_".join(segments)
    cleaned = re.sub(r"[^0-9a-zA-Z_]", "_", raw)
    return re.sub(r"_+", "_", cleaned).strip("_")


def _flatten_struct_columns(schema: StructType, root_column: str, path_segments: Optional[List[str]] = None,
                             depth: int = 0, max_depth: int = 5) -> List[str]:
    """
    Returns a list of complete Spark SQL select expressions for every leaf
    field in a (possibly nested) struct schema living under root_column
    (e.g. "_parsed"). Stops recursing into a field once it hits max_depth,
    or immediately for array/map fields -- those are wrapped in to_json()
    rather than flattened further or kept as a native complex type (see
    module docstring for why) -- EXCEPT an array whose elements are plain
    strings (e.g. Jira's "labels", "clauseNames", "projectKeys"), which
    gets a plain comma-joined string instead ("A, B, C" rather than
    '["A","B","C"]') since there's nothing lossy about that for a flat
    list of strings, and it's much more directly usable/readable than
    JSON-array bracket-and-quote syntax.

    path_segments tracks each nesting level as its own LIST ELEMENT, not a
    dot-joined string -- some real Jira field names contain a literal dot
    themselves (e.g. an app-configuration key like
    "com.atlassian.jira.plugin.system.customfieldtypes:atlassian-team" on
    the Team custom field type), and joining-then-splitting on "." would
    wrongly treat that single field's own name as several fake nesting
    levels, producing a [FIELD_NOT_FOUND] error trying to access a level
    that doesn't exist. Keeping segments as a list the whole way through
    means each one gets backtick-quoted as a single, whole identifier,
    dots and all.
    """
    if path_segments is None:
        path_segments = []
    exprs = []
    for field in schema.fields:
        full_segments = path_segments + [field.name]
        alias = _sanitize_alias(full_segments)
        quoted_path = f"`{root_column}`." + ".".join(f"`{seg}`" for seg in full_segments)
        if isinstance(field.dataType, StructType) and depth < max_depth:
            exprs.extend(_flatten_struct_columns(field.dataType, root_column, full_segments, depth + 1, max_depth))
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StringType):
            exprs.append(f"array_join({quoted_path}, ', ') AS `{alias}`")
        elif isinstance(field.dataType, (ArrayType, MapType, StructType)):
            # Array of anything OTHER than plain strings (objects, numbers,
            # etc.), map, or a struct we've stopped recursing into (too
            # deep) -- to_json handles all these cases correctly; there's
            # no clean flat representation for a variable-shape list of
            # objects the way there is for a list of strings.
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


def _drop_all_null_columns(df: DataFrame, protect: Optional[List[str]] = None) -> DataFrame:
    """
    Drops any column where every row is NULL across the whole table --
    e.g. a custom field nobody's ever populated on any issue. protect is a
    list of columns to keep regardless (natural keys, dedup ordering
    column) even if they happen to be all-null in a small/test dataset.
    """
    protect = set(protect or [])
    non_null_counts = df.select([
        F.count(F.col(f"`{c}`")).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    all_null_cols = [c for c, cnt in non_null_counts.items() if cnt == 0 and c not in protect]
    if all_null_cols:
        logger.info(f"Dropping {len(all_null_cols)} all-null column(s): {all_null_cols}")
        df = df.drop(*all_null_cols)
    return df


def run_auto_silver_standardize(spark, entity_name: str, natural_key_columns: List[str],
                                 bronze_schema: str = "bronze", silver_schema: str = "silver",
                                 dedup_order_column: str = "extracted_at",
                                 flatten_depth: int = 5, drop_empty_columns: bool = True) -> None:
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

    drop_empty_columns: if True (default), drops any column that's NULL
    on every single row -- e.g. a custom field nobody's ever populated.
    Natural keys and the dedup ordering column are never dropped even if
    all-null. Since Silver is fully recomputed every run, a field that
    later gets a real value will simply reappear on a subsequent run --
    nothing is lost by dropping it now, just not carried as dead weight
    while it's genuinely unused.
    """
    bronze_table = f"{bronze_schema}.{entity_name}"
    silver_table = f"{silver_schema}.{entity_name}"

    bronze_df = spark.table(bronze_table)
    standardized = auto_standardize(bronze_df, flatten_depth=flatten_depth)
    deduped = dedup_latest(standardized, key_cols=natural_key_columns, order_by_col=dedup_order_column)

    if dedup_order_column == "extracted_at" and "extracted_at" in deduped.columns:
        deduped = deduped.drop("extracted_at")

    if drop_empty_columns:
        deduped = _drop_all_null_columns(deduped, protect=natural_key_columns)

    logger.info(f"Auto Silver standardize (overwrite): {bronze_table} -> {silver_table}")
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
