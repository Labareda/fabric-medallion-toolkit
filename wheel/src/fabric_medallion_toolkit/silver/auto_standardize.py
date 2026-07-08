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

Arrays/maps are kept as their full content, but as a JSON STRING column
(or a comma-joined plain string, for a simple array of strings, or an
array of Jira's "select option" objects, or an array of user objects --
see below), not a native Spark array/struct-typed column. This is a
deliberate change from an earlier version of this function, which kept
them as native complex types -- that works fine in Spark/Delta itself,
but Fabric's SQL Analytics Endpoint (the auto-generated T-SQL view over
every Lakehouse table) cannot represent array or map columns AT ALL, and
silently fails to sync any table containing one ("Columns of the
specified data types are not supported for..."). Storing them as a JSON
string keeps the endpoint working for every table, at the cost of
needing get_json_object/from_json to query into that field's contents
when you actually need to -- if you want one specifically broken into
its own child table (e.g. issuelinks), explode_nested_array is still the
right tool for that; this function and that one solve different problems
and are meant to be used together, not as alternatives.

Field access is built using real DataFrame Column objects
(.getField(name)) rather than generating SQL expression text and handing
it to selectExpr. This isn't just a style choice: a field name is passed
as a plain Python string argument to .getField(), never parsed as SQL
syntax at all -- so a Jira field name containing dots, colons, hyphens,
or spaces (all of which occur in real custom field configuration keys)
simply cannot break this the way it can break string-built SQL, which
needed manual backtick-quoting and a sanitizing regex to handle safely.
That whole class of bug is structurally eliminated, not just handled for
the specific cases seen so far.
"""

import re
from typing import List, Optional, Callable

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, MapType, StringType

from fabric_medallion_toolkit.silver.standardize import dedup_latest
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("silver.auto_standardize")


def _struct_field_names(struct_type: StructType) -> List[str]:
    return [f.name for f in struct_type.fields]


def _flatten_struct_columns(schema: StructType, root_col: Column, path_segments: Optional[List[str]] = None,
                             depth: int = 0, max_depth: int = 5) -> List[Column]:
    """
    Returns a list of real DataFrame Columns (each already .alias()'d) for
    every leaf field in a (possibly nested) struct schema reachable from
    root_col (e.g. F.col("_parsed")). Stops recursing into a field once it
    hits max_depth, or immediately for array/map fields -- those are
    wrapped in to_json() rather than flattened further or kept as a
    native complex type (see module docstring for why), EXCEPT three
    common Jira shapes that get a clean comma-joined string instead of a
    raw JSON blob, since there's nothing lossy about flattening these
    specific, very regular shapes:

    - A plain array of strings (e.g. "labels", "clauseNames",
      "projectKeys") -> "A, B, C".
    - An array of Jira's "select option" objects, i.e. each element has an
      "id"/"self"/"value" shape (the near-universal shape for multi-select
      custom fields like Reporting, Workstream, Environment, etc.) -> just
      the "value"s, comma-joined, dropping the "id"/"self" noise.
    - An array of user objects (each element has "accountId"/"displayName",
      e.g. "people_involved") -> the "displayName"s, comma-joined.

    Anything else (arrays of genuinely varied/complex objects, e.g.
    issuelinks) still gets the full to_json() treatment -- there's no
    clean flat representation for those the way there is for these three
    very regular shapes.

    path_segments tracks each nesting level purely for building the OUTPUT
    column name (joined with underscores) -- it never round-trips back
    into a path Spark has to re-parse, so a field name containing a
    literal dot, colon, or hyphen is just an ordinary string here, not a
    hazard.
    """
    if path_segments is None:
        path_segments = []
    columns = []
    for field in schema.fields:
        full_segments = path_segments + [field.name]
        alias = _sanitize_alias(full_segments)
        field_col = root_col.getField(field.name)

        is_array = isinstance(field.dataType, ArrayType)
        element_type = field.dataType.elementType if is_array else None
        element_field_names = _struct_field_names(element_type) if isinstance(element_type, StructType) else []

        if isinstance(field.dataType, StructType) and depth < max_depth:
            columns.extend(_flatten_struct_columns(field.dataType, field_col, full_segments, depth + 1, max_depth))
        elif is_array and isinstance(element_type, StringType):
            columns.append(F.array_join(field_col, ", ").alias(alias))
        elif is_array and "value" in element_field_names:
            columns.append(F.array_join(F.transform(field_col, lambda x: x.getField("value")), ", ").alias(alias))
        elif is_array and "displayName" in element_field_names:
            columns.append(F.array_join(F.transform(field_col, lambda x: x.getField("displayName")), ", ").alias(alias))
        elif isinstance(field.dataType, (ArrayType, MapType, StructType)):
            # Array of anything OTHER than the three regular shapes above,
            # map, or a struct we've stopped recursing into (too deep) --
            # to_json handles all these cases correctly; there's no clean
            # flat representation for a variable-shape list of objects.
            columns.append(F.to_json(field_col).alias(alias))
        else:
            columns.append(field_col.alias(alias))
    return columns


def _sanitize_alias(segments: List[str]) -> str:
    """
    Builds a clean column name from path segments -- replaces any
    character that isn't a letter, digit, or underscore with an
    underscore (some real Jira field names contain colons/hyphens, e.g.
    app-configuration keys like
    "com.atlassian.jira.plugin.system.customfieldtypes:atlassian-team").
    Collapses repeated underscores that this can produce. This only ever
    affects the OUTPUT column's display name -- field ACCESS uses
    .getField() with the real, unmodified name (see above), so this
    sanitizing is purely cosmetic now, not load-bearing for correctness.
    """
    raw = "_".join(segments)
    cleaned = re.sub(r"[^0-9a-zA-Z_]", "_", raw)
    return re.sub(r"_+", "_", cleaned).strip("_")


def clean_adf_columns(df: DataFrame, adf_udf) -> DataFrame:
    """
    Finds every "X_content"/"X_type"/"X_version" column triple -- the
    signature shape Atlassian Document Format leaves behind once
    auto_standardize flattens a rich-text field's nested {content, type,
    version} structure -- converts "X_content" to plain readable text via
    adf_udf (pass a UDF wrapping extract_adf_text), renames it to plain
    "X", and drops the "_type"/"_version" siblings (structural metadata
    about the ADF document, not useful once it's plain text).

    Only acts on genuine triples (all three of _content/_type/_version
    present together) -- a column that merely happens to end in "_content"
    without its siblings is left alone, since that's not actually ADF.
    """
    content_cols = {c[:-len("_content")] for c in df.columns if c.endswith("_content")}
    type_cols = {c[:-len("_type")] for c in df.columns if c.endswith("_type")}
    version_cols = {c[:-len("_version")] for c in df.columns if c.endswith("_version")}
    adf_bases = content_cols & type_cols & version_cols

    for base in adf_bases:
        df = (
            df.withColumn(f"{base}_content", adf_udf(F.col(f"{base}_content")))
              .withColumnRenamed(f"{base}_content", base)
              .drop(f"{base}_type", f"{base}_version")
        )
    return df


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
    output_columns = _flatten_struct_columns(parsed_schema, root_col=F.col("_parsed"), max_depth=flatten_depth)
    return df.select(*output_columns, F.col("extracted_at"))


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
                                 flatten_depth: int = 5, drop_empty_columns: bool = True,
                                 exclude_columns: Optional[List[str]] = None,
                                 post_process: Optional[Callable[[DataFrame], DataFrame]] = None) -> None:
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

    exclude_columns: extra columns to drop regardless of nullness -- for
    fields that are redundant with a separate child table built via
    explode_nested_array elsewhere (e.g. "changelog_histories" on issues,
    once histories/history_items exist as their own tables), or anything
    else not worth carrying on this specific entity. "expand" is ALWAYS
    dropped in addition to whatever's passed here -- it's a Jira API
    metadata field present on most endpoints listing what COULD be
    requested via an expand param, never actual data, so it's pure noise
    on every entity it appears on.

    Entries may be exact column names OR glob-style patterns (using * and
    ? wildcards, matched via fnmatch) -- e.g. "*avatarUrls_16x16*" drops
    every column ending that way regardless of what's nested in front of
    it (fields_assignee_avatarUrls_16x16, fields_reporter_avatarUrls_16x16,
    fields_project_avatarUrls_16x16, etc.) without needing to enumerate
    each one by name.

    post_process: an optional DataFrame -> DataFrame function applied
    right before the final write (after dedup, after drop_empty_columns) --
    e.g. rename_customfield_columns, to apply friendly names driven by the
    "fields" reference table without needing a separate manual mapping
    step maintained outside this function.
    """
    bronze_table = f"{bronze_schema}.{entity_name}"
    silver_table = f"{silver_schema}.{entity_name}"

    bronze_df = spark.table(bronze_table)
    standardized = auto_standardize(bronze_df, flatten_depth=flatten_depth)

    import fnmatch
    always_exclude_patterns = ["expand", "self", "*_self"] + list(exclude_columns or [])
    to_exclude = [c for c in standardized.columns
                  if any(fnmatch.fnmatch(c, pat) for pat in always_exclude_patterns)]
    standardized = standardized.drop(*to_exclude)

    deduped = dedup_latest(standardized, key_cols=natural_key_columns, order_by_col=dedup_order_column)

    if dedup_order_column == "extracted_at" and "extracted_at" in deduped.columns:
        deduped = deduped.drop("extracted_at")

    if drop_empty_columns:
        deduped = _drop_all_null_columns(deduped, protect=natural_key_columns)

    if post_process is not None:
        deduped = post_process(deduped)

    logger.info(f"Auto Silver standardize (overwrite): {bronze_table} -> {silver_table}")
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
