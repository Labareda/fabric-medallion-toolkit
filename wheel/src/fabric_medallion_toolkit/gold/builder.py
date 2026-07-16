"""
One function, `merge()`, for building any Gold table. Reads everything it
needs from a TableSchema (or a GoldTableConfig, for the config-driven path)
so a notebook cell never repeats itself:

    schema = TableSchema(table_name="gold.dim_project", table_type="dim",
                          merge_fields=["project_key"])
    df = spark.sql("SELECT DISTINCT project_key FROM jira.issues")
    merge(spark, df, schema)

Every table's own key column is always literally called "key" -- see
gold/keys.py. dim and fact use the same MERGE path underneath (a fact's
merge_fields are just its grain, not a "changing attribute" concept the
way a dim's are); scd2 routes to gold/scd2.py's versioning logic instead.
"""

from typing import Optional, Union

from pyspark.sql import functions as F

from fabric_medallion_toolkit.config import GoldTableConfig, TableSchema
from fabric_medallion_toolkit.gold.keys import add_guid_key
from fabric_medallion_toolkit.gold.scd2 import merge_scd2
from fabric_medallion_toolkit.gold.unknown import add_unknown_member
from fabric_medallion_toolkit.utils.delta_merge import upsert_delta
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("gold.builder")

_VALID_TABLE_TYPES = {"dim", "scd2", "fact"}


def merge(spark, df, schema: Union[TableSchema, GoldTableConfig]) -> Optional[str]:
    """
    Builds/updates one Gold table from an already-built DataFrame. Adds the
    key column (named schema.key_column), then routes by schema.table_type:
      - "dim" / "fact": deterministic key, MERGE on merge_fields.
      - "scd2": versioned -- a changed row gets a new key, old row closes out.
    If schema.include_unknown_member is set (dim/scd2 only), a placeholder
    row is added first so fact lookups against this table never resolve to
    NULL for an unmatched natural key -- see gold/unknown.py.
    Returns the MERGE SQL that ran (None on first-time create, or for scd2
    which doesn't produce a single MERGE statement -- see log lines instead).
    """
    table_type = schema.table_type.lower()
    if table_type not in _VALID_TABLE_TYPES:
        raise ValueError(f"{schema.table_name}: table_type must be one of {_VALID_TABLE_TYPES}, got '{schema.table_type}'")
    if not schema.merge_fields:
        raise ValueError(f"{schema.table_name}: merge_fields must be set (used for both MERGE matching and the key column)")

    missing_merge_cols_early = sorted(set(schema.merge_fields) - set(df.columns))
    if missing_merge_cols_early:
        raise ValueError(f"{schema.table_name}: merge field(s) not found in the DataFrame at all: {missing_merge_cols_early}")

    # include_unknown_member=None (the default) means "auto": True for
    # dim/scd2, False for fact -- so you never need to type it out for an
    # ordinary dimension, but a fact table (where it's never valid at all)
    # doesn't error out just from using the default. Pass True/False
    # explicitly only if you want to override the automatic choice.
    # Resolved early (not just where it's applied further down) because it
    # also determines whether merge fields need string-casting -- see below.
    include_unknown = getattr(schema, "include_unknown_member", None)
    if include_unknown is None:
        include_unknown = table_type in ("dim", "scd2")
    if include_unknown and table_type == "fact":
        raise ValueError(f"{schema.table_name}: include_unknown_member is for dim/scd2 tables, not fact")

    if include_unknown:
        # Every merge field is cast to its DECLARED type (from `columns`,
        # falling back to "string" for old-style schemas that don't use
        # `columns` at all) -- unconditionally, whether "missing" was
        # explicitly set or falls back to schema.unknown_value. This
        # matters even when "missing" IS declared: declaring a sentinel
        # doesn't by itself guarantee the column already matches that
        # type at the source (e.g. a genuinely numeric Board_Id with
        # "type": "string", "missing": "Unknown" declared, but never
        # actually cast anywhere upstream) -- without this, that case
        # still fails inserting a string into a still-numeric column,
        # despite having done everything the schema seems to ask for.
        declared_sentinels = dict(getattr(schema, "merge_field_sentinels", None) or {})
        columns_spec = getattr(schema, "columns", None) or {}
        for col in schema.merge_fields:
            declared_type = columns_spec.get(col, {}).get("type", "string")
            df = df.withColumn(col, F.col(col).cast(declared_type))
            if col not in declared_sentinels:
                declared_sentinels[col] = getattr(schema, "unknown_value", "Unknown")
        df = add_unknown_member(df, declared_sentinels)

    column_defaults = getattr(schema, "column_defaults", None)
    if column_defaults:
        missing_for_defaults = sorted(set(column_defaults) - set(df.columns))
        if missing_for_defaults:
            raise ValueError(
                f"{schema.table_name}: column_defaults references column(s) not present in the "
                f"DataFrame at all: {missing_for_defaults}. column_defaults can coerce a column's "
                f"type/fill missing VALUES, but the column itself must already exist -- check your SELECT."
            )
        for col, spec in column_defaults.items():
            df = df.withColumn(col, F.coalesce(F.col(col).cast(spec["type"]), F.lit(spec["default"]).cast(spec["type"])))

    lookup_fallbacks = getattr(schema, "lookup_fallbacks", None)
    if lookup_fallbacks:
        missing_for_fallbacks = sorted(set(lookup_fallbacks) - set(df.columns))
        if missing_for_fallbacks:
            raise ValueError(
                f"{schema.table_name}: lookup_fallbacks references column(s) not present in the "
                f"DataFrame at all: {missing_for_fallbacks}. This resolves a null in a column YOUR "
                f"SELECT already joined and produced -- the column itself must already exist."
            )
        # Cache each unique (table, natural_key_column, unknown_value)
        # lookup so pointing several columns at the SAME dimension (e.g.
        # Author_Key and Update_Author_Key both -> Dim_Resource) only
        # queries that dimension once, not once per column.
        unknown_value_cache = {}
        for col, ref in lookup_fallbacks.items():
            unknown_value = ref.get("unknown_value", "Unknown")
            cache_key = (ref["table"], ref["natural_key_column"], ref["key_column"], unknown_value)
            if cache_key not in unknown_value_cache:
                unknown_row = (
                    spark.table(ref["table"])
                    .filter(F.col(ref["natural_key_column"]) == unknown_value)
                    .select(ref["key_column"]).limit(1).collect()
                )
                if not unknown_row:
                    raise ValueError(
                        f"{schema.table_name}: lookup_fallbacks for '{col}' points at {ref['table']}, "
                        f"but it has no row where {ref['natural_key_column']} = '{unknown_value}'. "
                        f"Build that dimension with include_unknown_member=True first."
                    )
                unknown_value_cache[cache_key] = unknown_row[0][ref["key_column"]]
            df = df.withColumn(col, F.coalesce(F.col(col), F.lit(unknown_value_cache[cache_key])))

    expected_columns = getattr(schema, "expected_columns", None)
    if expected_columns:
        actual_types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        missing = sorted(set(expected_columns) - set(actual_types))
        if missing:
            raise ValueError(
                f"{schema.table_name}: expected column(s) missing from the DataFrame: {missing}. "
                f"The source query may no longer be producing a column this table relies on."
            )
        mismatches = [
            f"'{col}' expected type '{expected_type}', got '{actual_types[col]}'"
            for col, expected_type in expected_columns.items()
            if actual_types[col] != expected_type
        ]
        if mismatches:
            raise ValueError(
                f"{schema.table_name}: column type drift detected (source data likely changed shape) -- "
                + "; ".join(mismatches)
            )

    # MATERIALIZE ONCE before the validation scans below. Everything from here
    # -- the null check, the duplicate check, add_guid_key, and the final
    # write -- reads df. If df carries an expensive un-materialized lineage
    # (e.g. Dim_Issue's two hierarchy walks + sort_path), EACH of those reads
    # re-executes that entire lineage from scratch. Persisting once here means
    # the upstream work runs a single time and every scan below hits the
    # materialized result. This is often the single biggest win for a
    # notebook where "merge is slow" -- the merge isn't slow, it's re-running
    # the whole pipeline feeding it, several times.
    df = df.persist()
    df.count()  # force it now

    # Merge-field integrity: every row needs a COMPLETE, UNIQUE natural key.
    # A null merge field would generate a key hashing partly from nothing
    # (silently wrong), and a duplicate combination means two source rows
    # would collide into the SAME generated key -- either corrupts the
    # table silently if left unchecked, so both are checked unconditionally,
    # not something you opt into.
    # (existence already checked earlier, before the string-cast step)

    null_filter = " OR ".join(f"`{col}` IS NULL" for col in schema.merge_fields)
    null_rows = df.filter(null_filter).limit(1).count()
    if null_rows > 0:
        raise ValueError(
            f"{schema.table_name}: merge field(s) {schema.merge_fields} contain NULL in at least one row -- "
            f"every row needs a complete natural key. Fix the source query (e.g. COALESCE to a real "
            f"sentinel) rather than merging with an incomplete key."
        )

    dup_rows = df.groupBy(*schema.merge_fields).count().filter("count > 1").limit(1).count()
    if dup_rows > 0:
        raise ValueError(
            f"{schema.table_name}: merge field(s) {schema.merge_fields} contain duplicate combinations -- "
            f"they must uniquely identify one row each. Two rows sharing the same merge field values "
            f"would generate the SAME key and collide during MERGE, silently losing one of them."
        )

    logger.info(f"Building {schema.table_name} (table_type={table_type})")

    if table_type == "scd2":
        merge_scd2(spark, df, merge_fields=schema.merge_fields, table_name=schema.table_name,
                   key_column=schema.key_column, tracked_columns=schema.tracked_columns)
        df.unpersist()
        return None

    # write_mode defaults to "merge" (incremental-safe). A schema can set
    # write_mode="overwrite" for a table that's fully rebuilt every run --
    # far cheaper than MERGE, which otherwise compares every rebuilt row
    # against the old table for no benefit. See upsert_delta.
    write_mode = getattr(schema, "write_mode", None) or "merge"
    if write_mode == "overwrite" and table_type == "scd2":
        raise ValueError(f"{schema.table_name}: write_mode='overwrite' is not valid with table_type='scd2' (scd2 has its own versioning write path)")
    keyed = add_guid_key(df, schema.merge_fields, schema.key_column)
    result_sql = upsert_delta(spark, keyed, schema.table_name, key_cols=schema.merge_fields,
                              write_mode=write_mode)
    df.unpersist()
    return result_sql


def build_gold_table(spark, config: GoldTableConfig) -> Optional[str]:
    """Config-driven path: runs config.select_sql, then merge()s the result. See config_loader.load_gold_config."""
    df = spark.sql(config.select_sql)
    return merge(spark, df, config)


def run_gold_model(spark, table_configs: list) -> dict:
    """Builds a list of GoldTableConfigs in order given -- dims before the facts that reference them."""
    results = {}
    for cfg in table_configs:
        results[cfg.table_name] = build_gold_table(spark, cfg)
    return results
