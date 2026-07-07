"""
Generic Silver step: reads a Bronze table, parses each record's raw_data
JSON according to a SilverEntityConfig's column_mappings, casts every
column to its declared type (dates/timestamps parsed with an explicit
format so "standardize dates" actually means something consistent across
sources), keeps only the LATEST row per natural key, and OVERWRITES the
Silver Delta table.

Silver is stateless/recomputed, not merged — but Bronze now holds full
history (append-only, one row per extraction), not just latest state, so
the dedup step here is load-bearing: it's what turns "every extraction
ever" into "current state per record". Every Silver run reads the full
current Bronze table and replaces Silver wholesale; that's cheap since it's
just JSON parsing + casting + a window dedup, not business logic. Gold is
where MERGE actually matters (stable surrogate keys across runs).

If a specific entity gets too large for a full overwrite to be practical,
that's a per-entity decision — swap run_silver_standardize's write for
upsert_delta() on just that entity, everything else about this file stays
the same.

Nothing in this file is Jira/CRM/whatever-specific — the SilverEntityConfig
is what makes it source-aware, not the code.
"""

import json
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from fabric_medallion_toolkit.config import SilverEntityConfig, ColumnMapping
from fabric_medallion_toolkit.utils.json_path import get_by_path
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("silver.standardize")

_SPARK_CAST = {
    "string": "string",
    "int": "int",
    "long": "long",
    "double": "double",
    "boolean": "boolean",
}


def _json_extract_udf_factory(json_path: str):
    def _extract(raw_json):
        if not raw_json:
            return None
        try:
            obj = json.loads(raw_json)
        except (TypeError, ValueError):
            return None
        val = get_by_path(obj, json_path, default=None)
        # Keep everything as a string out of the UDF; real typing happens
        # via explicit cast/to_date/to_timestamp below, so every mapping's
        # target type is honored consistently rather than guessed.
        if val is None:
            return None
        return val if isinstance(val, str) else json.dumps(val, default=str) if isinstance(val, (dict, list)) else str(val)
    return _extract


def flatten_and_standardize(bronze_df: DataFrame, config: SilverEntityConfig) -> DataFrame:
    """
    bronze_df: raw read of a Bronze landing table (has raw_data, _natural_key,
    _extracted_at, etc. — see bronze/landing.py for the exact schema).
    """
    df = bronze_df
    for mapping in config.column_mappings:
        extract_udf = F.udf(_json_extract_udf_factory(mapping.json_path), StringType())
        raw_col = f"_raw__{mapping.target_column}"
        df = df.withColumn(raw_col, extract_udf(F.col("raw_data")))

        if mapping.data_type == "date":
            typed = F.to_date(F.col(raw_col), mapping.date_format) if mapping.date_format \
                else F.to_date(F.col(raw_col))
        elif mapping.data_type == "timestamp":
            typed = F.to_timestamp(F.col(raw_col), mapping.date_format) if mapping.date_format \
                else F.to_timestamp(F.col(raw_col))
        elif mapping.data_type in _SPARK_CAST:
            typed = F.col(raw_col).cast(_SPARK_CAST[mapping.data_type])
        else:
            raise ValueError(
                f"Unknown data_type '{mapping.data_type}' for column '{mapping.target_column}'. "
                f"Use one of: string, int, long, double, boolean, date, timestamp."
            )

        df = df.withColumn(mapping.target_column, typed).drop(raw_col)

    # Only the mapped business columns survive into Silver -- _natural_key,
    # _source_system, _entity, _load_id, _ingest_date were Bronze-side
    # tracking metadata that never needed to leak into Silver's schema.
    # _extracted_at is the one exception, kept temporarily: dedup_latest
    # needs it (by default) to determine "latest per natural key" -- see
    # run_silver_standardize, which drops it again right after, so it never
    # actually reaches the final Silver table either.
    keep_cols = [m.target_column for m in config.column_mappings] + ["_extracted_at"]
    return df.select(*keep_cols)


def dedup_latest(df: DataFrame, key_cols: List[str], order_by_col: str) -> DataFrame:
    """
    Keeps only the most recent row per natural key. Load-bearing now that
    Bronze holds full history — this is what collapses N historical
    extractions of the same record down to current state. Defaults to
    ordering by _extracted_at; pass a source-side "updated" column here
    instead if the source's own timestamp is more trustworthy than
    ingestion time (e.g. to correctly handle a record that was re-extracted
    but genuinely didn't change).
    """
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_by_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def run_silver_standardize(spark, config: SilverEntityConfig, bronze_schema: str = "bronze",
                            silver_schema: str = "silver") -> None:
    """
    Full generic Silver step for one entity: read ALL of Bronze (full
    history) -> flatten & type -> dedup to latest per natural key ->
    OVERWRITE Silver. No merge, no key matching against the previous
    Silver state — Silver is fully recomputed every run.

    bronze_schema/silver_schema are normally both config.source_name — one
    Lakehouse schema per source in Bronze, mirrored in Silver — so the table
    lands as {source_name}.{entity_name} in both, no repeated source prefix.
    """
    bronze_table = f"{bronze_schema}.{config.entity_name}"
    silver_table = f"{silver_schema}.{config.entity_name}"

    bronze_df = spark.table(bronze_table)
    standardized = flatten_and_standardize(bronze_df, config)
    deduped = dedup_latest(standardized, key_cols=config.natural_key_columns,
                            order_by_col=config.dedup_order_column)

    # _extracted_at was only ever needed to compute "latest" above -- drop
    # it now so it doesn't appear in Silver. (If dedup_order_column was set
    # to something else -- e.g. a real "updated_at" business column instead
    # of the default -- there's nothing extra to drop; that column is a
    # legitimate mapped output the caller wants.)
    if config.dedup_order_column == "_extracted_at" and "_extracted_at" in deduped.columns:
        deduped = deduped.drop("_extracted_at")

    logger.info(f"Silver standardize (overwrite): {bronze_table} -> {silver_table}")
    # overwriteSchema=true because Silver is meant to be fully recomputed
    # every run, not carefully preserved like Gold -- if column mappings
    # changed (a type, an added/removed column), the new schema should just
    # take effect, not be blocked by Delta's default schema-protection.
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
