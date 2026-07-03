"""
Generic Gold builder, dispatched on GoldTableConfig.table_type ("dim" |
"scd2" | "fact"). Every table gets its key column (config.surrogate_key_column)
auto-generated as a deterministic GUID from config.merge_fields — see
gold/keys.py. That determinism is what keeps this builder simple: "dim" and
"fact" don't need any existing-table lookup at all, since the same merge
field values always hash to the same key.
"""

from typing import Optional, List

from fabric_medallion_toolkit.config import GoldTableConfig
from fabric_medallion_toolkit.gold.keys import add_guid_key
from fabric_medallion_toolkit.gold.scd2 import build_scd2_dimension
from fabric_medallion_toolkit.utils.delta_merge import upsert_delta
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("gold.builder")

_VALID_TABLE_TYPES = {"dim", "scd2", "fact"}


def build_gold_table(spark, config: GoldTableConfig) -> Optional[str]:
    """
    Config-driven path (used by run_gold_model / NB_Gold's JSON-config
    variant, if you use it): runs config.select_sql, adds the GUID key
    column, then routes by table_type. For a notebook where you build the
    DataFrame yourself and want to see it before merging, use merge_dim /
    merge_fact / merge_scd2 directly instead — see below.
    """
    table_type = config.table_type.lower()
    if table_type not in _VALID_TABLE_TYPES:
        raise ValueError(f"{config.table_name}: table_type must be one of {_VALID_TABLE_TYPES}, got '{config.table_type}'")
    if not config.merge_fields:
        raise ValueError(f"{config.table_name}: merge_fields must be set (used for both MERGE matching and the key column)")

    logger.info(f"Building {config.table_name} (table_type={table_type})")
    df = spark.sql(config.select_sql)

    if table_type == "scd2":
        merge_scd2(spark, df, merge_fields=config.merge_fields,
                   surrogate_key_column=config.surrogate_key_column,
                   table_name=config.table_name, tracked_columns=config.tracked_columns)
        return None

    merge_fn = merge_dim if table_type == "dim" else merge_fact
    return merge_fn(spark, df, config.table_name, config.merge_fields, config.surrogate_key_column)


def merge_dim(spark, df, table_name: str, merge_fields: List[str], surrogate_key_column: str = "row_key") -> Optional[str]:
    """
    The building block for a "dim"-style notebook cell:

        df_dim_project = spark.sql("SELECT DISTINCT project_key FROM jira.issues")
        merge_dim(spark, df_dim_project, "gold.dim_project", ["project_key"], "project_key_sk")

    Adds a deterministic GUID key column (from merge_fields) then MERGEs
    into table_name. Returns the MERGE SQL that ran (None on first-time
    create). Identical behavior to merge_fact — the two names exist so a
    notebook reads clearly, not because the logic differs.
    """
    if not merge_fields:
        raise ValueError(f"{table_name}: merge_fields must be a non-empty list")
    keyed = add_guid_key(df, merge_fields, surrogate_key_column)
    return upsert_delta(spark, keyed, table_name, key_cols=merge_fields)


def merge_fact(spark, df, table_name: str, merge_fields: List[str], surrogate_key_column: str = "row_key") -> Optional[str]:
    """Same as merge_dim — see its docstring. Separate name purely for notebook readability."""
    return merge_dim(spark, df, table_name, merge_fields, surrogate_key_column)


def merge_scd2(spark, df, merge_fields: List[str], surrogate_key_column: str,
               table_name: str, tracked_columns: Optional[List[str]] = None) -> dict:
    """
    The building block for an "scd2"-style notebook cell:

        df_dim_resource = spark.sql("SELECT DISTINCT assignee AS resource_name, department FROM jira.issues")
        merge_scd2(spark, df_dim_resource, ["resource_name"], "resource_key", "gold.dim_resource",
                   tracked_columns=["department"])

    Thin re-export of gold.scd2.build_scd2_dimension, named to match
    merge_dim/merge_fact for a consistent "build a DataFrame, then merge it"
    pattern regardless of table_type.
    """
    return build_scd2_dimension(spark, df, merge_fields, surrogate_key_column, table_name, tracked_columns)


def run_gold_model(spark, table_configs: list) -> dict:
    """
    Builds a list of GoldTableConfigs in the order given — put dims before
    the facts that reference them. Returns {table_name: merge_sql} for
    logging/documentation.
    """
    results = {}
    for cfg in table_configs:
        results[cfg.table_name] = build_gold_table(spark, cfg)
    return results
