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

    logger.info(f"Building {schema.table_name} (table_type={table_type})")

    if getattr(schema, "include_unknown_member", False):
        if table_type == "fact":
            raise ValueError(f"{schema.table_name}: include_unknown_member is for dim/scd2 tables, not fact")
        df = add_unknown_member(df, schema.merge_fields, getattr(schema, "unknown_value", "Unknown"))

    if table_type == "scd2":
        merge_scd2(spark, df, merge_fields=schema.merge_fields, table_name=schema.table_name,
                   key_column=schema.key_column, tracked_columns=schema.tracked_columns)
        return None

    keyed = add_guid_key(df, schema.merge_fields, schema.key_column)
    return upsert_delta(spark, keyed, schema.table_name, key_cols=schema.merge_fields)


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
