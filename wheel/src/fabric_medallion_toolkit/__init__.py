"""
fabric_medallion_toolkit
=========================

Everything is available from the top level -- no need to know which
submodule something lives in:

    import fabric_medallion_toolkit as fmt

    df = spark.sql("SELECT DISTINCT project_key FROM jira.issues")
    fmt.merge(spark, df, fmt.TableSchema(
        table_name="gold.dim_project", table_type="dim", merge_fields=["project_key"],
    ))
"""

__version__ = "0.3.47"

# --- config: the dataclasses used to describe sources, entities, columns, tables ---
from fabric_medallion_toolkit.config import (
    AuthConfig, LakehouseConfig, EntityConfig, SourceConfig,
    ColumnMapping, SilverEntityConfig, TableSchema, GoldTableConfig, DateDimensionConfig,
)
from fabric_medallion_toolkit.config_loader import (
    load_source_config, load_gold_config, load_date_dimension_config,
)

# --- bronze: extraction + landing ---
from fabric_medallion_toolkit.bronze import (
    RestExtractor, land_records, get_watermark, save_watermark, compute_new_watermark, extract_per_parent,
)

# --- silver: standardization ---
from fabric_medallion_toolkit.silver import (
    run_silver_standardize, flatten_and_standardize, dedup_latest, explode_nested_array,
    auto_standardize, run_auto_silver_standardize, clean_adf_columns, rename_customfield_columns, build_field_id_to_name,
)

# --- gold: merge, keys, lookups, date dimension ---
from fabric_medallion_toolkit.gold import (
    merge, build_gold_table, run_gold_model,
    build_date_dimension, add_date_dimension_sentinel, merge_scd2, add_guid_key, lookup_key, lookup_keys, add_unknown_member,
)

# --- utils, for the rarer case you need them directly ---
from fabric_medallion_toolkit.utils import get_logger, get_by_path, upsert_delta, build_merge_sql, refresh_sql_endpoint, refresh_sql_endpoints, extract_adf_text, topological_sort, build_medallion_run_order

__all__ = [
    # config
    "AuthConfig", "LakehouseConfig", "EntityConfig", "SourceConfig",
    "ColumnMapping", "SilverEntityConfig", "TableSchema", "GoldTableConfig", "DateDimensionConfig",
    "load_source_config", "load_gold_config", "load_date_dimension_config",
    # bronze
    "RestExtractor", "land_records", "get_watermark", "save_watermark", "compute_new_watermark", "extract_per_parent",
    # silver
    "run_silver_standardize", "flatten_and_standardize", "dedup_latest", "explode_nested_array",
    "auto_standardize", "run_auto_silver_standardize", "clean_adf_columns", "rename_customfield_columns", "build_field_id_to_name",
    # gold
    "merge", "build_gold_table", "run_gold_model",
    "build_date_dimension", "add_date_dimension_sentinel", "merge_scd2", "add_guid_key", "lookup_key", "lookup_keys", "add_unknown_member",
    # utils
    "get_logger", "get_by_path", "upsert_delta", "build_merge_sql", "refresh_sql_endpoint", "refresh_sql_endpoints", "extract_adf_text", "topological_sort", "build_medallion_run_order",
]
