"""
Loads a source's config from a single JSON file (one file per source,
filename = source name, e.g. jira.json). One file carries BOTH the
extraction config (auth, entities, pagination, watermarks) AND each
entity's Silver standardization config (column mappings) nested under
each entity -- since a B2S notebook does extraction and standardization
for one source in one place, its config lives in one place too.

Secrets are NEVER stored in the JSON -- auth blocks reference a Key Vault
name + secret name, resolved at runtime via a `secret_resolver` callback
(in a Fabric notebook, that's mssparkutils.credentials.getSecret; outside
Fabric, plug in a test double). Keeps the loader itself testable without
a Fabric runtime.
"""

import json
from typing import Callable, Dict, Any, List, Optional, Tuple

from fabric_medallion_toolkit.config import (
    SourceConfig, EntityConfig, AuthConfig, SilverEntityConfig, ColumnMapping,
    GoldTableConfig, DateDimensionConfig,
)

SecretResolver = Callable[[str, str], str]  # (akv_name, secret_name) -> secret value


def _load_auth(auth_json: Optional[Dict[str, Any]], secret_resolver: Optional[SecretResolver]) -> Optional[AuthConfig]:
    if not auth_json:
        return None
    secret = None
    if "secret_akv_name" in auth_json and "secret_akv_secret_name" in auth_json:
        if secret_resolver is None:
            raise ValueError(
                "Config references a Key Vault secret but no secret_resolver was provided to load_source_config()"
            )
        secret = secret_resolver(auth_json["secret_akv_name"], auth_json["secret_akv_secret_name"])
    elif "secret" in auth_json:
        secret = auth_json["secret"]  # only expected in local/test configs, never in a real deployed file

    return AuthConfig(
        kind=auth_json["kind"],
        username=auth_json.get("username"),
        secret=secret,
        api_key_header=auth_json.get("api_key_header"),
    )


def _load_entity(entity_json: Dict[str, Any]) -> EntityConfig:
    return EntityConfig(
        entity_name=entity_json["entity_name"],
        endpoint_path=entity_json.get("endpoint_path", ""),
        http_method=entity_json.get("http_method", "GET"),
        pagination_style=entity_json.get("pagination_style", "offset_limit"),
        page_size=entity_json.get("page_size", 100),
        cursor_param_name=entity_json.get("cursor_param_name", "cursor"),
        cursor_page_size_param_name=entity_json.get("cursor_page_size_param_name", "limit"),
        records_json_path=entity_json.get("records_json_path", ""),
        total_json_path=entity_json.get("total_json_path"),
        next_cursor_json_path=entity_json.get("next_cursor_json_path"),
        natural_key_field=entity_json.get("natural_key_field", "id"),
        extra_params=entity_json.get("extra_params", {}),
        extra_body=entity_json.get("extra_body", {}),
        incremental_column=entity_json.get("incremental_column"),
        watermark_params_template=entity_json.get("watermark_params_template", {}),
        watermark_body_template=entity_json.get("watermark_body_template", {}),
        initial_watermark_value=entity_json.get("initial_watermark_value", "1900-01-01T00:00:00"),
    )


def _load_silver(source_name: str, entity_name: str, silver_json: Optional[Dict[str, Any]]) -> Optional[SilverEntityConfig]:
    if not silver_json:
        return None
    mappings = [
        ColumnMapping(
            target_column=m["target_column"],
            json_path=m["json_path"],
            data_type=m.get("data_type", "string"),
            date_format=m.get("date_format"),
        )
        for m in silver_json["column_mappings"]
    ]
    return SilverEntityConfig(
        source_name=source_name,
        entity_name=entity_name,
        natural_key_columns=silver_json["natural_key_columns"],
        column_mappings=mappings,
        dedup_order_column=silver_json.get("dedup_order_column", "extracted_at"),
    )


def load_source_config(json_path_or_dict, secret_resolver: Optional[SecretResolver] = None
                        ) -> Tuple[SourceConfig, List[SilverEntityConfig]]:
    """
    Returns (source_config, silver_configs):
      - source_config: SourceConfig with its entities (for RestExtractor / land_records)
      - silver_configs: one SilverEntityConfig per entity that has a "silver" block
        (an entity with no "silver" block just won't be standardized -- fine for
        Bronze-only reference data you don't need in Silver)

    `json_path_or_dict` can be a file path (str) or an already-loaded dict.
    source_name is read from the JSON's "source_name" field -- keep it
    matching the filename (e.g. jira.json -> "source_name": "jira") for
    your own clarity; nothing in the loader enforces that match, since the
    file's content is the source of truth, not its name.
    """
    raw = json_path_or_dict
    if isinstance(raw, str):
        with open(raw) as f:
            raw = json.load(f)

    entities = [_load_entity(e) for e in raw.get("entities", [])]
    source_config = SourceConfig(
        source_name=raw["source_name"],
        base_url=raw["base_url"],
        auth=_load_auth(raw.get("auth"), secret_resolver),
        entities=entities,
        request_timeout_seconds=raw.get("request_timeout_seconds", 30),
        max_retries=raw.get("max_retries", 5),
    )

    silver_configs = []
    for e in raw.get("entities", []):
        sc = _load_silver(raw["source_name"], e["entity_name"], e.get("silver"))
        if sc:
            silver_configs.append(sc)

    return source_config, silver_configs


def load_gold_config(json_path_or_dict) -> List[GoldTableConfig]:
    """
    Loads a list of GoldTableConfig from a JSON file with a "tables" array
    -- the config-driven alternative to writing TableSchema + SQL directly
    in a notebook. Each table's key column is always "key" (not configurable
    -- see gold/keys.py), so there's no surrogate_key_column field to set.
    """
    raw = json_path_or_dict
    if isinstance(raw, str):
        with open(raw) as f:
            raw = json.load(f)

    return [
        GoldTableConfig(
            table_name=t["table_name"],
            select_sql=t["select_sql"],
            table_type=t.get("table_type", "fact"),
            merge_fields=t["merge_fields"],
            tracked_columns=t.get("tracked_columns"),
        )
        for t in raw["tables"]
    ]


def load_date_dimension_config(json_path_or_dict) -> Optional[DateDimensionConfig]:
    """Returns None if the JSON has no "date_dimension" block -- it's optional."""
    raw = json_path_or_dict
    if isinstance(raw, str):
        with open(raw) as f:
            raw = json.load(f)

    dd = raw.get("date_dimension")
    if not dd:
        return None
    return DateDimensionConfig(
        table_name=dd.get("table_name", "dim_date"),
        start_date=dd.get("start_date", "2020-01-01"),
        end_date=dd.get("end_date", "2035-12-31"),
        fiscal_year_start_month=dd.get("fiscal_year_start_month", 1),
    )
