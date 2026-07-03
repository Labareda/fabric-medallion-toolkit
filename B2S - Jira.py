# Fabric notebook source
# "B2S - Jira" — Bronze-to-Silver for the Jira source.
# One notebook per source: extraction + landing + Silver standardization for
# ALL of that source's entities, in one place. The pattern for a new source
# is: copy this notebook, rename it "B2S - <NewSource>", change SOURCE_NAME
# and CONFIG_PATH below, and write that source's own <name>.json.
#
# Attach BOTH the Bronze and Silver lakehouses to this notebook (Notebook ->
# Lakehouses -> Add), plus the Config lakehouse if config files live there
# separately from Bronze.

# CELL ********************
%pip install /lakehouse/default/Files/libs/fabric_medallion_toolkit-0.2.1-py3-none-any.whl

# CELL ********************
from datetime import datetime, timezone
from notebookutils import mssparkutils

from fabric_medallion_toolkit.config_loader import load_source_config
from fabric_medallion_toolkit.bronze import RestExtractor, land_records, get_watermark, save_watermark, compute_new_watermark
from fabric_medallion_toolkit.silver import run_silver_standardize

# CELL ********************
# --- Everything specific to this source lives right here, nowhere else ---
SOURCE_NAME = "jira"
CONFIG_PATH = "/lakehouse/default/Files/jira.json"   # in the Config lakehouse
SCHEMA = SOURCE_NAME   # Bronze/Silver schema = source name, e.g. "jira"

def resolve_secret(akv_name: str, secret_name: str) -> str:
    return mssparkutils.credentials.getSecret(akv_name, secret_name)

source_config, silver_configs = load_source_config(CONFIG_PATH, secret_resolver=resolve_secret)
silver_by_entity = {sc.entity_name: sc for sc in silver_configs}
extractor = RestExtractor(source_config)

# CELL ********************
# --- Bronze: extract + land every entity ---
for entity in source_config.entities:
    extraction_started_at = datetime.now(timezone.utc)

    use_watermark = bool(entity.incremental_column or entity.watermark_params_template or entity.watermark_body_template)
    watermark_value = get_watermark(spark, SOURCE_NAME, entity, bronze_schema=SCHEMA) if use_watermark else None
    print(f"[{entity.entity_name}] {'incremental, watermark >= ' + str(watermark_value) if use_watermark else 'full load'}")

    records = list(extractor.extract_entity(entity, watermark_value=watermark_value))
    count = land_records(spark, records, source_name=SOURCE_NAME, entity=entity, bronze_schema=SCHEMA)
    print(f"[{entity.entity_name}] landed {count} records")

    if use_watermark and count > 0:
        new_watermark = compute_new_watermark(records, entity, extraction_started_at)
        save_watermark(spark, SOURCE_NAME, entity, new_watermark, bronze_schema=SCHEMA)
        print(f"[{entity.entity_name}] watermark advanced to {new_watermark}")

# CELL ********************
# --- Silver: standardize every entity that has a "silver" block in the config ---
for entity_name, silver_cfg in silver_by_entity.items():
    run_silver_standardize(spark, silver_cfg, bronze_schema=SCHEMA, silver_schema=SCHEMA)
    print(f"[{entity_name}] standardized -> {SCHEMA}.{entity_name} (Silver)")

# CELL ********************
print(f"B2S - {SOURCE_NAME.title()} complete.")
