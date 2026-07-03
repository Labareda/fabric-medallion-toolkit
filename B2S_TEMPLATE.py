# Fabric notebook source
# TEMPLATE: "B2S - <SourceName>"
# Copy to sources/<name>/, rename the notebook "B2S - <SourceName>" in Fabric,
# fill in SOURCE_NAME and CONFIG_PATH below. Everything else works unchanged
# for any REST API source described by that source's <name>.json.
#
# For a non-REST source (ODBC, SFTP...), keep the Silver section as-is but
# replace the Bronze extraction section with your own code -- the only
# contract that matters is that `records` ends up as a list of plain dicts
# before calling land_records().
#
# Attach the Bronze, Silver, and Config lakehouses to this notebook, plus
# the env_medallion_toolkit Environment (or add a %pip install cell for the
# wheel if you're not using an Environment).

# CELL ********************
from datetime import datetime, timezone
from notebookutils import mssparkutils

from fabric_medallion_toolkit.config_loader import load_source_config
from fabric_medallion_toolkit.bronze import RestExtractor, land_records, get_watermark, save_watermark, compute_new_watermark
from fabric_medallion_toolkit.silver import run_silver_standardize

# CELL ********************
# --- Everything specific to this source lives right here, nowhere else ---
SOURCE_NAME = "REPLACE_WITH_SOURCE_SLUG"
CONFIG_PATH = f"/lakehouse/default/Files/{SOURCE_NAME}.json"   # in the Config lakehouse
SCHEMA = SOURCE_NAME   # Bronze/Silver schema = source name

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
