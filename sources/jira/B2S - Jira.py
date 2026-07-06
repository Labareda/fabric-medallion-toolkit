# Fabric notebook source
# "B2S - Jira" — Bronze-to-Silver for the Jira source.
# Attach Bronze, Silver, and Config lakehouses, plus env_medallion_toolkit.

# CELL ********************
from datetime import datetime, timezone
from notebookutils import mssparkutils
import fabric_medallion_toolkit as fmt

# CELL ********************
SOURCE_NAME = "jira"
CONFIG_PATH = "/lakehouse/default/Files/jira.json"   # in the Config lakehouse
SCHEMA = SOURCE_NAME

def resolve_secret(akv_name: str, secret_name: str) -> str:
    return mssparkutils.credentials.getSecret(akv_name, secret_name)

source_config, silver_configs = fmt.load_source_config(CONFIG_PATH, secret_resolver=resolve_secret)
silver_by_entity = {sc.entity_name: sc for sc in silver_configs}
extractor = fmt.RestExtractor(source_config)

# CELL ********************
for entity in source_config.entities:
    extraction_started_at = datetime.now(timezone.utc)

    use_watermark = bool(entity.incremental_column or entity.watermark_params_template or entity.watermark_body_template)
    watermark_value = fmt.get_watermark(spark, SOURCE_NAME, entity, bronze_schema=SCHEMA) if use_watermark else None
    print(f"[{entity.entity_name}] {'incremental, watermark >= ' + str(watermark_value) if use_watermark else 'full load'}")

    records = list(extractor.extract_entity(entity, watermark_value=watermark_value))
    count = fmt.land_records(spark, records, source_name=SOURCE_NAME, entity=entity, bronze_schema=SCHEMA)
    print(f"[{entity.entity_name}] landed {count} records")

    if use_watermark and count > 0:
        new_watermark = fmt.compute_new_watermark(records, entity, extraction_started_at)
        fmt.save_watermark(spark, SOURCE_NAME, entity, new_watermark, bronze_schema=SCHEMA)
        print(f"[{entity.entity_name}] watermark advanced to {new_watermark}")

# CELL ********************
for entity_name, silver_cfg in silver_by_entity.items():
    fmt.run_silver_standardize(spark, silver_cfg, bronze_schema=SCHEMA, silver_schema=SCHEMA)
    print(f"[{entity_name}] standardized -> {SCHEMA}.{entity_name} (Silver)")

# CELL ********************
print("B2S - Jira complete.")
