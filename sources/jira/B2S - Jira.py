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
# --- Per-project entities: Versions (not a single global list, one call per project) ---
project_keys = [r["_natural_key"] for r in spark.table(f"{SCHEMA}.projects").select("_natural_key").distinct().collect()]

versions_template = fmt.EntityConfig(
    entity_name="versions", endpoint_path="/rest/api/3/project/{parent_id}/versions",
    http_method="GET", pagination_style="none", records_json_path="", natural_key_field="id",
)
version_records = fmt.extract_per_parent(extractor, versions_template, parent_ids=project_keys, parent_field_name="_project_key")
count = fmt.land_records(spark, version_records, source_name=SOURCE_NAME, entity=versions_template, bronze_schema=SCHEMA)
print(f"[versions] landed {count} records across {len(project_keys)} projects")

versions_silver_cfg = fmt.SilverEntityConfig(
    source_name=SOURCE_NAME, entity_name="versions", natural_key_columns=["version_id"],
    column_mappings=[
        fmt.ColumnMapping("version_id", "id", "string"),
        fmt.ColumnMapping("version_name", "name", "string"),
        fmt.ColumnMapping("project_key", "_parent_id", "string"),
        fmt.ColumnMapping("is_released", "released", "boolean"),
        fmt.ColumnMapping("is_archived", "archived", "boolean"),
        fmt.ColumnMapping("release_date", "releaseDate", "date"),
        fmt.ColumnMapping("start_date", "startDate", "date"),
    ],
)
fmt.run_silver_standardize(spark, versions_silver_cfg, bronze_schema=SCHEMA, silver_schema=SCHEMA)
print("[versions] standardized -> jira.versions (Silver)")

# CELL ********************
# --- Per-board entities: Sprints (Scrum boards only -- Kanban boards are skipped automatically) ---
board_ids = [r["_natural_key"] for r in spark.table(f"{SCHEMA}.boards").select("_natural_key").distinct().collect()]

sprints_template = fmt.EntityConfig(
    entity_name="sprints", endpoint_path="/rest/agile/1.0/board/{parent_id}/sprint",
    http_method="GET", pagination_style="page_number", page_size=50,
    records_json_path="values", natural_key_field="id",
)
sprint_records = fmt.extract_per_parent(extractor, sprints_template, parent_ids=board_ids, parent_field_name="_board_id")
count = fmt.land_records(spark, sprint_records, source_name=SOURCE_NAME, entity=sprints_template, bronze_schema=SCHEMA)
print(f"[sprints] landed {count} records across {len(board_ids)} boards")

sprints_silver_cfg = fmt.SilverEntityConfig(
    source_name=SOURCE_NAME, entity_name="sprints", natural_key_columns=["sprint_id"],
    column_mappings=[
        fmt.ColumnMapping("sprint_id", "id", "string"),
        fmt.ColumnMapping("sprint_name", "name", "string"),
        fmt.ColumnMapping("board_id", "_parent_id", "string"),
        fmt.ColumnMapping("sprint_state", "state", "string"),
        fmt.ColumnMapping("sprint_goal", "goal", "string"),
        fmt.ColumnMapping("start_date", "startDate", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        fmt.ColumnMapping("end_date", "endDate", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        fmt.ColumnMapping("complete_date", "completeDate", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
    ],
)
fmt.run_silver_standardize(spark, sprints_silver_cfg, bronze_schema=SCHEMA, silver_schema=SCHEMA)
print("[sprints] standardized -> jira.sprints (Silver)")

# CELL ********************
print("B2S - Jira complete.")
