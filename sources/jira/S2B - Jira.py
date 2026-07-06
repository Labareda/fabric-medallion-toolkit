# Fabric notebook source
# "S2B - Jira" — Source-to-Bronze for the Jira source. Extraction only:
# pulls from the Jira API, lands raw JSON into Bronze. NO standardization
# happens here -- that's "B2S - Jira"'s job, reading from what this
# notebook lands. Run this one FIRST.
# Attach Bronze and Config lakehouses, plus env_medallion_toolkit.

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

source_config, _silver_configs = fmt.load_source_config(CONFIG_PATH, secret_resolver=resolve_secret)
extractor = fmt.RestExtractor(source_config)

# CELL ********************
# --- Main entities: everything listed in jira.json's "entities" array ---
failed_entities = []

for entity in source_config.entities:
    try:
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

    except Exception as exc:
        # One entity failing (permissions, a bad JQL, a transient API issue)
        # shouldn't stop every other entity from landing.
        print(f"[{entity.entity_name}] FAILED, skipping: {exc}")
        failed_entities.append(entity.entity_name)

# CELL ********************
# --- Per-project entities: Versions (one call per project, not a single global list) ---
project_keys = [r["_natural_key"] for r in spark.table(f"{SCHEMA}.projects").select("_natural_key").distinct().collect()] \
    if spark.catalog.tableExists(f"{SCHEMA}.projects") else []

versions_template = fmt.EntityConfig(
    entity_name="versions", endpoint_path="/rest/api/3/project/{parent_id}/versions",
    http_method="GET", pagination_style="none", records_json_path="", natural_key_field="id",
)
if project_keys:
    version_records = fmt.extract_per_parent(extractor, versions_template, parent_ids=project_keys, parent_field_name="_project_key")
    count = fmt.land_records(spark, version_records, source_name=SOURCE_NAME, entity=versions_template, bronze_schema=SCHEMA)
    print(f"[versions] landed {count} records across {len(project_keys)} projects")
else:
    print("[versions] skipping -- jira.projects doesn't exist (Projects extraction failed or hasn't run yet)")

# CELL ********************
# --- Per-project entities: Components (same pattern as Versions) ---
components_template = fmt.EntityConfig(
    entity_name="components", endpoint_path="/rest/api/3/project/{parent_id}/components",
    http_method="GET", pagination_style="none", records_json_path="", natural_key_field="id",
)
if project_keys:
    component_records = fmt.extract_per_parent(extractor, components_template, parent_ids=project_keys, parent_field_name="_project_key")
    count = fmt.land_records(spark, component_records, source_name=SOURCE_NAME, entity=components_template, bronze_schema=SCHEMA)
    print(f"[components] landed {count} records across {len(project_keys)} projects")
else:
    print("[components] skipping -- jira.projects doesn't exist (Projects extraction failed or hasn't run yet)")

# CELL ********************
# --- Per-board entities: Sprints (Scrum boards only -- Kanban boards are skipped automatically) ---
if not spark.catalog.tableExists(f"{SCHEMA}.boards"):
    print("[sprints] skipping -- jira.boards doesn't exist (Boards extraction failed or was never run, "
          "likely a permissions issue: this Jira instance/account may not have Jira Software/Agile access)")
else:
    board_ids = [r["_natural_key"] for r in spark.table(f"{SCHEMA}.boards").select("_natural_key").distinct().collect()]

    sprints_template = fmt.EntityConfig(
        entity_name="sprints", endpoint_path="/rest/agile/1.0/board/{parent_id}/sprint",
        http_method="GET", pagination_style="page_number", page_size=50,
        records_json_path="values", natural_key_field="id",
    )
    sprint_records = fmt.extract_per_parent(extractor, sprints_template, parent_ids=board_ids, parent_field_name="_board_id")
    count = fmt.land_records(spark, sprint_records, source_name=SOURCE_NAME, entity=sprints_template, bronze_schema=SCHEMA)
    print(f"[sprints] landed {count} records across {len(board_ids)} boards")

# CELL ********************
if failed_entities:
    print(f"S2B - Jira complete, WITH FAILURES in: {failed_entities} -- see messages above for why.")
else:
    print("S2B - Jira complete, all entities succeeded.")
